"""
CFO Dashboard API — read-only aggregation layer over MongoDB & Neo4j.

Endpoints:
    GET /dashboard/taxpayers               → GSTIN list for selector
    GET /dashboard/overview/{gstin}        → full CFO dataset (buyer perspective)
    GET /dashboard/vendor-network/{gstin}  → Neo4j vendor graph
"""

import asyncio
from fastapi import APIRouter, HTTPException

from backend.database import get_mongo_db, get_neo4j_driver

router = APIRouter(prefix="/dashboard", tags=["CFO Dashboard"])


# ────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────

def _to_float(val, default: float = 0.0) -> float:
    """Safely cast CSV-sourced values to float."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


STRIP_FIELDS = {"_id", "_source_file", "_source_row"}


def _clean(doc: dict | None) -> dict | None:
    """Strip internal MongoDB / ingestion metadata from a document."""
    if doc is None:
        return None
    return {k: v for k, v in doc.items() if k not in STRIP_FIELDS}


# ════════════════════════════════════════════
# GET /dashboard/taxpayers
# ════════════════════════════════════════════

@router.get("/taxpayers")
async def list_taxpayers():
    """Return all taxpayer GSTINs and names (for the company selector)."""
    db = get_mongo_db()
    cursor = db.Taxpayers.find(
        {}, {"_id": 0, "GSTIN": 1, "Name": 1, "Risk_Category": 1}
    ).sort("Name", 1)
    records = await cursor.to_list(length=500)
    return {"taxpayers": records}


# ════════════════════════════════════════════
# GET /dashboard/overview/{gstin}
# ════════════════════════════════════════════

@router.get("/overview/{gstin}")
async def dashboard_overview(gstin: str):
    """Comprehensive CFO dashboard data for a GSTIN (buyer perspective).

    Returns:
        taxpayer, itc_summary, purchase_register, gstr2b,
        vendor_risk, payment_warnings
    """
    db = get_mongo_db()

    # ── Taxpayer info ──
    taxpayer = await db.Taxpayers.find_one({"GSTIN": gstin}, {"_id": 0})
    if not taxpayer:
        raise HTTPException(status_code=404, detail=f"Taxpayer {gstin} not found.")

    # ── GSTR2B (buyer) ──
    gstr2b_raw = await db.GSTR2B.find(
        {"Buyer_GSTIN": gstin},
        {"_id": 0, "_source_file": 0, "_source_row": 0},
    ).to_list(length=1000)

    # ITC calculations
    total_itc = sum(_to_float(r.get("Tax")) for r in gstr2b_raw)
    eligible_itc = sum(
        _to_float(r.get("Tax"))
        for r in gstr2b_raw
        if r.get("ITC_Eligible") == "YES"
    )
    blocked_itc = sum(
        _to_float(r.get("Tax"))
        for r in gstr2b_raw
        if r.get("ITC_Eligible") == "NO"
    )

    # ── Purchase Register (buyer) ──
    pr_raw = await db.Purchase_Register.find(
        {"Buyer_GSTIN": gstin},
        {"_id": 0, "_source_file": 0, "_source_row": 0},
    ).to_list(length=1000)

    # ── Invoice look-up (to get Seller_GSTIN for purchase register rows) ──
    pr_inv_ids = [r["Invoice_ID"] for r in pr_raw if r.get("Invoice_ID")]
    g2b_inv_ids = [r["Invoice_ID"] for r in gstr2b_raw if r.get("Invoice_ID")]
    all_inv_ids = list(set(pr_inv_ids + g2b_inv_ids))

    invoices_raw = await db.Invoices.find(
        {"Invoice_ID": {"$in": all_inv_ids}},
        {"_id": 0, "_source_file": 0, "_source_row": 0},
    ).to_list(length=1000)
    inv_map = {i["Invoice_ID"]: i for i in invoices_raw}

    # ── Unique seller GSTINs ──
    seller_gstins_inv = {
        inv.get("Seller_GSTIN")
        for inv in invoices_raw
        if inv.get("Seller_GSTIN")
    }
    seller_gstins_g2b = {
        r.get("Seller_GSTIN") for r in gstr2b_raw if r.get("Seller_GSTIN")
    }
    all_seller_gstins = list(seller_gstins_inv | seller_gstins_g2b)

    # ── Seller Taxpayer info ──
    sellers = await db.Taxpayers.find(
        {"GSTIN": {"$in": all_seller_gstins}}, {"_id": 0}
    ).to_list(length=500)
    seller_info = {s["GSTIN"]: s for s in sellers}

    # ── GSTR1 for these invoices ──
    gstr1_raw = await db.GSTR1.find(
        {"Invoice_ID": {"$in": all_inv_ids}},
        {"_id": 0, "_source_file": 0, "_source_row": 0},
    ).to_list(length=1000)
    gstr1_map = {r["Invoice_ID"]: r for r in gstr1_raw}

    # ── GSTR3B for sellers ──
    gstr3b_raw = await db.GSTR3B.find(
        {"Seller_GSTIN": {"$in": all_seller_gstins}},
        {"_id": 0, "_source_file": 0, "_source_row": 0},
    ).to_list(length=1000)
    gstr3b_by_seller: dict[str, list] = {}
    for r in gstr3b_raw:
        gstr3b_by_seller.setdefault(r["Seller_GSTIN"], []).append(r)

    # ── EWayBills for these invoices ──
    ewb_raw = await db.EWayBill.find(
        {"Invoice_ID": {"$in": all_inv_ids}},
        {"_id": 0, "_source_file": 0, "_source_row": 0},
    ).to_list(length=1000)
    ewb_map = {r["Invoice_ID"]: r for r in ewb_raw}

    # ── GSTR2B index by Invoice_ID ──
    gstr2b_map = {r["Invoice_ID"]: r for r in gstr2b_raw if r.get("Invoice_ID")}

    # ────────────────────────────────────────
    # Vendor Risk Assessment
    # ────────────────────────────────────────
    vendor_risk: list[dict] = []
    for sg in all_seller_gstins:
        info = seller_info.get(sg, {})
        reasons: list[str] = []
        risk = "LOW"

        # Invoices from this seller to this buyer
        seller_inv_ids = [
            inv_id
            for inv_id, inv in inv_map.items()
            if inv.get("Seller_GSTIN") == sg
        ]

        # GSTR2B records from this seller
        seller_gstr2b = [r for r in gstr2b_raw if r.get("Seller_GSTIN") == sg]

        # Condition 1 — GSTR1 filed but GSTR3B payment not confirmed
        has_gstr1 = any(inv_id in gstr1_map for inv_id in seller_inv_ids)
        g3b_entries = gstr3b_by_seller.get(sg, [])
        has_unpaid = any(r.get("Payment_Confirmed") != "Y" for r in g3b_entries)
        if has_gstr1 and has_unpaid:
            reasons.append("GSTR1 filed but GSTR3B payment not confirmed")
            risk = "HIGH"

        # Condition 2 — ITC blocked
        if any(r.get("ITC_Eligible") == "NO" for r in seller_gstr2b):
            reasons.append("ITC blocked for one or more invoices")
            risk = "HIGH"

        # Condition 3 — Missing EWayBill
        missing_ewb = [iid for iid in seller_inv_ids if iid not in ewb_map]
        if missing_ewb:
            reasons.append(f"Missing EWayBill for: {', '.join(missing_ewb)}")
            if risk == "LOW":
                risk = "MEDIUM"

        if not reasons:
            reasons.append("No issues detected")

        vendor_risk.append({
            "gstin": sg,
            "name": info.get("Name", "Unknown"),
            "risk_level": risk,
            "reasons": reasons,
        })

    # Sort HIGH → MEDIUM → LOW
    _order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    vendor_risk.sort(key=lambda v: _order.get(v["risk_level"], 3))

    # ────────────────────────────────────────
    # Payment Warnings
    # ────────────────────────────────────────
    warnings: list[dict] = []

    # Invoice in Purchase Register but NOT in GSTR2B
    for pr in pr_raw:
        inv_id = pr.get("Invoice_ID")
        if inv_id and inv_id not in gstr2b_map:
            warnings.append({
                "invoice_id": inv_id,
                "severity": "CRITICAL",
                "message": (
                    f"Do not release payment for Invoice {inv_id} "
                    "— Tax link not found in GSTR2B"
                ),
            })

    # ITC blocked
    for inv_id, g2b in gstr2b_map.items():
        if g2b.get("ITC_Eligible") == "NO":
            warnings.append({
                "invoice_id": inv_id,
                "severity": "WARNING",
                "message": (
                    f"Invoice {inv_id} ITC blocked "
                    "due to supplier filing delay"
                ),
            })

    # ── Enrich purchase register with Seller_GSTIN ──
    enriched_pr = []
    for pr in pr_raw:
        inv_id = pr.get("Invoice_ID")
        inv = inv_map.get(inv_id, {})
        enriched_pr.append({
            "Invoice_ID": inv_id,
            "Seller_GSTIN": inv.get("Seller_GSTIN", "N/A"),
            "Value_Claimed": pr.get("Value_Claimed"),
            "Tax_Claimed": pr.get("Tax_Claimed"),
            "Claim_Date": pr.get("Claim_Date"),
        })

    return {
        "taxpayer": taxpayer,
        "itc_summary": {
            "total_itc": round(total_itc, 2),
            "eligible_itc": round(eligible_itc, 2),
            "blocked_itc": round(blocked_itc, 2),
        },
        "purchase_register": enriched_pr,
        "gstr2b": [
            {
                "Invoice_ID": r.get("Invoice_ID"),
                "Seller_GSTIN": r.get("Seller_GSTIN"),
                "Value": r.get("Value"),
                "Tax": r.get("Tax"),
                "ITC_Eligible": r.get("ITC_Eligible"),
            }
            for r in gstr2b_raw
        ],
        "vendor_risk": vendor_risk,
        "payment_warnings": warnings,
    }


# ════════════════════════════════════════════
# GET /dashboard/vendor-network/{gstin}
# ════════════════════════════════════════════

@router.get("/vendor-network/{gstin}")
async def vendor_network(gstin: str):
    """Neo4j: vendors connected to this GSTIN via invoice relationships.

    Uses the exact Cypher pattern specified:
        MATCH (a:Taxpayer)-[:ISSUED]->(:Invoice)-[:BILLED_TO]->(b:Taxpayer)
        WHERE a.gstin = $gstin
    """

    def _query():
        driver = get_neo4j_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (a:Taxpayer)-[:ISSUED]->(inv:Invoice)-[:BILLED_TO]->(b:Taxpayer)
                WHERE a.gstin = $gstin OR b.gstin = $gstin
                WITH CASE WHEN a.gstin = $gstin THEN 'SELLER' ELSE 'BUYER' END AS role,
                     CASE WHEN a.gstin = $gstin THEN b ELSE a END AS partner,
                     collect(DISTINCT inv.invoice_id) AS invoices,
                     sum(inv.value) AS total_value
                RETURN role,
                       partner.gstin AS partner_gstin,
                       partner.name AS partner_name,
                       partner.risk_category AS partner_risk,
                       invoices,
                       total_value
                """,
                gstin=gstin,
            )
            return [record.data() for record in result]

    records = await asyncio.to_thread(_query)
    return {"gstin": gstin, "connections": records}
