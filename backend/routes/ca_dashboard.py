"""
CA Dashboard API — read-only reconciliation layer over MongoDB.

Endpoints:
    GET /ca/clients                  → all GSTINs (for CA client selector)
    GET /ca/overview/{gstin}         → full CA reconciliation dataset (buyer perspective)
    GET /ca/invoice/{gstin}/{inv_id} → single invoice detail with reconciliation status
"""

from fastapi import APIRouter, HTTPException

from backend.database import get_mongo_db

router = APIRouter(prefix="/ca", tags=["CA Dashboard"])


# ────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────

def _to_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


STRIP = {"_id", "_source_file", "_source_row"}


def _clean(doc: dict | None) -> dict | None:
    if doc is None:
        return None
    return {k: v for k, v in doc.items() if k not in STRIP}


def _clean_list(docs: list[dict]) -> list[dict]:
    return [_clean(d) for d in docs]


# ════════════════════════════════════════════
# GET /ca/clients
# ════════════════════════════════════════════

@router.get("/clients")
async def list_clients():
    """Return all taxpayer GSTINs + names for the CA client selector."""
    db = get_mongo_db()
    cursor = db.Taxpayers.find(
        {}, {"_id": 0, "GSTIN": 1, "Name": 1, "Risk_Category": 1}
    ).sort("Name", 1)
    records = await cursor.to_list(length=500)
    return {"clients": records}


# ════════════════════════════════════════════
# GET /ca/overview/{gstin}
# ════════════════════════════════════════════

@router.get("/overview/{gstin}")
async def ca_overview(gstin: str):
    """Full CA reconciliation dataset for a client GSTIN (buyer perspective).

    Returns: taxpayer, itc_summary, reconciliation, missing_itc,
             filing_status, vendor_risk
    """
    db = get_mongo_db()

    # ── Taxpayer info ──
    taxpayer = await db.Taxpayers.find_one({"GSTIN": gstin}, {"_id": 0})
    if not taxpayer:
        raise HTTPException(status_code=404, detail=f"Taxpayer {gstin} not found.")

    # ── Purchase Register (buyer) ──
    pr_raw = await db.Purchase_Register.find(
        {"Buyer_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # ── GSTR2B (buyer) ──
    g2b_raw = await db.GSTR2B.find(
        {"Buyer_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # ── Build lookup maps ──
    pr_map: dict[str, dict] = {}
    for doc in pr_raw:
        inv_id = doc.get("Invoice_ID")
        if inv_id:
            pr_map[inv_id] = doc

    g2b_map: dict[str, dict] = {}
    for doc in g2b_raw:
        inv_id = doc.get("Invoice_ID")
        if inv_id:
            g2b_map[inv_id] = doc

    # ── ITC Summary ──
    total_itc = sum(_to_float(r.get("Tax")) for r in g2b_raw)
    eligible_itc = sum(
        _to_float(r.get("Tax")) for r in g2b_raw if r.get("ITC_Eligible") == "YES"
    )
    blocked_itc = sum(
        _to_float(r.get("Tax")) for r in g2b_raw if r.get("ITC_Eligible") == "NO"
    )

    # ── Reconciliation (Section 3) ──
    all_inv_ids = sorted(set(pr_map.keys()) | set(g2b_map.keys()))
    reconciliation: list[dict] = []
    missing_itc: list[dict] = []

    for inv_id in all_inv_ids:
        pr_doc = pr_map.get(inv_id)
        g2b_doc = g2b_map.get(inv_id)

        pr_val = _to_float(pr_doc.get("Value_Claimed")) if pr_doc else None
        g2b_val = _to_float(g2b_doc.get("Value")) if g2b_doc else None

        seller = "N/A"
        if g2b_doc and g2b_doc.get("Seller_GSTIN"):
            seller = g2b_doc["Seller_GSTIN"]
        elif pr_doc:
            # Look up Seller_GSTIN via Invoices collection
            inv_doc = await db.Invoices.find_one(
                {"Invoice_ID": inv_id}, {"_id": 0, "Seller_GSTIN": 1}
            )
            seller = inv_doc.get("Seller_GSTIN", "N/A") if inv_doc else "N/A"

        if pr_doc and g2b_doc:
            if abs((_to_float(pr_doc.get("Value_Claimed", 0))
                    - _to_float(g2b_doc.get("Value", 0)))) < 0.01:
                status = "MATCHED"
            else:
                status = "MISMATCH"
        elif pr_doc and not g2b_doc:
            status = "MISSING"
            missing_itc.append({
                "Invoice_ID": inv_id,
                "Seller_GSTIN": seller,
                "Tax_Claimed": pr_doc.get("Tax_Claimed"),
            })
        else:
            # In GSTR2B but not in Purchase Register — unusual but track it
            status = "MATCHED"

        reconciliation.append({
            "Invoice_ID": inv_id,
            "Seller_GSTIN": seller,
            "Purchase_Value": pr_val,
            "GSTR2B_Value": g2b_val,
            "Status": status,
        })

    # ── Unique seller GSTINs from purchase register + gstr2b invoices ──
    seller_gstins: set[str] = set()
    for doc in pr_raw:
        inv_doc = await db.Invoices.find_one(
            {"Invoice_ID": doc.get("Invoice_ID")}, {"_id": 0, "Seller_GSTIN": 1}
        )
        if inv_doc and inv_doc.get("Seller_GSTIN"):
            seller_gstins.add(inv_doc["Seller_GSTIN"])
    for doc in g2b_raw:
        if doc.get("Seller_GSTIN"):
            seller_gstins.add(doc["Seller_GSTIN"])

    seller_list = sorted(seller_gstins)

    # ── Filing Status (Section 5) — check seller compliance ──
    filing_status: list[dict] = []
    for sg in seller_list:
        # GSTR1: check if seller filed for any invoice with this buyer
        gstr1_entry = await db.GSTR1.find_one(
            {"Seller_GSTIN": sg, "Buyer_GSTIN": gstin},
            {"_id": 0, "Status": 1}
        )
        gstr1_status = gstr1_entry.get("Status", "NOT FILED") if gstr1_entry else "NOT FILED"

        # GSTR3B: check payment confirmation
        gstr3b_entry = await db.GSTR3B.find_one(
            {"Seller_GSTIN": sg},
            {"_id": 0, "Payment_Confirmed": 1}
        )
        payment = gstr3b_entry.get("Payment_Confirmed", "N") if gstr3b_entry else "N"

        filing_status.append({
            "Seller_GSTIN": sg,
            "GSTR1_Status": gstr1_status,
            "GSTR3B_Payment": "YES" if payment == "Y" else "NO",
        })

    # ── Vendor Risk (Section 8) ──
    vendor_risk: list[dict] = []
    for sg in seller_list:
        reasons: list[str] = []
        risk = "LOW"

        # Check ITC_Eligible == NO for this seller's invoices
        seller_g2b = [r for r in g2b_raw if r.get("Seller_GSTIN") == sg]
        if any(r.get("ITC_Eligible") == "NO" for r in seller_g2b):
            reasons.append("ITC blocked")
            risk = "HIGH"

        # Check Payment_Confirmed == N
        g3b = await db.GSTR3B.find_one(
            {"Seller_GSTIN": sg}, {"_id": 0, "Payment_Confirmed": 1}
        )
        if g3b and g3b.get("Payment_Confirmed") != "Y":
            reasons.append("GSTR3B payment not confirmed")
            risk = "HIGH"

        if not reasons:
            risk = "LOW"
            reasons.append("No issues")

        if risk != "LOW":
            vendor_risk.append({
                "Vendor_GSTIN": sg,
                "Risk_Level": risk,
                "Reasons": " | ".join(reasons),
            })

    # Sort HIGH first
    vendor_risk.sort(key=lambda v: 0 if v["Risk_Level"] == "HIGH" else 1)

    return {
        "taxpayer": taxpayer,
        "itc_summary": {
            "total_invoices": len(pr_raw),
            "total_itc": round(total_itc, 2),
            "eligible_itc": round(eligible_itc, 2),
            "blocked_itc": round(blocked_itc, 2),
        },
        "reconciliation": reconciliation,
        "missing_itc": missing_itc,
        "filing_status": filing_status,
        "vendor_risk": vendor_risk,
    }


# ════════════════════════════════════════════
# GET /ca/invoice/{gstin}/{inv_id}
# ════════════════════════════════════════════

@router.get("/invoice/{gstin}/{inv_id}")
async def ca_invoice_detail(gstin: str, inv_id: str):
    """Single invoice detail with reconciliation status and explanation."""
    db = get_mongo_db()

    # Purchase Register
    pr = await db.Purchase_Register.find_one(
        {"Buyer_GSTIN": gstin, "Invoice_ID": inv_id},
        {"_id": 0, "_source_file": 0, "_source_row": 0}
    )

    # GSTR2B
    g2b = await db.GSTR2B.find_one(
        {"Buyer_GSTIN": gstin, "Invoice_ID": inv_id},
        {"_id": 0, "_source_file": 0, "_source_row": 0}
    )

    # Invoice
    inv = await db.Invoices.find_one(
        {"Invoice_ID": inv_id},
        {"_id": 0, "_source_file": 0, "_source_row": 0}
    )

    # Guard: must belong to this buyer
    if not pr and not g2b:
        # Check if invoice exists but just doesn't belong to this GSTIN
        if inv:
            if inv.get("Buyer_GSTIN") != gstin and inv.get("Seller_GSTIN") != gstin:
                raise HTTPException(
                    status_code=403,
                    detail=f"Invoice {inv_id} does not belong to client {gstin}."
                )
        raise HTTPException(status_code=404, detail=f"Invoice {inv_id} not found for {gstin}.")

    seller = (g2b or {}).get("Seller_GSTIN") or (inv or {}).get("Seller_GSTIN", "N/A")

    # Reconciliation status
    if pr and g2b:
        pr_val = _to_float(pr.get("Value_Claimed"))
        g2b_val = _to_float(g2b.get("Value"))
        status = "MATCHED" if abs(pr_val - g2b_val) < 0.01 else "MISMATCH"
    elif pr and not g2b:
        status = "MISSING"
    else:
        status = "MATCHED"

    # ITC eligibility
    itc_eligible = g2b.get("ITC_Eligible", "UNKNOWN") if g2b else "UNKNOWN"

    # GSTR1 status
    gstr1 = await db.GSTR1.find_one(
        {"Invoice_ID": inv_id}, {"_id": 0, "Status": 1, "Filing_Date": 1}
    )
    gstr1_status = gstr1.get("Status", "NOT FILED") if gstr1 else "NOT FILED"

    # GSTR3B payment
    gstr3b = None
    if seller != "N/A":
        gstr3b = await db.GSTR3B.find_one(
            {"Seller_GSTIN": seller}, {"_id": 0, "Payment_Confirmed": 1}
        )
    payment = gstr3b.get("Payment_Confirmed", "N") if gstr3b else "N"

    # ── Explanation ──
    explanations: list[str] = []
    if status == "MISSING":
        explanations.append(
            f"Invoice {inv_id} missing because supplier did not file GSTR1."
        )
    elif status == "MISMATCH":
        explanations.append(
            f"Invoice {inv_id} mismatch because supplier reported different value."
        )
    else:
        explanations.append(f"Invoice {inv_id} fully reconciled.")

    if itc_eligible == "NO":
        explanations.append(f"Invoice {inv_id} ITC blocked — marked ineligible in GSTR2B.")
    if payment != "Y":
        explanations.append(f"Invoice {inv_id} risk — supplier GSTR3B payment not confirmed.")

    return {
        "invoice_id": inv_id,
        "seller_gstin": seller,
        "purchase_register": pr,
        "gstr2b": g2b,
        "invoice": inv,
        "reconciliation_status": status,
        "itc_eligible": itc_eligible,
        "gstr1_status": gstr1_status,
        "gstr3b_payment": "YES" if payment == "Y" else "NO",
        "explanations": explanations,
    }
