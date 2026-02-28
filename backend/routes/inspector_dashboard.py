"""
Inspector Dashboard API — full-access, read-only aggregation layer.

Endpoints:
    GET /inspector/summary        → global GST metrics
    GET /inspector/high-risk      → combined Mongo + Neo4j high-risk vendor list
    GET /inspector/gstin/{gstin}  → full profile for a single GSTIN
    GET /inspector/compliance     → GSTR1 vs GSTR3B vendor compliance table
    GET /inspector/fake-itc       → invoices in purchase_register but missing in gstr1
    GET /inspector/ewaybill-fraud → high-value invoices without matching EWayBill
"""

import asyncio
from fastapi import APIRouter, HTTPException

from backend.database import get_mongo_db, get_neo4j_driver

router = APIRouter(prefix="/inspector", tags=["Inspector Dashboard"])

STRIP = {"_id", "_source_file", "_source_row"}


def _to_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _clean(doc: dict | None) -> dict | None:
    if doc is None:
        return None
    return {k: v for k, v in doc.items() if k not in STRIP}


def _neo4j_read(query: str, **params) -> list[dict]:
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(query, **params)
        return [record.data() for record in result]


# ════════════════════════════════════════════
# GET /inspector/summary
# ════════════════════════════════════════════

@router.get("/summary")
async def global_summary():
    """Global GST metrics across all taxpayers."""
    db = get_mongo_db()

    total_taxpayers = await db.Taxpayers.count_documents({})
    total_invoices = await db.Invoices.count_documents({})

    # Total ITC claimed from GSTR2B
    g2b_all = await db.GSTR2B.find({}, {"_id": 0, "Tax": 1}).to_list(length=10000)
    total_itc = sum(_to_float(r.get("Tax")) for r in g2b_all)

    # High risk vendors from Taxpayers collection
    high_risk_count = await db.Taxpayers.count_documents({"Risk_Category": "HIGH"})

    return {
        "total_taxpayers": total_taxpayers,
        "total_invoices": total_invoices,
        "total_itc_claimed": round(total_itc, 2),
        "high_risk_vendors": high_risk_count,
    }


# ════════════════════════════════════════════
# GET /inspector/high-risk
# ════════════════════════════════════════════

@router.get("/high-risk")
async def high_risk_vendors():
    """Combined Mongo + Neo4j high-risk vendor detection.

    A vendor is HIGH RISK if ANY condition:
    1. Filed GSTR1 but Payment_Confirmed = "N"
    2. High count of ITC_Eligible = "NO"
    3. Multiple taxpayers sharing same IP (Neo4j)
    4. Involved in circular trading (Neo4j)
    """
    db = get_mongo_db()

    # --- All taxpayers ---
    all_tp = await db.Taxpayers.find({}, {"_id": 0}).to_list(length=1000)
    tp_map = {t["GSTIN"]: t for t in all_tp}

    risk_entries: dict[str, dict] = {}  # gstin -> {risk, reasons}

    # --- Condition 1: GSTR1 filed but GSTR3B payment != Y ---
    gstr1_sellers = set()
    gstr1_all = await db.GSTR1.find({}, {"_id": 0, "Seller_GSTIN": 1}).to_list(length=10000)
    for r in gstr1_all:
        gstr1_sellers.add(r.get("Seller_GSTIN"))

    gstr3b_all = await db.GSTR3B.find({}, {"_id": 0, "Seller_GSTIN": 1, "Payment_Confirmed": 1}).to_list(length=10000)
    unpaid_sellers: set[str] = set()
    for r in gstr3b_all:
        if r.get("Payment_Confirmed") != "Y":
            unpaid_sellers.add(r["Seller_GSTIN"])

    for sg in gstr1_sellers & unpaid_sellers:
        entry = risk_entries.setdefault(sg, {"reasons": [], "risk": "HIGH"})
        entry["reasons"].append("GSTR1 filed but GSTR3B payment not confirmed")

    # --- Condition 2: High count of ITC_Eligible = "NO" ---
    g2b_all = await db.GSTR2B.find({}, {"_id": 0, "Seller_GSTIN": 1, "ITC_Eligible": 1}).to_list(length=10000)
    blocked_count: dict[str, int] = {}
    for r in g2b_all:
        if r.get("ITC_Eligible") == "NO":
            sg = r.get("Seller_GSTIN", "")
            blocked_count[sg] = blocked_count.get(sg, 0) + 1

    for sg, cnt in blocked_count.items():
        if cnt >= 1:
            entry = risk_entries.setdefault(sg, {"reasons": [], "risk": "HIGH"})
            entry["reasons"].append(f"ITC blocked on {cnt} invoice(s)")

    # --- Condition 3: Shared IP address (Neo4j) ---
    try:
        shadow_q = """
        MATCH (t:Taxpayer)
        WHERE t.ip_address IS NOT NULL AND t.ip_address <> ''
        WITH t.ip_address AS ip, collect(t.gstin) AS members
        WHERE size(members) > 1
        RETURN ip, members
        """
        ip_clusters = await asyncio.to_thread(_neo4j_read, shadow_q)
        for cluster in ip_clusters:
            for gstin in cluster.get("members", []):
                entry = risk_entries.setdefault(gstin, {"reasons": [], "risk": "HIGH"})
                entry["reasons"].append(f"Shared IP: {cluster.get('ip')} ({len(cluster['members'])} entities)")
    except Exception:
        pass  # Neo4j optional; degrade gracefully

    # --- Condition 4: Circular trading (Neo4j) ---
    try:
        circles_q = """
        MATCH (a:Taxpayer)-[:ISSUED]->(:Invoice)-[:BILLED_TO]->(b:Taxpayer),
              (b)-[:ISSUED]->(:Invoice)-[:BILLED_TO]->(c:Taxpayer),
              (c)-[:ISSUED]->(:Invoice)-[:BILLED_TO]->(a)
        WHERE a.gstin < b.gstin AND b.gstin < c.gstin
        RETURN collect(DISTINCT a.gstin) + collect(DISTINCT b.gstin) + collect(DISTINCT c.gstin) AS involved
        """
        circle_res = await asyncio.to_thread(_neo4j_read, circles_q)
        circular_gstins: set[str] = set()
        for row in circle_res:
            for g in row.get("involved", []):
                circular_gstins.add(g)
        for gstin in circular_gstins:
            entry = risk_entries.setdefault(gstin, {"reasons": [], "risk": "HIGH"})
            entry["reasons"].append("Involved in circular trading")
    except Exception:
        pass

    # --- Build response ---
    result = []
    for gstin, info in risk_entries.items():
        tp = tp_map.get(gstin, {})
        result.append({
            "gstin": gstin,
            "name": tp.get("Name", "Unknown"),
            "risk_level": info["risk"],
            "reasons": info["reasons"],
        })

    result.sort(key=lambda v: (-len(v["reasons"]), v["gstin"]))
    return {"count": len(result), "vendors": result}


# ════════════════════════════════════════════
# GET /inspector/gstin/{gstin}
# ════════════════════════════════════════════

@router.get("/gstin/{gstin}")
async def gstin_profile(gstin: str):
    """Full profile for a single GSTIN — company info, all invoices, risk."""
    db = get_mongo_db()

    taxpayer = await db.Taxpayers.find_one({"GSTIN": gstin}, {"_id": 0})
    if not taxpayer:
        raise HTTPException(status_code=404, detail=f"GSTIN {gstin} not found.")

    # Invoices as seller
    sold = await db.Invoices.find(
        {"Seller_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # Invoices as buyer
    bought = await db.Invoices.find(
        {"Buyer_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # GSTR1 filings
    gstr1 = await db.GSTR1.find(
        {"Seller_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # GSTR3B
    gstr3b = await db.GSTR3B.find(
        {"Seller_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # GSTR2B (as buyer)
    gstr2b = await db.GSTR2B.find(
        {"Buyer_GSTIN": gstin}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=5000)

    # Risk score from Neo4j
    risk_data = None
    try:
        risk_q = """
        MATCH (t:Taxpayer {gstin: $gstin})
        OPTIONAL MATCH (t)-[:ISSUED|BILLED_TO*1..4]-(n:Taxpayer)
        WHERE n <> t
        WITH t, collect(DISTINCT n) AS neighbors
        RETURN t.risk_category AS own_risk,
               size(neighbors) AS total_neighbors,
               size([n IN neighbors WHERE n.risk_category = 'HIGH']) AS high_risk_count
        """
        rows = await asyncio.to_thread(_neo4j_read, risk_q, gstin=gstin)
        if rows:
            r = rows[0]
            own = r.get("own_risk", "UNKNOWN")
            hc = r.get("high_risk_count", 0) or 0
            base = {"HIGH": 40, "MEDIUM": 20, "LOW": 5}.get(own, 10)
            score = min(base + min(hc * 15, 60), 100)
            # Classification: 0-30 LOW, 31-60 MEDIUM, 61-100 HIGH
            risk_level = "HIGH" if score >= 61 else "MEDIUM" if score >= 31 else "LOW"
            risk_data = {"own_risk": own, "risk_score": score, "risk_level": risk_level, "high_risk_neighbors": hc}
    except Exception:
        pass

    # Compliance flags
    gstr1_filed = len(gstr1) > 0
    payment_confirmed = any(r.get("Payment_Confirmed") == "Y" for r in gstr3b) if gstr3b else False

    return {
        "taxpayer": taxpayer,
        "invoices_as_seller": sold,
        "invoices_as_buyer": bought,
        "gstr1_filings": len(gstr1),
        "gstr3b_filings": len(gstr3b),
        "gstr2b_claims": len(gstr2b),
        "risk": risk_data,
        "compliance": {
            "gstr1_filed": gstr1_filed,
            "payment_confirmed": payment_confirmed,
            "status": "COMPLIANT" if gstr1_filed and payment_confirmed else "NON-COMPLIANT",
        },
    }


# ════════════════════════════════════════════
# GET /inspector/compliance
# ════════════════════════════════════════════

@router.get("/compliance")
async def vendor_compliance():
    """GSTR1 vs GSTR3B compliance table for all sellers."""
    db = get_mongo_db()

    all_tp = await db.Taxpayers.find({}, {"_id": 0, "GSTIN": 1, "Name": 1}).to_list(length=1000)

    gstr1_all = await db.GSTR1.find({}, {"_id": 0, "Seller_GSTIN": 1}).to_list(length=10000)
    gstr1_sellers = set(r["Seller_GSTIN"] for r in gstr1_all if r.get("Seller_GSTIN"))

    gstr3b_all = await db.GSTR3B.find(
        {}, {"_id": 0, "Seller_GSTIN": 1, "Payment_Confirmed": 1}
    ).to_list(length=10000)

    payment_map: dict[str, bool] = {}
    for r in gstr3b_all:
        sg = r.get("Seller_GSTIN", "")
        if r.get("Payment_Confirmed") == "Y":
            payment_map[sg] = True
        elif sg not in payment_map:
            payment_map[sg] = False

    result = []
    for tp in all_tp:
        gstin = tp["GSTIN"]
        filed = gstin in gstr1_sellers
        paid = payment_map.get(gstin, False)
        result.append({
            "GSTIN": gstin,
            "Name": tp.get("Name", "Unknown"),
            "GSTR1_Filed": "YES" if filed else "NO",
            "Tax_Paid": "YES" if paid else "NO",
            "Compliance_Status": "COMPLIANT" if filed and paid else "NON-COMPLIANT",
        })

    result.sort(key=lambda v: (0 if v["Compliance_Status"] == "NON-COMPLIANT" else 1, v["GSTIN"]))
    return {"count": len(result), "compliance": result}


# ════════════════════════════════════════════
# GET /inspector/fake-itc
# ════════════════════════════════════════════

@router.get("/fake-itc")
async def fake_itc_detection():
    """Detect invoices in purchase_register but missing in gstr1."""
    db = get_mongo_db()

    pr_all = await db.Purchase_Register.find(
        {}, {"_id": 0, "Invoice_ID": 1, "Buyer_GSTIN": 1}
    ).to_list(length=10000)

    gstr1_all = await db.GSTR1.find(
        {}, {"_id": 0, "Invoice_ID": 1}
    ).to_list(length=10000)
    gstr1_inv_ids = set(r["Invoice_ID"] for r in gstr1_all if r.get("Invoice_ID"))

    # Invoices collection for Seller_GSTIN lookup
    inv_all = await db.Invoices.find(
        {}, {"_id": 0, "Invoice_ID": 1, "Seller_GSTIN": 1}
    ).to_list(length=10000)
    inv_seller = {r["Invoice_ID"]: r.get("Seller_GSTIN", "N/A") for r in inv_all}

    suspects = []
    for pr in pr_all:
        inv_id = pr.get("Invoice_ID")
        if inv_id and inv_id not in gstr1_inv_ids:
            suspects.append({
                "Invoice_ID": inv_id,
                "Buyer_GSTIN": pr.get("Buyer_GSTIN", "N/A"),
                "Seller_GSTIN": inv_seller.get(inv_id, "N/A"),
            })

    return {"count": len(suspects), "suspects": suspects}


# ════════════════════════════════════════════
# GET /inspector/ewaybill-fraud
# ════════════════════════════════════════════

@router.get("/ewaybill-fraud")
async def ewaybill_fraud():
    """High-value invoices (>50,000) without matching EWayBill."""
    db = get_mongo_db()

    invoices = await db.Invoices.find(
        {}, {"_id": 0, "_source_file": 0, "_source_row": 0}
    ).to_list(length=10000)

    ewb_all = await db.EWayBill.find(
        {}, {"_id": 0, "Invoice_ID": 1}
    ).to_list(length=10000)
    ewb_inv_ids = set(r["Invoice_ID"] for r in ewb_all if r.get("Invoice_ID"))

    suspects = []
    for inv in invoices:
        val = _to_float(inv.get("Value"))
        inv_id = inv.get("Invoice_ID", "")
        if val > 50000 and inv_id not in ewb_inv_ids:
            suspects.append({
                "Invoice_ID": inv_id,
                "Seller_GSTIN": inv.get("Seller_GSTIN", "N/A"),
                "Buyer_GSTIN": inv.get("Buyer_GSTIN", "N/A"),
                "Value": val,
            })

    suspects.sort(key=lambda v: -v["Value"])
    return {"count": len(suspects), "suspects": suspects}
