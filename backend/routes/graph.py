"""
Graph API endpoints.

POST /graph/sync                — trigger MongoDB → Neo4j sync
GET  /graph/audit/{inv_id}      — hybrid audit trail (Neo4j + MongoDB)
GET  /graph/detect-circles      — circular trading detection
GET  /graph/find-shadow-networks — shared IP / phone clusters
GET  /graph/risk-score/{gstin}  — 2-hop neighbourhood risk score
"""

import asyncio
from fastapi import APIRouter, HTTPException

from backend.database import get_mongo_db, get_neo4j_driver
from backend.graph_sync import sync_graph

router = APIRouter(prefix="/graph", tags=["Graph"])


# ────────────────────────────────────────────
# Helper — run a read-only Cypher query
# ────────────────────────────────────────────

def _neo4j_read(query: str, **params) -> list[dict]:
    """Execute a Cypher read query and return results as list of dicts."""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(query, **params)
        return [record.data() for record in result]


# ════════════════════════════════════════════
# POST /graph/sync
# ════════════════════════════════════════════

@router.post("/sync")
async def trigger_graph_sync():
    """Run the full MongoDB → Neo4j graph projection.
    Idempotent — safe to call multiple times."""
    report = await sync_graph()
    return report


# ════════════════════════════════════════════
# GET /graph/audit/{inv_id}  — HYBRID FETCH
# ════════════════════════════════════════════

AUDIT_CYPHER = """
MATCH (i:Invoice {invoice_id: $inv_id})
OPTIONAL MATCH (seller:Taxpayer)-[:ISSUED]->(i)
OPTIONAL MATCH (i)-[:BILLED_TO]->(buyer:Taxpayer)
OPTIONAL MATCH (i)-[:REPORTED_IN]->(gstr1:Return {type: 'GSTR1'})
OPTIONAL MATCH (gstr1)-[:SUMMARIZED_IN]->(gstr3b:Return {type: 'GSTR3B'})
OPTIONAL MATCH (i)-[:HAS_EWAYBILL]->(ewb:EWayBill)
OPTIONAL MATCH (:Taxpayer)-[itc:CLAIMED_ITC]->(i)
RETURN properties(i)     AS invoice,
       properties(seller) AS seller,
       properties(buyer)  AS buyer,
       properties(gstr1)  AS gstr1_return,
       properties(gstr3b) AS gstr3b_return,
       properties(ewb)    AS ewaybill,
       itc IS NOT NULL     AS itc_claimed,
       properties(itc)    AS itc_details
LIMIT 1
"""


@router.get("/audit/{inv_id}")
async def audit_invoice(inv_id: str):
    """Multi-hop audit trail for a single invoice.

    Combines:
      • Neo4j  — graph relationships, filing chain, compliance signals
      • MongoDB — raw documents from every source collection
    """
    # ── Neo4j: graph traversal ──
    graph_rows = await asyncio.to_thread(_neo4j_read, AUDIT_CYPHER, inv_id=inv_id)

    # ── MongoDB: raw source documents ──
    db = get_mongo_db()
    raw_invoice  = await db.Invoices.find_one({"Invoice_ID": inv_id}, {"_id": 0})
    gstr1_entry  = await db.GSTR1.find_one({"Invoice_ID": inv_id}, {"_id": 0})
    gstr2b_entry = await db.GSTR2B.find_one({"Invoice_ID": inv_id}, {"_id": 0})
    purchase_entry = await db.Purchase_Register.find_one({"Invoice_ID": inv_id}, {"_id": 0})
    ewaybill_entry = await db.EWayBill.find_one({"Invoice_ID": inv_id}, {"_id": 0})

    # Look up GSTR3B for the seller of this invoice
    seller_gstin = raw_invoice.get("Seller_GSTIN") if raw_invoice else None
    gstr3b_entry = None
    if seller_gstin:
        gstr3b_cursor = db.GSTR3B.find(
            {"Seller_GSTIN": seller_gstin}, {"_id": 0}
        ).limit(10)
        gstr3b_entry = await gstr3b_cursor.to_list(length=10)

    if not graph_rows and not raw_invoice:
        raise HTTPException(status_code=404, detail=f"Invoice {inv_id} not found.")

    graph = graph_rows[0] if graph_rows else {}

    # ── Compliance analysis ──
    gstr1_filed   = graph.get("gstr1_return") is not None
    gstr3b_filed  = graph.get("gstr3b_return") is not None
    gstr1_status  = (graph.get("gstr1_return") or {}).get("status", "MISSING")
    gstr3b_payment = (graph.get("gstr3b_return") or {}).get("payment_confirmed", "N")
    itc_eligible  = (gstr2b_entry or {}).get("ITC_Eligible", "UNKNOWN")
    itc_claimed   = graph.get("itc_claimed", False)
    ewb_present   = graph.get("ewaybill") is not None

    flags: list[str] = []
    if not gstr1_filed:
        flags.append("GSTR1_NOT_FILED")
    elif gstr1_status != "FILED":
        flags.append(f"GSTR1_STATUS_{gstr1_status}")
    if not gstr3b_filed:
        flags.append("GSTR3B_NOT_FILED")
    elif gstr3b_payment != "Y":
        flags.append("GSTR3B_PAYMENT_NOT_CONFIRMED")
    if itc_claimed and itc_eligible == "NO":
        flags.append("ITC_MISMATCH_CLAIMED_BUT_NOT_ELIGIBLE")
    if not ewb_present:
        flags.append("EWAYBILL_MISSING")

    return {
        "invoice_id": inv_id,
        "graph_data": graph,
        "mongo_data": {
            "invoice": raw_invoice,
            "gstr1": gstr1_entry,
            "gstr2b": gstr2b_entry,
            "gstr3b_seller_filings": gstr3b_entry,
            "purchase_register": purchase_entry,
            "ewaybill": ewaybill_entry,
        },
        "compliance": {
            "gstr1_filed": gstr1_filed,
            "gstr1_status": gstr1_status,
            "gstr3b_filed": gstr3b_filed,
            "gstr3b_payment_confirmed": gstr3b_payment == "Y",
            "itc_eligible": itc_eligible,
            "itc_claimed": itc_claimed,
            "itc_mismatch": itc_claimed and itc_eligible == "NO",
            "ewaybill_present": ewb_present,
            "flags": flags,
        },
    }


# ════════════════════════════════════════════
# GET /graph/detect-circles
# ════════════════════════════════════════════

CIRCLES_CYPHER = """
MATCH (a:Taxpayer)-[:ISSUED]->(i1:Invoice)-[:BILLED_TO]->(b:Taxpayer),
      (b)-[:ISSUED]->(i2:Invoice)-[:BILLED_TO]->(c:Taxpayer),
      (c)-[:ISSUED]->(i3:Invoice)-[:BILLED_TO]->(a)
WHERE a.gstin < b.gstin AND b.gstin < c.gstin
WITH a, b, c,
     collect(DISTINCT i1.invoice_id) AS inv_a_to_b,
     collect(DISTINCT i2.invoice_id) AS inv_b_to_c,
     collect(DISTINCT i3.invoice_id) AS inv_c_to_a
RETURN a.gstin   AS gstin_a, a.name AS name_a, a.risk_category AS risk_a,
       b.gstin   AS gstin_b, b.name AS name_b, b.risk_category AS risk_b,
       c.gstin   AS gstin_c, c.name AS name_c, c.risk_category AS risk_c,
       inv_a_to_b, inv_b_to_c, inv_c_to_a
"""


@router.get("/detect-circles")
async def detect_circular_trading():
    """Detect 3-party circular trading loops: A → B → C → A.
    Returns involved GSTINs, names, risk categories, and invoice IDs."""
    records = await asyncio.to_thread(_neo4j_read, CIRCLES_CYPHER)
    return {"circles_found": len(records), "circles": records}


# ════════════════════════════════════════════
# GET /graph/find-shadow-networks
# ════════════════════════════════════════════

SHADOW_CYPHER = """
MATCH (t:Taxpayer)
WHERE t.ip_address IS NOT NULL AND t.ip_address <> ''
WITH t.ip_address AS shared_value, 'IP_ADDRESS' AS match_type,
     collect({gstin: t.gstin, name: t.name, risk: t.risk_category, phone: t.phone}) AS members
WHERE size(members) > 1
RETURN shared_value, match_type, members, size(members) AS cluster_size
UNION ALL
MATCH (t:Taxpayer)
WHERE t.phone IS NOT NULL AND t.phone <> ''
WITH t.phone AS shared_value, 'PHONE' AS match_type,
     collect({gstin: t.gstin, name: t.name, risk: t.risk_category, ip: t.ip_address}) AS members
WHERE size(members) > 1
RETURN shared_value, match_type, members, size(members) AS cluster_size
"""


@router.get("/find-shadow-networks")
async def find_shadow_networks():
    """Detect taxpayers sharing the same IP address or phone number.
    Returns clusters grouped by shared attribute."""
    records = await asyncio.to_thread(_neo4j_read, SHADOW_CYPHER)
    return {"networks_found": len(records), "networks": records}


# ════════════════════════════════════════════
# GET /graph/risk-score/{gstin}
# ════════════════════════════════════════════

RISK_CYPHER = """
MATCH (t:Taxpayer {gstin: $gstin})
OPTIONAL MATCH (t)-[:ISSUED|BILLED_TO*1..4]-(neighbor:Taxpayer)
WHERE neighbor <> t
WITH t, collect(DISTINCT neighbor) AS neighbors
RETURN t.gstin          AS gstin,
       t.name           AS name,
       t.risk_category  AS own_risk,
       size(neighbors)  AS total_neighbors,
       size([n IN neighbors WHERE n.risk_category = 'HIGH'])   AS high_risk_count,
       size([n IN neighbors WHERE n.risk_category = 'MEDIUM']) AS medium_risk_count,
       [n IN neighbors WHERE n.risk_category = 'HIGH'
        | {gstin: n.gstin, name: n.name}]                     AS high_risk_neighbors,
       [n IN neighbors WHERE n.risk_category = 'MEDIUM'
        | {gstin: n.gstin, name: n.name}]                     AS medium_risk_neighbors
"""


@router.get("/risk-score/{gstin}")
async def risk_score(gstin: str):
    """Compute neighbourhood risk score for a taxpayer.

    Examines all taxpayers within 2 invoice-hops and calculates
    a risk score (0-100) based on proximity to high/medium-risk entities.
    """
    records = await asyncio.to_thread(_neo4j_read, RISK_CYPHER, gstin=gstin)
    if not records:
        raise HTTPException(status_code=404, detail=f"Taxpayer {gstin} not found.")

    rec = records[0]
    own_risk      = rec.get("own_risk", "UNKNOWN")
    high_count    = rec.get("high_risk_count", 0) or 0
    medium_count  = rec.get("medium_risk_count", 0) or 0

    # ── Score calculation ──
    # Base: own risk category
    base = {"HIGH": 40, "MEDIUM": 20, "LOW": 5}.get(own_risk, 10)
    # Neighbour influence (capped at 60)
    neighbour_score = min(high_count * 15 + medium_count * 5, 60)
    score = min(base + neighbour_score, 100)

    return {
        "gstin": rec.get("gstin"),
        "name": rec.get("name"),
        "own_risk": own_risk,
        "risk_score": score,
        "total_neighbors": rec.get("total_neighbors", 0),
        "high_risk_neighbors": rec.get("high_risk_neighbors", []),
        "medium_risk_neighbors": rec.get("medium_risk_neighbors", []),
    }
