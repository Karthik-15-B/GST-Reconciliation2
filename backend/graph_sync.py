"""
Graph projection engine: MongoDB → Neo4j one-way sync.

Reads structured GST data from MongoDB collections and projects nodes
and relationships into Neo4j.  Every write uses MERGE for idempotency —
safe to re-run any number of times without creating duplicates.

Collections consumed:
    Taxpayers, Invoices, GSTR1, GSTR2B, GSTR3B, EWayBill, Purchase_Register

Node types created:
    (:Taxpayer {gstin})
    (:Invoice  {invoice_id})
    (:Return   {return_id, type, gstin, period})
    (:EWayBill {ewaybill_no})

Relationship types created:
    (Taxpayer)-[:ISSUED]->(Invoice)
    (Invoice)-[:BILLED_TO]->(Taxpayer)
    (Taxpayer)-[:FILED]->(Return)
    (Invoice)-[:REPORTED_IN]->(Return)
    (Return {GSTR1})-[:SUMMARIZED_IN]->(Return {GSTR3B})
    (Invoice)-[:HAS_EWAYBILL]->(EWayBill)
    (Taxpayer)-[:CLAIMED_ITC]->(Invoice)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from backend.database import get_mongo_db, get_neo4j_driver

logger = logging.getLogger("graph_sync")
logger.setLevel(logging.INFO)

BATCH_SIZE = 50  # records per UNWIND batch


# ════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════

def _safe_float(val, default: float = 0.0) -> float:
    """Convert a value to float, returning default on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _derive_month(date_str: str) -> str:
    """'2026-01-19' → 'Jan'.  Returns 'Unknown' on bad input."""
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").strftime("%b")
    except (ValueError, AttributeError):
        return "Unknown"


def _neo4j_batch(driver, query: str, batch: list[dict]) -> None:
    """Execute a batched Cypher write via UNWIND $batch."""
    if not batch:
        return
    with driver.session() as session:
        session.run(query, batch=batch).consume()


async def _batch_write(
    driver, query: str, records: list[dict], batch_size: int = BATCH_SIZE
) -> tuple[int, int]:
    """Write records to Neo4j in batches.  Returns (written, errors)."""
    written, errors = 0, 0
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        try:
            await asyncio.to_thread(_neo4j_batch, driver, query, chunk)
            written += len(chunk)
        except Exception as exc:
            errors += len(chunk)
            logger.error("Batch write failed (offset %d): %s", i, exc)
    return written, errors


# ════════════════════════════════════════════════════════
# Constraints / Indexes
# ════════════════════════════════════════════════════════

async def _ensure_constraints(driver) -> None:
    """Create uniqueness constraints (idempotent, safe to re-run)."""
    stmts = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Taxpayer) REQUIRE t.gstin IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Invoice)  REQUIRE i.invoice_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (e:EWayBill) REQUIRE e.ewaybill_no IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Return)   REQUIRE r.return_id IS UNIQUE",
    ]

    def _run():
        with driver.session() as session:
            for s in stmts:
                try:
                    session.run(s).consume()
                except Exception as exc:
                    logger.warning("Constraint skip: %s", exc)

    await asyncio.to_thread(_run)


# ════════════════════════════════════════════════════════
# Step 1 — Taxpayer nodes
# ════════════════════════════════════════════════════════

async def _sync_taxpayers(db, driver) -> dict:
    docs = await db.Taxpayers.find({}, {"_id": 0}).to_list(None)
    query = """
    UNWIND $batch AS row
    MERGE (t:Taxpayer {gstin: row.gstin})
    SET t.name           = row.name,
        t.risk_category  = row.risk_category,
        t.ip_address     = row.ip_address,
        t.phone          = row.phone
    """
    records, skipped = [], 0
    for d in docs:
        try:
            records.append({
                "gstin":         d["GSTIN"],
                "name":          d.get("Name", ""),
                "risk_category": d.get("Risk_Category", "UNKNOWN"),
                "ip_address":    d.get("IP_Address", ""),
                "phone":         d.get("Phone", ""),
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("Taxpayer skipped (missing key): %s", exc)

    written, errs = await _batch_write(driver, query, records)
    return {"step": "Taxpayers", "read": len(docs), "written": written,
            "skipped": skipped, "batch_errors": errs}


# ════════════════════════════════════════════════════════
# Step 2 — Invoice nodes + ISSUED / BILLED_TO
# ════════════════════════════════════════════════════════

async def _sync_invoices(db, driver) -> dict:
    docs = await db.Invoices.find({}, {"_id": 0}).to_list(None)
    query = """
    UNWIND $batch AS row
    MERGE (i:Invoice {invoice_id: row.invoice_id})
    SET i.value        = row.value,
        i.invoice_date = row.invoice_date,
        i.seller_gstin = row.seller_gstin,
        i.buyer_gstin  = row.buyer_gstin
    WITH i, row
    MERGE (s:Taxpayer {gstin: row.seller_gstin})
    MERGE (b:Taxpayer {gstin: row.buyer_gstin})
    MERGE (s)-[:ISSUED]->(i)
    MERGE (i)-[:BILLED_TO]->(b)
    """
    records, skipped = [], 0
    for d in docs:
        try:
            records.append({
                "invoice_id":   d["Invoice_ID"],
                "value":        _safe_float(d.get("Value")),
                "invoice_date": d.get("Invoice_Date", ""),
                "seller_gstin": d["Seller_GSTIN"],
                "buyer_gstin":  d["Buyer_GSTIN"],
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("Invoice skipped: %s", exc)

    written, errs = await _batch_write(driver, query, records)
    return {"step": "Invoices + ISSUED/BILLED_TO", "read": len(docs),
            "written": written, "skipped": skipped, "batch_errors": errs}


# ════════════════════════════════════════════════════════
# Step 3 — GSTR1 → Return nodes, FILED, REPORTED_IN,
#           + enrich Invoice with filing status
# ════════════════════════════════════════════════════════

async def _sync_gstr1(db, driver) -> dict:
    docs = await db.GSTR1.find({}, {"_id": 0}).to_list(None)

    # 3a — enrich Invoice nodes with GSTR1 filing info
    enrich_q = """
    UNWIND $batch AS row
    MERGE (i:Invoice {invoice_id: row.invoice_id})
    SET i.gstr1_status      = row.status,
        i.gstr1_filing_date = row.filing_date,
        i.gstr1_tax         = row.tax
    """

    # 3b — create Return(GSTR1) nodes + relationships
    return_q = """
    UNWIND $batch AS row
    MERGE (r:Return {return_id: row.return_id})
    SET r.type = 'GSTR1', r.gstin = row.gstin, r.period = row.period,
        r.status = row.status, r.filing_date = row.filing_date
    WITH r, row
    MERGE (t:Taxpayer {gstin: row.gstin})
    MERGE (t)-[:FILED]->(r)
    WITH r, row
    MERGE (i:Invoice {invoice_id: row.invoice_id})
    MERGE (i)-[:REPORTED_IN]->(r)
    """

    enrich_recs, return_recs, skipped = [], [], 0
    for d in docs:
        try:
            gstin      = d["Seller_GSTIN"]
            inv_id     = d["Invoice_ID"]
            status     = d.get("Status", "UNKNOWN")
            filing_dt  = d.get("Filing_Date", "")
            period     = _derive_month(filing_dt)
            tax        = _safe_float(d.get("Tax"))

            enrich_recs.append({
                "invoice_id": inv_id, "status": status,
                "filing_date": filing_dt, "tax": tax,
            })
            return_recs.append({
                "return_id": f"GSTR1_{gstin}_{period}",
                "gstin": gstin, "period": period,
                "status": status, "filing_date": filing_dt,
                "invoice_id": inv_id,
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("GSTR1 skipped: %s", exc)

    w1, e1 = await _batch_write(driver, enrich_q, enrich_recs)
    w2, e2 = await _batch_write(driver, return_q, return_recs)
    return {"step": "GSTR1 → Returns + enrichment", "read": len(docs),
            "enriched": w1, "returns_written": w2,
            "skipped": skipped, "batch_errors": e1 + e2}


# ════════════════════════════════════════════════════════
# Step 4 — GSTR2B → enrich Invoice w/ ITC eligibility
#           + CLAIMED_ITC where eligible
# ════════════════════════════════════════════════════════

async def _sync_gstr2b(db, driver) -> dict:
    docs = await db.GSTR2B.find({}, {"_id": 0}).to_list(None)
    query = """
    UNWIND $batch AS row
    MERGE (i:Invoice {invoice_id: row.invoice_id})
    SET i.gstr2b_itc_eligible = row.itc_eligible,
        i.gstr2b_buyer_gstin  = row.buyer_gstin,
        i.gstr2b_tax          = row.tax
    WITH i, row
    WHERE row.itc_eligible = 'YES'
    MERGE (b:Taxpayer {gstin: row.buyer_gstin})
    MERGE (b)-[:CLAIMED_ITC]->(i)
    """
    records, skipped = [], 0
    for d in docs:
        try:
            records.append({
                "invoice_id":   d["Invoice_ID"],
                "buyer_gstin":  d["Buyer_GSTIN"],
                "itc_eligible": d.get("ITC_Eligible", "NO"),
                "tax":          _safe_float(d.get("Tax")),
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("GSTR2B skipped: %s", exc)

    written, errs = await _batch_write(driver, query, records)
    return {"step": "GSTR2B → ITC enrichment", "read": len(docs),
            "written": written, "skipped": skipped, "batch_errors": errs}


# ════════════════════════════════════════════════════════
# Step 5 — GSTR3B → Return nodes + FILED
# ════════════════════════════════════════════════════════

async def _sync_gstr3b(db, driver) -> dict:
    docs = await db.GSTR3B.find({}, {"_id": 0}).to_list(None)
    query = """
    UNWIND $batch AS row
    MERGE (r:Return {return_id: row.return_id})
    SET r.type              = 'GSTR3B',
        r.gstin             = row.gstin,
        r.period            = row.period,
        r.tax_paid          = row.tax_paid,
        r.payment_confirmed = row.payment_confirmed
    WITH r, row
    MERGE (t:Taxpayer {gstin: row.gstin})
    MERGE (t)-[:FILED]->(r)
    """
    records, skipped = [], 0
    for d in docs:
        try:
            gstin = d["Seller_GSTIN"]
            month = d.get("Month", "Unknown")
            records.append({
                "return_id":          f"GSTR3B_{gstin}_{month}",
                "gstin":              gstin,
                "period":             month,
                "tax_paid":           _safe_float(d.get("Tax_Paid")),
                "payment_confirmed":  d.get("Payment_Confirmed", "N"),
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("GSTR3B skipped: %s", exc)

    written, errs = await _batch_write(driver, query, records)
    return {"step": "GSTR3B → Return nodes + FILED", "read": len(docs),
            "written": written, "skipped": skipped, "batch_errors": errs}


# ════════════════════════════════════════════════════════
# Step 6 — Link GSTR1 Returns → GSTR3B Returns
# ════════════════════════════════════════════════════════

async def _link_gstr1_to_gstr3b(driver) -> dict:
    """MERGE (GSTR1)-[:SUMMARIZED_IN]->(GSTR3B) for same seller + period."""
    query = """
    MATCH (g1:Return {type: 'GSTR1'}), (g3:Return {type: 'GSTR3B'})
    WHERE g1.gstin = g3.gstin AND g1.period = g3.period
    MERGE (g1)-[:SUMMARIZED_IN]->(g3)
    """

    def _run():
        with driver.session() as session:
            summary = session.run(query).consume()
            return summary.counters.relationships_created

    created = await asyncio.to_thread(_run)
    return {"step": "GSTR1 ↔ GSTR3B linking (SUMMARIZED_IN)",
            "relationships_created": created}


# ════════════════════════════════════════════════════════
# Step 7 — EWayBill nodes + HAS_EWAYBILL
# ════════════════════════════════════════════════════════

async def _sync_ewaybills(db, driver) -> dict:
    docs = await db.EWayBill.find({}, {"_id": 0}).to_list(None)
    query = """
    UNWIND $batch AS row
    MERGE (e:EWayBill {ewaybill_no: row.ewaybill_no})
    SET e.value        = row.value,
        e.distance     = row.distance,
        e.date         = row.date,
        e.seller_gstin = row.seller_gstin,
        e.buyer_gstin  = row.buyer_gstin
    WITH e, row
    MERGE (i:Invoice {invoice_id: row.invoice_id})
    MERGE (i)-[:HAS_EWAYBILL]->(e)
    """
    records, skipped = [], 0
    for d in docs:
        try:
            records.append({
                "ewaybill_no":  d["EWayBill_No"],
                "invoice_id":   d["Invoice_ID"],
                "seller_gstin": d.get("Seller_GSTIN", ""),
                "buyer_gstin":  d.get("Buyer_GSTIN", ""),
                "value":        _safe_float(d.get("Value")),
                "distance":     _safe_float(d.get("Distance")),
                "date":         d.get("Date", ""),
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("EWayBill skipped: %s", exc)

    written, errs = await _batch_write(driver, query, records)
    return {"step": "EWayBills + HAS_EWAYBILL", "read": len(docs),
            "written": written, "skipped": skipped, "batch_errors": errs}


# ════════════════════════════════════════════════════════
# Step 8 — Purchase_Register → CLAIMED_ITC (with amounts)
# ════════════════════════════════════════════════════════

async def _sync_purchase_register(db, driver) -> dict:
    docs = await db.Purchase_Register.find({}, {"_id": 0}).to_list(None)
    query = """
    UNWIND $batch AS row
    MERGE (b:Taxpayer {gstin: row.buyer_gstin})
    MERGE (i:Invoice  {invoice_id: row.invoice_id})
    MERGE (b)-[r:CLAIMED_ITC]->(i)
    SET r.value_claimed = row.value_claimed,
        r.tax_claimed   = row.tax_claimed,
        r.claim_date    = row.claim_date
    """
    records, skipped = [], 0
    for d in docs:
        try:
            records.append({
                "buyer_gstin":  d["Buyer_GSTIN"],
                "invoice_id":   d["Invoice_ID"],
                "value_claimed": _safe_float(d.get("Value_Claimed")),
                "tax_claimed":   _safe_float(d.get("Tax_Claimed")),
                "claim_date":    d.get("Claim_Date", ""),
            })
        except KeyError as exc:
            skipped += 1
            logger.warning("Purchase_Register skipped: %s", exc)

    written, errs = await _batch_write(driver, query, records)
    return {"step": "Purchase_Register → CLAIMED_ITC", "read": len(docs),
            "written": written, "skipped": skipped, "batch_errors": errs}


# ════════════════════════════════════════════════════════
# Orchestrator
# ════════════════════════════════════════════════════════

async def sync_graph() -> dict[str, Any]:
    """Full MongoDB → Neo4j graph sync.

    Idempotent, deterministic, fault-tolerant.
    Returns a detailed report of every step.
    """
    db = get_mongo_db()
    driver = get_neo4j_driver()
    started = datetime.now(timezone.utc)
    steps: list[dict] = []

    try:
        # 0. Constraints / indexes
        await _ensure_constraints(driver)
        steps.append({"step": "Constraints", "status": "ok"})

        # 1-2. Core nodes
        steps.append(await _sync_taxpayers(db, driver))
        steps.append(await _sync_invoices(db, driver))

        # 3-5. Returns & enrichment
        steps.append(await _sync_gstr1(db, driver))
        steps.append(await _sync_gstr2b(db, driver))
        steps.append(await _sync_gstr3b(db, driver))

        # 6. Cross-link GSTR1 ↔ GSTR3B
        steps.append(await _link_gstr1_to_gstr3b(driver))

        # 7-8. E-way bills & purchase claims
        steps.append(await _sync_ewaybills(db, driver))
        steps.append(await _sync_purchase_register(db, driver))

        status = "completed"
    except Exception as exc:
        steps.append({"step": "FATAL", "error": str(exc)})
        status = "failed"
        logger.exception("Graph sync failed")

    finished = datetime.now(timezone.utc)
    return {
        "status": status,
        "started": started.isoformat(),
        "finished": finished.isoformat(),
        "duration_seconds": round((finished - started).total_seconds(), 2),
        "steps": steps,
    }
