"""
Ingestion endpoint — triggers a full scan of backend/files/
and loads every valid CSV/JSON file into MongoDB.
"""

from fastapi import APIRouter
from backend.ingestion import ingest_all_files

router = APIRouter(prefix="/ingest", tags=["Ingestion"])


@router.post("")
async def run_ingestion():
    """Scan the files/ directory and ingest all valid files into MongoDB.

    Re-runnable: unchanged files are automatically skipped.
    Returns a detailed report of every file processed.
    """
    report = await ingest_all_files()
    return report


@router.get("/status")
async def ingestion_status():
    """Return the log of previously ingested files."""
    from backend.database import get_mongo_db

    db = get_mongo_db()
    cursor = db["_ingestion_log"].find({}, {"_id": 0}).sort("ingested_at", -1)
    logs = await cursor.to_list(length=100)
    return {"ingested_files": len(logs), "log": logs}
