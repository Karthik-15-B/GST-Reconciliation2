"""
File ingestion engine.

Reads every valid JSON / CSV file from backend/files/,
parses it into records, and upserts them into MongoDB.
Each file maps to a collection named after the file (without extension).

Design goals:
  - Fault-tolerant: bad files are skipped and logged, never crash the loop.
  - Re-runnable:    uses a content hash per file stored in an _ingestion_log
                    collection; unchanged files are skipped on re-run.
  - Logged:         returns a detailed report of every action taken.
"""

import csv
import hashlib
import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.database import get_mongo_db

logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)

# Directory containing the data files
FILES_DIR = Path(__file__).resolve().parent / "files"

# MongoDB collection that tracks what has already been ingested
LOG_COLLECTION = "_ingestion_log"


# ════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════

def _file_hash(content: bytes) -> str:
    """Return a SHA-256 hex digest for the raw file content."""
    return hashlib.sha256(content).hexdigest()


def _collection_name(filename: str) -> str:
    """Derive a MongoDB collection name from the file name.
    e.g. 'GSTR1.csv' → 'GSTR1', 'invoices.json' → 'invoices'
    """
    return Path(filename).stem


def _parse_csv(raw: bytes, filename: str) -> list[dict[str, Any]]:
    """Parse CSV bytes into a list of dicts (one per row)."""
    text = raw.decode("utf-8-sig")  # handles BOM if present
    reader = csv.DictReader(io.StringIO(text))
    records: list[dict[str, Any]] = []
    for i, row in enumerate(reader, start=2):  # row 1 = header
        # Strip whitespace from keys and values
        clean = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
        clean["_source_file"] = filename
        clean["_source_row"] = i
        records.append(clean)
    return records


def _parse_json(raw: bytes, filename: str) -> list[dict[str, Any]]:
    """Parse JSON bytes into a list of dicts.
    Accepts either a JSON array or a single JSON object.
    """
    data = json.loads(raw)
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = [data]
    else:
        raise ValueError(f"Unexpected JSON root type: {type(data).__name__}")

    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            raise ValueError(f"Record {i} is {type(rec).__name__}, expected dict.")
        rec["_source_file"] = filename
        rec["_source_row"] = i + 1
    return records


PARSERS = {
    ".csv": _parse_csv,
    ".json": _parse_json,
}


# ════════════════════════════════════════════════════════
# Core ingestion
# ════════════════════════════════════════════════════════

async def ingest_all_files() -> dict[str, Any]:
    """Scan files/ directory and ingest every valid file into MongoDB.

    Returns a summary report dict:
    {
        "files_found": int,
        "files_ingested": int,
        "files_skipped_unchanged": int,
        "files_skipped_error": int,
        "details": [
            {"file": str, "collection": str, "records": int, "status": str, "message": str},
            ...
        ],
        "timestamp": str,
    }
    """
    db = get_mongo_db()
    log_col = db[LOG_COLLECTION]

    # Discover files
    if not FILES_DIR.is_dir():
        return {
            "files_found": 0,
            "files_ingested": 0,
            "files_skipped_unchanged": 0,
            "files_skipped_error": 0,
            "details": [],
            "error": f"Directory not found: {FILES_DIR}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    all_files = sorted(
        f for f in FILES_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in PARSERS
    )

    report: dict[str, Any] = {
        "files_found": len(all_files),
        "files_ingested": 0,
        "files_skipped_unchanged": 0,
        "files_skipped_error": 0,
        "details": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    for filepath in all_files:
        detail = {"file": filepath.name, "collection": _collection_name(filepath.name)}
        try:
            raw = filepath.read_bytes()
            content_hash = _file_hash(raw)

            # ── Check if this exact file version was already ingested ──
            existing = await log_col.find_one({
                "file": filepath.name,
                "content_hash": content_hash,
            })
            if existing:
                detail.update(status="skipped", message="File unchanged since last ingestion.",
                              records=0)
                report["files_skipped_unchanged"] += 1
                report["details"].append(detail)
                logger.info("SKIP (unchanged): %s", filepath.name)
                continue

            # ── Parse ──
            parser = PARSERS[filepath.suffix.lower()]
            records = parser(raw, filepath.name)
            if not records:
                detail.update(status="skipped", message="File parsed but contains 0 records.",
                              records=0)
                report["files_skipped_error"] += 1
                report["details"].append(detail)
                logger.warning("SKIP (empty): %s", filepath.name)
                continue

            # ── Insert into MongoDB ──
            col_name = _collection_name(filepath.name)
            collection = db[col_name]

            # Drop previous data for this file (makes re-runs idempotent)
            await collection.delete_many({"_source_file": filepath.name})
            result = await collection.insert_many(records)

            inserted_count = len(result.inserted_ids)

            # ── Record in ingestion log ──
            await log_col.update_one(
                {"file": filepath.name},
                {"$set": {
                    "file": filepath.name,
                    "collection": col_name,
                    "content_hash": content_hash,
                    "records_inserted": inserted_count,
                    "ingested_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )

            detail.update(status="ingested", records=inserted_count,
                          message=f"Inserted {inserted_count} record(s).")
            report["files_ingested"] += 1
            logger.info("INGESTED: %s → %s (%d records)", filepath.name, col_name, inserted_count)

        except Exception as exc:
            detail.update(status="error", records=0, message=str(exc))
            report["files_skipped_error"] += 1
            logger.error("ERROR: %s → %s", filepath.name, exc)

        report["details"].append(detail)

    return report
