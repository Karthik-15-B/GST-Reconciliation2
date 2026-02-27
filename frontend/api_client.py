"""
HTTP client helpers for calling the FastAPI backend.
All frontend ↔ backend communication goes through this module.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
TIMEOUT = 10  # seconds


def _get(path: str) -> dict:
    """Send a GET request to the backend and return JSON."""
    resp = requests.get(f"{BACKEND_URL}{path}", timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, payload: dict) -> dict:
    """Send a POST request to the backend and return JSON."""
    resp = requests.post(f"{BACKEND_URL}{path}", json=payload, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ── Health ────────────────────────────

def get_health() -> dict:
    """GET /health → {mongodb: UP/DOWN, neo4j: UP/DOWN}"""
    return _get("/health")


# ── MongoDB test ──────────────────────

def insert_mongo_test(name: str, value: str) -> dict:
    """POST /test/mongo"""
    return _post("/test/mongo", {"name": name, "value": value})


def fetch_mongo_tests() -> dict:
    """GET /test/mongo"""
    return _get("/test/mongo")


# ── Neo4j test ────────────────────────

def insert_neo4j_test(node1: str, node2: str, relationship: str = "KNOWS") -> dict:
    """POST /test/neo4j"""
    return _post("/test/neo4j", {
        "node1_name": node1,
        "node2_name": node2,
        "relationship": relationship,
    })


def fetch_neo4j_tests() -> dict:
    """GET /test/neo4j"""
    return _get("/test/neo4j")
