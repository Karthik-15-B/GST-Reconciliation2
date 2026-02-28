"""
Seed the MongoDB `users` collection from taxpayers.csv.

Creates:
  - One CFO user per taxpayer (username = GSTIN, role = CFO)
  - One CA user (username = ca_demo, role = CA, clients = all GSTINs)
  - One Inspector user (username = inspector_demo, role = INSPECTOR)

All users share the same demo password: demo@123

Run:
    python -m backend.seed_users
"""

import asyncio
import csv
import os
from pathlib import Path

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
import certifi

load_dotenv()

DEMO_PASSWORD = "demo@123"
CSV_PATH = Path(__file__).resolve().parent / "files" / "Taxpayers.csv"


async def seed():
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME", "gst_reconciliation")

    client = AsyncIOMotorClient(
        uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
        tlsCAFile=certifi.where(),
    )
    db = client[db_name]
    col = db["users"]

    # ── Drop existing users for a clean seed ──
    await col.drop()
    print("[seed] Dropped existing users collection.")

    # ── Read taxpayers CSV ──
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        taxpayers = list(reader)

    all_gstins = [t["GSTIN"] for t in taxpayers]
    print(f"[seed] Found {len(taxpayers)} taxpayers in CSV.")

    # ── Build user documents ──
    users = []

    # 1) CFO users — one per taxpayer
    for t in taxpayers:
        users.append({
            "username": t["GSTIN"],
            "password": DEMO_PASSWORD,
            "role": "CFO",
            "gstin": t["GSTIN"],
            "name": t["Name"],
            "clients": [],
        })

    # 2) CA user — has all GSTINs as clients
    users.append({
        "username": "ca_demo",
        "password": DEMO_PASSWORD,
        "role": "CA",
        "gstin": "",
        "name": "CA Demo User",
        "clients": all_gstins,
    })

    # 3) Inspector user — full access
    users.append({
        "username": "inspector_demo",
        "password": DEMO_PASSWORD,
        "role": "INSPECTOR",
        "gstin": "",
        "name": "Inspector Demo User",
        "clients": [],
    })

    # ── Insert ──
    result = await col.insert_many(users)
    print(f"[seed] Inserted {len(result.inserted_ids)} users into 'users' collection.")
    print(f"       - {len(taxpayers)} CFO users (username = GSTIN)")
    print(f"       - 1 CA user   (username = ca_demo)")
    print(f"       - 1 Inspector (username = inspector_demo)")
    print(f"       - Shared password: {DEMO_PASSWORD}")

    # ── Create unique index on username ──
    await col.create_index("username", unique=True)
    print("[seed] Created unique index on 'username'.")

    client.close()
    print("[seed] Done.")


if __name__ == "__main__":
    asyncio.run(seed())
