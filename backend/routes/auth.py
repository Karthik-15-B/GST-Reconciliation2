"""
Authentication routes.

POST /auth/login   — validate credentials, return user profile
GET  /auth/users   — list all users (admin/debug only)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.database import get_mongo_db

router = APIRouter(prefix="/auth", tags=["Authentication"])


# ── Request / Response models ─────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    authenticated: bool
    username: str
    role: str
    gstin: str = ""
    name: str = ""
    clients: list[str] = []


# ── POST /auth/login ─────────────────────────────────

@router.post("/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    """Validate username + password against MongoDB users collection."""
    db = get_mongo_db()
    user = await db["users"].find_one(
        {"username": req.username},
        {"_id": 0},
    )

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username")

    if user.get("password") != req.password:
        raise HTTPException(status_code=401, detail="Invalid password")

    return LoginResponse(
        authenticated=True,
        username=user["username"],
        role=user["role"],
        gstin=user.get("gstin", ""),
        name=user.get("name", ""),
        clients=user.get("clients", []),
    )


# ── GET /auth/users (debug) ──────────────────────────

@router.get("/users")
async def list_users():
    """Return all users (passwords masked). For debug / admin use."""
    db = get_mongo_db()
    cursor = db["users"].find({}, {"_id": 0, "password": 0})
    users = await cursor.to_list(length=500)
    return {"count": len(users), "users": users}
