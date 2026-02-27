"""
Health check endpoint.
Returns UP / DOWN status for MongoDB Atlas and Neo4j Aura.
"""

from fastapi import APIRouter
from backend.database import ping_mongo, ping_neo4j
from backend.models import HealthStatus

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthStatus)
async def health_check():
    """Ping both databases and report their status."""
    mongo_ok = await ping_mongo()
    neo4j_ok = ping_neo4j()
    return HealthStatus(
        mongodb="UP" if mongo_ok else "DOWN",
        neo4j="UP" if neo4j_ok else "DOWN",
    )
