"""
Minimal Pydantic models used across the backend.
No business logic — only shapes for API requests / responses.
"""

from pydantic import BaseModel, Field


# ── Health ────────────────────────────
class HealthStatus(BaseModel):
    """Response model for the /health endpoint."""
    mongodb: str = Field(..., description="UP or DOWN")
    neo4j: str = Field(..., description="UP or DOWN")


# ── MongoDB test ──────────────────────
class MongoTestDoc(BaseModel):
    """Payload for inserting a test document into MongoDB."""
    name: str = Field(..., min_length=1, max_length=100, examples=["test_item"])
    value: str = Field(..., min_length=1, max_length=200, examples=["hello"])


class MongoTestResponse(BaseModel):
    """Response after inserting a test document."""
    inserted_id: str
    message: str


# ── Neo4j test ────────────────────────
class Neo4jTestPayload(BaseModel):
    """Payload for creating demo nodes + relationship in Neo4j."""
    node1_name: str = Field(..., min_length=1, max_length=100, examples=["Alice"])
    node2_name: str = Field(..., min_length=1, max_length=100, examples=["Bob"])
    relationship: str = Field(
        default="KNOWS",
        min_length=1,
        max_length=50,
        examples=["KNOWS"],
    )


class Neo4jTestResponse(BaseModel):
    """Response after creating demo nodes + relationship."""
    message: str
