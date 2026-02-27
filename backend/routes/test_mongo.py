"""
MongoDB test endpoints — insert and fetch dummy documents.
Collection: test_collection
"""

from fastapi import APIRouter, HTTPException
from backend.database import get_mongo_db
from backend.models import MongoTestDoc, MongoTestResponse

router = APIRouter(prefix="/test", tags=["MongoDB Test"])


@router.post("/mongo", response_model=MongoTestResponse)
async def insert_test_document(doc: MongoTestDoc):
    """Insert a simple document into the test collection."""
    try:
        db = get_mongo_db()
        result = await db.test_collection.insert_one(doc.model_dump())
        return MongoTestResponse(
            inserted_id=str(result.inserted_id),
            message="Document inserted successfully.",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/mongo")
async def fetch_test_documents():
    """Return all documents from the test collection (capped at 50)."""
    try:
        db = get_mongo_db()
        cursor = db.test_collection.find({}, {"_id": 0}).limit(50)
        docs = await cursor.to_list(length=50)
        return {"count": len(docs), "documents": docs}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
