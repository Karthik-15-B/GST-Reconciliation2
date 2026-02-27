"""
Neo4j test endpoints — create and fetch demo nodes / relationships.
"""

from fastapi import APIRouter, HTTPException
from backend.database import get_neo4j_driver
from backend.models import Neo4jTestPayload, Neo4jTestResponse

router = APIRouter(prefix="/test", tags=["Neo4j Test"])


@router.post("/neo4j", response_model=Neo4jTestResponse)
def create_test_nodes(payload: Neo4jTestPayload):
    """Create two Person nodes and a relationship between them."""
    query = (
        "MERGE (a:Person {name: $node1}) "
        "MERGE (b:Person {name: $node2}) "
        "MERGE (a)-[r:%s]->(b) "
        "RETURN a.name AS from_node, b.name AS to_node, type(r) AS rel"
    ) % payload.relationship  # relationship types can't be parameterised in Cypher
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            result = session.run(
                query,
                node1=payload.node1_name,
                node2=payload.node2_name,
            )
            record = result.single()
            msg = (
                f"Created ({record['from_node']})"
                f"-[:{record['rel']}]->"
                f"({record['to_node']})"
            )
        return Neo4jTestResponse(message=msg)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/neo4j")
def fetch_test_nodes():
    """Return all Person nodes and their relationships (capped at 50)."""
    query = (
        "MATCH (a:Person)-[r]->(b:Person) "
        "RETURN a.name AS from_node, type(r) AS relationship, b.name AS to_node "
        "LIMIT 50"
    )
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            result = session.run(query)
            records = [
                {
                    "from": rec["from_node"],
                    "relationship": rec["relationship"],
                    "to": rec["to_node"],
                }
                for rec in result
            ]
        return {"count": len(records), "relationships": records}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
