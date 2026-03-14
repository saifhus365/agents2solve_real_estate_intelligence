"""GET /api/health — service health check endpoint."""

from __future__ import annotations

import structlog
from fastapi import APIRouter

from backend.db.neo4j_client import neo4j_client
from backend.db.pinecone_client import pinecone_client
from backend.db.postgres_client import pg_client
from backend.models.schemas import HealthResponse

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check connectivity to Neo4j, PostgreSQL, and Pinecone."""
    pg_ok = await pg_client.healthcheck()
    neo4j_ok = await neo4j_client.healthcheck()
    pine_ok = await pinecone_client.healthcheck()

    all_ok = pg_ok and neo4j_ok and pine_ok
    some_ok = pg_ok or neo4j_ok or pine_ok

    if all_ok:
        status = "healthy"
    elif some_ok:
        status = "degraded"
    else:
        status = "unhealthy"

    logger.info(
        "health.check",
        status=status,
        postgres=pg_ok,
        neo4j=neo4j_ok,
        pinecone=pine_ok,
    )

    return HealthResponse(
        status=status,
        neo4j=neo4j_ok,
        postgres=pg_ok,
        pinecone=pine_ok,
    )
