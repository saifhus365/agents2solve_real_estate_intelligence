"""FastAPI application entry point with lifespan management."""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.chat import router as chat_router
from backend.api.eval import router as eval_router
from backend.api.health import router as health_router
from backend.db.neo4j_client import neo4j_client
from backend.db.pinecone_client import pinecone_client
from backend.db.postgres_client import pg_client

logger = structlog.get_logger(__name__)

# ── Structured logging configuration ─────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)


# ── Lifespan: init / teardown DB connections ─────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN201, ARG001
    """Initialise database connections on startup, close on shutdown."""
    logger.info("app.starting")
    await pg_client.connect()
    await neo4j_client.connect()
    await pinecone_client.connect()
    logger.info("app.started")
    yield
    logger.info("app.shutting_down")
    await pinecone_client.close()
    await neo4j_client.close()
    await pg_client.close()
    logger.info("app.stopped")


# ── Application factory ─────────────────────────────────────────────────────

app = FastAPI(
    title="Dubai Real Estate Intelligence Co-Pilot",
    description=(
        "GraphRAG co-pilot for Dubai real estate professionals. "
        "Combines Neo4j graph traversal, BAAI vector search, and "
        "PostgreSQL timeseries queries with LLM-powered answer generation."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS (allow Vite dev server) ─────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────────────────

app.include_router(chat_router, prefix="/api")
app.include_router(health_router, prefix="/api")
app.include_router(eval_router, prefix="/api")
