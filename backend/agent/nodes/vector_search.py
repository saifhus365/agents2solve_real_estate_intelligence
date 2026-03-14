"""Vector search node — embed query with BAAI/bge-large-en-v1.5 and
retrieve top-k from Pinecone."""

from __future__ import annotations

import time

import structlog
from sentence_transformers import SentenceTransformer

from backend.agent.state import AgentState
from backend.config import get_settings
from backend.db.pinecone_client import pinecone_client

logger = structlog.get_logger(__name__)

_model: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _model  # noqa: PLW0603
    if _model is None:
        settings = get_settings()
        _model = SentenceTransformer(settings.embedding_model)
        logger.info("vector_search.model_loaded", model=settings.embedding_model)
    return _model


async def vector_search(state: AgentState) -> AgentState:
    """Embed the query and search Pinecone for relevant chunks."""
    start = time.perf_counter()
    settings = get_settings()
    model = _get_model()

    query_embedding = model.encode(state["query"]).tolist()

    raw_results = pinecone_client.query(
        vector=query_embedding,
        top_k=settings.vector_top_k,
        include_metadata=True,
    )

    vector_results = []
    for match in raw_results:
        meta = match.get("metadata", {})
        vector_results.append({
            "id": match["id"],
            "score": match["score"],
            "text": meta.get("text", ""),
            "source": meta.get("source", ""),
            "url": meta.get("url", ""),
            "title": meta.get("title", ""),
            "published_date": meta.get("published_date", ""),
            "type": meta.get("type", "vector"),
        })

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "vector_search.complete",
        results=len(vector_results),
        latency_ms=round(elapsed, 1),
    )

    return {**state, "vector_results": vector_results}
