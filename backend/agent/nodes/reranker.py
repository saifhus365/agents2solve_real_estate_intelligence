"""Cross-encoder reranker node.

Uses cross-encoder/ms-marco-MiniLM-L-6-v2 to rerank the combined
graph + vector + SQL results against the original query. Returns top 8.
"""

from __future__ import annotations

import json
import time

import structlog
from sentence_transformers import CrossEncoder

from backend.agent.state import AgentState
from backend.config import get_settings

logger = structlog.get_logger(__name__)

_reranker: CrossEncoder | None = None


def _get_reranker() -> CrossEncoder:
    global _reranker  # noqa: PLW0603
    if _reranker is None:
        settings = get_settings()
        _reranker = CrossEncoder(settings.reranker_model)
        logger.info("reranker.model_loaded", model=settings.reranker_model)
    return _reranker


def _result_to_text(result: dict) -> str:
    """Convert a result dict to a searchable text string."""
    if "text" in result and result["text"]:
        return str(result["text"])
    # For graph / SQL results, serialise the whole dict
    return json.dumps(result, default=str)


async def rerank_results(state: AgentState) -> AgentState:
    """Combine all results and rerank with a cross-encoder."""
    start = time.perf_counter()
    settings = get_settings()
    query = state["query"]

    # Gather all candidates
    candidates: list[dict] = []

    for r in state.get("graph_results", []):
        candidates.append({**r, "_source_type": "graph"})

    for r in state.get("vector_results", []):
        candidates.append({**r, "_source_type": "vector"})

    for r in state.get("sql_results", []):
        candidates.append({**r, "_source_type": "sql"})

    if not candidates:
        logger.info("reranker.no_candidates")
        return {**state, "reranked_context": []}

    # Build query-document pairs
    texts = [_result_to_text(c) for c in candidates]
    pairs = [(query, t) for t in texts]

    reranker = _get_reranker()
    scores = reranker.predict(pairs)

    # Attach scores and sort descending
    for i, candidate in enumerate(candidates):
        candidate["_rerank_score"] = float(scores[i])

    candidates.sort(key=lambda c: c["_rerank_score"], reverse=True)
    top_k = candidates[: settings.rerank_top_k]

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "reranker.complete",
        total_candidates=len(candidates),
        top_k=len(top_k),
        latency_ms=round(elapsed, 1),
    )

    return {**state, "reranked_context": top_k}
