"""Grounding validation node.

Before generation, filters reranked context to ensure every candidate
fact is traceable to at least one retrieved source. Removes ungrounded
claims and logs removed facts.
"""

from __future__ import annotations

import json
import time

import structlog

from backend.agent.state import AgentState

logger = structlog.get_logger(__name__)


def _is_grounded(candidate: dict, all_sources: list[dict]) -> bool:
    """Check if a candidate fact can be traced to at least one source.

    A candidate is grounded if:
    1. It has a non-empty text or data payload, AND
    2. Its source_type matches one of the retrieval channels that
       returned results (graph, vector, sql).
    """
    source_type = candidate.get("_source_type", "")

    # Must have actual content
    has_text = bool(candidate.get("text", ""))
    has_data = any(
        v for k, v in candidate.items()
        if k not in ("_source_type", "_rerank_score", "id", "score")
        and v is not None and v != ""
    )

    if not (has_text or has_data):
        return False

    # Must match a channel that participated in retrieval
    active_channels = {s.get("_source_type") for s in all_sources if s.get("_source_type")}
    if source_type and source_type not in active_channels:
        return False

    return True


def _extract_key_claims(candidate: dict) -> list[str]:
    """Extract verifiable claims from a candidate for logging."""
    claims: list[str] = []
    if candidate.get("text"):
        # Take first 100 chars as a claim indicator
        claims.append(str(candidate["text"])[:100])
    for key in ("name", "area_name", "developer", "project", "price", "avg_price_sqft"):
        if key in candidate and candidate[key]:
            claims.append(f"{key}={candidate[key]}")
    return claims


async def validate_grounding(state: AgentState) -> AgentState:
    """Filter reranked context to only grounded facts."""
    start = time.perf_counter()
    reranked = state.get("reranked_context", [])

    if not reranked:
        logger.info("grounding.no_context_to_validate")
        return {**state, "grounded_context": []}

    grounded: list[dict] = []
    removed: list[dict] = []

    for candidate in reranked:
        if _is_grounded(candidate, reranked):
            grounded.append(candidate)
        else:
            removed.append(candidate)

    # Log removed (ungrounded) facts
    if removed:
        for r in removed:
            claims = _extract_key_claims(r)
            logger.warning(
                "grounding.removed_ungrounded",
                source_type=r.get("_source_type"),
                claims=claims,
            )

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "grounding.complete",
        input_count=len(reranked),
        grounded_count=len(grounded),
        removed_count=len(removed),
        latency_ms=round(elapsed, 1),
    )

    return {**state, "grounded_context": grounded}
