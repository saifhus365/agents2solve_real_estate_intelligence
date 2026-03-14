"""POST /api/chat — SSE streaming chat endpoint.

Runs the LangGraph agent, streams tokens via Server-Sent Events,
and logs every query/response to PostgreSQL chat_logs.
"""

from __future__ import annotations

import json
import time

import structlog
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from backend.agent.graph import run_agent
from backend.agent.nodes.generator import generate_answer_stream
from backend.agent.nodes.classifier import classify_query
from backend.agent.nodes.cypher_gen import generate_cypher
from backend.agent.nodes.graph_query import execute_graph_query
from backend.agent.nodes.grounding import validate_grounding
from backend.agent.nodes.reranker import rerank_results
from backend.agent.nodes.sql_query import run_sql_query
from backend.agent.nodes.vector_search import vector_search
from backend.agent.state import AgentState
from backend.db.postgres_client import pg_client
from backend.models.schemas import ChatRequest

logger = structlog.get_logger(__name__)
router = APIRouter()


async def _log_chat(
    session_id: str,
    query: str,
    query_type: str,
    answer: str,
    citations: list[dict],
    latency_ms: float,
) -> None:
    """Persist the query/response to the chat_logs table."""
    try:
        await pg_client.execute(
            """
            INSERT INTO chat_logs (session_id, query, query_type, answer, citations, latency_ms)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            """,
            session_id,
            query,
            query_type,
            answer,
            json.dumps(citations, default=str),
            latency_ms,
        )
    except Exception:
        logger.error("chat.log_failed", exc_info=True)


async def _run_pipeline_and_stream(request: ChatRequest):
    """Execute the agent pipeline up to grounding, then stream generation."""
    start = time.perf_counter()

    # Build initial state
    state: AgentState = {
        "query": request.query,
        "query_type": "",
        "cypher_query": "",
        "graph_results": [],
        "vector_results": [],
        "sql_results": [],
        "reranked_context": [],
        "grounded_context": [],
        "answer": "",
        "citations": [],
        "latency_ms": 0.0,
        "error": "",
    }

    try:
        # Step 1: Classify
        state = await classify_query(state)

        # Step 2: Retrieve (based on query type)
        qt = state.get("query_type", "hybrid")
        if qt == "graph":
            state = await generate_cypher(state)
            state = await execute_graph_query(state)
        elif qt == "vector":
            state = await vector_search(state)
        elif qt == "timeseries":
            state = await run_sql_query(state)
        else:  # hybrid
            state = await generate_cypher(state)
            state = await execute_graph_query(state)
            state = await vector_search(state)

        # Step 3: Rerank
        state = await rerank_results(state)

        # Step 4: Ground
        state = await validate_grounding(state)

        # Step 5: Stream generation
        full_answer = ""
        all_citations: list[dict] = []

        async for event in generate_answer_stream(state):
            if event["type"] == "token":
                full_answer += event.get("content", "")
            if event["type"] == "citations":
                all_citations = event.get("data", [])

            yield f"data: {json.dumps(event, default=str)}\n\n"

        # Log to DB
        elapsed = (time.perf_counter() - start) * 1000
        await _log_chat(
            session_id=request.session_id,
            query=request.query,
            query_type=state.get("query_type", ""),
            answer=full_answer,
            citations=all_citations,
            latency_ms=round(elapsed, 1),
        )

    except Exception as exc:
        logger.error("chat.pipeline_error", exc_info=True)
        error_event = {"type": "error", "message": str(exc)}
        yield f"data: {json.dumps(error_event)}\n\n"


@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    """SSE streaming chat endpoint.

    Accepts a query and session_id, runs the full GraphRAG pipeline,
    and streams back tokens, citations, and a done event.
    """
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    return StreamingResponse(
        _run_pipeline_and_stream(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
