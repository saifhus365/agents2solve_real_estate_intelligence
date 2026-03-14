"""LangGraph state machine — wires all agent nodes into a conditional
graph that routes by query_type and converges at reranker → grounding → generator."""

from __future__ import annotations

import time

import structlog
from langgraph.graph import END, StateGraph

from backend.agent.nodes.classifier import classify_query
from backend.agent.nodes.cypher_gen import generate_cypher
from backend.agent.nodes.generator import generate_answer
from backend.agent.nodes.graph_query import execute_graph_query
from backend.agent.nodes.grounding import validate_grounding
from backend.agent.nodes.reranker import rerank_results
from backend.agent.nodes.sql_query import run_sql_query
from backend.agent.nodes.vector_search import vector_search
from backend.agent.state import AgentState

logger = structlog.get_logger(__name__)

# ── Routing function ─────────────────────────────────────────────────────────


def route_by_query_type(state: AgentState) -> str | list[str]:
    """Return the next node(s) based on query_type classification."""
    qt = state.get("query_type", "hybrid")

    if qt == "graph":
        return "cypher_gen"
    if qt == "vector":
        return "vector_search"
    if qt == "timeseries":
        return "sql_query"
    # hybrid → fan out to both graph and vector paths
    return ["cypher_gen", "vector_search"]


# ── Graph builder ────────────────────────────────────────────────────────────


def build_agent_graph() -> StateGraph:
    """Construct and compile the LangGraph agent pipeline.

    Flow:
        classify → route_by_query_type
            ├─ graph     → cypher_gen → graph_query → reranker
            ├─ vector    → vector_search → reranker
            ├─ timeseries→ sql_query → reranker
            └─ hybrid    → [cypher_gen → graph_query, vector_search] → reranker
        reranker → grounding → generator → END
    """
    graph = StateGraph(AgentState)

    # ── Add nodes ────────────────────────────────────────────────────────
    graph.add_node("classifier", classify_query)
    graph.add_node("cypher_gen", generate_cypher)
    graph.add_node("graph_query", execute_graph_query)
    graph.add_node("vector_search", vector_search)
    graph.add_node("sql_query", run_sql_query)
    graph.add_node("reranker", rerank_results)
    graph.add_node("grounding", validate_grounding)
    graph.add_node("generator", generate_answer)

    # ── Set entry point ──────────────────────────────────────────────────
    graph.set_entry_point("classifier")

    # ── Conditional branching after classification ────────────────────────
    graph.add_conditional_edges(
        "classifier",
        route_by_query_type,
        {
            "cypher_gen": "cypher_gen",
            "vector_search": "vector_search",
            "sql_query": "sql_query",
        },
    )

    # ── Convergence edges ────────────────────────────────────────────────
    graph.add_edge("cypher_gen", "graph_query")
    graph.add_edge("graph_query", "reranker")
    graph.add_edge("vector_search", "reranker")
    graph.add_edge("sql_query", "reranker")

    # ── Linear tail ──────────────────────────────────────────────────────
    graph.add_edge("reranker", "grounding")
    graph.add_edge("grounding", "generator")
    graph.add_edge("generator", END)

    return graph


# ── Compiled graph (module-level singleton) ──────────────────────────────────

_compiled_graph = None


def get_compiled_graph():  # noqa: ANN201
    """Return the compiled LangGraph runnable (lazy singleton)."""
    global _compiled_graph  # noqa: PLW0603
    if _compiled_graph is None:
        graph = build_agent_graph()
        _compiled_graph = graph.compile()
        logger.info("agent_graph.compiled")
    return _compiled_graph


async def run_agent(query: str) -> AgentState:
    """Execute the full agent pipeline for a query.

    Returns the final AgentState with answer, citations, and latency.
    """
    start = time.perf_counter()

    initial_state: AgentState = {
        "query": query,
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

    compiled = get_compiled_graph()
    final_state = await compiled.ainvoke(initial_state)

    elapsed = (time.perf_counter() - start) * 1000
    final_state["latency_ms"] = round(elapsed, 1)

    logger.info(
        "agent.run_complete",
        query=query[:80],
        query_type=final_state.get("query_type"),
        latency_ms=final_state["latency_ms"],
    )

    return final_state
