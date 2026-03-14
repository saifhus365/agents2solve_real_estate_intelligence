"""LangGraph AgentState TypedDict — the single source of truth flowing
through every node in the pipeline."""

from __future__ import annotations

from typing import TypedDict


class AgentState(TypedDict, total=False):
    """Mutable state bag passed between LangGraph nodes.

    Keys correspond 1-to-1 with the specification:
    - query:            Original user question
    - query_type:       Classification result ("graph" | "vector" | "hybrid" | "timeseries")
    - cypher_query:     Generated Cypher statement (may be empty)
    - graph_results:    Records returned from Neo4j
    - vector_results:   Chunks returned from Pinecone
    - sql_results:      Rows returned from PostgreSQL timeseries queries
    - reranked_context: Combined & cross-encoder re-ranked results (top 8)
    - grounded_context: Subset of reranked_context that passed grounding validation
    - answer:           Final generated answer text
    - citations:        Citation objects attached to the answer
    - latency_ms:       End-to-end latency in milliseconds
    - error:            Optional error message for downstream handling
    """

    query: str
    query_type: str
    cypher_query: str
    graph_results: list[dict]
    vector_results: list[dict]
    sql_results: list[dict]
    reranked_context: list[dict]
    grounded_context: list[dict]
    answer: str
    citations: list[dict]
    latency_ms: float
    error: str
