"""Graph query execution node — runs Cypher against Neo4j and returns
subgraph records as a list of dicts."""

from __future__ import annotations

import time

import structlog

from backend.agent.state import AgentState
from backend.db.neo4j_client import neo4j_client

logger = structlog.get_logger(__name__)


async def execute_graph_query(state: AgentState) -> AgentState:
    """Execute the Cypher query from state against Neo4j.

    On syntax/runtime errors, logs the bad Cypher and returns an empty list
    so the pipeline can continue to the generator with whatever other
    context is available.
    """
    start = time.perf_counter()
    cypher = state.get("cypher_query", "")

    if not cypher:
        logger.warning("graph_query.no_cypher")
        return {**state, "graph_results": []}

    try:
        records = await neo4j_client.execute_cypher(cypher)
        # Flatten Neo4j records into plain dicts for downstream use
        graph_results = []
        for record in records:
            entry: dict = {}
            for key, value in record.items():
                # Handle Neo4j Node objects
                if hasattr(value, "items"):
                    entry[key] = dict(value)
                else:
                    entry[key] = value
            graph_results.append(entry)

        elapsed = (time.perf_counter() - start) * 1000
        logger.info(
            "graph_query.success",
            cypher=cypher[:120],
            rows=len(graph_results),
            latency_ms=round(elapsed, 1),
        )
        return {**state, "graph_results": graph_results}

    except Exception:
        elapsed = (time.perf_counter() - start) * 1000
        logger.error(
            "graph_query.cypher_error",
            cypher=cypher[:200],
            latency_ms=round(elapsed, 1),
            exc_info=True,
        )
        return {**state, "graph_results": []}
