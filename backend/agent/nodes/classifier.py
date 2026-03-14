"""Query type classifier node.

Uses a few-shot LLM prompt to classify inbound queries into one of:
  graph | vector | hybrid | timeseries
"""

from __future__ import annotations

import time

import structlog
from openai import AsyncOpenAI

from backend.agent.state import AgentState
from backend.config import get_settings

logger = structlog.get_logger(__name__)

SYSTEM_PROMPT = """\
You are a query classifier for a Dubai real estate intelligence system.
Classify the user query into exactly ONE of these categories:

- "graph"      — requires multi-hop entity traversal across developer, project, area, 
                  unit, or transaction nodes (e.g. "Which developer launched projects in Downtown?")
- "timeseries" — asks about price trends, transaction counts, or aggregates over time periods
                  (e.g. "Average price per sqft in JVC over the last 6 months")
- "vector"     — asks about news, analyst opinions, policy documents, market sentiment
                  (e.g. "What did analysts say about Dubai Marina supply risk?")
- "hybrid"     — requires BOTH entity lookup AND semantic context
                  (e.g. "Areas near metro with increasing off-plan ratio" or comparison queries)

Respond with ONLY the category name, nothing else.

Examples:
User: What is Emaar's on-time delivery rate?
Category: graph

User: Average price trend in Business Bay last quarter?
Category: timeseries

User: What do recent news articles say about oversupply?
Category: vector

User: Which areas within 800m of a metro station have increasing off-plan ratio?
Category: hybrid

User: Show me all developers who launched in JVC in 2024 and what analysts said about them.
Category: hybrid
"""


async def classify_query(state: AgentState) -> AgentState:
    """Classify the user query and set state['query_type']."""
    start = time.perf_counter()
    settings = get_settings()

    client = AsyncOpenAI(
        base_url=settings.nvidia_base_url,
        api_key=settings.nvidia_api_key,
    )

    response = await client.chat.completions.create(
        model=settings.nvidia_model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": state["query"]},
        ],
        temperature=0.0,
        max_tokens=10,
    )

    raw = response.choices[0].message.content.strip().lower()
    query_type = raw if raw in ("graph", "vector", "hybrid", "timeseries") else "hybrid"

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "classifier.result",
        query=state["query"][:80],
        query_type=query_type,
        latency_ms=round(elapsed, 1),
    )

    return {**state, "query_type": query_type}
