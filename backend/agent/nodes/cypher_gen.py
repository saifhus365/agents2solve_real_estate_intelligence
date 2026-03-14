"""Cypher query generator node.

Uses the NVIDIA LLM with the full graph schema as context to produce
a Cypher query. Includes 5 few-shot examples covering common patterns.
"""

from __future__ import annotations

import time

import structlog
from openai import AsyncOpenAI

from backend.agent.state import AgentState
from backend.config import get_settings
from backend.models.graph_schema import get_schema_description

logger = structlog.get_logger(__name__)

FEW_SHOT_EXAMPLES = """
Example 1:
User: Which developers have projects in Downtown Dubai?
Cypher:
MATCH (d:Developer)-[:LAUNCHED]->(p:Project)-[:LOCATED_IN]->(a:Area {name: 'Downtown Dubai'})
RETURN d.name AS developer, p.name AS project, a.name AS area

Example 2:
User: What is the average price per sqft in Business Bay?
Cypher:
MATCH (a:Area {name: 'Business Bay'})
RETURN a.name AS area, a.avg_price_sqft AS avg_price_sqft, a.yoy_price_change AS yoy_change

Example 3:
User: Show me Emaar projects and their areas
Cypher:
MATCH (d:Developer {name: 'Emaar'})-[:LAUNCHED]->(p:Project)-[:LOCATED_IN]->(a:Area)
RETURN d.name AS developer, p.name AS project, a.name AS area, p.status AS status

Example 4:
User: Which areas are near a metro station within 500m?
Cypher:
MATCH (a:Area)-[r:NEAR_STATION]->(m:MetroStation)
WHERE r.distance_m <= 500
RETURN a.name AS area, m.name AS station, r.distance_m AS distance

Example 5:
User: What RERA permits were granted to Damac?
Cypher:
MATCH (r:RERAPermit)-[:GRANTED_TO]->(d:Developer {name: 'Damac'})
RETURN r.permit_number, r.issue_date, r.status, d.name AS developer
"""

SYSTEM_PROMPT = f"""\
You are a Cypher query generator for a Dubai real estate Neo4j knowledge graph.

{get_schema_description()}

Rules:
1. Output ONLY the Cypher query — no explanations, no markdown fences.
2. Use MATCH / WHERE / RETURN — never use DETACH DELETE or write operations.
3. Use case-insensitive matching with toLower() when filtering on names.
4. Always return node properties needed to answer the question.
5. For multi-hop queries, follow relationship chains as defined in the schema.
6. If the query is ambiguous, prefer returning more data with LIMIT 25.

{FEW_SHOT_EXAMPLES}
Now generate Cypher for the user's question.
"""


async def generate_cypher(state: AgentState) -> AgentState:
    """Generate a Cypher query from the user question using the LLM."""
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
        max_tokens=500,
    )

    cypher = response.choices[0].message.content.strip()
    # Strip potential markdown fences
    if cypher.startswith("```"):
        lines = cypher.split("\n")
        cypher = "\n".join(
            line for line in lines if not line.startswith("```")
        ).strip()

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "cypher_gen.generated",
        cypher=cypher[:200],
        latency_ms=round(elapsed, 1),
    )

    return {**state, "cypher_query": cypher}
