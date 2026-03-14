"""SQL query node — parameterised template-matched timeseries queries.

NEVER generates raw SQL from user input. All queries use parameterised
templates matched by intent keywords.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone

import structlog

from backend.agent.state import AgentState
from backend.db.postgres_client import pg_client

logger = structlog.get_logger(__name__)

# ── SQL template registry ────────────────────────────────────────────────────

TEMPLATES: dict[str, dict] = {
    "avg_price_by_area": {
        "keywords": ["average price", "avg price", "price per sqft", "mean price"],
        "sql": """
            SELECT area_name,
                   ROUND(AVG(price_sqft)::numeric, 2) AS avg_price_sqft,
                   COUNT(*) AS transaction_count
            FROM transactions
            WHERE area_name ILIKE $1
              AND date >= $2 AND date <= $3
            GROUP BY area_name
        """,
        "params_builder": "_build_area_period_params",
    },
    "price_trend_monthly": {
        "keywords": ["price trend", "price over time", "monthly price", "price history"],
        "sql": """
            SELECT DATE_TRUNC('month', date) AS month,
                   area_name,
                   ROUND(AVG(price_sqft)::numeric, 2) AS avg_price_sqft,
                   COUNT(*) AS tx_count
            FROM transactions
            WHERE area_name ILIKE $1
              AND date >= $2 AND date <= $3
            GROUP BY month, area_name
            ORDER BY month
        """,
        "params_builder": "_build_area_period_params",
    },
    "transaction_volume": {
        "keywords": ["transaction volume", "how many transactions", "transaction count", "number of deals"],
        "sql": """
            SELECT DATE_TRUNC('month', date) AS month,
                   area_name,
                   COUNT(*) AS transaction_count,
                   SUM(price) AS total_value
            FROM transactions
            WHERE ($1 = '%%' OR area_name ILIKE $1)
              AND date >= $2 AND date <= $3
            GROUP BY month, area_name
            ORDER BY month DESC
        """,
        "params_builder": "_build_area_period_params",
    },
    "top_areas_by_price": {
        "keywords": ["top areas", "most expensive", "highest price", "best performing"],
        "sql": """
            SELECT area_name,
                   ROUND(AVG(price_sqft)::numeric, 2) AS avg_price_sqft,
                   COUNT(*) AS tx_count
            FROM transactions
            WHERE date >= $1 AND date <= $2
            GROUP BY area_name
            HAVING COUNT(*) >= 5
            ORDER BY avg_price_sqft DESC
            LIMIT 15
        """,
        "params_builder": "_build_period_params",
    },
    "yoy_comparison": {
        "keywords": ["year over year", "yoy", "compared to last year", "year-on-year"],
        "sql": """
            WITH current_period AS (
                SELECT area_name,
                       ROUND(AVG(price_sqft)::numeric, 2) AS current_avg
                FROM transactions
                WHERE area_name ILIKE $1
                  AND date >= $2 AND date <= $3
                GROUP BY area_name
            ),
            previous_period AS (
                SELECT area_name,
                       ROUND(AVG(price_sqft)::numeric, 2) AS prev_avg
                FROM transactions
                WHERE area_name ILIKE $1
                  AND date >= $4 AND date <= $5
                GROUP BY area_name
            )
            SELECT c.area_name,
                   c.current_avg,
                   p.prev_avg,
                   ROUND(((c.current_avg - p.prev_avg) / NULLIF(p.prev_avg, 0) * 100)::numeric, 2)
                       AS yoy_change_pct
            FROM current_period c
            LEFT JOIN previous_period p ON c.area_name = p.area_name
        """,
        "params_builder": "_build_yoy_params",
    },
    "flip_analysis": {
        "keywords": ["flip", "resale", "transacted more than", "sold twice", "repeat sale"],
        "sql": """
            SELECT t.area_name,
                   t.project_name,
                   t.unit_type,
                   t.bedrooms,
                   COUNT(*) AS sale_count,
                   MIN(t.date) AS first_sale,
                   MAX(t.date) AS latest_sale,
                   MAX(t.price) - MIN(t.price) AS price_gain,
                   EXTRACT(DAY FROM MAX(t.date) - MIN(t.date)) AS days_between
            FROM transactions t
            WHERE ($1 = '%%' OR t.area_name ILIKE $1)
              AND t.bedrooms = COALESCE($2, t.bedrooms)
            GROUP BY t.area_name, t.project_name, t.unit_type, t.bedrooms
            HAVING COUNT(*) >= 2
            ORDER BY price_gain DESC
            LIMIT 20
        """,
        "params_builder": "_build_flip_params",
    },
}


# ── Parameter builders ───────────────────────────────────────────────────────


def _extract_area(query: str) -> str:
    """Extract area name from query (best-effort regex)."""
    known_areas = [
        "JVC", "Jumeirah Village Circle", "Downtown Dubai", "Dubai Marina",
        "Business Bay", "Dubai Hills", "Dubai Hills Estate", "Palm Jumeirah",
        "JBR", "Jumeirah Beach Residence", "Arabian Ranches", "DAMAC Hills",
        "Dubai Creek Harbour", "MBR City", "Al Barsha", "Sports City",
        "Motor City", "International City", "Discovery Gardens",
        "Jumeirah Lake Towers", "JLT", "Dubai South", "Town Square",
    ]
    query_lower = query.lower()
    for area in known_areas:
        if area.lower() in query_lower:
            return f"%{area}%"
    return "%"


def _extract_period(query: str) -> tuple[datetime, datetime]:
    """Extract time period from query (best-effort)."""
    now = datetime.now(tz=timezone.utc)
    query_lower = query.lower()

    if "last 6 months" in query_lower or "past 6 months" in query_lower:
        return (now - timedelta(days=180), now)
    if "last year" in query_lower or "past year" in query_lower:
        return (now - timedelta(days=365), now)
    if "last quarter" in query_lower:
        return (now - timedelta(days=90), now)
    if "last month" in query_lower:
        return (now - timedelta(days=30), now)

    # Check for specific years
    year_match = re.search(r'\b(20\d{2})\b', query)
    if year_match:
        year = int(year_match.group(1))
        return (datetime(year, 1, 1, tzinfo=timezone.utc), datetime(year, 12, 31, tzinfo=timezone.utc))

    # Default: last 12 months
    return (now - timedelta(days=365), now)


def _build_area_period_params(query: str) -> tuple:
    area = _extract_area(query)
    start, end = _extract_period(query)
    return (area, start, end)


def _build_period_params(query: str) -> tuple:
    start, end = _extract_period(query)
    return (start, end)


def _build_yoy_params(query: str) -> tuple:
    area = _extract_area(query)
    now = datetime.now(tz=timezone.utc)
    current_start = now - timedelta(days=365)
    current_end = now
    prev_start = now - timedelta(days=730)
    prev_end = now - timedelta(days=365)
    return (area, current_start, current_end, prev_start, prev_end)


def _build_flip_params(query: str) -> tuple:
    area = _extract_area(query)
    bedrooms_match = re.search(r'(\d+)\s*-?\s*bed', query.lower())
    bedrooms = int(bedrooms_match.group(1)) if bedrooms_match else None
    return (area, bedrooms)


PARAM_BUILDERS = {
    "_build_area_period_params": _build_area_period_params,
    "_build_period_params": _build_period_params,
    "_build_yoy_params": _build_yoy_params,
    "_build_flip_params": _build_flip_params,
}


# ── Template matcher ─────────────────────────────────────────────────────────


def _match_template(query: str) -> tuple[str, str, tuple] | None:
    """Match a query to a SQL template by keyword scoring."""
    query_lower = query.lower()
    best_match: tuple[str, int] | None = None

    for name, template in TEMPLATES.items():
        score = sum(1 for kw in template["keywords"] if kw in query_lower)
        if score > 0 and (best_match is None or score > best_match[1]):
            best_match = (name, score)

    if best_match is None:
        return None

    name = best_match[0]
    template = TEMPLATES[name]
    builder = PARAM_BUILDERS[template["params_builder"]]
    params = builder(query)
    return name, template["sql"], params


# ── Node entrypoint ──────────────────────────────────────────────────────────


async def run_sql_query(state: AgentState) -> AgentState:
    """Match the user query to a SQL template and execute it."""
    start = time.perf_counter()
    query = state["query"]

    match = _match_template(query)
    if match is None:
        logger.info("sql_query.no_template_match", query=query[:80])
        return {**state, "sql_results": []}

    template_name, sql, params = match
    logger.info("sql_query.matched", template=template_name)

    try:
        rows = await pg_client.fetch(sql, *params)
        # Convert datetime/date objects to strings for serialisation
        sql_results = []
        for row in rows:
            cleaned: dict = {}
            for k, v in row.items():
                if isinstance(v, datetime):
                    cleaned[k] = v.isoformat()
                else:
                    cleaned[k] = v
            sql_results.append(cleaned)

        elapsed = (time.perf_counter() - start) * 1000
        logger.info(
            "sql_query.complete",
            template=template_name,
            rows=len(sql_results),
            latency_ms=round(elapsed, 1),
        )
        return {**state, "sql_results": sql_results}

    except Exception:
        elapsed = (time.perf_counter() - start) * 1000
        logger.error(
            "sql_query.error",
            template=template_name,
            latency_ms=round(elapsed, 1),
            exc_info=True,
        )
        return {**state, "sql_results": []}
