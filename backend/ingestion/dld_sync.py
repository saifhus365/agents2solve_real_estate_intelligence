"""DLD (Dubai Land Department) data sync via Dubai Pulse API.

OAuth2 flow → paginated transaction fetch → PostgreSQL upsert → Neo4j area stats.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import structlog

from backend.config import get_settings
from backend.db.neo4j_client import neo4j_client
from backend.db.postgres_client import pg_client

logger = structlog.get_logger(__name__)

UAE_TZ = timezone(timedelta(hours=4))

# ── Token management ─────────────────────────────────────────────────────────

_token_cache: dict[str, str | float] = {}


async def _get_bearer_token(redis_client=None) -> str:  # noqa: ANN001
    """Obtain a DLD bearer token, caching in Redis (or in-memory fallback).

    Token has 30 min TTL; we refresh 5 min before expiry.
    """
    now = datetime.now(tz=UAE_TZ).timestamp()

    # Check in-memory cache first
    if _token_cache.get("token") and float(_token_cache.get("expires_at", 0)) > now + 300:
        return str(_token_cache["token"])

    # Check Redis cache
    if redis_client:
        cached = await redis_client.get("dld:access_token")
        if cached:
            return cached.decode() if isinstance(cached, bytes) else cached

    settings = get_settings()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            settings.dld_auth_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "client_id": settings.dld_client_id,
                "client_secret": settings.dld_client_secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    token = data["access_token"]
    expires_in = int(data.get("expires_in", 1800))

    # Cache in-memory
    _token_cache["token"] = token
    _token_cache["expires_at"] = now + expires_in

    # Cache in Redis with TTL
    if redis_client:
        await redis_client.set("dld:access_token", token, ex=expires_in - 300)

    logger.info("dld.token_acquired", expires_in=expires_in)
    return token


# ── Transaction fetch & upsert ───────────────────────────────────────────────


async def _fetch_transactions_page(
    token: str,
    offset: int = 0,
    limit: int = 1000,
) -> list[dict]:
    """Fetch one page of transactions from the DLD API."""
    settings = get_settings()
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            settings.dld_transactions_url,
            headers={"Authorization": f"Bearer {token}"},
            params={"offset": offset, "limit": limit},
        )
        resp.raise_for_status()
        return resp.json().get("data", resp.json().get("result", []))


def _map_transaction(raw: dict) -> tuple:
    """Map a raw DLD API record to a Postgres-ready tuple."""
    return (
        str(raw.get("transaction_id", raw.get("id", ""))),
        raw.get("transaction_date") or raw.get("date"),
        float(raw.get("amount", 0) or 0),
        float(raw.get("price_per_sqft", 0) or 0),
        raw.get("transaction_type", ""),
        raw.get("procedure_name", raw.get("procedure_type", "")),
        raw.get("area_name", ""),
        raw.get("project_name", ""),
        raw.get("developer_name", raw.get("master_developer", "")),
        int(raw["bedrooms"]) if raw.get("bedrooms") else None,
        float(raw["area_sqft"]) if raw.get("area_sqft") else None,
        raw.get("unit_type", raw.get("property_type", "")),
        json.dumps(raw),
    )


UPSERT_SQL = """
INSERT INTO transactions (
    transaction_id, date, price, price_sqft, transaction_type,
    procedure_type, area_name, project_name, developer_name,
    bedrooms, area_sqft, unit_type, raw_json
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (transaction_id) DO UPDATE SET
    price = EXCLUDED.price,
    price_sqft = EXCLUDED.price_sqft,
    area_name = EXCLUDED.area_name,
    updated_at = NOW()
"""


async def _upsert_batch_postgres(records: list[dict]) -> int:
    """Upsert a batch of raw DLD records into PostgreSQL."""
    rows = [_map_transaction(r) for r in records]
    await pg_client.executemany(UPSERT_SQL, rows)
    return len(rows)


# ── Neo4j area stats update ─────────────────────────────────────────────────


async def _update_area_stats() -> None:
    """Recompute area-level aggregate stats and upsert into Neo4j."""
    area_rows = await pg_client.fetch(
        """
        SELECT
            area_name,
            AVG(price_sqft)                              AS avg_price_sqft,
            COUNT(*)                                      AS tx_count,
            COUNT(*) FILTER (WHERE procedure_type ILIKE '%%off%%plan%%')::FLOAT
                / GREATEST(COUNT(*), 1)                   AS off_plan_ratio
        FROM transactions
        WHERE area_name IS NOT NULL AND area_name != ''
        GROUP BY area_name
        """
    )
    for row in area_rows:
        area_name = row["area_name"]
        area_id = area_name.lower().replace(" ", "_")

        # Compute YoY change
        yoy_row = await pg_client.fetchrow(
            """
            SELECT
                AVG(CASE WHEN date >= NOW() - INTERVAL '1 year' THEN price_sqft END) AS current_avg,
                AVG(CASE WHEN date >= NOW() - INTERVAL '2 years'
                         AND date < NOW() - INTERVAL '1 year' THEN price_sqft END) AS prev_avg
            FROM transactions
            WHERE area_name = $1
            """,
            area_name,
        )
        yoy_change = 0.0
        if yoy_row and yoy_row.get("prev_avg") and yoy_row["prev_avg"] > 0:
            yoy_change = (
                (yoy_row["current_avg"] - yoy_row["prev_avg"])
                / yoy_row["prev_avg"]
                * 100
            )

        await neo4j_client.upsert_node(
            "Area",
            area_id,
            {
                "name": area_name,
                "zone": "",
                "avg_price_sqft": round(row["avg_price_sqft"] or 0, 2),
                "yoy_price_change": round(yoy_change, 2),
                "off_plan_ratio": round(row["off_plan_ratio"] or 0, 4),
            },
        )
    logger.info("dld.area_stats_updated", areas=len(area_rows))


# ── Main sync entrypoint ────────────────────────────────────────────────────


async def run_dld_sync(
    full_load: bool = False,
    redis_client=None,  # noqa: ANN001
) -> dict:
    """Run the DLD sync: fetch transactions, upsert PG, update Neo4j areas.

    Args:
        full_load: If True, paginate through all historical data.
        redis_client: Optional Redis client for token caching.

    Returns:
        Summary dict with total_fetched, total_upserted.
    """
    token = await _get_bearer_token(redis_client)
    total_fetched = 0
    total_upserted = 0
    offset = 0
    limit = 1000

    while True:
        logger.info("dld.fetching_page", offset=offset, limit=limit)
        records = await _fetch_transactions_page(token, offset=offset, limit=limit)
        if not records:
            break

        total_fetched += len(records)
        upserted = await _upsert_batch_postgres(records)
        total_upserted += upserted
        logger.info("dld.batch_upserted", batch_size=upserted, total=total_upserted)

        if not full_load or len(records) < limit:
            break
        offset += limit

        # Refresh token proactively every 500 pages
        if offset % (500 * limit) == 0:
            token = await _get_bearer_token(redis_client)

    # Update Neo4j area statistics after all inserts
    await _update_area_stats()

    summary = {"total_fetched": total_fetched, "total_upserted": total_upserted}
    logger.info("dld.sync_complete", **summary)
    return summary
