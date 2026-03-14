"""Async PostgreSQL client using asyncpg with connection pooling."""

from __future__ import annotations

from typing import Any

import asyncpg
import structlog

from backend.config import get_settings

logger = structlog.get_logger(__name__)


class PostgresClient:
    """Thin wrapper around an asyncpg connection pool."""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Create the connection pool and ensure core tables exist."""
        settings = get_settings()
        self._pool = await asyncpg.create_pool(
            dsn=settings.asyncpg_dsn,
            min_size=2,
            max_size=10,
        )
        logger.info("postgres.connected")
        await self._ensure_tables()

    async def close(self) -> None:
        """Close the pool."""
        if self._pool:
            await self._pool.close()
            logger.info("postgres.closed")

    # ── Query helpers ────────────────────────────────────────────────────

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        """Execute a SELECT and return rows as dicts."""
        assert self._pool, "PostgresClient not connected"
        rows = await self._pool.fetch(query, *args)
        return [dict(r) for r in rows]

    async def fetchrow(
        self,
        query: str,
        *args: Any,
    ) -> dict | None:
        """Execute a SELECT and return the first row as dict."""
        assert self._pool, "PostgresClient not connected"
        row = await self._pool.fetchrow(query, *args)
        return dict(row) if row else None

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """Execute a DML statement (INSERT / UPDATE / DELETE)."""
        assert self._pool, "PostgresClient not connected"
        return await self._pool.execute(query, *args)

    async def executemany(
        self,
        query: str,
        args: list[tuple],
    ) -> None:
        """Execute a statement for each set of parameters."""
        assert self._pool, "PostgresClient not connected"
        await self._pool.executemany(query, args)

    # ── Health ───────────────────────────────────────────────────────────

    async def healthcheck(self) -> bool:
        """Return True if the database is reachable."""
        try:
            row = await self.fetchrow("SELECT 1 AS ok")
            return row is not None
        except Exception:
            logger.warning("postgres.healthcheck.failed", exc_info=True)
            return False

    # ── Schema bootstrap ─────────────────────────────────────────────────

    async def _ensure_tables(self) -> None:
        """Create core tables if they don't exist."""
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id  TEXT PRIMARY KEY,
                date            TIMESTAMPTZ NOT NULL,
                price           DOUBLE PRECISION NOT NULL,
                price_sqft      DOUBLE PRECISION,
                transaction_type TEXT,
                procedure_type  TEXT,
                area_name       TEXT,
                project_name    TEXT,
                developer_name  TEXT,
                bedrooms        INT,
                area_sqft       DOUBLE PRECISION,
                unit_type       TEXT,
                raw_json        JSONB,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS chat_logs (
                id              SERIAL PRIMARY KEY,
                session_id      TEXT NOT NULL,
                query           TEXT NOT NULL,
                query_type      TEXT,
                answer          TEXT,
                citations       JSONB,
                latency_ms      DOUBLE PRECISION,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS eval_logs (
                id              SERIAL PRIMARY KEY,
                run_name        TEXT NOT NULL,
                question_id     TEXT,
                question        TEXT,
                expected_answer TEXT,
                actual_answer   TEXT,
                metrics         JSONB,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_date
            ON transactions (date)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_area
            ON transactions (area_name)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_chat_logs_session
            ON chat_logs (session_id)
            """,
        ]
        assert self._pool, "PostgresClient not connected"
        async with self._pool.acquire() as conn:
            for stmt in ddl:
                await conn.execute(stmt)
        logger.info("postgres.tables_ensured")


# Singleton instance
pg_client = PostgresClient()
