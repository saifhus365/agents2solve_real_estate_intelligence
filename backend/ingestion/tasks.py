"""Celery app and beat schedule for periodic data ingestion tasks."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab

from backend.config import get_settings

settings = get_settings()

celery_app = Celery(
    "dubai_copilot",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Dubai",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)


# ── Helper to run async functions inside Celery (sync) tasks ─────────────────


def _run_async(coro):  # noqa: ANN001, ANN202
    """Run an async coroutine inside a sync Celery task."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── Tasks ────────────────────────────────────────────────────────────────────


@celery_app.task(name="tasks.dld_daily_sync", bind=True, max_retries=3)
def dld_daily_sync(self) -> dict:  # noqa: ANN001
    """Daily DLD transaction sync at 02:00 UAE time.

    On first run (empty transactions table), performs a full historical load.
    Subsequent runs fetch only the latest batch.
    """
    from backend.db.neo4j_client import neo4j_client
    from backend.db.postgres_client import pg_client
    from backend.ingestion.dld_sync import run_dld_sync

    async def _inner() -> dict:
        await pg_client.connect()
        await neo4j_client.connect()
        try:
            # Check if this is a first run
            row = await pg_client.fetchrow("SELECT COUNT(*) AS cnt FROM transactions")
            is_first_run = (row or {}).get("cnt", 0) == 0
            return await run_dld_sync(full_load=is_first_run)
        finally:
            await neo4j_client.close()
            await pg_client.close()

    try:
        return _run_async(_inner())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@celery_app.task(name="tasks.document_reingestion", bind=True, max_retries=2)
def document_reingestion(self) -> dict:  # noqa: ANN001
    """Weekly re-ingestion of news RSS feeds into Pinecone."""
    from backend.db.pinecone_client import pinecone_client
    from backend.ingestion.document_loader import ingest_rss_feeds

    async def _inner() -> dict:
        await pinecone_client.connect()
        try:
            total = await ingest_rss_feeds()
            return {"total_chunks": total}
        finally:
            await pinecone_client.close()

    try:
        return _run_async(_inner())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=120 * (self.request.retries + 1))


@celery_app.task(name="tasks.rebuild_graph", bind=True, max_retries=1)
def rebuild_graph(self) -> dict:  # noqa: ANN001
    """On-demand full graph rebuild from PostgreSQL data."""
    from backend.db.neo4j_client import neo4j_client
    from backend.db.postgres_client import pg_client
    from backend.ingestion.graph_builder import run_full_graph_build

    async def _inner() -> dict:
        await pg_client.connect()
        await neo4j_client.connect()
        try:
            return await run_full_graph_build()
        finally:
            await neo4j_client.close()
            await pg_client.close()

    try:
        return _run_async(_inner())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=300)


# ── Beat schedule ────────────────────────────────────────────────────────────

celery_app.conf.beat_schedule = {
    "dld-daily-sync": {
        "task": "tasks.dld_daily_sync",
        "schedule": crontab(hour=2, minute=0),  # 02:00 Asia/Dubai
        "options": {"queue": "ingestion"},
    },
    "document-weekly-reingestion": {
        "task": "tasks.document_reingestion",
        "schedule": timedelta(days=7),
        "options": {"queue": "ingestion"},
    },
}
