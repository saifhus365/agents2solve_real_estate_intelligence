"""CSV-based data loader for demo / offline mode.

Reads the publicly downloadable DLD CSV files (transactions, projects,
developers, units, residential_sale_index) and loads them into
PostgreSQL + Neo4j — no API credentials required.

Usage:
    python -m backend.ingestion.csv_loader          # load all CSVs
    python -m backend.ingestion.csv_loader --limit 5000  # load first 5000 rows
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import structlog

from backend.config import get_settings
from backend.db.neo4j_client import neo4j_client
from backend.db.postgres_client import pg_client
from backend.ingestion.graph_builder import (
    create_proximity_relationships,
    initialise_schema,
    seed_metro_stations,
    seed_tram_stops,
)

logger = structlog.get_logger(__name__)

# ── Column mapping helpers ───────────────────────────────────────────────────
# The DLD CSVs can have varying column names; these mappings cover known variants


def _find_col(row: dict, candidates: list[str], default: str = "") -> str:
    """Return the value of the first matching column name."""
    for c in candidates:
        if c in row and row[c]:
            return str(row[c]).strip()
    return default


def _safe_float(val: str | None, default: float = 0.0) -> float:
    """Parse a float, returning default on failure."""
    if not val:
        return default
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return default


def _safe_int(val: str | None) -> int | None:
    """Parse an int, returning None on failure."""
    if not val:
        return None
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return None


def _parse_date(val: str | None) -> str | None:
    """Best-effort date parsing, returns ISO format or None."""
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).isoformat()
        except ValueError:
            continue
    return val.strip()


# ── PostgreSQL table DDL for extra CSV data ──────────────────────────────────

PROJECTS_DDL = """
CREATE TABLE IF NOT EXISTS projects (
    project_id      TEXT PRIMARY KEY,
    project_name    TEXT,
    developer_name  TEXT,
    area_name       TEXT,
    status          TEXT,
    total_units     INT,
    off_plan        BOOLEAN DEFAULT FALSE,
    raw_json        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
)
"""

DEVELOPERS_DDL = """
CREATE TABLE IF NOT EXISTS developers (
    developer_id    TEXT PRIMARY KEY,
    developer_name  TEXT,
    tier            TEXT DEFAULT 'standard',
    total_projects  INT DEFAULT 0,
    raw_json        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
)
"""

UNITS_DDL = """
CREATE TABLE IF NOT EXISTS units_freehold (
    unit_id         TEXT PRIMARY KEY,
    project_name    TEXT,
    area_name       TEXT,
    unit_type       TEXT,
    bedrooms        INT,
    area_sqft       DOUBLE PRECISION,
    floor_no        INT,
    raw_json        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
)
"""


# ── CSV readers ──────────────────────────────────────────────────────────────


def _read_csv(filepath: Path, limit: int | None = None) -> list[dict]:
    """Read a CSV and return rows as dicts. Handles BOM and encoding issues."""
    rows: list[dict] = []
    try:
        with open(filepath, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                rows.append(row)
    except UnicodeDecodeError:
        with open(filepath, encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                rows.append(row)
    return rows


# ── Transactions loader ─────────────────────────────────────────────────────

UPSERT_TX_SQL = """
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


async def load_transactions_csv(filepath: Path, limit: int | None = None) -> int:
    """Load transactions.csv into PostgreSQL."""
    rows = _read_csv(filepath, limit)
    if not rows:
        logger.warning("csv_loader.no_transaction_rows", path=str(filepath))
        return 0

    logger.info("csv_loader.transactions_read", rows=len(rows))
    batch_size = 500
    total = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        tuples = []
        for row in batch:
            tx_id = _find_col(row, [
                "Transaction Number", "transaction_id", "TRANSACTION_NUMBER",
                "Trans Group No.", "id",
            ])
            if not tx_id:
                tx_id = hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()

            date_str = _find_col(row, [
                "Instance Date", "transaction_date", "INSTANCE_DATE", "date",
                "Transaction Date",
            ])
            price = _safe_float(_find_col(row, [
                "Trans Value", "amount", "TRANS_VALUE", "price", "Actual Worth",
            ]))
            price_sqft = _safe_float(_find_col(row, [
                "Meter Sale Price", "price_per_sqft", "PRICE_PER_SQFT",
                "price_sqft",
            ]))
            tx_type = _find_col(row, [
                "Trans Group", "transaction_type", "TRANS_GROUP", "Group",
            ])
            procedure = _find_col(row, [
                "Procedure Name", "procedure_type", "procedure_name",
                "PROCEDURE_NAME",
            ])
            area = _find_col(row, [
                "Area", "area_name", "AREA_EN", "Area Name",
            ])
            project = _find_col(row, [
                "Project", "project_name", "PROJECT_EN", "Project Name",
            ])
            developer = _find_col(row, [
                "Master Developer", "developer_name", "MASTER_DEVELOPER",
                "Developer",
            ])
            bedrooms = _safe_int(_find_col(row, [
                "No. of Rooms", "bedrooms", "ROOMS_EN", "Rooms",
            ]) or None)
            sqft = _safe_float(_find_col(row, [
                "Procedure Area", "area_sqft", "PROCEDURE_AREA",
                "Area (sq.ft)",
            ]))
            unit_type = _find_col(row, [
                "Property Type", "unit_type", "PROPERTY_TYPE_EN",
                "Usage", "Property Usage",
            ])

            tuples.append((
                tx_id, _parse_date(date_str), price, price_sqft,
                tx_type, procedure, area, project, developer,
                bedrooms, sqft if sqft > 0 else None, unit_type,
                json.dumps(row),
            ))

        await pg_client.executemany(UPSERT_TX_SQL, tuples)
        total += len(tuples)
        logger.info("csv_loader.tx_batch", loaded=total, of=len(rows))

    logger.info("csv_loader.transactions_done", total=total)
    return total


# ── Projects loader ──────────────────────────────────────────────────────────


async def load_projects_csv(filepath: Path, limit: int | None = None) -> int:
    """Load projects.csv into PostgreSQL + Neo4j."""
    await pg_client.execute(PROJECTS_DDL)
    rows = _read_csv(filepath, limit)
    if not rows:
        return 0

    total = 0
    for row in rows:
        proj_name = _find_col(row, ["Project Name", "project_name", "PROJECT_EN"])
        if not proj_name:
            continue

        proj_id = proj_name.lower().replace(" ", "_")
        dev_name = _find_col(row, ["Developer", "developer_name", "DEVELOPER_EN", "Master Developer"])
        area_name = _find_col(row, ["Area", "area_name", "AREA_EN"])
        status = _find_col(row, ["Status", "status", "PROJECT_STATUS"])
        total_units = _safe_int(_find_col(row, ["Total Units", "total_units"])) or 0

        # PostgreSQL
        await pg_client.execute(
            """
            INSERT INTO projects (project_id, project_name, developer_name, area_name, status, total_units, raw_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ON CONFLICT (project_id) DO UPDATE SET
                developer_name = EXCLUDED.developer_name,
                area_name = EXCLUDED.area_name,
                status = EXCLUDED.status,
                total_units = EXCLUDED.total_units
            """,
            proj_id, proj_name, dev_name, area_name, status, total_units,
            json.dumps(row),
        )

        # Neo4j — Project node
        await neo4j_client.upsert_node("Project", proj_id, {
            "name": proj_name,
            "status": status or "unknown",
            "handover_date": _find_col(row, ["Handover Date", "handover_date"]),
            "total_units": total_units,
            "off_plan": status.lower() in ("off-plan", "off plan", "under construction")
            if status else False,
        })

        # Developer → Project
        if dev_name:
            dev_id = dev_name.lower().replace(" ", "_")
            await neo4j_client.upsert_node("Developer", dev_id, {
                "name": dev_name, "tier": "standard", "on_time_delivery_rate": 0.0,
            })
            await neo4j_client.create_relationship(
                "Developer", dev_id, "LAUNCHED", "Project", proj_id,
            )

        # Project → Area
        if area_name:
            area_id = area_name.lower().replace(" ", "_")
            await neo4j_client.upsert_node("Area", area_id, {
                "name": area_name, "zone": "", "avg_price_sqft": 0,
                "yoy_price_change": 0, "off_plan_ratio": 0,
            })
            await neo4j_client.create_relationship(
                "Project", proj_id, "LOCATED_IN", "Area", area_id,
            )

        total += 1

    logger.info("csv_loader.projects_done", total=total)
    return total


# ── Developers loader ────────────────────────────────────────────────────────


async def load_developers_csv(filepath: Path, limit: int | None = None) -> int:
    """Load developers.csv into PostgreSQL + Neo4j."""
    await pg_client.execute(DEVELOPERS_DDL)
    rows = _read_csv(filepath, limit)
    if not rows:
        return 0

    total = 0
    for row in rows:
        dev_name = _find_col(row, [
            "Developer Name", "developer_name", "DEVELOPER_EN", "Developer",
        ])
        if not dev_name:
            continue

        dev_id = dev_name.lower().replace(" ", "_")
        total_projects = _safe_int(_find_col(row, ["Total Projects", "total_projects"])) or 0

        # PostgreSQL
        await pg_client.execute(
            """
            INSERT INTO developers (developer_id, developer_name, total_projects, raw_json)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (developer_id) DO UPDATE SET
                total_projects = EXCLUDED.total_projects
            """,
            dev_id, dev_name, total_projects, json.dumps(row),
        )

        # Neo4j
        await neo4j_client.upsert_node("Developer", dev_id, {
            "name": dev_name,
            "tier": "standard",
            "on_time_delivery_rate": 0.0,
        })
        total += 1

    logger.info("csv_loader.developers_done", total=total)
    return total


# ── Area stats recomputation ─────────────────────────────────────────────────


async def update_area_stats_from_pg() -> None:
    """Recompute area-level stats from PostgreSQL and push to Neo4j."""
    area_rows = await pg_client.fetch(
        """
        SELECT
            area_name,
            AVG(price_sqft) AS avg_price_sqft,
            COUNT(*) AS tx_count,
            COUNT(*) FILTER (WHERE procedure_type ILIKE '%%off%%plan%%')::FLOAT
                / GREATEST(COUNT(*), 1) AS off_plan_ratio
        FROM transactions
        WHERE area_name IS NOT NULL AND area_name != ''
        GROUP BY area_name
        """
    )
    for row in area_rows:
        area_name = row["area_name"]
        area_id = area_name.lower().replace(" ", "_")

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

        await neo4j_client.upsert_node("Area", area_id, {
            "name": area_name,
            "zone": "",
            "avg_price_sqft": round(row["avg_price_sqft"] or 0, 2),
            "yoy_price_change": round(yoy_change, 2),
            "off_plan_ratio": round(row["off_plan_ratio"] or 0, 4),
        })

    logger.info("csv_loader.area_stats_updated", areas=len(area_rows))


# ── Main orchestrator ────────────────────────────────────────────────────────


async def run_csv_load(data_dir: str | None = None, limit: int | None = None) -> dict:
    """Load all available CSVs into PostgreSQL and Neo4j.

    Args:
        data_dir: Path to directory with CSV files. Defaults to settings.data_dir.
        limit: Max rows per CSV (for fast demo loading). None = load all.

    Returns:
        Summary dict with counts.
    """
    settings = get_settings()
    base = Path(data_dir or settings.data_dir)

    if not base.exists():
        logger.error("csv_loader.data_dir_not_found", path=str(base))
        return {"error": f"Data directory not found: {base}"}

    # Connect
    await pg_client.connect()
    await neo4j_client.connect()

    try:
        # Initialise Neo4j schema
        await initialise_schema()
        await seed_metro_stations()
        await seed_tram_stops()

        summary: dict[str, int] = {}

        # Developers first (so we have nodes to link)
        dev_path = base / "developers.csv"
        if dev_path.exists():
            summary["developers"] = await load_developers_csv(dev_path, limit)

        # Projects (links to developers + areas)
        proj_path = base / "projects.csv"
        if proj_path.exists():
            summary["projects"] = await load_projects_csv(proj_path, limit)

        # Transactions (the big one)
        tx_path = base / "transactions.csv"
        if tx_path.exists():
            summary["transactions"] = await load_transactions_csv(tx_path, limit)

        # Update area stats from loaded transactions
        await update_area_stats_from_pg()

        # Build proximity relationships
        await create_proximity_relationships()

        logger.info("csv_loader.complete", **summary)
        return summary

    finally:
        await neo4j_client.close()
        await pg_client.close()


# ── CLI entrypoint ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load DLD CSV data into PostgreSQL + Neo4j")
    parser.add_argument("--data-dir", default=None, help="Path to CSV files directory")
    parser.add_argument("--limit", type=int, default=None, help="Max rows per CSV (for fast demo)")
    args = parser.parse_args()

    result = asyncio.run(run_csv_load(data_dir=args.data_dir, limit=args.limit))
    print(f"\n✅ CSV load complete: {json.dumps(result, indent=2)}")
