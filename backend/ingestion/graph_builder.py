"""Build the full Neo4j knowledge graph from PostgreSQL transaction data.

Creates constraints/indexes, then bulk-loads Area, Developer, Project,
Unit, Transaction nodes and all relationships.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog

from backend.db.neo4j_client import neo4j_client
from backend.db.postgres_client import pg_client
from backend.models.graph_schema import CONSTRAINT_STATEMENTS, INDEX_STATEMENTS

logger = structlog.get_logger(__name__)


# ── Static seed data ────────────────────────────────────────────────────────

METRO_STATIONS = [
    {"id": "ms_red_01", "name": "Rashidiya", "line": "Red", "lat": 25.2317, "lng": 55.3937},
    {"id": "ms_red_02", "name": "UAE Exchange", "line": "Red", "lat": 25.0657, "lng": 55.1197},
    {"id": "ms_red_03", "name": "Burj Khalifa/Dubai Mall", "line": "Red", "lat": 25.2000, "lng": 55.2708},
    {"id": "ms_red_04", "name": "DMCC", "line": "Red", "lat": 25.0700, "lng": 55.1400},
    {"id": "ms_red_05", "name": "Nakheel", "line": "Red", "lat": 25.0900, "lng": 55.1350},
    {"id": "ms_red_06", "name": "Mall of the Emirates", "line": "Red", "lat": 25.1181, "lng": 55.2006},
    {"id": "ms_green_01", "name": "Creek", "line": "Green", "lat": 25.2600, "lng": 55.3300},
    {"id": "ms_green_02", "name": "Healthcare City", "line": "Green", "lat": 25.2340, "lng": 55.3180},
    {"id": "ms_green_03", "name": "Stadium", "line": "Green", "lat": 25.2280, "lng": 55.3100},
    {"id": "ms_green_04", "name": "Bur Juman", "line": "Green", "lat": 25.2529, "lng": 55.3022},
]

TRAM_STOPS = [
    {"id": "ts_01", "name": "Dubai Marina Mall", "lat": 25.0771, "lng": 55.1392},
    {"id": "ts_02", "name": "Marina Towers", "lat": 25.0755, "lng": 55.1370},
    {"id": "ts_03", "name": "JBR 1", "lat": 25.0800, "lng": 55.1340},
    {"id": "ts_04", "name": "JBR 2", "lat": 25.0830, "lng": 55.1310},
    {"id": "ts_05", "name": "Media City", "lat": 25.0920, "lng": 55.1540},
    {"id": "ts_06", "name": "Knowledge Village", "lat": 25.0990, "lng": 55.1610},
]


# ── Schema initialisation ───────────────────────────────────────────────────


async def initialise_schema() -> None:
    """Create all uniqueness constraints and indexes."""
    await neo4j_client.run_ddl(CONSTRAINT_STATEMENTS + INDEX_STATEMENTS)
    logger.info("graph_builder.schema_initialised")


# ── Seed static nodes ───────────────────────────────────────────────────────


async def seed_metro_stations() -> None:
    """Upsert all metro station nodes."""
    for station in METRO_STATIONS:
        await neo4j_client.upsert_node("MetroStation", station["id"], station)
    logger.info("graph_builder.metro_stations_seeded", count=len(METRO_STATIONS))


async def seed_tram_stops() -> None:
    """Upsert all tram stop nodes."""
    for stop in TRAM_STOPS:
        await neo4j_client.upsert_node("TramStop", stop["id"], stop)
    logger.info("graph_builder.tram_stops_seeded", count=len(TRAM_STOPS))


# ── Build graph from transactions ────────────────────────────────────────────


async def build_graph_from_transactions(batch_size: int = 500) -> dict:
    """Read transactions from PostgreSQL and build the full Neo4j graph.

    Creates Area, Developer, Project, Unit, Transaction nodes and
    all inter-node relationships.

    Returns:
        Summary dict with counts per node type created/updated.
    """
    counts = {
        "areas": 0, "developers": 0, "projects": 0,
        "units": 0, "transactions": 0, "relationships": 0,
    }
    seen_areas: set[str] = set()
    seen_developers: set[str] = set()
    seen_projects: set[str] = set()

    offset = 0
    while True:
        rows = await pg_client.fetch(
            "SELECT * FROM transactions ORDER BY date LIMIT $1 OFFSET $2",
            batch_size,
            offset,
        )
        if not rows:
            break

        for row in rows:
            area_name = row.get("area_name", "")
            dev_name = row.get("developer_name", "")
            project_name = row.get("project_name", "")
            tx_id = row["transaction_id"]

            # ── Area node
            if area_name and area_name not in seen_areas:
                area_id = area_name.lower().replace(" ", "_")
                await neo4j_client.upsert_node("Area", area_id, {
                    "name": area_name, "zone": "", "avg_price_sqft": 0,
                    "yoy_price_change": 0, "off_plan_ratio": 0,
                })
                seen_areas.add(area_name)
                counts["areas"] += 1

            # ── Developer node
            if dev_name and dev_name not in seen_developers:
                dev_id = dev_name.lower().replace(" ", "_")
                await neo4j_client.upsert_node("Developer", dev_id, {
                    "name": dev_name, "tier": "standard", "on_time_delivery_rate": 0.0,
                })
                seen_developers.add(dev_name)
                counts["developers"] += 1

            # ── Project node
            if project_name and project_name not in seen_projects:
                proj_id = project_name.lower().replace(" ", "_")
                await neo4j_client.upsert_node("Project", proj_id, {
                    "name": project_name, "status": "unknown",
                    "handover_date": "", "total_units": 0, "off_plan": False,
                })
                seen_projects.add(project_name)
                counts["projects"] += 1

                # Developer -[:LAUNCHED]-> Project
                if dev_name:
                    dev_id = dev_name.lower().replace(" ", "_")
                    await neo4j_client.create_relationship(
                        "Developer", dev_id, "LAUNCHED", "Project", proj_id,
                    )
                    counts["relationships"] += 1

                # Project -[:LOCATED_IN]-> Area
                if area_name:
                    area_id = area_name.lower().replace(" ", "_")
                    await neo4j_client.create_relationship(
                        "Project", proj_id, "LOCATED_IN", "Area", area_id,
                    )
                    counts["relationships"] += 1

            # ── Unit node
            unit_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{project_name}:{tx_id}"))
            await neo4j_client.upsert_node("Unit", unit_id, {
                "type": row.get("unit_type", ""),
                "bedrooms": row.get("bedrooms") or 0,
                "area_sqft": row.get("area_sqft") or 0,
                "floor": 0,
            })
            counts["units"] += 1

            # Unit -[:PART_OF]-> Project
            if project_name:
                proj_id = project_name.lower().replace(" ", "_")
                await neo4j_client.create_relationship(
                    "Unit", unit_id, "PART_OF", "Project", proj_id,
                )
                counts["relationships"] += 1

            # ── Transaction node
            tx_date = row.get("date")
            if isinstance(tx_date, datetime):
                tx_date_str = tx_date.isoformat()
            else:
                tx_date_str = str(tx_date) if tx_date else ""

            await neo4j_client.upsert_node("Transaction", tx_id, {
                "date": tx_date_str,
                "price": row.get("price", 0),
                "price_sqft": row.get("price_sqft", 0),
                "type": row.get("transaction_type", ""),
                "procedure_type": row.get("procedure_type", ""),
            })
            counts["transactions"] += 1

            # Transaction -[:INVOLVES]-> Unit
            await neo4j_client.create_relationship(
                "Transaction", tx_id, "INVOLVES", "Unit", unit_id,
            )
            counts["relationships"] += 1

        offset += batch_size
        logger.info("graph_builder.batch_processed", offset=offset, counts=counts)

    logger.info("graph_builder.complete", **counts)
    return counts


# ── Proximity relationships ──────────────────────────────────────────────────

AREA_STATION_PROXIMITY = {
    "dubai_marina": [("ms_red_04", 400), ("ms_red_05", 600)],
    "jumeirah_lake_towers": [("ms_red_04", 500)],
    "downtown_dubai": [("ms_red_03", 200)],
    "al_quoz": [("ms_red_06", 700)],
    "bur_dubai": [("ms_green_04", 300)],
    "healthcare_city": [("ms_green_02", 250)],
    "dubai_hills_estate": [],
    "jumeirah_village_circle": [],
    "business_bay": [("ms_red_03", 800)],
}

AREA_TRAM_PROXIMITY = {
    "dubai_marina": [("ts_01", 200), ("ts_02", 350)],
    "jumeirah_beach_residence": [("ts_03", 150), ("ts_04", 300)],
    "media_city": [("ts_05", 250)],
    "knowledge_village": [("ts_06", 200)],
}


async def create_proximity_relationships() -> None:
    """Create NEAR_STATION and NEAR_TRAM relationships."""
    for area_id, stations in AREA_STATION_PROXIMITY.items():
        for station_id, distance in stations:
            await neo4j_client.create_relationship(
                "Area", area_id, "NEAR_STATION", "MetroStation", station_id,
                properties={"distance_m": float(distance)},
            )

    for area_id, stops in AREA_TRAM_PROXIMITY.items():
        for stop_id, distance in stops:
            await neo4j_client.create_relationship(
                "Area", area_id, "NEAR_TRAM", "TramStop", stop_id,
                properties={"distance_m": float(distance)},
            )

    logger.info("graph_builder.proximity_relationships_created")


# ── Full build orchestrator ──────────────────────────────────────────────────


async def run_full_graph_build() -> dict:
    """Execute the complete graph build pipeline."""
    await initialise_schema()
    await seed_metro_stations()
    await seed_tram_stops()
    counts = await build_graph_from_transactions()
    await create_proximity_relationships()
    return counts
