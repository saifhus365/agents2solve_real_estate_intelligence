"""Neo4j graph schema — node labels, relationship types, and Cypher DDL."""

from __future__ import annotations

from dataclasses import dataclass, field

# ── Node label constants ─────────────────────────────────────────────────────

AREA = "Area"
SUB_AREA = "SubArea"
DEVELOPER = "Developer"
PROJECT = "Project"
UNIT = "Unit"
TRANSACTION = "Transaction"
METRO_STATION = "MetroStation"
TRAM_STOP = "TramStop"
NEWS_ARTICLE = "NewsArticle"
RERA_PERMIT = "RERAPermit"

ALL_LABELS: list[str] = [
    AREA, SUB_AREA, DEVELOPER, PROJECT, UNIT,
    TRANSACTION, METRO_STATION, TRAM_STOP, NEWS_ARTICLE, RERA_PERMIT,
]

# ── Relationship type constants ──────────────────────────────────────────────

REL_LAUNCHED = "LAUNCHED"
REL_LOCATED_IN = "LOCATED_IN"
REL_PART_OF = "PART_OF"
REL_INVOLVES = "INVOLVES"
REL_NEAR_STATION = "NEAR_STATION"
REL_NEAR_TRAM = "NEAR_TRAM"
REL_CONTAINS = "CONTAINS"
REL_MENTIONS = "MENTIONS"
REL_GRANTED_TO = "GRANTED_TO"
REL_FOR_PROJECT = "FOR_PROJECT"
REL_HAS_TRACK_RECORD = "HAS_TRACK_RECORD"

ALL_RELATIONSHIPS: list[str] = [
    REL_LAUNCHED, REL_LOCATED_IN, REL_PART_OF, REL_INVOLVES,
    REL_NEAR_STATION, REL_NEAR_TRAM, REL_CONTAINS, REL_MENTIONS,
    REL_GRANTED_TO, REL_FOR_PROJECT, REL_HAS_TRACK_RECORD,
]


# ── Node property specs (used for Cypher generation context) ─────────────────


@dataclass
class NodeSpec:
    """Describes a graph node label and its required properties."""

    label: str
    properties: list[str] = field(default_factory=list)


NODE_SPECS: list[NodeSpec] = [
    NodeSpec(AREA, ["id", "name", "zone", "avg_price_sqft", "yoy_price_change", "off_plan_ratio"]),
    NodeSpec(SUB_AREA, ["id", "name", "area_id"]),
    NodeSpec(DEVELOPER, ["id", "name", "tier", "on_time_delivery_rate"]),
    NodeSpec(PROJECT, ["id", "name", "status", "handover_date", "total_units", "off_plan"]),
    NodeSpec(UNIT, ["id", "type", "bedrooms", "area_sqft", "floor"]),
    NodeSpec(TRANSACTION, ["id", "date", "price", "price_sqft", "type", "procedure_type"]),
    NodeSpec(METRO_STATION, ["id", "name", "line", "lat", "lng"]),
    NodeSpec(TRAM_STOP, ["id", "name", "lat", "lng"]),
    NodeSpec(NEWS_ARTICLE, ["id", "title", "source", "published_date", "url", "chunk_id"]),
    NodeSpec(RERA_PERMIT, ["id", "permit_number", "issue_date", "status"]),
]


@dataclass
class RelSpec:
    """Describes a graph relationship with source/target labels and properties."""

    rel_type: str
    source_label: str
    target_label: str
    properties: list[str] = field(default_factory=list)


REL_SPECS: list[RelSpec] = [
    RelSpec(REL_LAUNCHED, DEVELOPER, PROJECT),
    RelSpec(REL_LOCATED_IN, PROJECT, AREA),
    RelSpec(REL_PART_OF, UNIT, PROJECT),
    RelSpec(REL_INVOLVES, TRANSACTION, UNIT),
    RelSpec(REL_NEAR_STATION, AREA, METRO_STATION, ["distance_m"]),
    RelSpec(REL_NEAR_TRAM, AREA, TRAM_STOP, ["distance_m"]),
    RelSpec(REL_CONTAINS, AREA, SUB_AREA),
    RelSpec(REL_MENTIONS, NEWS_ARTICLE, DEVELOPER),
    RelSpec(REL_MENTIONS, NEWS_ARTICLE, AREA),
    RelSpec(REL_GRANTED_TO, RERA_PERMIT, DEVELOPER),
    RelSpec(REL_FOR_PROJECT, RERA_PERMIT, PROJECT),
    RelSpec(REL_HAS_TRACK_RECORD, DEVELOPER, AREA, ["projects_delivered", "avg_delay_days"]),
]


# ── Cypher DDL for schema initialisation ─────────────────────────────────────

CONSTRAINT_STATEMENTS: list[str] = [
    f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE"
    for label in ALL_LABELS
]

INDEX_STATEMENTS: list[str] = [
    "CREATE INDEX IF NOT EXISTS FOR (a:Area) ON (a.name)",
    "CREATE INDEX IF NOT EXISTS FOR (d:Developer) ON (d.name)",
    "CREATE INDEX IF NOT EXISTS FOR (p:Project) ON (p.name)",
    "CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.date)",
    "CREATE INDEX IF NOT EXISTS FOR (m:MetroStation) ON (m.name)",
    "CREATE INDEX IF NOT EXISTS FOR (ts:TramStop) ON (ts.name)",
]


def get_schema_description() -> str:
    """Return a human-readable description of the full graph schema.

    This is injected into LLM prompts for Cypher generation.
    """
    lines: list[str] = ["## Neo4j Graph Schema\n", "### Node Labels\n"]
    for spec in NODE_SPECS:
        props = ", ".join(spec.properties)
        lines.append(f"- (:{spec.label} {{ {props} }})")

    lines.append("\n### Relationships\n")
    for spec in REL_SPECS:
        props_str = ""
        if spec.properties:
            props_str = " { " + ", ".join(f"{p}: ..." for p in spec.properties) + " }"
        lines.append(
            f"- (:{spec.source_label})-[:{spec.rel_type}{props_str}]->(:{spec.target_label})"
        )
    return "\n".join(lines)
