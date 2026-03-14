"""Async Neo4j driver wrapper with connection management and helpers."""

from __future__ import annotations

import structlog
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession

from backend.config import get_settings

logger = structlog.get_logger(__name__)


class Neo4jClient:
    """Thin async wrapper around the official Neo4j async driver."""

    def __init__(self) -> None:
        self._driver: AsyncDriver | None = None

    async def connect(self) -> None:
        """Initialise the async driver."""
        settings = get_settings()
        self._driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        logger.info("neo4j.connected", uri=settings.neo4j_uri)

    async def close(self) -> None:
        """Gracefully close the driver."""
        if self._driver:
            await self._driver.close()
            logger.info("neo4j.closed")

    def _session(self) -> AsyncSession:
        assert self._driver, "Neo4jClient not connected — call connect() first"
        return self._driver.session()

    # ── Query helpers ────────────────────────────────────────────────────

    async def execute_cypher(
        self,
        query: str,
        parameters: dict | None = None,
    ) -> list[dict]:
        """Run a Cypher query and return all records as dicts."""
        async with self._session() as session:
            result = await session.run(query, parameters or {})
            records = [record.data() async for record in result]
            logger.debug("neo4j.query", query=query[:120], rows=len(records))
            return records

    async def upsert_node(
        self,
        label: str,
        node_id: str,
        properties: dict,
    ) -> None:
        """MERGE a node by id and SET its properties."""
        props_set = ", ".join(f"n.{k} = ${k}" for k in properties)
        cypher = f"MERGE (n:{label} {{id: $id}}) SET {props_set}"
        params = {"id": node_id, **properties}
        async with self._session() as session:
            await session.run(cypher, params)

    async def create_relationship(
        self,
        source_label: str,
        source_id: str,
        rel_type: str,
        target_label: str,
        target_id: str,
        properties: dict | None = None,
    ) -> None:
        """MERGE a relationship between two nodes identified by id."""
        props_str = ""
        params: dict = {"src_id": source_id, "tgt_id": target_id}
        if properties:
            props_str = " SET " + ", ".join(f"r.{k} = ${k}" for k in properties)
            params.update(properties)
        cypher = (
            f"MATCH (a:{source_label} {{id: $src_id}}), (b:{target_label} {{id: $tgt_id}}) "
            f"MERGE (a)-[r:{rel_type}]->(b){props_str}"
        )
        async with self._session() as session:
            await session.run(cypher, params)

    async def run_ddl(self, statements: list[str]) -> None:
        """Execute a list of DDL statements (constraints, indexes)."""
        async with self._session() as session:
            for stmt in statements:
                await session.run(stmt)
                logger.debug("neo4j.ddl", statement=stmt[:100])

    async def healthcheck(self) -> bool:
        """Return True if the database is reachable."""
        try:
            await self.execute_cypher("RETURN 1 AS ok")
            return True
        except Exception:
            logger.warning("neo4j.healthcheck.failed", exc_info=True)
            return False


# Singleton instance
neo4j_client = Neo4jClient()
