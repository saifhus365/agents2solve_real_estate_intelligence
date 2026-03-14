"""Pinecone serverless index wrapper for vector operations."""

from __future__ import annotations

from typing import Any

import structlog
from pinecone import Pinecone, ServerlessSpec

from backend.config import get_settings

logger = structlog.get_logger(__name__)


class PineconeClient:
    """Wrapper around the Pinecone SDK for the dubai-realestate index."""

    def __init__(self) -> None:
        self._index: Any = None
        self._pc: Pinecone | None = None

    async def connect(self) -> None:
        """Initialise the Pinecone client and ensure the index exists."""
        settings = get_settings()
        self._pc = Pinecone(api_key=settings.pinecone_api_key)

        existing_indexes = [idx.name for idx in self._pc.list_indexes()]
        if settings.pinecone_index not in existing_indexes:
            self._pc.create_index(
                name=settings.pinecone_index,
                dimension=settings.embedding_dim,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region=settings.pinecone_env,
                ),
            )
            logger.info(
                "pinecone.index_created",
                index=settings.pinecone_index,
                dim=settings.embedding_dim,
            )

        self._index = self._pc.Index(settings.pinecone_index)
        logger.info("pinecone.connected", index=settings.pinecone_index)

    async def close(self) -> None:
        """No persistent connection to close; reset references."""
        self._index = None
        self._pc = None
        logger.info("pinecone.closed")

    # ── Vector operations ────────────────────────────────────────────────

    def upsert_vectors(
        self,
        vectors: list[dict],
        namespace: str = "",
    ) -> None:
        """Upsert a batch of vectors.

        Each vector dict must contain:
            - id: str
            - values: list[float]
            - metadata: dict (optional)
        """
        assert self._index, "PineconeClient not connected"
        batch_size = 100
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i : i + batch_size]
            self._index.upsert(vectors=batch, namespace=namespace)
        logger.debug("pinecone.upserted", count=len(vectors), namespace=namespace)

    def query(
        self,
        vector: list[float],
        top_k: int = 15,
        namespace: str = "",
        filter_dict: dict | None = None,
        include_metadata: bool = True,
    ) -> list[dict]:
        """Query the index and return top_k matches."""
        assert self._index, "PineconeClient not connected"
        response = self._index.query(
            vector=vector,
            top_k=top_k,
            namespace=namespace,
            filter=filter_dict,
            include_metadata=include_metadata,
        )
        results = []
        for match in response.get("matches", []):
            results.append(
                {
                    "id": match["id"],
                    "score": match["score"],
                    "metadata": match.get("metadata", {}),
                }
            )
        logger.debug("pinecone.queried", top_k=top_k, results=len(results))
        return results

    async def healthcheck(self) -> bool:
        """Return True if the index is reachable."""
        try:
            assert self._index is not None
            self._index.describe_index_stats()
            return True
        except Exception:
            logger.warning("pinecone.healthcheck.failed", exc_info=True)
            return False


# Singleton instance
pinecone_client = PineconeClient()
