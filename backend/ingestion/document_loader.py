"""Document loader: RERA PDFs + news RSS → chunk → embed → Pinecone."""

from __future__ import annotations

import hashlib
import io
import re
import uuid
from datetime import datetime, timezone
from typing import Generator

import feedparser
import httpx
import structlog
from PyPDF2 import PdfReader
from sentence_transformers import SentenceTransformer

from backend.config import get_settings
from backend.db.pinecone_client import pinecone_client

logger = structlog.get_logger(__name__)

# ── Embedding model (loaded lazily) ─────────────────────────────────────────

_embed_model: SentenceTransformer | None = None


def _get_embed_model() -> SentenceTransformer:
    global _embed_model  # noqa: PLW0603
    if _embed_model is None:
        settings = get_settings()
        _embed_model = SentenceTransformer(settings.embedding_model)
        logger.info("embedding_model.loaded", model=settings.embedding_model)
    return _embed_model


# ── Chunking ─────────────────────────────────────────────────────────────────


def _chunk_text(
    text: str,
    chunk_size: int = 512,
    overlap: int = 64,
) -> Generator[str, None, None]:
    """Split text into overlapping token-approximated chunks."""
    words = text.split()
    start = 0
    while start < len(words):
        end = start + chunk_size
        yield " ".join(words[start:end])
        start += chunk_size - overlap


def _clean_text(text: str) -> str:
    """Normalise whitespace and strip control characters."""
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ── PDF ingestion ────────────────────────────────────────────────────────────


async def ingest_pdf(
    pdf_url: str | None = None,
    pdf_bytes: bytes | None = None,
    source_name: str = "RERA Document",
) -> int:
    """Download (or accept raw bytes of) a PDF, chunk, embed, and upsert.

    Returns:
        Number of chunks upserted.
    """
    if pdf_bytes is None and pdf_url:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(pdf_url)
            resp.raise_for_status()
            pdf_bytes = resp.content

    if not pdf_bytes:
        logger.warning("document_loader.no_pdf_content")
        return 0

    reader = PdfReader(io.BytesIO(pdf_bytes))
    full_text = " ".join(
        _clean_text(page.extract_text() or "") for page in reader.pages
    )

    if not full_text.strip():
        logger.warning("document_loader.empty_pdf", source=source_name)
        return 0

    settings = get_settings()
    model = _get_embed_model()
    chunks = list(_chunk_text(full_text, settings.chunk_size, settings.chunk_overlap))

    vectors: list[dict] = []
    for i, chunk in enumerate(chunks):
        chunk_id = hashlib.md5(f"{source_name}:{i}:{chunk[:50]}".encode()).hexdigest()
        embedding = model.encode(chunk).tolist()
        vectors.append(
            {
                "id": chunk_id,
                "values": embedding,
                "metadata": {
                    "text": chunk,
                    "source": source_name,
                    "url": pdf_url or "",
                    "chunk_index": i,
                    "type": "pdf",
                    "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
                },
            }
        )

    pinecone_client.upsert_vectors(vectors)
    logger.info("document_loader.pdf_ingested", source=source_name, chunks=len(vectors))
    return len(vectors)


# ── RSS / news ingestion ────────────────────────────────────────────────────

DEFAULT_FEEDS = [
    "https://gulfnews.com/rss/business/property",
    "https://www.arabianbusiness.com/rss/real-estate",
    "https://www.zawya.com/mena/en/rss/real-estate",
]


async def ingest_rss_feeds(
    feed_urls: list[str] | None = None,
) -> int:
    """Parse RSS feeds, chunk articles, embed, and upsert to Pinecone.

    Returns:
        Total chunks upserted across all articles.
    """
    urls = feed_urls or DEFAULT_FEEDS
    settings = get_settings()
    model = _get_embed_model()
    total_chunks = 0

    for feed_url in urls:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(feed_url)
                resp.raise_for_status()
                feed_content = resp.text
        except Exception:
            logger.warning("document_loader.feed_fetch_failed", url=feed_url, exc_info=True)
            continue

        feed = feedparser.parse(feed_content)
        for entry in feed.entries:
            title = entry.get("title", "")
            summary = entry.get("summary", entry.get("description", ""))
            link = entry.get("link", "")
            published = entry.get("published", "")

            article_text = _clean_text(f"{title}. {summary}")
            if len(article_text) < 50:
                continue

            chunks = list(
                _chunk_text(article_text, settings.chunk_size, settings.chunk_overlap)
            )
            vectors: list[dict] = []
            for i, chunk in enumerate(chunks):
                chunk_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{link}:{i}"))
                embedding = model.encode(chunk).tolist()
                vectors.append(
                    {
                        "id": chunk_id,
                        "values": embedding,
                        "metadata": {
                            "text": chunk,
                            "source": feed_url,
                            "title": title,
                            "url": link,
                            "published_date": published,
                            "chunk_index": i,
                            "type": "news",
                            "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
                        },
                    }
                )

            if vectors:
                pinecone_client.upsert_vectors(vectors)
                total_chunks += len(vectors)

        logger.info(
            "document_loader.feed_ingested",
            feed=feed_url,
            articles=len(feed.entries),
        )

    logger.info("document_loader.rss_complete", total_chunks=total_chunks)
    return total_chunks
