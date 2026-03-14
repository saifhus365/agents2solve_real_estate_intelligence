"""Pydantic v2 models for API request/response payloads and internal DTOs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ── API payloads ─────────────────────────────────────────────────────────────


class ChatRequest(BaseModel):
    """Incoming chat request."""

    query: str = Field(..., min_length=1, max_length=2000)
    session_id: str = Field(..., min_length=1)


class Citation(BaseModel):
    """Single citation attached to an answer claim."""

    source_name: str
    url: str | None = None
    source_type: Literal["graph", "vector", "sql"] = "vector"
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)


class SSEEvent(BaseModel):
    """Server-Sent Event payload."""

    type: Literal["token", "citations", "done", "error"]
    content: str | None = None
    data: list[Citation] | None = None
    latency_ms: float | None = None
    message: str | None = None


class ChatResponse(BaseModel):
    """Aggregated chat response (non-streaming variant)."""

    answer: str
    citations: list[Citation] = Field(default_factory=list)
    query_type: str = ""
    latency_ms: float = 0.0


# ── Eval payloads ────────────────────────────────────────────────────────────


class EvalRequest(BaseModel):
    """Trigger an evaluation run."""

    ablation: bool = False
    run_name: str | None = None


class EvalMetrics(BaseModel):
    """Metrics produced by a single evaluation run."""

    answer_faithfulness: float = 0.0
    retrieval_recall_at_10: float = 0.0
    cypher_accuracy: float = 0.0
    multihop_accuracy: float = 0.0
    hallucination_rate: float = 0.0
    p95_latency_ms: float = 0.0


class EvalResult(BaseModel):
    """Full result of one evaluation run."""

    run_name: str
    metrics: EvalMetrics
    total_questions: int = 50
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None


# ── Domain DTOs ──────────────────────────────────────────────────────────────


class TransactionDTO(BaseModel):
    """Lightweight representation of a DLD transaction."""

    transaction_id: str
    date: datetime
    price: float
    price_sqft: float
    transaction_type: str = ""
    procedure_type: str = ""
    area_name: str = ""
    project_name: str = ""
    developer_name: str = ""
    bedrooms: int | None = None
    area_sqft: float | None = None
    unit_type: str = ""


class AreaDTO(BaseModel):
    """Aggregated area statistics."""

    area_id: str
    name: str
    zone: str = ""
    avg_price_sqft: float = 0.0
    yoy_price_change: float = 0.0
    off_plan_ratio: float = 0.0


class HealthResponse(BaseModel):
    """Health check response."""

    status: Literal["healthy", "degraded", "unhealthy"]
    neo4j: bool = False
    postgres: bool = False
    pinecone: bool = False
