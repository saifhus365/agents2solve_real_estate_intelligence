"""Evaluation metrics for the Dubai Real Estate Intelligence Co-Pilot.

Computes faithfulness, retrieval recall, Cypher accuracy, multi-hop accuracy,
hallucination rate, and p95 latency.
"""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger(__name__)


def answer_faithfulness(
    answer: str,
    grounded_context: list[dict],
) -> float:
    """Fraction of answer claims traceable to the retrieved context.

    Heuristic: split the answer into sentences, then check if each sentence
    shares significant word overlap with at least one context chunk.
    """
    if not answer.strip():
        return 0.0

    sentences = [s.strip() for s in re.split(r'[.!?]\s+', answer) if len(s.strip()) > 10]
    if not sentences:
        return 1.0

    context_texts: list[str] = []
    for ctx in grounded_context:
        text = ctx.get("text", "")
        if not text:
            text = " ".join(str(v) for v in ctx.values() if v)
        context_texts.append(text.lower())

    full_context = " ".join(context_texts)
    traceable = 0

    for sentence in sentences:
        words = set(re.findall(r'\b\w{4,}\b', sentence.lower()))
        if not words:
            traceable += 1
            continue
        overlap = sum(1 for w in words if w in full_context)
        if overlap / len(words) >= 0.3:
            traceable += 1

    return traceable / len(sentences)


def retrieval_recall_at_k(
    expected_entities: list[str],
    retrieved_results: list[dict],
    k: int = 10,
) -> float:
    """Fraction of expected entities found in the top-k retrieved results."""
    if not expected_entities:
        return 1.0

    top_k = retrieved_results[:k]
    retrieved_text = " ".join(
        str(v).lower()
        for result in top_k
        for v in result.values()
        if v
    )

    found = sum(
        1 for entity in expected_entities
        if entity.lower() in retrieved_text
    )
    return found / len(expected_entities)


def cypher_accuracy(
    actual_results: list[dict],
    expected_entities: list[str],
) -> float:
    """Fraction of graph queries returning the correct entity set.

    Checks if expected entity names appear in the Cypher query results.
    """
    if not expected_entities:
        return 1.0

    if not actual_results:
        return 0.0

    results_text = " ".join(
        str(v).lower()
        for row in actual_results
        for v in row.values()
        if v
    )

    found = sum(
        1 for entity in expected_entities
        if entity.lower() in results_text
    )
    return found / len(expected_entities)


def hallucination_rate(
    answer: str,
    grounded_context: list[dict],
) -> float:
    """Fraction of answer claims NOT traceable to retrieved context.

    Inverse of faithfulness.
    """
    faith = answer_faithfulness(answer, grounded_context)
    return 1.0 - faith


def multihop_accuracy(
    graph_results: list[dict],
    vector_results: list[dict],
    expected_entities: list[str],
) -> dict[str, float]:
    """Compare graph-only vs vector-only retrieval accuracy.

    Returns dict with 'graph_accuracy', 'vector_accuracy', and 'hybrid_accuracy'.
    """
    graph_acc = cypher_accuracy(graph_results, expected_entities)
    vector_acc = retrieval_recall_at_k(expected_entities, vector_results, k=10)
    combined = graph_results + vector_results
    hybrid_acc = retrieval_recall_at_k(expected_entities, combined, k=10)

    return {
        "graph_accuracy": graph_acc,
        "vector_accuracy": vector_acc,
        "hybrid_accuracy": hybrid_acc,
    }


def p95_latency(latencies: list[float]) -> float:
    """Compute the 95th percentile latency in milliseconds."""
    if not latencies:
        return 0.0
    return float(np.percentile(latencies, 95))


def compute_all_metrics(
    results: list[dict[str, Any]],
) -> dict[str, float]:
    """Aggregate all metrics across a list of evaluation results.

    Each result dict should contain:
        - answer: str
        - grounded_context: list[dict]
        - graph_results: list[dict]
        - vector_results: list[dict]
        - expected_entities: list[str]
        - latency_ms: float
        - category: str
        - query_type: str
    """
    faithfulness_scores: list[float] = []
    recall_scores: list[float] = []
    cypher_scores: list[float] = []
    hallucination_scores: list[float] = []
    multihop_graph_scores: list[float] = []
    multihop_vector_scores: list[float] = []
    multihop_hybrid_scores: list[float] = []
    latencies: list[float] = []

    for r in results:
        answer = r.get("answer", "")
        grounded = r.get("grounded_context", [])
        graph = r.get("graph_results", [])
        vector = r.get("vector_results", [])
        expected = r.get("expected_entities", [])
        latency = r.get("latency_ms", 0.0)
        category = r.get("category", "")

        faithfulness_scores.append(answer_faithfulness(answer, grounded))
        hallucination_scores.append(hallucination_rate(answer, grounded))
        latencies.append(latency)

        combined = graph + vector
        recall_scores.append(retrieval_recall_at_k(expected, combined))

        if r.get("query_type") in ("graph", "hybrid"):
            cypher_scores.append(cypher_accuracy(graph, expected))

        if category == "multihop":
            mh = multihop_accuracy(graph, vector, expected)
            multihop_graph_scores.append(mh["graph_accuracy"])
            multihop_vector_scores.append(mh["vector_accuracy"])
            multihop_hybrid_scores.append(mh["hybrid_accuracy"])

    return {
        "answer_faithfulness": float(np.mean(faithfulness_scores)) if faithfulness_scores else 0.0,
        "retrieval_recall_at_10": float(np.mean(recall_scores)) if recall_scores else 0.0,
        "cypher_accuracy": float(np.mean(cypher_scores)) if cypher_scores else 0.0,
        "multihop_accuracy_graph": float(np.mean(multihop_graph_scores)) if multihop_graph_scores else 0.0,
        "multihop_accuracy_vector": float(np.mean(multihop_vector_scores)) if multihop_vector_scores else 0.0,
        "multihop_accuracy_hybrid": float(np.mean(multihop_hybrid_scores)) if multihop_hybrid_scores else 0.0,
        "hallucination_rate": float(np.mean(hallucination_scores)) if hallucination_scores else 0.0,
        "p95_latency_ms": p95_latency(latencies),
    }
