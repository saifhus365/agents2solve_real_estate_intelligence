"""Evaluation runner — runs gold Q&A pairs through the agent, computes
metrics, and logs results to W&B. Supports ablation runs."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog
import wandb

from backend.agent.graph import run_agent
from backend.config import get_settings
from backend.db.postgres_client import pg_client
from backend.evaluation.metrics import compute_all_metrics

logger = structlog.get_logger(__name__)

GOLD_DATASET_PATH = Path(__file__).parent / "gold_dataset.json"


def _load_gold_dataset() -> list[dict]:
    """Load the 50 gold Q&A pairs from JSON."""
    with open(GOLD_DATASET_PATH) as f:
        return json.load(f)


async def _run_single_question(
    question: dict,
    run_mode: str = "hybrid",
) -> dict:
    """Run the agent for a single gold question and capture results.

    Args:
        question: A gold dataset entry.
        run_mode: "hybrid" (default), "vector_only", or "graph_only".

    Returns:
        Result dict with answer, context, latency, and metadata.
    """
    start = time.perf_counter()

    try:
        state = await run_agent(question["question"])
        elapsed = (time.perf_counter() - start) * 1000

        result = {
            "id": question["id"],
            "category": question["category"],
            "question": question["question"],
            "expected_answer": question["expected_answer"],
            "expected_entities": question.get("expected_entities", []),
            "expected_query_type": question.get("expected_query_type", ""),
            "answer": state.get("answer", ""),
            "query_type": state.get("query_type", ""),
            "graph_results": state.get("graph_results", []),
            "vector_results": state.get("vector_results", []),
            "sql_results": state.get("sql_results", []),
            "grounded_context": state.get("grounded_context", []),
            "latency_ms": round(elapsed, 1),
            "error": state.get("error", ""),
        }
    except Exception as exc:
        elapsed = (time.perf_counter() - start) * 1000
        result = {
            "id": question["id"],
            "category": question["category"],
            "question": question["question"],
            "expected_answer": question["expected_answer"],
            "expected_entities": question.get("expected_entities", []),
            "expected_query_type": question.get("expected_query_type", ""),
            "answer": "",
            "query_type": "",
            "graph_results": [],
            "vector_results": [],
            "sql_results": [],
            "grounded_context": [],
            "latency_ms": round(elapsed, 1),
            "error": str(exc),
        }

    return result


async def _log_to_postgres(
    run_name: str,
    question: dict,
    result: dict,
    metrics: dict,
) -> None:
    """Persist individual eval result to PostgreSQL."""
    try:
        await pg_client.execute(
            """
            INSERT INTO eval_logs (run_name, question_id, question, expected_answer, actual_answer, metrics)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """,
            run_name,
            question["id"],
            question["question"],
            question["expected_answer"],
            result.get("answer", ""),
            json.dumps(metrics, default=str),
        )
    except Exception:
        logger.error("eval.postgres_log_failed", question_id=question["id"], exc_info=True)


async def run_evaluation(
    run_name: str = "eval-run",
    ablation: bool = False,
) -> dict:
    """Execute the full evaluation pipeline.

    Args:
        run_name: Name for this eval run (used in W&B and PG logs).
        ablation: If True, run additional vector-only and graph-only runs.

    Returns:
        Dict with aggregated metrics.
    """
    settings = get_settings()
    gold_data = _load_gold_dataset()

    logger.info("eval.starting", run_name=run_name, questions=len(gold_data), ablation=ablation)

    # ── Main hybrid run ──────────────────────────────────────────────────
    wandb.init(
        project=settings.wandb_project,
        name=f"{run_name}-hybrid",
        config={"mode": "hybrid", "questions": len(gold_data)},
    )

    all_results: list[dict] = []
    for i, question in enumerate(gold_data):
        logger.info("eval.running_question", idx=i + 1, qid=question["id"])
        result = await _run_single_question(question, run_mode="hybrid")
        all_results.append(result)

        # Log individual result to W&B
        wandb.log({
            "question_id": question["id"],
            "category": question["category"],
            "latency_ms": result["latency_ms"],
            "has_error": bool(result.get("error")),
        })

        # Log to PG
        per_question_metrics = {
            "latency_ms": result["latency_ms"],
            "has_answer": bool(result.get("answer")),
            "query_type": result.get("query_type", ""),
        }
        await _log_to_postgres(run_name, question, result, per_question_metrics)

    # Compute aggregate metrics
    metrics = compute_all_metrics(all_results)
    wandb.log(metrics)
    wandb.finish()

    logger.info("eval.hybrid_complete", metrics=metrics)

    # ── Ablation runs (if requested) ─────────────────────────────────────
    if ablation:
        # Multi-hop subset only
        multihop_questions = [q for q in gold_data if q["category"] == "multihop"]

        for mode in ("vector_only", "graph_only"):
            wandb.init(
                project=settings.wandb_project,
                name=f"{run_name}-{mode}",
                config={"mode": mode, "questions": len(multihop_questions)},
            )

            mode_results: list[dict] = []
            for question in multihop_questions:
                result = await _run_single_question(question, run_mode=mode)
                mode_results.append(result)

                wandb.log({
                    "question_id": question["id"],
                    "latency_ms": result["latency_ms"],
                })

            mode_metrics = compute_all_metrics(mode_results)
            wandb.log(mode_metrics)
            wandb.finish()

            logger.info(f"eval.{mode}_complete", metrics=mode_metrics)

    final_result = {
        "run_name": run_name,
        "metrics": metrics,
        "total_questions": len(gold_data),
        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    logger.info("eval.complete", **final_result)
    return final_result
