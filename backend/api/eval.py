"""POST /api/eval — trigger an evaluation run as a background task."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, BackgroundTasks

from backend.models.schemas import EvalRequest

logger = structlog.get_logger(__name__)
router = APIRouter()


async def _run_eval_in_background(run_name: str, ablation: bool) -> None:
    """Execute the eval runner asynchronously."""
    from backend.evaluation.eval_runner import run_evaluation

    try:
        await run_evaluation(run_name=run_name, ablation=ablation)
    except Exception:
        logger.error("eval.background_error", run_name=run_name, exc_info=True)


@router.post("/eval")
async def trigger_eval(
    request: EvalRequest,
    background_tasks: BackgroundTasks,
) -> dict:
    """Trigger an evaluation run.

    The eval run executes in the background. Check W&B for results.
    """
    run_name = request.run_name or "eval-run"
    background_tasks.add_task(
        _run_eval_in_background,
        run_name=run_name,
        ablation=request.ablation,
    )
    logger.info("eval.triggered", run_name=run_name, ablation=request.ablation)
    return {"status": "started", "run_name": run_name, "ablation": request.ablation}
