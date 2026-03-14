"""Answer generator node — streams the final response via NVIDIA API
with citations attached to every factual claim."""

from __future__ import annotations

import json
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import structlog
from openai import AsyncOpenAI

from backend.agent.state import AgentState
from backend.config import get_settings

logger = structlog.get_logger(__name__)

SYSTEM_PROMPT = """\
You are the Dubai Real Estate Intelligence Co-Pilot. You help real estate 
professionals understand the Dubai property market with data-backed answers.

RULES:
1. Answer ONLY from the provided context below — never make up data.
2. CITE every factual claim by referencing the source in square brackets, 
   e.g. [Source: DLD Transactions] or [Source: Gulf News].
3. If the context is empty or insufficient, respond with:
   "I don't have data on this. Please refine your question or check back 
   after the next data sync."
4. Use markdown formatting: headers, bullet points, bold for key figures.
5. When presenting prices, use AED and format with commas.
6. For percentages, round to one decimal place.
7. Be concise but comprehensive — real estate professionals value precision.
"""


def _build_context_block(grounded_context: list[dict]) -> str:
    """Serialise grounded context into a text block for the LLM."""
    if not grounded_context:
        return "No context available."

    blocks: list[str] = []
    for i, ctx in enumerate(grounded_context, 1):
        source_type = ctx.get("_source_type", "unknown")
        text = ctx.get("text", "")
        if not text:
            # Serialise non-text results
            filtered = {
                k: v for k, v in ctx.items()
                if not k.startswith("_") and k not in ("id", "score")
            }
            text = json.dumps(filtered, default=str)

        source_name = ctx.get("source", ctx.get("title", f"Source {i}"))
        url = ctx.get("url", "")
        blocks.append(
            f"[{i}] ({source_type}) {source_name}\n"
            f"URL: {url}\n"
            f"{text}"
        )

    return "\n\n---\n\n".join(blocks)


def _extract_citations(grounded_context: list[dict]) -> list[dict]:
    """Build citation objects from grounded context."""
    citations: list[dict] = []
    seen: set[str] = set()

    for ctx in grounded_context:
        source_name = ctx.get("source", ctx.get("title", "Unknown"))
        url = ctx.get("url", "")
        key = f"{source_name}:{url}"

        if key not in seen:
            seen.add(key)
            citations.append({
                "source_name": source_name,
                "url": url,
                "source_type": ctx.get("_source_type", "vector"),
                "retrieved_at": datetime.now(tz=timezone.utc).isoformat(),
            })

    return citations


async def generate_answer(state: AgentState) -> AgentState:
    """Generate the final answer (non-streaming) with citations."""
    start = time.perf_counter()
    settings = get_settings()
    grounded = state.get("grounded_context", [])

    context_block = _build_context_block(grounded)
    citations = _extract_citations(grounded)

    client = AsyncOpenAI(
        base_url=settings.nvidia_base_url,
        api_key=settings.nvidia_api_key,
    )

    user_message = (
        f"Context:\n{context_block}\n\n"
        f"Question: {state['query']}\n\n"
        f"Answer with citations:"
    )

    response = await client.chat.completions.create(
        model=settings.nvidia_model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.2,
        max_tokens=1500,
    )

    answer = response.choices[0].message.content.strip()
    elapsed = (time.perf_counter() - start) * 1000

    logger.info(
        "generator.complete",
        answer_len=len(answer),
        citations=len(citations),
        latency_ms=round(elapsed, 1),
    )

    return {
        **state,
        "answer": answer,
        "citations": citations,
        "latency_ms": elapsed,
    }


async def generate_answer_stream(state: AgentState) -> AsyncGenerator[dict, None]:
    """Stream the final answer token-by-token via NVIDIA API.

    Yields SSE-compatible event dicts:
      {"type": "token", "content": "..."}
      {"type": "citations", "data": [...]}
      {"type": "done", "latency_ms": ...}
    """
    start = time.perf_counter()
    settings = get_settings()
    grounded = state.get("grounded_context", [])

    context_block = _build_context_block(grounded)
    citations = _extract_citations(grounded)

    client = AsyncOpenAI(
        base_url=settings.nvidia_base_url,
        api_key=settings.nvidia_api_key,
    )

    user_message = (
        f"Context:\n{context_block}\n\n"
        f"Question: {state['query']}\n\n"
        f"Answer with citations:"
    )

    stream = await client.chat.completions.create(
        model=settings.nvidia_model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.2,
        max_tokens=1500,
        stream=True,
    )

    full_answer = ""
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            token = chunk.choices[0].delta.content
            full_answer += token
            yield {"type": "token", "content": token}

    # After streaming completes, send citations
    yield {"type": "citations", "data": citations}

    elapsed = (time.perf_counter() - start) * 1000
    yield {"type": "done", "latency_ms": round(elapsed, 1)}

    logger.info(
        "generator.stream_complete",
        answer_len=len(full_answer),
        citations=len(citations),
        latency_ms=round(elapsed, 1),
    )
