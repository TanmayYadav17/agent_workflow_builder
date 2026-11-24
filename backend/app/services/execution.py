"""
Execution engine — walks the topological order and executes each node.
LLM nodes hit a dummy streaming endpoint; all others run locally.

Streaming: yields Server-Sent Event lines for the /pipelines/execute/stream endpoint.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from app.models.schemas import (
    ExecutionRun,
    NodeExecutionResult,
    NodeStatus,
    PipelineEdge,
    PipelineNode,
)
from app.services.dag import topological_sort
from app.services.store import save_run

# ---------------------------------------------------------------------------
# Dummy LLM API — replace with real endpoints when available
# ---------------------------------------------------------------------------

DUMMY_LLM_STREAM_URL = "http://dummy-llm.local/v1/chat/completions/stream"
DUMMY_LLM_URL = "http://dummy-llm.local/v1/chat/completions"


async def _call_llm_streaming(
    model: str,
    system_prompt: str,
    prompt: str,
    context: Dict[str, Any],
) -> AsyncGenerator[str, None]:
    """
    Simulated streaming LLM response.
    Replace the body with a real httpx SSE call when you wire up a real API key.
    Each chunk yields a token string.
    """
    # Simulate a streaming response with realistic-looking chunks
    full_response = (
        f"[{model}] Processing: {prompt[:80]}{'...' if len(prompt) > 80 else ''}\n\n"
        "This is a simulated streaming response from the dummy LLM API. "
        "In production, replace this with a real OpenAI / Anthropic / Gemini call. "
        "The pipeline executed successfully with the provided context."
    )
    words = full_response.split(" ")
    for i, word in enumerate(words):
        await asyncio.sleep(0.04)  # simulate token latency
        yield word + (" " if i < len(words) - 1 else "")


async def _call_llm(
    model: str,
    system_prompt: str,
    prompt: str,
    context: Dict[str, Any],
) -> str:
    """Non-streaming LLM call (for the synchronous execute endpoint)."""
    tokens = []
    async for token in _call_llm_streaming(model, system_prompt, prompt, context):
        tokens.append(token)
    return "".join(tokens)


# ---------------------------------------------------------------------------
# Individual node executors
# ---------------------------------------------------------------------------

async def _execute_node(
    node: PipelineNode,
    context: Dict[str, Any],
) -> Any:
    """
    Run a single node and return its output value.
    `context` holds the already-computed outputs of upstream nodes.
    """
    ntype = node.type
    data = node.data

    # ── Input node ──────────────────────────────────────────────────────────
    if ntype in ("customInput",):
        return context.get(node.id) or data.name or node.id

    # ── LLM node ────────────────────────────────────────────────────────────
    if ntype in ("customLLM", "llmNode"):
        model = data.selectedModel or "gpt-4"
        sys_prompt = data.systemPrompt or ""
        prompt = data.prompt or ""
        return await _call_llm(model, sys_prompt, prompt, context)

    # ── Text node ───────────────────────────────────────────────────────────
    if ntype in ("customText", "textNode"):
        text = data.text or ""
        # Interpolate {{variable}} references from context
        for var, val in context.items():
            text = text.replace(f"{{{{{var}}}}}", str(val))
        return text

    # ── API node ────────────────────────────────────────────────────────────
    if ntype in ("customAPI", "apiNode"):
        url = data.url or "https://api.example.com"
        method = (data.method or "GET").upper()
        # Dummy response — in production perform a real HTTP call
        return {
            "status": 200,
            "url": url,
            "method": method,
            "body": f"[Dummy {method} response from {url}]",
        }

    # ── Math node ───────────────────────────────────────────────────────────
    if ntype in ("customMath", "mathNode"):
        op = data.operation or "Add"
        raw = data.value or "0"
        try:
            operand = float(raw)
        except ValueError:
            operand = 0
        # Gather numeric upstream values
        upstream_vals = [
            float(v) for v in context.values() if isinstance(v, (int, float))
        ]
        base = upstream_vals[0] if upstream_vals else 0
        ops = {
            "Add": base + operand,
            "Subtract": base - operand,
            "Multiply": base * operand,
            "Divide": base / operand if operand != 0 else None,
        }
        return ops.get(op, base)

    # ── Condition node ───────────────────────────────────────────────────────
    if ntype in ("customCondition", "conditionNode"):
        cond = data.condition or "Equals"
        ref = data.value or ""
        upstream = next(iter(context.values()), "") if context else ""
        result = {
            "Equals": str(upstream) == ref,
            "NotEquals": str(upstream) != ref,
            "GreaterThan": float(upstream) > float(ref) if ref else False,
            "LessThan": float(upstream) < float(ref) if ref else False,
        }.get(cond, False)
        return result

    # ── Transform node ───────────────────────────────────────────────────────
    if ntype in ("customTransform", "transformNode"):
        transform = data.transform or "Uppercase"
        upstream = str(next(iter(context.values()), ""))
        funcs = {
            "Uppercase": upstream.upper(),
            "Lowercase": upstream.lower(),
            "Trim": upstream.strip(),
            "Reverse": upstream[::-1],
        }
        return funcs.get(transform, upstream)

    # ── Output node ──────────────────────────────────────────────────────────
    if ntype in ("customOutput", "outputNode"):
        return next(iter(context.values()), None)

    # ── Unknown node ─────────────────────────────────────────────────────────
    return f"[No executor for node type '{ntype}']"


# ---------------------------------------------------------------------------
# Full pipeline execution (non-streaming)
# ---------------------------------------------------------------------------

async def execute_pipeline(
    nodes: List[PipelineNode],
    edges: List[PipelineEdge],
    inputs: Dict[str, Any],
    pipeline_id: Optional[str] = None,
) -> ExecutionRun:
    run = ExecutionRun(
        run_id=str(uuid.uuid4()),
        pipeline_id=pipeline_id,
        status=NodeStatus.running,
        started_at=datetime.utcnow(),
    )

    order = topological_sort(nodes, edges)
    if order is None:
        run.status = NodeStatus.error
        run.error = "Pipeline contains a cycle and cannot be executed."
        run.finished_at = datetime.utcnow()
        save_run(run)
        return run

    node_map = {n.id: n for n in nodes}

    # Build edge map: target → list[source]
    incoming: Dict[str, List[str]] = {n.id: [] for n in nodes}
    for edge in edges:
        if edge.target in incoming:
            incoming[edge.target].append(edge.source)

    # Global output context: node_id → output value
    context: Dict[str, Any] = {**inputs}

    for node_id in order:
        node = node_map[node_id]
        # Build local context for this node from its upstream outputs
        local_ctx = {src: context[src] for src in incoming[node_id] if src in context}

        result = NodeExecutionResult(
            node_id=node_id,
            node_type=node.type,
            status=NodeStatus.running,
            started_at=datetime.utcnow(),
        )
        t0 = time.perf_counter()

        try:
            output = await _execute_node(node, local_ctx)
            result.output = output
            result.status = NodeStatus.success
            context[node_id] = output
        except Exception as exc:
            result.status = NodeStatus.error
            result.error = str(exc)

        result.finished_at = datetime.utcnow()
        result.duration_ms = (time.perf_counter() - t0) * 1000
        run.node_results.append(result)

        if result.status == NodeStatus.error:
            run.status = NodeStatus.error
            run.error = f"Node '{node_id}' failed: {result.error}"
            run.finished_at = datetime.utcnow()
            save_run(run)
            return run

    run.status = NodeStatus.success
    run.finished_at = datetime.utcnow()
    save_run(run)
    return run


# ---------------------------------------------------------------------------
# Streaming execution (SSE generator)
# ---------------------------------------------------------------------------

async def execute_pipeline_stream(
    nodes: List[PipelineNode],
    edges: List[PipelineEdge],
    inputs: Dict[str, Any],
    pipeline_id: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """
    Yields SSE-formatted strings:
        data: <json>\n\n
    Events:
        {type: "run_start",   run_id: ...}
        {type: "node_start",  node_id: ..., node_type: ...}
        {type: "llm_token",   node_id: ..., token: ...}
        {type: "node_done",   node_id: ..., status: ..., output: ...}
        {type: "run_done",    status: ..., error: ...}
    """
    run_id = str(uuid.uuid4())

    def sse(payload: dict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    yield sse({"type": "run_start", "run_id": run_id, "pipeline_id": pipeline_id})

    order = topological_sort(nodes, edges)
    if order is None:
        yield sse({"type": "run_done", "status": "error", "error": "Cycle detected in pipeline."})
        return

    node_map = {n.id: n for n in nodes}
    incoming: Dict[str, List[str]] = {n.id: [] for n in nodes}
    for edge in edges:
        if edge.target in incoming:
            incoming[edge.target].append(edge.source)

    context: Dict[str, Any] = {**inputs}
    run_status = NodeStatus.success
    run_error = None

    for node_id in order:
        node = node_map[node_id]
        local_ctx = {src: context[src] for src in incoming[node_id] if src in context}

        yield sse({"type": "node_start", "node_id": node_id, "node_type": node.type})
        t0 = time.perf_counter()

        try:
            # LLM nodes get special streaming treatment
            if node.type in ("customLLM", "llmNode"):
                data = node.data
                model = data.selectedModel or "gpt-4"
                sys_prompt = data.systemPrompt or ""
                prompt = data.prompt or ""
                tokens: List[str] = []
                async for token in _call_llm_streaming(model, sys_prompt, prompt, local_ctx):
                    tokens.append(token)
                    yield sse({"type": "llm_token", "node_id": node_id, "token": token})
                output = "".join(tokens)
            else:
                output = await _execute_node(node, local_ctx)

            context[node_id] = output
            duration = (time.perf_counter() - t0) * 1000
            yield sse({
                "type": "node_done",
                "node_id": node_id,
                "status": "success",
                "output": output if not isinstance(output, (dict, list)) else output,
                "duration_ms": round(duration, 2),
            })

        except Exception as exc:
            run_status = NodeStatus.error
            run_error = str(exc)
            yield sse({
                "type": "node_done",
                "node_id": node_id,
                "status": "error",
                "error": str(exc),
            })
            break

    yield sse({"type": "run_done", "run_id": run_id, "status": run_status.value, "error": run_error})
