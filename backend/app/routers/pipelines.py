"""
Pipelines router — CRUD + parse (DAG analysis) + execute (async + streaming).
All routes are mounted under /pipelines in main.py.
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from app.models.schemas import (
    ExecutionHistoryItem,
    ExecutionRequest,
    ExecutionRun,
    PipelineCreate,
    PipelineParseRequest,
    PipelineParseResponse,
    PipelineResponse,
    PipelineUpdate,
)
from app.services import store
from app.services.dag import is_dag, node_type_counts, topological_sort
from app.services.execution import execute_pipeline, execute_pipeline_stream

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])


# ---------------------------------------------------------------------------
# 1. Parse / DAG analysis   POST /pipelines/parse
# ---------------------------------------------------------------------------

@router.post("/parse", response_model=PipelineParseResponse)
async def parse_pipeline(payload: PipelineParseRequest):
    """
    Accepts the current React-flow graph (nodes + edges) and returns:
    - node / edge counts
    - whether it forms a valid DAG
    - topological execution order
    """
    dag_valid = is_dag(payload.nodes, payload.edges)
    topo = topological_sort(payload.nodes, payload.edges)
    return PipelineParseResponse(
        num_nodes=len(payload.nodes),
        num_edges=len(payload.edges),
        is_dag=dag_valid,
        topological_order=topo,
        node_types=node_type_counts(payload.nodes),
    )


# ---------------------------------------------------------------------------
# 2. CRUD
# ---------------------------------------------------------------------------

@router.get("/", response_model=List[PipelineResponse])
async def list_pipelines():
    """Return all saved pipelines."""
    return store.list_pipelines()


@router.post("/", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def create_pipeline(payload: PipelineCreate):
    """Persist a new pipeline."""
    return store.create_pipeline(
        name=payload.name,
        description=payload.description,
        nodes=payload.nodes,
        edges=payload.edges,
    )


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: str):
    pipeline = store.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_id}' not found.",
        )
    return pipeline


@router.patch("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(pipeline_id: str, payload: PipelineUpdate):
    updated = store.update_pipeline(
        pipeline_id=pipeline_id,
        name=payload.name,
        description=payload.description,
        nodes=payload.nodes,
        edges=payload.edges,
    )
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_id}' not found.",
        )
    return updated


@router.delete("/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pipeline(pipeline_id: str):
    deleted = store.delete_pipeline(pipeline_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_id}' not found.",
        )


# ---------------------------------------------------------------------------
# 3. Execution — async (non-streaming)   POST /pipelines/execute
# ---------------------------------------------------------------------------

@router.post("/execute", response_model=ExecutionRun)
async def execute_pipeline_route(payload: ExecutionRequest):
    """
    Execute a pipeline and return the full execution report once done.
    LLM nodes will block until all tokens are collected.
    """
    run = await execute_pipeline(
        nodes=payload.nodes,
        edges=payload.edges,
        inputs=payload.inputs,
        pipeline_id=payload.pipeline_id,
    )
    return run


# ---------------------------------------------------------------------------
# 4. Execution — streaming SSE   POST /pipelines/execute/stream
# ---------------------------------------------------------------------------

@router.post("/execute/stream")
async def execute_pipeline_stream_route(payload: ExecutionRequest):
    """
    Execute a pipeline and stream node events + LLM tokens as Server-Sent Events.

    Event shape (each line is `data: <json>\\n\\n`):
      - {"type": "run_start",  "run_id": "..."}
      - {"type": "node_start", "node_id": "...", "node_type": "..."}
      - {"type": "llm_token",  "node_id": "...", "token": "..."}    ← streaming tokens
      - {"type": "node_done",  "node_id": "...", "status": "success"|"error", ...}
      - {"type": "run_done",   "status": "success"|"error", "run_id": "..."}
    """
    return StreamingResponse(
        execute_pipeline_stream(
            nodes=payload.nodes,
            edges=payload.edges,
            inputs=payload.inputs,
            pipeline_id=payload.pipeline_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# 5. Execution history
# ---------------------------------------------------------------------------

@router.get("/runs/history", response_model=List[ExecutionHistoryItem])
async def execution_history(
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
):
    """Return a summary list of past execution runs (newest first)."""
    runs = store.list_runs(pipeline_id=pipeline_id)
    return [
        ExecutionHistoryItem(
            run_id=r.run_id,
            pipeline_id=r.pipeline_id,
            status=r.status,
            num_nodes=len(r.node_results),
            started_at=r.started_at,
            finished_at=r.finished_at,
            error=r.error,
        )
        for r in runs
    ]


@router.get("/runs/{run_id}", response_model=ExecutionRun)
async def get_run(run_id: str):
    """Return the full execution report for a single run."""
    run = store.get_run(run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found.",
        )
    return run
