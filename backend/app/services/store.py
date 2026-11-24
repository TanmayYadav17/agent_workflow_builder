"""
In-memory stores for pipelines and execution history.
In production, swap these for a database (Postgres, MongoDB, etc.).
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional

from app.models.schemas import ExecutionRun, PipelineResponse, PipelineNode, PipelineEdge


# ---------------------------------------------------------------------------
# Pipeline store
# ---------------------------------------------------------------------------

_pipelines: Dict[str, PipelineResponse] = {}


def list_pipelines() -> List[PipelineResponse]:
    return list(_pipelines.values())


def get_pipeline(pipeline_id: str) -> Optional[PipelineResponse]:
    return _pipelines.get(pipeline_id)


def create_pipeline(
    name: str,
    description: Optional[str],
    nodes: List[PipelineNode],
    edges: List[PipelineEdge],
) -> PipelineResponse:
    now = datetime.utcnow()
    pipeline = PipelineResponse(
        id=str(uuid.uuid4()),
        name=name,
        description=description,
        nodes=nodes,
        edges=edges,
        created_at=now,
        updated_at=now,
    )
    _pipelines[pipeline.id] = pipeline
    return pipeline


def update_pipeline(
    pipeline_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    nodes: Optional[List[PipelineNode]] = None,
    edges: Optional[List[PipelineEdge]] = None,
) -> Optional[PipelineResponse]:
    existing = _pipelines.get(pipeline_id)
    if not existing:
        return None
    updated = existing.model_copy(
        update={
            "name": name if name is not None else existing.name,
            "description": description if description is not None else existing.description,
            "nodes": nodes if nodes is not None else existing.nodes,
            "edges": edges if edges is not None else existing.edges,
            "updated_at": datetime.utcnow(),
        }
    )
    _pipelines[pipeline_id] = updated
    return updated


def delete_pipeline(pipeline_id: str) -> bool:
    if pipeline_id in _pipelines:
        del _pipelines[pipeline_id]
        return True
    return False


# ---------------------------------------------------------------------------
# Execution history store
# ---------------------------------------------------------------------------

_execution_runs: Dict[str, ExecutionRun] = {}


def save_run(run: ExecutionRun) -> None:
    _execution_runs[run.run_id] = run


def get_run(run_id: str) -> Optional[ExecutionRun]:
    return _execution_runs.get(run_id)


def list_runs(pipeline_id: Optional[str] = None) -> List[ExecutionRun]:
    runs = list(_execution_runs.values())
    if pipeline_id:
        runs = [r for r in runs if r.pipeline_id == pipeline_id]
    # Newest first
    return sorted(runs, key=lambda r: r.started_at, reverse=True)
