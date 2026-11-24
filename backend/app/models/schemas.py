"""
Pydantic schemas — the single source of truth for every request/response body.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Node / Edge primitives (mirror ReactFlow's data shape)
# ---------------------------------------------------------------------------

class NodeData(BaseModel):
    """Arbitrary per-node data bag coming from the React store."""
    name: Optional[str] = None
    outputs: Optional[List[str]] = None
    # LLM node
    selectedModel: Optional[str] = None
    prompt: Optional[str] = None
    systemPrompt: Optional[str] = None
    # Input node
    inputType: Optional[str] = None
    # Output node
    outputType: Optional[str] = None
    # API node
    url: Optional[str] = None
    method: Optional[str] = None
    # Condition node
    condition: Optional[str] = None
    value: Optional[str] = None
    # Math node
    operation: Optional[str] = None
    # Transform node
    transform: Optional[str] = None
    # Text node
    text: Optional[str] = None
    variables: Optional[List[str]] = None

    model_config = {"extra": "allow"}


class NodePosition(BaseModel):
    x: float
    y: float


class PipelineNode(BaseModel):
    id: str
    type: str
    position: NodePosition
    data: NodeData = Field(default_factory=NodeData)

    model_config = {"extra": "allow"}


class PipelineEdge(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None
    type: Optional[str] = None
    animated: Optional[bool] = None

    model_config = {"extra": "allow"}


# ---------------------------------------------------------------------------
# Pipeline CRUD schemas
# ---------------------------------------------------------------------------

class PipelineCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    nodes: List[PipelineNode] = Field(default_factory=list)
    edges: List[PipelineEdge] = Field(default_factory=list)


class PipelineUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    nodes: Optional[List[PipelineNode]] = None
    edges: Optional[List[PipelineEdge]] = None


class PipelineResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Parse / DAG-analysis schemas  (used by /pipelines/parse)
# ---------------------------------------------------------------------------

class PipelineParseRequest(BaseModel):
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]


class PipelineParseResponse(BaseModel):
    num_nodes: int
    num_edges: int
    is_dag: bool
    topological_order: Optional[List[str]] = None
    node_types: Dict[str, int] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Execution schemas
# ---------------------------------------------------------------------------

class NodeStatus(str, Enum):
    pending = "pending"
    running = "running"
    success = "success"
    error = "error"
    skipped = "skipped"


class NodeExecutionResult(BaseModel):
    node_id: str
    node_type: str
    status: NodeStatus
    output: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: Optional[float] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ExecutionRun(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: Optional[str] = None
    status: NodeStatus = NodeStatus.pending
    node_results: List[NodeExecutionResult] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    error: Optional[str] = None


class ExecutionRequest(BaseModel):
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    inputs: Dict[str, Any] = Field(default_factory=dict)
    pipeline_id: Optional[str] = None


class ExecutionHistoryItem(BaseModel):
    run_id: str
    pipeline_id: Optional[str]
    status: NodeStatus
    num_nodes: int
    started_at: datetime
    finished_at: Optional[datetime]
    error: Optional[str]


# ---------------------------------------------------------------------------
# Error schemas
# ---------------------------------------------------------------------------

class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Optional[Any] = None


class ErrorResponse(BaseModel):
    error: ErrorDetail
