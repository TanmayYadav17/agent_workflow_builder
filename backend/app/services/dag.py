"""
DAG utilities — topological sort & cycle detection.
Works on plain lists of PipelineNode / PipelineEdge.
"""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple

from app.models.schemas import PipelineEdge, PipelineNode


def build_graph(
    nodes: List[PipelineNode], edges: List[PipelineEdge]
) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
    """Return adjacency list and in-degree map."""
    adj: Dict[str, List[str]] = {n.id: [] for n in nodes}
    in_degree: Dict[str, int] = {n.id: 0 for n in nodes}

    for edge in edges:
        if edge.source in adj and edge.target in adj:
            adj[edge.source].append(edge.target)
            in_degree[edge.target] += 1

    return adj, in_degree


def topological_sort(
    nodes: List[PipelineNode], edges: List[PipelineEdge]
) -> Optional[List[str]]:
    """
    Kahn's algorithm.
    Returns ordered node IDs, or None if the graph has a cycle.
    """
    adj, in_degree = build_graph(nodes, edges)

    queue: deque[str] = deque(
        [nid for nid, deg in in_degree.items() if deg == 0]
    )
    order: List[str] = []

    while queue:
        node_id = queue.popleft()
        order.append(node_id)
        for neighbour in adj[node_id]:
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    return order if len(order) == len(nodes) else None


def is_dag(nodes: List[PipelineNode], edges: List[PipelineEdge]) -> bool:
    return topological_sort(nodes, edges) is not None


def node_type_counts(nodes: List[PipelineNode]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for node in nodes:
        counts[node.type] += 1
    return dict(counts)
