# SPDX-License-Identifier: Apache-2.0
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from ray.klein.api.resource_edge import ResourceEdge
from ray.klein.api.resource_node import ResourceNode


@dataclass(slots=True)
class ResourcePlan:
    """Serializable resource overrides for a stream graph."""

    nodes: dict[str, ResourceNode]
    edges: list[ResourceEdge]

    def __post_init__(self) -> None:
        self.nodes = dict(self.nodes)
        self.edges = list(self.edges)

    def __getitem__(self, node_name: str) -> ResourceNode:
        return self.nodes[node_name]

    def update_node(
        self,
        node_name: str,
        **overrides: Any,
    ) -> ResourceNode:
        """Replace one node through its validated immutable contract.

        Only execution-tuning fields may change; graph identity stays fixed.
        Passing ``None`` explicitly clears an existing override.
        """

        current = self.nodes[node_name]
        allowed = {"num_cpus", "num_gpus", "concurrency", "batch_size", "async_buffer_size"}
        unknown = overrides.keys() - allowed
        if unknown:
            raise TypeError(f"unsupported resource overrides: {', '.join(sorted(unknown))}")
        updated = replace(current, **overrides)
        self.nodes[node_name] = updated
        return updated

    def write(self, file_path: str | Path) -> None:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def from_json(cls, value: str) -> "ResourcePlan":
        payload = json.loads(value)
        if not isinstance(payload, dict):
            raise ValueError("resource plan must be a JSON object")

        nodes: dict[str, ResourceNode] = {}
        for raw_node in payload.get("nodes", []):
            if not isinstance(raw_node, dict):
                raise ValueError("resource plan nodes must be JSON objects")
            node_data: dict[str, Any] = dict(raw_node)
            concurrency = node_data.get("concurrency")
            if isinstance(concurrency, list):
                node_data["concurrency"] = tuple(concurrency)
            node = ResourceNode(**node_data)
            if node.key in nodes:
                raise ValueError(f"duplicate resource node {node.key!r}")
            nodes[node.key] = node

        raw_edges = payload.get("edges", [])
        if not isinstance(raw_edges, list):
            raise ValueError("resource plan edges must be a JSON array")
        if not all(isinstance(edge, dict) for edge in raw_edges):
            raise ValueError("resource plan edges must be JSON objects")
        edges = [ResourceEdge(**edge) for edge in raw_edges]
        return cls(nodes, edges)

    @classmethod
    def read(cls, file_path: str | Path) -> "ResourcePlan":
        return cls.from_json(Path(file_path).read_text(encoding="utf-8"))

    def is_compatible_with(self, other: "ResourcePlan") -> bool:
        return self.nodes.keys() == other.nodes.keys()

    def to_json(self) -> str:
        payload = {
            "nodes": [asdict(node) for node in self.nodes.values()],
            "edges": [asdict(edge) for edge in self.edges],
        }
        return json.dumps(payload, indent=2)

    def __str__(self) -> str:
        return self.to_json()
