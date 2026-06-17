"""工作流定义加载器：YAML / JSON / Dict"""
from __future__ import annotations

import json
from typing import Any

from .models import WorkflowDefinition, WorkflowNode


def load_workflow_from_dict(data: dict[str, Any]) -> WorkflowDefinition:
    """从字典加载工作流定义"""
    wf_data = data.get("workflow", data)
    nodes = []
    for n in wf_data.get("nodes", []):
        nodes.append(
            WorkflowNode(
                id=n["id"],
                name=n.get("name", n["id"]),
                action=n["action"],
                params=n.get("params", {}),
                depends_on=n.get("depends_on", []),
                requires_approval=n.get("requires_approval", False),
                approval_reason=n.get("approval_reason", ""),
                retry_on_failure=n.get("retry_on_failure", False),
                max_retries=n.get("max_retries", 1),
                condition=n.get("condition"),
                timeout_seconds=n.get("timeout_seconds", 300),
            )
        )
    return WorkflowDefinition(
        id=wf_data["id"],
        name=wf_data.get("name", wf_data["id"]),
        description=wf_data.get("description", ""),
        nodes=nodes,
        global_timeout=wf_data.get("global_timeout", 1800),
        max_parallel=wf_data.get("max_parallel", 3),
    )


def load_workflow(yaml_str: str) -> WorkflowDefinition:
    """从YAML字符串加载工作流定义"""
    import yaml

    data = yaml.safe_load(yaml_str)
    return load_workflow_from_dict(data)


def load_workflow_from_json(json_str: str) -> WorkflowDefinition:
    """从JSON字符串加载工作流定义"""
    data = json.loads(json_str)
    return load_workflow_from_dict(data)
