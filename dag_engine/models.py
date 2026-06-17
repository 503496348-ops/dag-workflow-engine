"""数据模型：节点、工作流定义、执行结果、运行状态"""
from __future__ import annotations

import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class NodeStatus(str, Enum):
    """节点执行状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    NEEDS_APPROVAL = "needs_approval"
    SKIPPED = "skipped"


class WorkflowNode(BaseModel):
    """DAG中的一个节点"""
    id: str
    name: str = ""
    action: str                                    # 执行的动作类型
    params: dict[str, Any] = {}
    depends_on: list[str] = []                     # 依赖的上游节点ID
    requires_approval: bool = False                # 是否需要人工审批
    approval_reason: str = ""
    retry_on_failure: bool = False
    max_retries: int = 1
    condition: str | None = None                   # 条件表达式
    timeout_seconds: int = 300

    def model_post_init(self, __context: Any) -> None:
        if not self.name:
            self.name = self.id


class WorkflowDefinition(BaseModel):
    """完整的DAG工作流定义"""
    id: str
    name: str = ""
    description: str = ""
    nodes: list[WorkflowNode]
    global_timeout: int = 1800
    max_parallel: int = 3

    def model_post_init(self, __context: Any) -> None:
        if not self.name:
            self.name = self.id

    def validate_dag(self) -> list[str]:
        """验证DAG结构，返回错误列表（空=合法）"""
        errors = []
        node_ids = {n.id for n in self.nodes}
        for node in self.nodes:
            for dep in node.depends_on:
                if dep not in node_ids:
                    errors.append(f"Node '{node.id}' depends on unknown node '{dep}'")
        # 检测环
        visited = set()
        path = set()
        adj: dict[str, list[str]] = {}
        for node in self.nodes:
            adj[node.id] = list(node.depends_on)
        def dfs(nid: str) -> bool:
            if nid in path:
                return True
            if nid in visited:
                return False
            visited.add(nid)
            path.add(nid)
            for dep in adj.get(nid, []):
                if dfs(dep):
                    errors.append(f"Cycle detected involving node '{nid}'")
                    return True
            path.discard(nid)
            return False
        for nid in node_ids:
            dfs(nid)
        return errors


class NodeResult(BaseModel):
    """节点执行结果"""
    node_id: str
    status: NodeStatus
    output: dict[str, Any] = {}
    error: str | None = None
    duration_ms: int = 0
    approval_payload: dict[str, Any] | None = None


class WorkflowState(BaseModel):
    """工作流运行时状态"""
    workflow_id: str
    run_id: str = Field(default_factory=lambda: f"run-{uuid.uuid4().hex[:8]}")
    status: str = "running"
    node_results: dict[str, NodeResult] = {}
    started_at: str = ""
    completed_at: str | None = None

    @property
    def is_running(self) -> bool:
        return self.status == "running"

    @property
    def is_completed(self) -> bool:
        return self.status == "completed"

    @property
    def is_failed(self) -> bool:
        return self.status == "failed"

    @property
    def pending_approvals(self) -> list[str]:
        return [
            nid for nid, r in self.node_results.items()
            if r.status == NodeStatus.NEEDS_APPROVAL
        ]

    def get_output(self, node_id: str) -> dict[str, Any]:
        result = self.node_results.get(node_id)
        return result.output if result else {}
