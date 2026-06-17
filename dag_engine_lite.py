"""
DAG Workflow Engine Lite v1.0
=============================
从 PRAETOR ITOps 提炼的轻量DAG工作流引擎。
嵌入白龙马医生 SKILL.md 体系，零外部服务依赖。

核心能力：
  1. 拓扑排序 + 并行执行无依赖节点
  2. 审批暂停/恢复（高危操作人工确认）
  3. 条件分支（基于上游节点结果）
  4. 失败重试（指数退避）
  5. 级联跳过（失败节点的下游全部跳过）
  6. YAML/Dict 加载（方便 SKILL.md 配置）

依赖：pydantic >= 2.0, PyYAML（仅YAML加载时需要）

用法：
    from dag_engine_lite import DAGWorkflowEngine, NodeExecutor, load_workflow

    # 定义执行器
    class MyExecutor(NodeExecutor):
        async def execute(self, node):
            ...

    # 从YAML加载并运行
    workflow = load_workflow(yaml_str)
    engine = DAGWorkflowEngine(workflow)
    state = await engine.run(MyExecutor())
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

__version__ = "1.0.0"
__all__ = [
    "NodeStatus", "WorkflowNode", "WorkflowDefinition",
    "NodeResult", "WorkflowState", "NodeExecutor",
    "ConditionEvaluator", "DAGWorkflowEngine",
    "load_workflow", "load_workflow_from_dict",
]


# ─── 数据模型 ───────────────────────────────────

class NodeStatus(str, Enum):
    """节点状态"""
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


# ─── 节点执行器 ─────────────────────────────────

class NodeExecutor(ABC):
    """
    节点执行器接口。
    继承此类，实现 execute() 方法来定义每个节点的具体行为。
    """

    @abstractmethod
    async def execute(self, node: WorkflowNode) -> NodeResult:
        """执行单个节点，返回 NodeResult"""
        ...

    async def on_approval_response(
        self, node: WorkflowNode, approved: bool, reason: str = ""
    ) -> NodeResult | None:
        """审批回调。返回 None 表示继续正常执行。"""
        if not approved:
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.FAILED,
                error=f"Rejected: {reason}",
            )
        return None


# ─── 条件求值器 ─────────────────────────────────

class ConditionEvaluator:
    """
    评估节点条件表达式。
    支持格式：
      - "node_id.status == 'completed'"
      - "node_id.output.field_name == 'value'"
      - "node_id.succeeded"  (shorthand)
      - "node_id.failed"    (shorthand)
    """

    @staticmethod
    def evaluate(condition: str, node_results: dict[str, NodeResult]) -> bool:
        try:
            condition = condition.strip()

            # 简写形式
            if "." in condition and " " not in condition:
                parts = condition.split(".", 1)
                node_id, field = parts
                result = node_results.get(node_id)
                if not result:
                    return False
                if field == "succeeded":
                    return result.status == NodeStatus.COMPLETED
                if field == "failed":
                    return result.status == NodeStatus.FAILED

            # 完整表达式
            for op in ["==", "!=", ">=", "<=", ">", "<"]:
                if op in condition:
                    left, right = condition.split(op, 1)
                    left, right = left.strip(), right.strip().strip(chr(39) + chr(34))

                    parts = left.split(".", 1)
                    node_id = parts[0]
                    field = parts[1] if len(parts) > 1 else "status"

                    result = node_results.get(node_id)
                    if not result:
                        return False

                    if field == "status":
                        actual = result.status.value
                    elif field.startswith("output."):
                        key = field.split(".", 1)[1]
                        actual = str(result.output.get(key, ""))
                    else:
                        actual = str(getattr(result, field, ""))

                    if op == "==":   return actual == right
                    elif op == "!=": return actual != right
                    elif op == ">":  return float(actual) > float(right)
                    elif op == "<":  return float(actual) < float(right)
                    elif op == ">=": return float(actual) >= float(right)
                    elif op == "<=": return float(actual) <= float(right)

            return True  # 无法解析时默认通过
        except Exception as e:
            logger.warning(f"Condition eval failed for '{condition}': {e}")
            return True


# ─── DAG 工作流引擎 ─────────────────────────────

class DAGWorkflowEngine:
    """
    轻量DAG工作流引擎。
    拓扑排序 → 并行执行 → 审批暂停 → 条件分支 → 级联跳过。
    """

    def __init__(self, workflow: WorkflowDefinition):
        self.workflow = workflow
        self.state = WorkflowState(
            workflow_id=workflow.id,
            started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        self._node_map: dict[str, WorkflowNode] = {}
        self._adj: dict[str, list[str]] = defaultdict(list)
        self._in_degree: dict[str, int] = {}
        self._paused_events: dict[str, asyncio.Event] = {}
        self._evaluator = ConditionEvaluator()
        self._build_graph()

    def _build_graph(self):
        for node in self.workflow.nodes:
            self._node_map[node.id] = node
            self._in_degree.setdefault(node.id, 0)
        for node in self.workflow.nodes:
            for dep in node.depends_on:
                if dep not in self._node_map:
                    raise ValueError(f"Node '{node.id}' depends on unknown node '{dep}'")
                self._adj[dep].append(node.id)
                self._in_degree[node.id] = self._in_degree.get(node.id, 0) + 1

    def get_ready_nodes(self) -> list[str]:
        ready = []
        for node_id, degree in self._in_degree.items():
            if degree != 0 or node_id in self.state.node_results:
                continue
            node = self._node_map[node_id]
            if node.condition:
                if not self._evaluator.evaluate(node.condition, self.state.node_results):
                    self.state.node_results[node_id] = NodeResult(
                        node_id=node_id, status=NodeStatus.SKIPPED,
                        error=f"Skipped: condition not met ({node.condition})",
                    )
                    self._skip_descendants(node_id)
                    continue
            ready.append(node_id)
        return ready

    def _release_successors(self, completed_node_id: str):
        for successor in self._adj.get(completed_node_id, []):
            self._in_degree[successor] = max(0, self._in_degree[successor] - 1)

    def _skip_descendants(self, failed_node_id: str):
        visited = set()
        queue = list(self._adj.get(failed_node_id, []))
        while queue:
            nid = queue.pop(0)
            if nid in visited:
                continue
            visited.add(nid)
            if nid not in self.state.node_results:
                self.state.node_results[nid] = NodeResult(
                    node_id=nid, status=NodeStatus.SKIPPED,
                    error=f"Skipped: upstream '{failed_node_id}' failed",
                )
                queue.extend(self._adj.get(nid, []))

    async def execute_node(self, node_id: str, executor: NodeExecutor) -> NodeResult:
        node = self._node_map[node_id]
        max_attempts = node.max_retries if node.retry_on_failure else 1

        if node.requires_approval:
            approval_result = await self._handle_approval(node, executor)
            if approval_result:
                return approval_result

        last_result = None
        for attempt in range(max_attempts):
            try:
                result = await asyncio.wait_for(
                    executor.execute(node), timeout=node.timeout_seconds
                )
                if result.status == NodeStatus.COMPLETED:
                    return result
                last_result = result
                if not node.retry_on_failure:
                    return result
            except asyncio.TimeoutError:
                last_result = NodeResult(
                    node_id=node_id, status=NodeStatus.FAILED,
                    error=f"Timeout after {node.timeout_seconds}s (attempt {attempt + 1})",
                )
            except Exception as e:
                last_result = NodeResult(
                    node_id=node_id, status=NodeStatus.FAILED,
                    error=f"{type(e).__name__}: {e} (attempt {attempt + 1})",
                )
            if attempt < max_attempts - 1:
                wait = 2 ** attempt
                logger.info(f"[{node_id}] Retry {attempt + 1}/{max_attempts} in {wait}s")
                await asyncio.sleep(wait)

        return last_result or NodeResult(
            node_id=node_id, status=NodeStatus.FAILED, error="Unknown error"
        )

    async def _handle_approval(self, node: WorkflowNode, executor: NodeExecutor) -> NodeResult | None:
        logger.info(f"[{node.id}] Requires approval: {node.approval_reason}")
        self.state.node_results[node.id] = NodeResult(
            node_id=node.id, status=NodeStatus.NEEDS_APPROVAL,
            approval_payload={
                "node_id": node.id, "action": node.action,
                "reason": node.approval_reason, "params": node.params,
            },
        )
        event = asyncio.Event()
        self._paused_events[node.id] = event
        await event.wait()

        result = self.state.node_results.get(node.id)
        if result and result.approval_payload:
            approved = result.approval_payload.get("approved", False)
            reason = result.approval_payload.get("rejection_reason", "")
            return await executor.on_approval_response(node, approved, reason)
        return None

    def approve_node(self, node_id: str, approved: bool = True, reason: str = ""):
        if node_id not in self._paused_events:
            logger.warning(f"No paused node '{node_id}' to approve")
            return
        result = self.state.node_results.get(node_id)
        if result:
            result.approval_payload = {"approved": approved, "rejection_reason": reason}
        self._paused_events[node_id].set()

    async def run(self, executor: NodeExecutor) -> WorkflowState:
        semaphore = asyncio.Semaphore(self.workflow.max_parallel)

        async def _run_node(node_id: str):
            async with semaphore:
                start = time.time()
                logger.info(f"[{node_id}] Starting execution")
                result = await self.execute_node(node_id, executor)
                result.duration_ms = int((time.time() - start) * 1000)
                self.state.node_results[node_id] = result
                if result.status == NodeStatus.COMPLETED:
                    self._release_successors(node_id)
                else:
                    self._skip_descendants(node_id)
                logger.info(f"[{node_id}] {result.status.value} ({result.duration_ms}ms)")
                return result

        while True:
            ready = self.get_ready_nodes()
            if not ready:
                total = len(self.workflow.nodes)
                done = len(self.state.node_results)
                if done >= total:
                    break
                if self._paused_events:
                    await asyncio.sleep(0.5)
                    continue
                logger.warning("No ready nodes but workflow incomplete")
                break
            tasks = [_run_node(nid) for nid in ready]
            await asyncio.gather(*tasks, return_exceptions=True)

        failed = [r for r in self.state.node_results.values() if r.status == NodeStatus.FAILED]
        self.state.status = "failed" if failed else "completed"
        self.state.completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self.state

    def get_summary(self) -> str:
        lines = [
            f"## Workflow: {self.workflow.name}",
            f"Status: {self.state.status}",
            f"Run ID: {self.state.run_id}",
            f"Started: {self.state.started_at}",
            f"Completed: {self.state.completed_at or 'in progress'}",
            "",
            "### Node Results",
        ]
        icons = {
            NodeStatus.COMPLETED: "✅", NodeStatus.FAILED: "❌",
            NodeStatus.SKIPPED: "⏭️", NodeStatus.NEEDS_APPROVAL: "⏳",
            NodeStatus.RUNNING: "🔄", NodeStatus.PENDING: "⏸️",
        }
        for node in self.workflow.nodes:
            result = self.state.node_results.get(node.id)
            if result:
                icon = icons.get(result.status, "❓")
                lines.append(f"- {icon} {node.name} ({result.status.value}, {result.duration_ms}ms)")
                if result.error:
                    lines.append(f"  Error: {result.error}")
            else:
                lines.append(f"- ⏸️ {node.name} (not executed)")
        return "\n".join(lines)


# ─── 加载器 ─────────────────────────────────────

def load_workflow_from_dict(data: dict) -> WorkflowDefinition:
    """从字典加载工作流定义"""
    wf_data = data.get("workflow", data)
    nodes = []
    for n in wf_data.get("nodes", []):
        nodes.append(WorkflowNode(
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
        ))
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
