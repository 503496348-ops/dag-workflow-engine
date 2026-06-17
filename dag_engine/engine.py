"""DAG工作流引擎核心"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict

from .models import (
    NodeResult,
    NodeStatus,
    WorkflowDefinition,
    WorkflowNode,
    WorkflowState,
)
from .conditions import ConditionEvaluator
from .executor import NodeExecutor

logger = logging.getLogger(__name__)


class DAGWorkflowEngine:
    """
    轻量DAG工作流引擎。

    核心循环：拓扑排序 → 并行执行就绪节点 → 审批暂停 → 条件分支 → 级联跳过

    用法：
        engine = DAGWorkflowEngine(workflow_definition)
        state = await engine.run(executor)

    审批暂停：
        engine.approve_node("node_id", approved=True)

    获取摘要：
        print(engine.get_summary())
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
        """构建邻接表和入度表"""
        for node in self.workflow.nodes:
            self._node_map[node.id] = node
            self._in_degree.setdefault(node.id, 0)
        for node in self.workflow.nodes:
            for dep in node.depends_on:
                if dep not in self._node_map:
                    raise ValueError(
                        f"Node '{node.id}' depends on unknown node '{dep}'"
                    )
                self._adj[dep].append(node.id)
                self._in_degree[node.id] = self._in_degree.get(node.id, 0) + 1

    def get_ready_nodes(self) -> list[str]:
        """获取所有入度为0且未执行的节点"""
        ready = []
        for node_id, degree in self._in_degree.items():
            if degree != 0 or node_id in self.state.node_results:
                continue
            node = self._node_map[node_id]
            if node.condition:
                if not self._evaluator.evaluate(
                    node.condition, self.state.node_results
                ):
                    self.state.node_results[node_id] = NodeResult(
                        node_id=node_id, status=NodeStatus.SKIPPED,
                        error=f"Skipped: condition not met ({node.condition})",
                    )
                    self._skip_descendants(node_id)
                    continue
            ready.append(node_id)
        return ready

    def _release_successors(self, completed_node_id: str):
        """完成一个节点后，释放后继节点的入度"""
        for successor in self._adj.get(completed_node_id, []):
            self._in_degree[successor] = max(0, self._in_degree[successor] - 1)

    def _skip_descendants(self, failed_node_id: str):
        """失败节点的后继全部标记为跳过（级联）"""
        visited = set()
        queue = list(self._adj.get(failed_node_id, []))
        while queue:
            nid = queue.pop(0)
            if nid in visited:
                continue
            visited.add(nid)
            if nid not in self.state.node_results:
                self.state.node_results[nid] = NodeResult(
                    node_id=nid,
                    status=NodeStatus.SKIPPED,
                    error=f"Skipped: upstream '{failed_node_id}' failed",
                )
                queue.extend(self._adj.get(nid, []))

    async def execute_node(
        self, node_id: str, executor: NodeExecutor
    ) -> NodeResult:
        """执行单个节点，含审批检查和重试逻辑"""
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
                    node_id=node_id,
                    status=NodeStatus.FAILED,
                    error=f"Timeout after {node.timeout_seconds}s (attempt {attempt + 1})",
                )
            except Exception as e:
                last_result = NodeResult(
                    node_id=node_id,
                    status=NodeStatus.FAILED,
                    error=f"{type(e).__name__}: {e} (attempt {attempt + 1})",
                )
            if attempt < max_attempts - 1:
                wait = 2 ** attempt
                logger.info(f"[{node_id}] Retry {attempt + 1}/{max_attempts} in {wait}s")
                await asyncio.sleep(wait)

        return last_result or NodeResult(
            node_id=node_id, status=NodeStatus.FAILED, error="Unknown error"
        )

    async def _handle_approval(
        self, node: WorkflowNode, executor: NodeExecutor
    ) -> NodeResult | None:
        """处理审批暂停。返回 None = 继续执行。"""
        logger.info(f"[{node.id}] Requires approval: {node.approval_reason}")
        self.state.node_results[node.id] = NodeResult(
            node_id=node.id,
            status=NodeStatus.NEEDS_APPROVAL,
            approval_payload={
                "node_id": node.id,
                "action": node.action,
                "reason": node.approval_reason,
                "params": node.params,
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

    def approve_node(
        self, node_id: str, approved: bool = True, reason: str = ""
    ):
        """外部调用：审批/拒绝一个暂停的节点"""
        if node_id not in self._paused_events:
            logger.warning(f"No paused node '{node_id}' to approve")
            return
        result = self.state.node_results.get(node_id)
        if result:
            result.approval_payload = {
                "approved": approved,
                "rejection_reason": reason,
            }
        self._paused_events[node_id].set()

    async def run(self, executor: NodeExecutor) -> WorkflowState:
        """
        执行整个DAG工作流。
        核心循环：获取就绪节点 → 并行执行 → 更新状态 → 重复
        """
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
                logger.info(
                    f"[{node_id}] {result.status.value} ({result.duration_ms}ms)"
                )
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

        failed = [
            r for r in self.state.node_results.values()
            if r.status == NodeStatus.FAILED
        ]
        self.state.status = "failed" if failed else "completed"
        self.state.completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self.state

    def get_summary(self) -> str:
        """生成人类可读的工作流执行摘要"""
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
            NodeStatus.COMPLETED: "✅",
            NodeStatus.FAILED: "❌",
            NodeStatus.SKIPPED: "⏭️",
            NodeStatus.NEEDS_APPROVAL: "⏳",
            NodeStatus.RUNNING: "🔄",
            NodeStatus.PENDING: "⏸️",
        }
        for node in self.workflow.nodes:
            result = self.state.node_results.get(node.id)
            if result:
                icon = icons.get(result.status, "❓")
                lines.append(
                    f"- {icon} {node.name} ({result.status.value}, {result.duration_ms}ms)"
                )
                if result.error:
                    lines.append(f"  Error: {result.error}")
            else:
                lines.append(f"- ⏸️ {node.name} (not executed)")
        return "\n".join(lines)
