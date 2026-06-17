"""节点执行器抽象基类"""
from __future__ import annotations

from abc import ABC, abstractmethod

from .models import NodeResult, NodeStatus, WorkflowNode


class NodeExecutor(ABC):
    """
    节点执行器接口。
    继承此类，实现 execute() 方法来定义每个节点的具体行为。

    示例：
        class MyExecutor(NodeExecutor):
            async def execute(self, node: WorkflowNode) -> NodeResult:
                if node.action == "check":
                    return NodeResult(
                        node_id=node.id,
                        status=NodeStatus.COMPLETED,
                        output={"ok": True},
                    )
                return NodeResult(
                    node_id=node.id,
                    status=NodeStatus.FAILED,
                    error=f"Unknown action: {node.action}",
                )
    """

    @abstractmethod
    async def execute(self, node: WorkflowNode) -> NodeResult:
        """执行单个节点，返回 NodeResult"""
        ...

    async def on_approval_response(
        self, node: WorkflowNode, approved: bool, reason: str = ""
    ) -> NodeResult | None:
        """
        审批回调。
        approved=True: 返回 None 继续执行。
        approved=False: 返回失败结果。
        """
        if not approved:
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.FAILED,
                error=f"Rejected: {reason}",
            )
        return None
