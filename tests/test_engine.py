"""DAG工作流引擎端到端测试"""
import asyncio
import time
import sys

import pytest

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dag_engine import (
    DAGWorkflowEngine,
    NodeExecutor,
    NodeResult,
    NodeStatus,
    WorkflowDefinition,
    WorkflowNode,
    WorkflowState,
    load_workflow,
)


class DiagnosticNodeExecutor(NodeExecutor):
    """模拟诊断执行器"""

    def __init__(self):
        self.execution_log: list[str] = []

    async def execute(self, node: WorkflowNode) -> NodeResult:
        self.execution_log.append(node.id)
        action = node.action

        if action == "check_service":
            service = node.params.get("service", "unknown")
            await asyncio.sleep(0.05)
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.COMPLETED,
                output={"service": service, "status": "healthy"},
            )

        elif action == "check_memory":
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.COMPLETED,
                output={"facts": 107, "health": "good"},
            )

        elif action == "restart_service":
            await asyncio.sleep(0.05)
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.COMPLETED,
                output={"restarted": True},
            )

        elif action == "generate_report":
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.COMPLETED,
                output={"report_path": "/tmp/report.md"},
            )

        elif action == "fail_test":
            if not hasattr(self, "_fail_count"):
                self._fail_count = {}
            count = self._fail_count.get(node.id, 0) + 1
            self._fail_count[node.id] = count
            if count < node.max_retries:
                return NodeResult(
                    node_id=node.id,
                    status=NodeStatus.FAILED,
                    error=f"Simulated failure #{count}",
                )
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.COMPLETED,
                output={"recovered_after": count},
            )

        return NodeResult(
            node_id=node.id,
            status=NodeStatus.COMPLETED,
            output={"action": action},
        )

    async def on_approval_response(self, node, approved, reason=""):
        if not approved:
            return NodeResult(
                node_id=node.id,
                status=NodeStatus.FAILED,
                error=f"Human rejected: {reason}",
            )
        return None


# ─── 测试 ─────────────────────────────────────

@pytest.fixture
def executor():
    return DiagnosticNodeExecutor()


async def test_basic_workflow(executor):
    """基本工作流：拓扑排序 + 并行执行"""
    workflow = WorkflowDefinition(
        id="test-basic",
        name="基础健康检查",
        nodes=[
            WorkflowNode(id="check-gw", name="检查Gateway", action="check_service",
                         params={"service": "gateway"}),
            WorkflowNode(id="check-llm", name="检查LLM", action="check_service",
                         params={"service": "llm"}),
            WorkflowNode(id="check-mem", name="检查记忆", action="check_memory",
                         depends_on=["check-gw"]),
            WorkflowNode(id="check-skills", name="检查技能", action="check_service",
                         depends_on=["check-gw"]),
            WorkflowNode(id="report", name="生成报告", action="generate_report",
                         depends_on=["check-mem", "check-skills", "check-llm"]),
        ],
    )

    engine = DAGWorkflowEngine(workflow)
    state = await engine.run(executor)

    assert state.status == "completed"
    assert len(state.node_results) == 5
    assert state.node_results["check-gw"].status == NodeStatus.COMPLETED
    assert state.node_results["report"].status == NodeStatus.COMPLETED

    # 验证拓扑排序
    assert executor.execution_log.index("check-gw") < executor.execution_log.index("check-mem")
    assert executor.execution_log.index("check-gw") < executor.execution_log.index("check-skills")


async def test_approval_workflow(executor):
    """审批暂停/恢复"""
    workflow = WorkflowDefinition(
        id="test-approval",
        name="带审批的诊断",
        nodes=[
            WorkflowNode(id="check", name="检查", action="check_service",
                         params={"service": "gateway"}),
            WorkflowNode(id="restart", name="重启", action="restart_service",
                         depends_on=["check"],
                         requires_approval=True,
                         approval_reason="重启会导致中断"),
            WorkflowNode(id="verify", name="验证", action="check_service",
                         depends_on=["restart"]),
        ],
    )

    engine = DAGWorkflowEngine(workflow)
    run_task = asyncio.create_task(engine.run(executor))
    await asyncio.sleep(0.3)

    restart_result = engine.state.node_results.get("restart")
    assert restart_result is not None
    assert restart_result.status == NodeStatus.NEEDS_APPROVAL

    engine.approve_node("restart", approved=True)
    state = await run_task

    assert state.status == "completed"
    assert state.node_results["restart"].status == NodeStatus.COMPLETED
    assert state.node_results["verify"].status == NodeStatus.COMPLETED


async def test_approval_reject(executor):
    """审批拒绝 + 级联跳过"""
    workflow = WorkflowDefinition(
        id="test-reject",
        name="审批拒绝测试",
        nodes=[
            WorkflowNode(id="check", name="检查", action="check_service",
                         params={"service": "gateway"}),
            WorkflowNode(id="danger", name="危险操作", action="restart_service",
                         depends_on=["check"],
                         requires_approval=True,
                         approval_reason="会被拒绝"),
            WorkflowNode(id="after", name="后续", action="check_memory",
                         depends_on=["danger"]),
        ],
    )

    engine = DAGWorkflowEngine(workflow)
    run_task = asyncio.create_task(engine.run(executor))
    await asyncio.sleep(0.3)

    engine.approve_node("danger", approved=False, reason="不允许")
    state = await run_task

    assert state.status == "failed"
    assert state.node_results["danger"].status == NodeStatus.FAILED
    after = state.node_results.get("after")
    assert after is not None
    assert after.status == NodeStatus.SKIPPED


async def test_conditional_workflow(executor):
    """条件分支"""
    workflow = WorkflowDefinition(
        id="test-conditional",
        name="条件分支测试",
        nodes=[
            WorkflowNode(id="check", name="检查", action="check_service",
                         params={"service": "gateway"}),
            WorkflowNode(id="deep", name="深度诊断", action="check_service",
                         depends_on=["check"],
                         condition="check.status == 'completed'"),
            WorkflowNode(id="report", name="报告", action="generate_report",
                         depends_on=["deep"]),
        ],
    )

    engine = DAGWorkflowEngine(workflow)
    state = await engine.run(executor)

    assert state.status == "completed"
    assert state.node_results["deep"].status == NodeStatus.COMPLETED


async def test_conditional_skip(executor):
    """条件不满足时跳过"""
    workflow = WorkflowDefinition(
        id="test-cond-skip",
        name="条件跳过测试",
        nodes=[
            WorkflowNode(id="check", name="检查", action="check_service",
                         params={"service": "gateway"}),
            WorkflowNode(id="maybe", name="可能跳过", action="check_service",
                         depends_on=["check"],
                         condition="check.status == 'failed'"),
            WorkflowNode(id="after", name="后续", action="generate_report",
                         depends_on=["maybe"]),
        ],
    )

    engine = DAGWorkflowEngine(workflow)
    state = await engine.run(executor)

    assert state.node_results["maybe"].status == NodeStatus.SKIPPED
    assert state.node_results["after"].status == NodeStatus.SKIPPED


async def test_retry_workflow(executor):
    """失败重试"""
    workflow = WorkflowDefinition(
        id="test-retry",
        name="重试测试",
        nodes=[
            WorkflowNode(id="flaky", name="不稳定操作", action="fail_test",
                         retry_on_failure=True, max_retries=3),
        ],
    )

    engine = DAGWorkflowEngine(workflow)
    state = await engine.run(executor)

    assert state.status == "completed"
    assert state.node_results["flaky"].status == NodeStatus.COMPLETED
    assert state.node_results["flaky"].output.get("recovered_after") == 3


async def test_yaml_loading(executor):
    """YAML加载"""
    yaml_str = """
workflow:
  id: hermes-health
  name: Hermes 全链路健康检查
  max_parallel: 2
  nodes:
    - id: check-gateway
      name: 检查Gateway
      action: check_service
      params:
        service: gateway
    - id: check-llm
      name: 检查LLM
      action: check_service
      params:
        service: llm
    - id: check-memory
      name: 检查记忆
      action: check_memory
      depends_on:
        - check-gateway
    - id: report
      name: 生成报告
      action: generate_report
      depends_on:
        - check-memory
        - check-llm
"""
    workflow = load_workflow(yaml_str)
    assert workflow.id == "hermes-health"
    assert len(workflow.nodes) == 4
    assert workflow.max_parallel == 2

    engine = DAGWorkflowEngine(workflow)
    state = await engine.run(executor)

    assert state.status == "completed"
    assert len(state.node_results) == 4


async def test_dag_validation():
    """DAG验证：检测无效依赖和环"""
    # 无效依赖
    workflow = WorkflowDefinition(
        id="test-invalid",
        name="Invalid",
        nodes=[
            WorkflowNode(id="a", action="x", depends_on=["nonexistent"]),
        ],
    )
    errors = workflow.validate_dag()
    assert len(errors) == 1
    assert "nonexistent" in errors[0]

    # 环检测
    workflow = WorkflowDefinition(
        id="test-cycle",
        name="Cycle",
        nodes=[
            WorkflowNode(id="a", action="x", depends_on=["c"]),
            WorkflowNode(id="b", action="x", depends_on=["a"]),
            WorkflowNode(id="c", action="x", depends_on=["b"]),
        ],
    )
    errors = workflow.validate_dag()
    assert len(errors) > 0


async def test_workflow_state_properties():
    """WorkflowState属性"""
    state = WorkflowState(workflow_id="test")
    assert state.is_running
    assert not state.is_completed
    assert not state.is_failed
    assert state.pending_approvals == []
