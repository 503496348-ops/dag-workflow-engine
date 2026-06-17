"""
DAG Workflow Engine
===================
从 PRAETOR ITOps 提炼的轻量DAG工作流引擎。
支持拓扑排序、并行执行、审批暂停、条件分支、失败重试。

Usage:
    from dag_engine import DAGWorkflowEngine, NodeExecutor, load_workflow
"""

from .models import (
    NodeStatus,
    NodeResult,
    WorkflowNode,
    WorkflowDefinition,
    WorkflowState,
)
from .executor import NodeExecutor
from .conditions import ConditionEvaluator
from .engine import DAGWorkflowEngine
from .loader import load_workflow, load_workflow_from_dict, load_workflow_from_json

__version__ = "1.0.0"
__all__ = [
    "NodeStatus",
    "NodeResult",
    "WorkflowNode",
    "WorkflowDefinition",
    "WorkflowState",
    "NodeExecutor",
    "ConditionEvaluator",
    "DAGWorkflowEngine",
    "load_workflow",
    "load_workflow_from_dict",
    "load_workflow_from_json",
]
