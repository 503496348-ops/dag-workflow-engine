"""CLI接口：验证和运行工作流定义"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from .engine import DAGWorkflowEngine
from .loader import load_workflow, load_workflow_from_json
from .models import NodeResult, NodeStatus, WorkflowNode
from .executor import NodeExecutor


class CLIDryRunExecutor(NodeExecutor):
    """干跑执行器：打印节点信息但不实际执行"""

    async def execute(self, node: WorkflowNode) -> NodeResult:
        print(f"  [DRY-RUN] Would execute: {node.name} (action={node.action})")
        return NodeResult(
            node_id=node.id,
            status=NodeStatus.COMPLETED,
            output={"dry_run": True},
        )


def main():
    parser = argparse.ArgumentParser(
        prog="dag-engine",
        description="DAG Workflow Engine — validate and run workflow definitions",
    )
    sub = parser.add_subparsers(dest="command")

    # validate
    p_val = sub.add_parser("validate", help="Validate a workflow YAML/JSON file")
    p_val.add_argument("file", help="Path to workflow file (.yaml/.json)")

    # run
    p_run = sub.add_parser("run", help="Run a workflow (requires executor module)")
    p_run.add_argument("file", help="Path to workflow file (.yaml/.json)")
    p_run.add_argument("--dry-run", action="store_true", help="Dry run (no real execution)")
    p_run.add_argument("--parallel", type=int, default=None, help="Max parallel nodes")

    # info
    p_info = sub.add_parser("info", help="Show workflow structure")
    p_info.add_argument("file", help="Path to workflow file (.yaml/.json)")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not args.command:
        parser.print_help()
        sys.exit(1)

    path = Path(args.file)
    content = path.read_text(encoding="utf-8")

    if path.suffix in (".yaml", ".yml"):
        workflow = load_workflow(content)
    elif path.suffix == ".json":
        workflow = load_workflow_from_json(content)
    else:
        print(f"Unsupported file format: {path.suffix}")
        sys.exit(1)

    if args.command == "validate":
        errors = workflow.validate_dag()
        if errors:
            print("❌ Validation failed:")
            for e in errors:
                print(f"  - {e}")
            sys.exit(1)
        print(f"✅ Valid workflow: {workflow.name} ({len(workflow.nodes)} nodes)")

    elif args.command == "info":
        errors = workflow.validate_dag()
        print(f"Workflow: {workflow.name}")
        print(f"ID: {workflow.id}")
        if workflow.description:
            print(f"Description: {workflow.description}")
        print(f"Nodes: {len(workflow.nodes)}")
        print(f"Max parallel: {workflow.max_parallel}")
        print(f"Global timeout: {workflow.global_timeout}s")
        if errors:
            print(f"\n⚠️  Validation errors:")
            for e in errors:
                print(f"  - {e}")
        print(f"\nNodes:")
        for node in workflow.nodes:
            deps = f" (depends: {', '.join(node.depends_on)})" if node.depends_on else ""
            approval = " 🔒" if node.requires_approval else ""
            cond = f" [if: {node.condition}]" if node.condition else ""
            print(f"  - {node.id}: {node.name} [action={node.action}]{deps}{approval}{cond}")

    elif args.command == "run":
        if args.parallel:
            workflow.max_parallel = args.parallel

        if args.dry_run:
            executor = CLIDryRunExecutor()
        else:
            print("Error: Non-dry-run requires a custom executor. Use --dry-run or write a Python script.")
            sys.exit(1)

        engine = DAGWorkflowEngine(workflow)
        state = asyncio.run(engine.run(executor))
        print(f"\n{engine.get_summary()}")
        sys.exit(0 if state.status == "completed" else 1)


if __name__ == "__main__":
    main()
