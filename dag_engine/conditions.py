"""条件表达式求值器"""
from __future__ import annotations

import logging
from typing import Any

from .models import NodeResult, NodeStatus

logger = logging.getLogger(__name__)


class ConditionEvaluator:
    """
    评估节点的条件表达式。

    支持格式：
      - "node_id.status == 'completed'"
      - "node_id.output.field_name == 'value'"
      - "node_id.succeeded"  (shorthand)
      - "node_id.failed"    (shorthand)
      - "node_id.output.count > 5"
    """

    OPERATORS = ["==", "!=", ">=", "<=", ">", "<"]

    @staticmethod
    def evaluate(condition: str, node_results: dict[str, NodeResult]) -> bool:
        try:
            condition = condition.strip()

            # 简写形式：node_id.succeeded / node_id.failed
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
            for op in ConditionEvaluator.OPERATORS:
                if op in condition:
                    left, right = condition.split(op, 1)
                    left = left.strip()
                    right = right.strip().strip("'\"")

                    parts = left.split(".", 1)
                    node_id = parts[0]
                    field = parts[1] if len(parts) > 1 else "status"

                    result = node_results.get(node_id)
                    if not result:
                        return False

                    actual = ConditionEvaluator._resolve_field(result, field)

                    return ConditionEvaluator._compare(actual, right, op)

            return True  # 无法解析时默认通过
        except Exception as e:
            logger.warning(f"Condition eval failed: {e}")
            return True

    @staticmethod
    def _resolve_field(result: NodeResult, field: str) -> Any:
        if field == "status":
            return result.status.value
        if field.startswith("output."):
            key = field.split(".", 1)[1]
            return result.output.get(key, "")
        return str(getattr(result, field, ""))

    @staticmethod
    def _compare(actual: Any, expected: str, op: str) -> bool:
        if op == "==":
            return str(actual) == expected
        if op == "!=":
            return str(actual) != expected
        try:
            a, b = float(actual), float(expected)
            if op == ">":
                return a > b
            if op == "<":
                return a < b
            if op == ">=":
                return a >= b
            if op == "<=":
                return a <= b
        except (ValueError, TypeError):
            return False
        return False
