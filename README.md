# DAG Workflow Engine

从 PRAETOR ITOps 平台提炼的轻量 DAG 工作流引擎。纯 Python，零外部服务依赖。

## 核心能力

| 能力 | 说明 |
|------|------|
| 拓扑排序 | 自动确定节点执行顺序 |
| 并行执行 | 无依赖的节点同时运行，可配置最大并行数 |
| 审批暂停 | 高危操作暂停等待人工确认，支持批准/拒绝 |
| 条件分支 | 基于上游节点结果决定是否执行 |
| 失败重试 | 指数退避，可配置最大重试次数 |
| 级联跳过 | 失败节点的下游全部标记为 SKIPPED |
| YAML/JSON 加载 | 从文件定义工作流，方便 SKILL.md 集成 |

## 快速开始

```python
import asyncio
from dag_engine import DAGWorkflowEngine, NodeExecutor, NodeResult, NodeStatus, load_workflow

# 1. 定义执行器
class MyExecutor(NodeExecutor):
    async def execute(self, node):
        if node.action == "check":
            return NodeResult(node_id=node.id, status=NodeStatus.COMPLETED, output={"ok": True})
        return NodeResult(node_id=node.id, status=NodeStatus.COMPLETED)

# 2. 从YAML加载工作流
workflow = load_workflow("""
workflow:
  id: health-check
  name: 健康检查
  nodes:
    - id: check-gw
      name: 检查Gateway
      action: check
    - id: check-llm
      name: 检查LLM
      action: check
    - id: report
      name: 生成报告
      action: check
      depends_on: [check-gw, check-llm]
""")

# 3. 运行
engine = DAGWorkflowEngine(workflow)
state = asyncio.run(engine.run(MyExecutor()))
print(engine.get_summary())
```

## 审批暂停

```python
# 高危操作节点
nodes:
  - id: restart-gw
    name: 重启Gateway
    action: restart
    requires_approval: true
    approval_reason: 重启会导致30秒中断

# 运行后暂停，外部审批
run_task = asyncio.create_task(engine.run(executor))
await asyncio.sleep(1)

# 批准
engine.approve_node("restart-gw", approved=True)

# 或拒绝（下游自动级联跳过）
engine.approve_node("restart-gw", approved=False, reason="不允许重启")
```

## 条件分支

```python
nodes:
  - id: check
    action: check_service
  - id: deep-diagnose
    action: deep_check
    depends_on: [check]
    condition: "check.status == 'completed'"
  - id: generate-report
    action: report
    depends_on: [deep-diagnose]
```

支持的条件格式：
- `node_id.status == 'completed'`
- `node_id.output.field_name == 'value'`
- `node_id.succeeded` / `node_id.failed`（简写）
- 比较运算：`==`, `!=`, `>`, `<`, `>=`, `<=`

## CLI

```bash
# 验证工作流文件
dag-engine validate workflow.yaml

# 查看工作流结构
dag-engine info workflow.yaml

# 干跑（不实际执行）
dag-engine run workflow.yaml --dry-run
```

## 项目结构

```
dag_engine/
├── __init__.py      # 公开API
├── models.py        # 数据模型（Node, Workflow, State）
├── executor.py      # 执行器抽象基类
├── conditions.py    # 条件表达式求值器
├── engine.py        # DAG引擎核心
├── loader.py        # YAML/JSON加载器
└── cli.py           # CLI接口
```

## 设计思路

从 PRAETOR ITOps Enterprise 的 Orchestrator + Agent 架构提炼，保留核心编排模式，去掉微服务/Redis/MCP Gateway：

- **拓扑排序** → 邻接表 + 入度表，BFS 释放后继
- **并行执行** → asyncio.Semaphore 控制最大并行数
- **审批暂停** → asyncio.Event 暂停/恢复节点
- **级联跳过** → 失败节点的后继 BFS 全部标记 SKIPPED
- **条件分支** → 节点就绪时检查条件表达式

## License

MIT
