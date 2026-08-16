from __future__ import annotations

import asyncio

from echo.execution_mesh import ExecutionMesh, ExecutionTask, WorkerResult
from echo.federation import FederatedExecutor


class Worker:
    def __init__(self, worker_id, capabilities, fitness):
        self.worker_id = worker_id
        self.capabilities = frozenset(capabilities)
        self.fitness = fitness
        self.calls = []

    async def execute(self, task, context):
        self.calls.append(task.task_id)
        await asyncio.sleep(0.01)
        return WorkerResult(
            output={"worker": self.worker_id, "task": task.task_id},
            terminal={"ok": True},
            agent_steps=1,
            tool_calls=1,
        )


def test_federation_routes_each_boundary_to_strongest_specialist():
    mesh = ExecutionMesh(
        [
            ExecutionTask("reason", "analyze", required_capabilities=("reasoning",)),
            ExecutionTask("kernel", "compile", required_capabilities=("gpu",)),
            ExecutionTask(
                "compose",
                "compose",
                dependencies=("reason", "kernel"),
                required_capabilities=("code",),
            ),
        ],
        max_concurrency=3,
    )
    general = Worker(
        "general",
        {"reasoning", "code", "gpu"},
        {"reasoning": 0.6, "code": 0.7, "gpu": 0.2},
    )
    reasoning = Worker(
        "reasoning-specialist",
        {"reasoning", "code"},
        {"reasoning": 1.0, "code": 0.85},
    )
    gpu = Worker("gpu-specialist", {"gpu"}, {"gpu": 1.0})

    checkpoints = []
    result = asyncio.run(
        FederatedExecutor(mesh).run_to_completion(
            [general, reasoning, gpu],
            checkpoint=checkpoints.append,
        )
    )

    assert reasoning.calls == ["reason", "compose"]
    assert gpu.calls == ["kernel"]
    assert general.calls == []
    assert result["succeeded"] == ["compose", "kernel", "reason"]
    assert result["waves"] == 2
    assert len(checkpoints) == 2
    assert checkpoints[-1].digest == mesh.snapshot().digest


def test_federation_reports_unroutable_work_without_fake_execution():
    mesh = ExecutionMesh(
        [ExecutionTask("formal", "prove", required_capabilities=("lean",))]
    )
    python_worker = Worker("python", {"python"}, {"python": 1.0})

    result = asyncio.run(
        FederatedExecutor(mesh).run_to_completion([python_worker])
    )

    assert result["succeeded"] == []
    assert result["unroutable"] == ["formal"]
    assert python_worker.calls == []


def test_federation_parallelizes_across_worker_types_in_same_wave():
    mesh = ExecutionMesh(
        [
            ExecutionTask("a", "reason", required_capabilities=("reasoning",)),
            ExecutionTask("b", "gpu", required_capabilities=("gpu",)),
        ],
        max_concurrency=2,
    )
    reasoning = Worker("r", {"reasoning"}, {"reasoning": 1.0})
    gpu = Worker("g", {"gpu"}, {"gpu": 1.0})

    assignments = asyncio.run(
        FederatedExecutor(mesh).run_wave([reasoning, gpu])
    )
    mapping = {item.task.task_id: item.backend.worker_id for item in assignments}
    assert mapping == {"a": "r", "b": "g"}
