from __future__ import annotations

import asyncio

import pytest

from echo.execution_mesh import (
    ExecutionMesh,
    ExecutionTask,
    ResourceEnvelope,
    TaskState,
    WorkerResult,
)


class FakeWorker:
    def __init__(self, *, fail: set[str] | None = None) -> None:
        self.worker_id = "fake-worker"
        self.capabilities = frozenset({"analysis", "code", "network"})
        self.fail = fail or set()
        self.active = 0
        self.max_active = 0
        self.contexts = {}

    async def execute(self, task, context):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.contexts[task.task_id] = context
        try:
            await asyncio.sleep(0.01)
            if task.task_id in self.fail:
                raise RuntimeError(f"boom:{task.task_id}")
            return WorkerResult(
                output={"task": task.task_id, "value": task.payload.get("value", 1)},
                stream=(f"stream:{task.task_id}",),
                terminal={"completed": True, "task": task.task_id},
                agent_steps=2,
                tool_calls=1,
            )
        finally:
            self.active -= 1


def test_parallel_dag_and_direct_dependency_chaining():
    mesh = ExecutionMesh(
        [
            ExecutionTask("research-a", "research", payload={"value": 11}),
            ExecutionTask("research-b", "research", payload={"value": 22}),
            ExecutionTask(
                "synthesize",
                "synthesize",
                dependencies=("research-a", "research-b"),
                priority=10,
            ),
        ],
        max_concurrency=2,
    )
    worker = FakeWorker()
    result = asyncio.run(mesh.run_to_completion(worker))

    assert result["succeeded"] == ["research-a", "research-b", "synthesize"]
    assert result["failed"] == []
    assert result["blocked"] == []
    assert worker.max_active == 2
    synthesis = worker.contexts["synthesize"]
    assert synthesis.dependency_outputs["research-a"]["value"] == 11
    assert synthesis.dependency_outputs["research-b"]["value"] == 22
    assert synthesis.dependency_terminals["research-a"]["completed"] is True


def test_snapshot_rehydrates_and_continues_with_replacement_worker():
    mesh = ExecutionMesh(
        [
            ExecutionTask("map", "map"),
            ExecutionTask("build", "build", dependencies=("map",)),
        ],
        max_concurrency=1,
    )
    first_worker = FakeWorker()
    assert asyncio.run(mesh.run_wave(first_worker)) == ("map",)

    snapshot = mesh.snapshot()
    replacement = ExecutionMesh.from_snapshot(snapshot)
    second_worker = FakeWorker()
    result = asyncio.run(replacement.run_to_completion(second_worker))

    assert replacement.runtime["map"].state == TaskState.SUCCEEDED
    assert replacement.runtime["build"].state == TaskState.SUCCEEDED
    assert second_worker.contexts["build"].dependency_outputs["map"]["task"] == "map"
    assert result["receipt_head"]
    assert replacement.receipts[-1].previous_hash == replacement.receipts[-2].content_hash


def test_snapshot_tampering_fails_closed():
    mesh = ExecutionMesh([ExecutionTask("a", "run")])
    snapshot = mesh.snapshot().as_dict()
    snapshot["payload"]["max_concurrency"] = 999
    with pytest.raises(ValueError, match="digest mismatch"):
        ExecutionMesh.from_snapshot(snapshot)


def test_stale_worker_lease_is_recovered_without_losing_task():
    clock = [100.0]
    mesh = ExecutionMesh(
        [ExecutionTask("a", "run")],
        lease_seconds=10,
        clock=lambda: clock[0],
    )
    task = mesh.tasks["a"]
    mesh._lease(task, "dead-worker")
    mesh.runtime["a"].state = TaskState.RUNNING
    clock[0] = 111.0

    assert mesh.recover_stale_leases() == ("a",)
    assert mesh.runtime["a"].state == TaskState.PENDING
    assert mesh.runtime["a"].last_error == "stale lease recovered"


def test_failure_isolation_blocks_only_dependent_branch():
    mesh = ExecutionMesh(
        [
            ExecutionTask("bad-root", "run", max_attempts=1),
            ExecutionTask("good-root", "run"),
            ExecutionTask("bad-child", "run", dependencies=("bad-root",)),
            ExecutionTask("good-child", "run", dependencies=("good-root",)),
        ],
        max_concurrency=4,
    )
    result = asyncio.run(mesh.run_to_completion(FakeWorker(fail={"bad-root"})))

    assert result["failed"] == ["bad-root"]
    assert result["blocked"] == ["bad-child"]
    assert result["succeeded"] == ["good-child", "good-root"]
    assert mesh.runtime["good-child"].state == TaskState.SUCCEEDED


def test_resource_envelope_is_hard_boundary_not_minimization_target():
    class HungryWorker(FakeWorker):
        async def execute(self, task, context):
            return WorkerResult(
                output={"substantial": True},
                terminal={"completed": True},
                agent_steps=9,
                tool_calls=12,
            )

    mesh = ExecutionMesh(
        [
            ExecutionTask(
                "deep-work",
                "deep",
                max_attempts=1,
                resources=ResourceEnvelope(
                    max_agent_steps=8,
                    max_tool_calls=20,
                    max_output_bytes=100_000,
                ),
            )
        ]
    )
    result = asyncio.run(mesh.run_to_completion(HungryWorker()))
    assert result["failed"] == ["deep-work"]
    assert "agent-step budget exceeded" in mesh.runtime["deep-work"].last_error


def test_capability_routing_does_not_send_task_to_incapable_worker():
    mesh = ExecutionMesh(
        [
            ExecutionTask(
                "gpu-specialist",
                "kernel",
                required_capabilities=("gpu",),
            )
        ]
    )
    result = asyncio.run(mesh.run_to_completion(FakeWorker()))
    assert result["incomplete"] == ["gpu-specialist"]
    assert mesh.runtime["gpu-specialist"].attempts == 0


def test_stream_and_terminal_completion_are_separate_state():
    mesh = ExecutionMesh([ExecutionTask("streaming", "stream")])
    asyncio.run(mesh.run_to_completion(FakeWorker()))
    result = mesh.results["streaming"]
    assert result.stream == ("stream:streaming",)
    assert result.terminal == {"completed": True, "task": "streaming"}
