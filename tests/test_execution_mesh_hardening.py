from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from echo.execution_mesh import (
    ExecutionMesh,
    ExecutionSnapshot,
    ExecutionTask,
    TaskState,
    WorkerResult,
)


class Worker:
    worker_id = "worker"
    capabilities = frozenset({"code"})

    async def execute(self, task, context):
        return WorkerResult(
            output={"task": task.task_id},
            terminal={"ok": True},
            agent_steps=1,
            tool_calls=1,
        )


def test_stale_lease_honors_attempt_budget_and_fails_terminally():
    clock = [100.0]
    mesh = ExecutionMesh(
        [ExecutionTask("a", "run", max_attempts=1)],
        lease_seconds=5,
        clock=lambda: clock[0],
    )
    task = mesh.tasks["a"]
    mesh._lease(task, "dead")
    mesh.runtime["a"].state = TaskState.RUNNING
    mesh.runtime["a"].attempts = 1
    clock[0] = 106.0

    assert mesh.recover_stale_leases() == ("a",)
    assert mesh.runtime["a"].state == TaskState.FAILED
    assert mesh.receipts[-1].outcome == "failed"


def test_cancelled_execution_clears_running_lease_and_preserves_retry():
    started = asyncio.Event()

    class SlowWorker(Worker):
        async def execute(self, task, context):
            started.set()
            await asyncio.sleep(60)
            return WorkerResult(output={"never": True})

    async def scenario():
        mesh = ExecutionMesh([ExecutionTask("a", "run", max_attempts=2)])
        task = mesh.tasks["a"]
        mesh._lease(task, "worker")
        execution = asyncio.create_task(mesh._execute_one(SlowWorker(), task))
        await started.wait()
        execution.cancel()
        with pytest.raises(asyncio.CancelledError):
            await execution
        return mesh

    mesh = asyncio.run(scenario())
    assert mesh.runtime["a"].state == TaskState.PENDING
    assert mesh.runtime["a"].lease_owner == ""
    assert mesh.receipts[-1].outcome == "cancelled-retry"


def test_expired_attempt_cannot_commit_stale_output():
    clock = [100.0]

    class ExpiringWorker(Worker):
        async def execute(self, task, context):
            clock[0] = 200.0
            return await super().execute(task, context)

    mesh = ExecutionMesh(
        [ExecutionTask("a", "run", max_attempts=2)],
        lease_seconds=10,
        clock=lambda: clock[0],
    )
    asyncio.run(mesh.run_wave(ExpiringWorker()))

    assert "a" not in mesh.results
    assert mesh.runtime["a"].state == TaskState.PENDING
    assert "stale execution result rejected" in mesh.runtime["a"].last_error


def test_snapshot_uses_relative_lease_time_across_clock_domains():
    source_clock = [1_000.0]
    mesh = ExecutionMesh(
        [ExecutionTask("a", "run")],
        lease_seconds=30,
        clock=lambda: source_clock[0],
    )
    mesh._lease(mesh.tasks["a"], "worker")
    mesh.runtime["a"].state = TaskState.RUNNING
    mesh.runtime["a"].attempts = 1
    source_clock[0] = 1_010.0
    snapshot = mesh.snapshot()

    target_clock = [9_000_000.0]
    restored = ExecutionMesh.from_snapshot(snapshot, clock=lambda: target_clock[0])
    assert restored.runtime["a"].state == TaskState.RUNNING
    assert restored.runtime["a"].lease_expires_at == pytest.approx(9_000_020.0)


def _redigest(payload):
    import hashlib
    import json

    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def test_rehydrate_rejects_succeeded_task_without_result_even_if_snapshot_redigested():
    mesh = ExecutionMesh([ExecutionTask("a", "run")])
    asyncio.run(mesh.run_to_completion(Worker()))
    snapshot = mesh.snapshot()
    payload = dict(snapshot.payload)
    payload["results"] = {}
    malformed = ExecutionSnapshot(payload=payload, digest=_redigest(payload))

    with pytest.raises(ValueError, match="missing its result"):
        ExecutionMesh.from_snapshot(malformed)


def test_rehydrate_revalidates_receipt_hash_chain_even_if_snapshot_redigested():
    mesh = ExecutionMesh([ExecutionTask("a", "run")])
    asyncio.run(mesh.run_to_completion(Worker()))
    snapshot = mesh.snapshot()
    payload = dict(snapshot.payload)
    receipts = [dict(item) for item in payload["receipts"]]
    receipts[0]["content_hash"] = "0" * 64
    payload["receipts"] = receipts
    forged = ExecutionSnapshot(payload=payload, digest=_redigest(payload))

    with pytest.raises(ValueError, match="content_hash mismatch"):
        ExecutionMesh.from_snapshot(forged)
