from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from echo.durable_execution import LeaseToken
from echo.execution_mesh import (
    ExecutionContext,
    ExecutionTask,
    ResourceEnvelope,
    WorkerResult,
)
from echo.stateless_dispatch import (
    ContinuationCursor,
    WorkerCatalogSnapshot,
    build_dispatch_envelope,
    build_dispatch_wave,
)


@dataclass(frozen=True)
class FakeBackend:
    worker_id: str
    capabilities: frozenset[str]

    async def execute(self, task, context) -> WorkerResult:
        return WorkerResult(output={"task": task.task_id, "attempt": context.attempt})


@dataclass(frozen=True)
class FakeAssignment:
    task: ExecutionTask
    backend: FakeBackend
    token: LeaseToken
    context: ExecutionContext


def fixture_assignment(*, epoch: int = 7) -> FakeAssignment:
    task = ExecutionTask(
        task_id="counterengineer-mcp",
        operation="source.counterengineer",
        payload={"source": "mcp-2026-07-28", "mode": "mechanics"},
        required_capabilities=("research", "python"),
        workspace_id="echo:frontier:2026-08-16",
        timeout_seconds=90.0,
        resources=ResourceEnvelope(
            max_agent_steps=12, max_tool_calls=20, max_output_bytes=512_000
        ),
    )
    backend = FakeBackend("worker-a", frozenset({"python", "research", "network"}))
    token = LeaseToken(
        run_id="run-2026-08-16",
        task_id=task.task_id,
        worker_id=backend.worker_id,
        epoch=epoch,
        expires_at=datetime(2026, 8, 16, 19, 0, tzinfo=timezone.utc),
    )
    context = ExecutionContext(
        workspace_id=task.workspace_id,
        dependency_outputs={"frontier": {"events": 4}},
        dependency_terminals={"frontier": {"status": "succeeded"}},
        attempt=2,
    )
    return FakeAssignment(task=task, backend=backend, token=token, context=context)


def test_dispatch_envelope_is_self_describing_and_deterministic() -> None:
    assignment = fixture_assignment()
    first = build_dispatch_envelope(
        assignment.task, assignment.context, assignment.token, assignment.backend
    )
    second = build_dispatch_envelope(
        assignment.task, assignment.context, assignment.token, assignment.backend
    )

    assert first.as_dict() == second.as_dict()
    assert first.content_digest == second.content_digest
    assert first.dispatch_key == second.dispatch_key
    assert first.payload["mode"] == "mechanics"
    assert first.resources["max_tool_calls"] == 20
    assert first.dependency_outputs["frontier"]["events"] == 4
    assert first.route_headers()["Echo-Operation"] == "source.counterengineer"
    assert first.route_headers()["Echo-Lease-Epoch"] == "7"


def test_dispatch_rejects_worker_or_capability_mismatch() -> None:
    assignment = fixture_assignment()
    wrong_worker = FakeBackend("worker-b", frozenset({"python", "research"}))
    with pytest.raises(ValueError, match="lease token worker"):
        build_dispatch_envelope(
            assignment.task, assignment.context, assignment.token, wrong_worker
        )

    weak_worker = FakeBackend("worker-a", frozenset({"python"}))
    with pytest.raises(ValueError, match="lacks required"):
        build_dispatch_envelope(
            assignment.task, assignment.context, assignment.token, weak_worker
        )


def test_continuation_is_fenced_by_lease_epoch() -> None:
    assignment = fixture_assignment(epoch=11)
    envelope = build_dispatch_envelope(
        assignment.task, assignment.context, assignment.token, assignment.backend
    )
    cursor = ContinuationCursor(
        run_id=envelope.run_id,
        task_id=envelope.task_id,
        lease_epoch=envelope.lease_epoch,
        round_index=0,
        previous_digest="",
    )
    cursor.validate_for(envelope)
    next_cursor = cursor.advance({"need": "additional-evidence"})
    assert next_cursor.round_index == 1
    assert len(next_cursor.previous_digest) == 64

    replacement = fixture_assignment(epoch=12)
    replacement_envelope = build_dispatch_envelope(
        replacement.task,
        replacement.context,
        replacement.token,
        replacement.backend,
    )
    with pytest.raises(ValueError, match="stale continuation"):
        next_cursor.validate_for(replacement_envelope)


def test_worker_catalog_is_order_independent_and_cacheable() -> None:
    a = FakeBackend("a", frozenset({"python", "research"}))
    b = FakeBackend("b", frozenset({"rust", "wasm", "sandbox"}))
    left = WorkerCatalogSnapshot.from_backends([b, a])
    right = WorkerCatalogSnapshot.from_backends([a, b])
    assert left == right
    assert len(left.digest) == 64
    assert left.cache_hint == f'"{left.digest}"'


def test_wave_export_preserves_per_assignment_fencing() -> None:
    first = fixture_assignment(epoch=4)
    second_task = ExecutionTask(
        task_id="isolation-probe",
        operation="runtime.sandbox.probe",
        payload={"runtime": "wasmtime"},
        required_capabilities=("wasm",),
        workspace_id="echo:experiment:wasm",
    )
    second_backend = FakeBackend("worker-wasm", frozenset({"wasm", "sandbox"}))
    second = FakeAssignment(
        task=second_task,
        backend=second_backend,
        token=LeaseToken(
            run_id="run-2026-08-16",
            task_id=second_task.task_id,
            worker_id=second_backend.worker_id,
            epoch=9,
            expires_at=datetime(2026, 8, 16, 19, 5, tzinfo=timezone.utc),
        ),
        context=ExecutionContext(
            workspace_id=second_task.workspace_id,
            dependency_outputs={},
            dependency_terminals={},
            attempt=1,
        ),
    )
    wave = build_dispatch_wave([first, second])
    assert [item.task_id for item in wave] == ["counterengineer-mcp", "isolation-probe"]
    assert [item.lease_epoch for item in wave] == [4, 9]
    assert len({item.dispatch_key for item in wave}) == 2
