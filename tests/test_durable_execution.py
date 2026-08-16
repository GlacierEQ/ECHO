from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from echo.db import get_session, init_db
from echo.durable_execution import (
    DurableEventORM,
    DurableExecutionStore,
    DurableTaskORM,
    StaleLeaseError,
)
from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState, WorkerResult


@pytest.fixture()
def store(tmp_path):
    engine = init_db(tmp_path / "durable.db")
    with get_session(engine) as session:
        yield DurableExecutionStore(session)


def mesh() -> ExecutionMesh:
    return ExecutionMesh(
        [
            ExecutionTask(
                "research",
                "research",
                required_capabilities=("reasoning",),
                priority=10,
            ),
            ExecutionTask(
                "build",
                "build",
                dependencies=("research",),
                required_capabilities=("code",),
            ),
        ],
        max_concurrency=4,
    )


def result(value: str) -> WorkerResult:
    return WorkerResult(
        output={"value": value},
        stream=(f"stream:{value}",),
        terminal={"completed": True},
        agent_steps=4,
        tool_calls=3,
    )


def test_durable_claim_requires_dependency_success_and_routes_capability(store):
    execution = mesh()
    store.ensure_run("run-1", execution)

    assert store.claim_next("run-1", "code-worker", frozenset({"code"})) is None

    lease = store.claim_next(
        "run-1", "reasoning-worker", frozenset({"reasoning"}), lease_seconds=60
    )
    assert lease is not None
    assert lease.task_id == "research"
    assert lease.epoch == 1
    store.mark_running(lease)
    store.complete(lease, result("mapped"))

    build = store.claim_next("run-1", "code-worker", frozenset({"code"}))
    assert build is not None
    assert build.task_id == "build"


def test_fencing_token_rejects_worker_after_expiry_and_reclaim(store):
    execution = ExecutionMesh([ExecutionTask("a", "run")])
    store.ensure_run("run-fence", execution)
    start = datetime(2026, 8, 16, 6, 0, tzinfo=timezone.utc)

    first = store.claim_next(
        "run-fence", "worker-a", frozenset(), lease_seconds=10, now=start
    )
    assert first is not None

    store.recover_expired("run-fence", now=start + timedelta(seconds=11))
    second = store.claim_next(
        "run-fence",
        "worker-b",
        frozenset(),
        lease_seconds=30,
        now=start + timedelta(seconds=11),
    )
    assert second is not None
    assert second.epoch == first.epoch + 1

    with pytest.raises(StaleLeaseError, match="stale"):
        store.complete(first, result("stale"), now=start + timedelta(seconds=12))

    store.complete(second, result("winner"), now=start + timedelta(seconds=12))
    restored = store.restore_mesh("run-fence")
    assert restored.results["a"].output["value"] == "winner"


def test_heartbeat_extends_live_fencing_token(store):
    execution = ExecutionMesh([ExecutionTask("a", "run")])
    store.ensure_run("run-heartbeat", execution)
    start = datetime(2026, 8, 16, 6, 0, tzinfo=timezone.utc)
    lease = store.claim_next(
        "run-heartbeat", "worker", frozenset(), lease_seconds=10, now=start
    )
    assert lease is not None

    renewed = store.heartbeat(
        lease, lease_seconds=30, now=start + timedelta(seconds=5)
    )
    assert renewed.epoch == lease.epoch
    assert renewed.expires_at == start + timedelta(seconds=35)
    store.complete(renewed, result("done"), now=start + timedelta(seconds=20))


def test_append_only_history_is_hash_chained_and_detects_tampering(store):
    execution = ExecutionMesh([ExecutionTask("a", "run")])
    store.ensure_run("run-history", execution)
    lease = store.claim_next("run-history", "worker", frozenset())
    assert lease is not None
    store.mark_running(lease)
    store.complete(lease, result("done"))

    history = store.history("run-history")
    assert [item["event_type"] for item in history] == [
        "run_created",
        "task_leased",
        "task_running",
        "task_succeeded",
    ]
    assert store.verify_history("run-history") is True

    event = store.session.scalar(
        select(DurableEventORM)
        .where(DurableEventORM.run_id == "run-history")
        .order_by(DurableEventORM.id.desc())
    )
    event.details = {"tampered": True}
    store.session.flush()
    assert store.verify_history("run-history") is False


def test_snapshot_plus_task_overlay_survives_crash_between_task_commit_and_checkpoint(store):
    execution = mesh()
    store.ensure_run("run-crash", execution)
    store.save_snapshot("run-crash", execution.snapshot())

    lease = store.claim_next(
        "run-crash", "reasoning-worker", frozenset({"reasoning"})
    )
    assert lease is not None
    store.complete(lease, result("persisted-before-crash"))
    # Deliberately do not write a second snapshot: process dies here.

    restored = store.restore_mesh("run-crash")
    assert restored.runtime["research"].state == TaskState.SUCCEEDED
    assert restored.results["research"].output["value"] == "persisted-before-crash"
    assert restored.runtime["build"].state == TaskState.PENDING


def test_definition_drift_cannot_reuse_existing_run_identity(store):
    store.ensure_run("run-definition", ExecutionMesh([ExecutionTask("a", "run")]))
    changed = ExecutionMesh([ExecutionTask("a", "different-operation")])
    with pytest.raises(ValueError, match="definition hash mismatch"):
        store.ensure_run("run-definition", changed)


def test_retry_preserves_history_and_releases_work(store):
    execution = ExecutionMesh([ExecutionTask("a", "run", max_attempts=2)])
    store.ensure_run("run-retry", execution)
    first = store.claim_next("run-retry", "worker-a", frozenset())
    assert first is not None
    assert store.fail(first, "transient", retry=True) == TaskState.PENDING.value

    second = store.claim_next("run-retry", "worker-b", frozenset())
    assert second is not None
    assert second.epoch == 2
    store.fail(second, "terminal", retry=True)

    row = store.session.scalar(
        select(DurableTaskORM).where(DurableTaskORM.run_id == "run-retry")
    )
    assert row.status == TaskState.FAILED.value
    assert store.verify_history("run-retry") is True


def test_postgres_claim_contract_contains_skip_locked():
    sql = DurableExecutionStore.postgres_claim_sql().upper()
    assert "FOR UPDATE SKIP LOCKED" in sql
