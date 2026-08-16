from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from echo.db import init_db
from echo.durable_execution import DurableExecutionStore, DurableTaskORM
from echo.durable_federation import DurableFederatedExecutor
from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState, WorkerResult


class CancellableWorker:
    worker_id = "slow-specialist"
    capabilities = frozenset({"reasoning"})
    fitness = {"reasoning": 1.0}

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = False

    async def execute(self, task, context):
        self.started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return WorkerResult(
            output={"unexpected": True},
            terminal={"completed": True},
            agent_steps=1,
            tool_calls=0,
        )


def test_heartbeat_persistence_failure_cancels_compute_and_releases_retry(tmp_path):
    engine = init_db(tmp_path / "heartbeat-failure.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        store = DurableExecutionStore(session)
        mesh = ExecutionMesh(
            [
                ExecutionTask(
                    "deep",
                    "reason",
                    required_capabilities=("reasoning",),
                    max_attempts=2,
                )
            ],
            lease_seconds=1.0,
        )
        store.ensure_run("run-heartbeat-failure", mesh)
        session.commit()
        worker = CancellableWorker()
        executor = DurableFederatedExecutor(
            store,
            "run-heartbeat-failure",
            heartbeat_interval=0.01,
        )

        original_heartbeat = store.heartbeat

        def fail_heartbeat(*args, **kwargs):
            raise RuntimeError("database heartbeat unavailable")

        store.heartbeat = fail_heartbeat  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="wave compute cancelled"):
            asyncio.run(executor.run_wave([worker]))
        store.heartbeat = original_heartbeat  # type: ignore[method-assign]

        assert worker.cancelled is True
        row = session.scalar(
            select(DurableTaskORM).where(
                DurableTaskORM.run_id == "run-heartbeat-failure"
            )
        )
        assert row.status == TaskState.PENDING.value
        assert row.lease_owner == ""
        assert row.attempts == 1
        assert "heartbeat persistence failure" in row.last_error
    finally:
        session.close()


def test_explicit_zero_lease_is_rejected_instead_of_silently_using_default(tmp_path):
    engine = init_db(tmp_path / "zero-lease.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        store = DurableExecutionStore(session)
        store.ensure_run("run-zero", ExecutionMesh([ExecutionTask("a", "run")]))
        session.commit()
        with pytest.raises(ValueError, match="lease_seconds must be positive"):
            DurableFederatedExecutor(store, "run-zero", lease_seconds=0)
    finally:
        session.close()
