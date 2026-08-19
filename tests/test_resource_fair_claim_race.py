from __future__ import annotations

import asyncio

from sqlalchemy import select
from sqlalchemy.orm import Session

from echo.db import init_db
from echo.durable_execution import DurableEventORM, DurableExecutionStore, DurableTaskORM
from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState, WorkerResult
from echo.resource_fair_execution import ResourceFairDurableExecutor
from echo.resource_scheduler import SchedulingIntent, schedule_task


class Worker:
    worker_id = "gpu"
    capabilities = frozenset({"gpu"})
    fitness = {"gpu": 1.0}
    resources = {"gpu": 2.0}
    used_resources = {}
    topology = {}
    worker_version = "v1"
    draining = False
    inflight = 0
    max_inflight = 2

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(self, task, context):
        self.calls.append(task.task_id)
        return WorkerResult(
            output={"task": task.task_id},
            terminal={"completed": True},
            agent_steps=1,
            tool_calls=0,
        )


class AlwaysLoseSecondGangClaim(ResourceFairDurableExecutor):
    """Simulate another consumer winning g2 after in-memory gang planning."""

    def _claim_exact(self, task, backend, *, now=None):
        if task.task_id == "g2":
            return None
        return super()._claim_exact(task, backend, now=now)


def gang_task(task_id: str):
    return schedule_task(
        ExecutionTask(task_id, "gpu", required_capabilities=("gpu",)),
        SchedulingIntent(
            fairness_key="gpu-gang",
            resources={"gpu": 1},
            placement_group="gang",
            gang=True,
        ),
    )


def test_gang_claim_race_rolls_back_every_member_and_executes_nothing(tmp_path):
    engine = init_db(tmp_path / "gang-race.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        store = DurableExecutionStore(session)
        mesh = ExecutionMesh(
            [gang_task("g1"), gang_task("g2")],
            max_concurrency=2,
            lease_seconds=30,
        )
        store.ensure_run("run-gang-race", mesh)
        session.commit()
        worker = Worker()

        result = asyncio.run(
            AlwaysLoseSecondGangClaim(
                store,
                "run-gang-race",
                max_claim_replans=3,
            ).run_to_completion([worker])
        )

        assert result["succeeded"] == []
        assert result["incomplete"] == ["g1", "g2"]
        assert result["scheduler_deferred"] == ["g1", "g2"]
        assert worker.calls == []

        rows = session.scalars(
            select(DurableTaskORM)
            .where(DurableTaskORM.run_id == "run-gang-race")
            .order_by(DurableTaskORM.task_id)
            .execution_options(populate_existing=True)
        ).all()
        assert [row.status for row in rows] == [
            TaskState.PENDING.value,
            TaskState.PENDING.value,
        ]
        assert [row.attempts for row in rows] == [0, 0]
        assert [row.lease_owner for row in rows] == ["", ""]

        lease_or_schedule_events = session.scalars(
            select(DurableEventORM).where(
                DurableEventORM.run_id == "run-gang-race",
                DurableEventORM.event_type.in_(
                    ["task_leased", "resource_fair_scheduled"]
                ),
            )
        ).all()
        assert lease_or_schedule_events == []
    finally:
        session.close()
