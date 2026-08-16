from __future__ import annotations

import asyncio

from sqlalchemy import select
from sqlalchemy.orm import Session

from echo.db import init_db
from echo.durable_execution import DurableEventORM, DurableExecutionStore
from echo.execution_mesh import ExecutionMesh, ExecutionTask, WorkerResult
from echo.resource_fair_execution import ResourceFairDurableExecutor
from echo.resource_scheduler import ResourceFairScheduler, SchedulingIntent, schedule_task


class Worker:
    def __init__(
        self,
        worker_id,
        capabilities,
        *,
        fitness=None,
        resources=None,
        topology=None,
        worker_version="",
        draining=False,
        inflight=0,
        max_inflight=None,
    ):
        self.worker_id = worker_id
        self.capabilities = frozenset(capabilities)
        self.fitness = fitness or {}
        self.resources = resources or {}
        self.topology = topology or {}
        self.worker_version = worker_version
        self.draining = draining
        self.inflight = inflight
        self.max_inflight = max_inflight
        self.calls = []

    async def execute(self, task, context):
        self.calls.append(task.task_id)
        return WorkerResult(
            output={"worker": self.worker_id, "task": task.task_id},
            terminal={"completed": True},
            agent_steps=1,
            tool_calls=1,
        )


def test_resource_intent_survives_database_restore_and_routes_specialist(tmp_path):
    engine = init_db(tmp_path / "resource-fair.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        task = schedule_task(
            ExecutionTask("gpu-job", "kernel", required_capabilities=("gpu",)),
            SchedulingIntent(
                fairness_key="research",
                resources={"gpu": 1, "memory_gb": 24},
                topology={"region": "west"},
                required_worker_version="v2",
            ),
        )
        mesh = ExecutionMesh([task], lease_seconds=30)
        store = DurableExecutionStore(session)
        store.ensure_run("run-resource", mesh)
        session.commit()

        restored = store.restore_mesh("run-resource")
        restored_intent = SchedulingIntent.from_task(restored.tasks["gpu-job"])
        assert restored_intent.resources == {"gpu": 1.0, "memory_gb": 24.0}
        assert restored_intent.required_worker_version == "v2"

        weak = Worker(
            "weak",
            {"gpu"},
            fitness={"gpu": 2.0},
            resources={"gpu": 1, "memory_gb": 8},
            topology={"region": "west"},
            worker_version="v2",
        )
        strong = Worker(
            "strong",
            {"gpu"},
            fitness={"gpu": 1.0},
            resources={"gpu": 1, "memory_gb": 48},
            topology={"region": "west"},
            worker_version="v2",
        )
        result = asyncio.run(
            ResourceFairDurableExecutor(store, "run-resource").run_to_completion(
                [weak, strong]
            )
        )
        assert result["succeeded"] == ["gpu-job"]
        assert weak.calls == []
        assert strong.calls == ["gpu-job"]

        event = session.scalar(
            select(DurableEventORM).where(
                DurableEventORM.run_id == "run-resource",
                DurableEventORM.event_type == "resource_fair_scheduled",
            )
        )
        assert event.details["resources"] == {"gpu": 1.0, "memory_gb": 24.0}
        assert event.details["required_worker_version"] == "v2"
    finally:
        session.close()


def test_backpressure_returns_work_pending_without_fake_attempt(tmp_path):
    engine = init_db(tmp_path / "backpressure.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        task = schedule_task(
            ExecutionTask("queued", "code", required_capabilities=("code",)),
            SchedulingIntent(fairness_key="tenant-a"),
        )
        mesh = ExecutionMesh([task])
        store = DurableExecutionStore(session)
        store.ensure_run("run-backpressure", mesh)
        session.commit()

        saturated = Worker(
            "worker",
            {"code"},
            inflight=4,
            max_inflight=4,
        )
        result = asyncio.run(
            ResourceFairDurableExecutor(
                store,
                "run-backpressure",
            ).run_to_completion([saturated])
        )
        assert result["succeeded"] == []
        assert result["scheduler_backpressured"] == ["queued"]
        assert result["incomplete"] == ["queued"]
        restored = store.restore_mesh("run-backpressure")
        assert restored.runtime["queued"].attempts == 0
    finally:
        session.close()


def test_fairness_recomputed_from_durable_progress_after_process_replacement(tmp_path):
    engine = init_db(tmp_path / "fairness-replay.db")
    first_session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        tasks = [
            schedule_task(
                ExecutionTask("a1", "code", required_capabilities=("code",), priority=100),
                SchedulingIntent(fairness_key="A"),
            ),
            schedule_task(
                ExecutionTask("a2", "code", required_capabilities=("code",), priority=100),
                SchedulingIntent(fairness_key="A"),
            ),
            schedule_task(
                ExecutionTask("b1", "code", required_capabilities=("code",), priority=1),
                SchedulingIntent(fairness_key="B"),
            ),
        ]
        mesh = ExecutionMesh(tasks, max_concurrency=1)
        store = DurableExecutionStore(first_session)
        store.ensure_run("run-fairness", mesh)
        first_session.commit()
        worker = Worker("w1", {"code"})
        wave = asyncio.run(
            ResourceFairDurableExecutor(store, "run-fairness").run_wave([worker])
        )
        assert [item.task_id for item in wave] == ["a1"]
    finally:
        first_session.close()

    second_session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        replacement_store = DurableExecutionStore(second_session)
        replacement = Worker("w2", {"code"})
        wave = asyncio.run(
            ResourceFairDurableExecutor(
                replacement_store,
                "run-fairness",
            ).run_wave([replacement])
        )
        assert [item.task_id for item in wave] == ["b1"]
        assert replacement.calls == ["b1"]
    finally:
        second_session.close()


def test_atomic_gang_waits_when_resource_bundle_cannot_fit(tmp_path):
    engine = init_db(tmp_path / "gang.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        tasks = [
            schedule_task(
                ExecutionTask("g1", "gpu", required_capabilities=("gpu",)),
                SchedulingIntent(
                    seats=2,
                    resources={"gpu": 1},
                    placement_group="pair",
                    gang=True,
                ),
            ),
            schedule_task(
                ExecutionTask("g2", "gpu", required_capabilities=("gpu",)),
                SchedulingIntent(
                    seats=2,
                    resources={"gpu": 1},
                    placement_group="pair",
                    gang=True,
                ),
            ),
        ]
        mesh = ExecutionMesh(tasks, max_concurrency=3)
        store = DurableExecutionStore(session)
        store.ensure_run("run-gang", mesh)
        session.commit()
        gpu = Worker("gpu", {"gpu"}, resources={"gpu": 2})
        result = asyncio.run(
            ResourceFairDurableExecutor(store, "run-gang").run_to_completion([gpu])
        )
        assert result["succeeded"] == []
        assert result["scheduler_backpressured"] == ["g1", "g2"]
        assert gpu.calls == []
    finally:
        session.close()
