from __future__ import annotations

import asyncio

from sqlalchemy import select
from sqlalchemy.orm import Session

from echo.db import init_db
from echo.durable_execution import (
    DurableEventORM,
    DurableExecutionStore,
    DurableSnapshotORM,
    DurableTaskORM,
)
from echo.durable_federation import DurableFederatedExecutor
from echo.execution_mesh import ExecutionContext, ExecutionMesh, ExecutionTask, WorkerResult
from echo.interposition import FunctionalInterceptor, InterposedWorker


class Worker:
    def __init__(
        self,
        worker_id: str,
        capabilities: set[str],
        fitness: dict[str, float],
        *,
        delay: float = 0.0,
        fail: set[str] | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.capabilities = frozenset(capabilities)
        self.fitness = fitness
        self.delay = delay
        self.fail = fail or set()
        self.calls: list[str] = []
        self.contexts: dict[str, ExecutionContext] = {}

    async def execute(self, task, context):
        self.calls.append(task.task_id)
        self.contexts[task.task_id] = context
        if self.delay:
            await asyncio.sleep(self.delay)
        if task.task_id in self.fail:
            raise RuntimeError(f"boom:{task.task_id}")
        return WorkerResult(
            output={
                "worker": self.worker_id,
                "task": task.task_id,
                "value": task.payload.get("value", task.task_id),
            },
            stream=(f"stream:{task.task_id}",),
            terminal={"completed": True, "worker": self.worker_id},
            agent_steps=2,
            tool_calls=1,
        )


def make_store(tmp_path, name="durable-federation.db"):
    engine = init_db(tmp_path / name)
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    return engine, session, DurableExecutionStore(session)


def test_specialist_routing_is_durable_across_multi_wave_dag(tmp_path):
    _engine, session, store = make_store(tmp_path)
    try:
        mesh = ExecutionMesh(
            [
                ExecutionTask(
                    "reason",
                    "analyze",
                    payload={"value": "reasoned"},
                    required_capabilities=("reasoning",),
                ),
                ExecutionTask(
                    "kernel",
                    "compile",
                    payload={"value": "accelerated"},
                    required_capabilities=("gpu",),
                ),
                ExecutionTask(
                    "compose",
                    "compose",
                    dependencies=("reason", "kernel"),
                    required_capabilities=("code",),
                ),
            ],
            max_concurrency=3,
            lease_seconds=30,
        )
        store.ensure_run("run-specialists", mesh)
        session.commit()

        general = Worker(
            "general",
            {"reasoning", "code", "gpu"},
            {"reasoning": 0.5, "code": 0.5, "gpu": 0.2},
        )
        reasoning = Worker(
            "reasoning-specialist",
            {"reasoning", "code"},
            {"reasoning": 1.0, "code": 0.95},
        )
        gpu = Worker("gpu-specialist", {"gpu"}, {"gpu": 1.0})

        result = asyncio.run(
            DurableFederatedExecutor(store, "run-specialists").run_to_completion(
                [general, reasoning, gpu]
            )
        )

        assert result["succeeded"] == ["compose", "kernel", "reason"]
        assert result["failed"] == []
        assert result["blocked"] == []
        assert result["waves"] == 2
        assert reasoning.calls == ["reason", "compose"]
        assert gpu.calls == ["kernel"]
        assert general.calls == []
        assert reasoning.contexts["compose"].dependency_outputs["reason"]["value"] == "reasoned"
        assert reasoning.contexts["compose"].dependency_outputs["kernel"]["value"] == "accelerated"
        assert result["receipt_head"]

        lease_events = session.scalars(
            select(DurableEventORM)
            .where(
                DurableEventORM.run_id == "run-specialists",
                DurableEventORM.event_type == "task_leased",
            )
            .order_by(DurableEventORM.id)
        ).all()
        routed = {event.task_id: event.details for event in lease_events}
        assert routed["reason"]["routing_fitness"] == 1.0
        assert routed["kernel"]["routing_fitness"] == 1.0
        assert routed["compose"]["routing_fitness"] == 0.95
        assert session.get(DurableSnapshotORM, "run-specialists") is not None
    finally:
        session.close()


def test_process_replacement_resumes_without_repeating_completed_wave(tmp_path):
    engine, first_session, first_store = make_store(tmp_path, "replacement.db")
    try:
        mesh = ExecutionMesh(
            [
                ExecutionTask("map", "map", required_capabilities=("reasoning",)),
                ExecutionTask(
                    "build",
                    "build",
                    dependencies=("map",),
                    required_capabilities=("code",),
                ),
            ],
            max_concurrency=1,
            lease_seconds=20,
        )
        first_store.ensure_run("run-replace", mesh)
        first_session.commit()
        first_reasoner = Worker("reasoner-v1", {"reasoning"}, {"reasoning": 1.0})
        first_executor = DurableFederatedExecutor(first_store, "run-replace")
        wave = asyncio.run(first_executor.run_wave([first_reasoner]))
        assert [item.task_id for item in wave] == ["map"]
        assert first_reasoner.calls == ["map"]
    finally:
        first_session.close()

    second_session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        second_store = DurableExecutionStore(second_session)
        replacement_reasoner = Worker(
            "reasoner-v2", {"reasoning"}, {"reasoning": 1.0}
        )
        builder = Worker("builder-v2", {"code"}, {"code": 1.0})
        result = asyncio.run(
            DurableFederatedExecutor(second_store, "run-replace").run_to_completion(
                [replacement_reasoner, builder]
            )
        )
        assert result["succeeded"] == ["build", "map"]
        assert replacement_reasoner.calls == []
        assert builder.calls == ["build"]
        assert builder.contexts["build"].dependency_outputs["map"]["task"] == "map"
    finally:
        second_session.close()


def test_heartbeat_keeps_long_specialist_execution_alive(tmp_path):
    _engine, session, store = make_store(tmp_path, "heartbeat.db")
    try:
        mesh = ExecutionMesh(
            [ExecutionTask("deep", "deep", required_capabilities=("reasoning",))],
            lease_seconds=0.25,
        )
        store.ensure_run("run-heartbeat", mesh)
        session.commit()
        slow = Worker(
            "slow-specialist",
            {"reasoning"},
            {"reasoning": 1.0},
            delay=0.45,
        )
        result = asyncio.run(
            DurableFederatedExecutor(
                store,
                "run-heartbeat",
                lease_seconds=0.25,
                heartbeat_interval=0.05,
            ).run_to_completion([slow])
        )
        assert result["succeeded"] == ["deep"]
        event_types = [
            item["event_type"] for item in store.history("run-heartbeat")
        ]
        assert "lease_heartbeat" in event_types
        assert "lease_expired" not in event_types
        assert "lease_expired_exhausted" not in event_types
    finally:
        session.close()


def test_failure_isolation_survives_durable_federation(tmp_path):
    _engine, session, store = make_store(tmp_path, "isolation.db")
    try:
        mesh = ExecutionMesh(
            [
                ExecutionTask(
                    "bad-root",
                    "reason",
                    required_capabilities=("reasoning",),
                    max_attempts=1,
                ),
                ExecutionTask(
                    "good-root",
                    "gpu",
                    required_capabilities=("gpu",),
                ),
                ExecutionTask(
                    "bad-child",
                    "build",
                    dependencies=("bad-root",),
                    required_capabilities=("code",),
                ),
                ExecutionTask(
                    "good-child",
                    "build",
                    dependencies=("good-root",),
                    required_capabilities=("code",),
                ),
            ],
            max_concurrency=4,
        )
        store.ensure_run("run-isolation", mesh)
        session.commit()
        reasoner = Worker(
            "reasoner",
            {"reasoning", "code"},
            {"reasoning": 1.0, "code": 1.0},
            fail={"bad-root"},
        )
        gpu = Worker("gpu", {"gpu"}, {"gpu": 1.0})
        result = asyncio.run(
            DurableFederatedExecutor(store, "run-isolation").run_to_completion(
                [reasoner, gpu]
            )
        )
        assert result["failed"] == ["bad-root"]
        assert result["blocked"] == ["bad-child"]
        assert result["succeeded"] == ["good-child", "good-root"]
        assert reasoner.calls == ["bad-root", "good-child"]
        assert gpu.calls == ["good-root"]
    finally:
        session.close()


def test_interposition_composes_with_durable_specialist_execution(tmp_path):
    _engine, session, store = make_store(tmp_path, "interposition.db")
    try:
        mesh = ExecutionMesh(
            [ExecutionTask("coded", "code", required_capabilities=("code",))]
        )
        store.ensure_run("run-interposed", mesh)
        session.commit()
        base = Worker("code-specialist", {"code"}, {"code": 1.0})
        events: list[str] = []

        async def before(task, context):
            events.append("before")
            return context

        async def after(task, context, worker_result):
            events.append("after")
            return WorkerResult(
                output={**worker_result.output, "interposed": True},
                stream=worker_result.stream,
                terminal={**worker_result.terminal, "audited": True},
                agent_steps=worker_result.agent_steps,
                tool_calls=worker_result.tool_calls,
            )

        wrapped = InterposedWorker(
            base,
            [FunctionalInterceptor(before, after)],
        )
        result = asyncio.run(
            DurableFederatedExecutor(store, "run-interposed").run_to_completion(
                [wrapped]
            )
        )
        assert result["succeeded"] == ["coded"]
        restored = store.restore_mesh("run-interposed")
        assert restored.results["coded"].output["interposed"] is True
        assert restored.results["coded"].terminal["audited"] is True
        assert events == ["before", "after"]
    finally:
        session.close()


def test_unroutable_specialist_work_is_never_fake_executed(tmp_path):
    _engine, session, store = make_store(tmp_path, "unroutable.db")
    try:
        mesh = ExecutionMesh(
            [ExecutionTask("proof", "prove", required_capabilities=("lean",))]
        )
        store.ensure_run("run-unroutable", mesh)
        session.commit()
        python = Worker("python", {"python"}, {"python": 1.0})
        result = asyncio.run(
            DurableFederatedExecutor(store, "run-unroutable").run_to_completion(
                [python]
            )
        )
        assert result["succeeded"] == []
        assert result["unroutable"] == ["proof"]
        assert python.calls == []
        row = session.scalar(
            select(DurableTaskORM).where(
                DurableTaskORM.run_id == "run-unroutable"
            )
        )
        assert row.attempts == 0
    finally:
        session.close()
