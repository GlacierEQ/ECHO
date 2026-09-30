from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from echo.capacity_control import (
    CapacityActionKind,
    CapacityControlLoop,
    CapacityControlReceiptORM,
    CapacityControlStore,
    CapacityController,
    CapacityPolicy,
    CapacityPool,
    WorkerInstance,
)
from echo.db import init_db
from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState
from echo.resource_scheduler import SchedulingIntent, schedule_task


def task(task_id, capability, *, resources=None, version="", topology=None):
    return schedule_task(
        ExecutionTask(task_id, "run", required_capabilities=(capability,)),
        SchedulingIntent(
            resources=resources or {},
            required_worker_version=version,
            topology=topology or {},
        ),
    )


def pool(
    pool_id="gpu",
    *,
    capabilities=frozenset({"gpu"}),
    resources=None,
    version="v1",
    min_replicas=0,
    max_replicas=100,
    max_inflight=1,
):
    return CapacityPool(
        pool_id=pool_id,
        capabilities=capabilities,
        resources_per_replica=resources or {"gpu": 1, "memory_gb": 48},
        target_version=version,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        max_inflight_per_replica=max_inflight,
        fitness={"gpu": 1.0, "code": 1.0},
    )


def controller(tmp_path, *, policy=None):
    engine = init_db(tmp_path / "capacity.db")
    session = Session(bind=engine, autoflush=False, expire_on_commit=False)
    store = CapacityControlStore(session)
    return session, store, CapacityController(store, policy=policy)


def test_gpu_queue_scales_from_resource_shape_not_host_cpu_metric(tmp_path):
    session, _store, control = controller(tmp_path)
    try:
        mesh = ExecutionMesh(
            [
                task("a", "gpu", resources={"gpu": 1, "memory_gb": 40}),
                task("b", "gpu", resources={"gpu": 1, "memory_gb": 40}),
            ]
        )
        plan = control.plan(mesh, [pool()], [])
        demand = plan.demand[0]
        assert demand.required_replicas == 2
        # 75% target utilization adds headroom above the exact packing floor.
        assert demand.desired_replicas == 3
        scale = next(action for action in plan.actions if action.kind == CapacityActionKind.SCALE)
        assert scale.desired_replicas == 3
        assert set(scale.task_ids) == {"a", "b"}
    finally:
        session.close()


def test_multidimensional_bin_packing_does_not_underestimate_gpu_memory_mix(tmp_path):
    session, _store, control = controller(
        tmp_path,
        policy=CapacityPolicy(target_utilization=1.0),
    )
    try:
        mesh = ExecutionMesh(
            [
                task("a", "gpu", resources={"gpu": 1, "memory_gb": 30}),
                task("b", "gpu", resources={"gpu": 1, "memory_gb": 30}),
                task("c", "gpu", resources={"gpu": 1, "memory_gb": 10}),
            ]
        )
        p = pool(resources={"gpu": 2, "memory_gb": 48}, max_inflight=2)
        plan = control.plan(mesh, [p], [])
        assert plan.demand[0].required_replicas == 2
    finally:
        session.close()


def test_scale_down_requires_stabilization_and_never_drains_active_owner(tmp_path):
    policy = CapacityPolicy(
        target_utilization=1.0,
        scale_down_stabilization_seconds=60,
    )
    session, _store, control = controller(tmp_path, policy=policy)
    try:
        mesh = ExecutionMesh([task("active", "gpu")])
        mesh.runtime["active"].state = TaskState.RUNNING
        mesh.runtime["active"].lease_owner = "w1"
        mesh.runtime["active"].lease_expires_at = 10**12
        workers = [
            WorkerInstance("w1", "gpu", version="v1", inflight=1),
            WorkerInstance("w2", "gpu", version="v1", inflight=0),
            WorkerInstance("w3", "gpu", version="v1", inflight=0),
        ]
        now = datetime(2026, 8, 16, 8, 0, tzinfo=timezone.utc)
        first = control.plan(mesh, [pool()], workers, now=now)
        assert any(
            action.kind == CapacityActionKind.HOLD
            and "stabilization" in action.reason
            for action in first.actions
        )
        session.commit()

        second = control.plan(
            mesh,
            [pool()],
            workers,
            now=now + timedelta(seconds=61),
        )
        drains = [a.worker_id for a in second.actions if a.kind == CapacityActionKind.DRAIN]
        assert "w1" not in drains
        assert drains == ["w2", "w3"]
    finally:
        session.close()


def test_version_rollout_establishes_replacements_before_old_generation_drain(tmp_path):
    session, _store, control = controller(
        tmp_path,
        policy=CapacityPolicy(target_utilization=1.0),
    )
    try:
        mesh = ExecutionMesh([task("queued", "gpu", version="v2")])
        target_pool = pool(version="v2")
        old = [
            WorkerInstance("old-1", "gpu", version="v1"),
            WorkerInstance("old-2", "gpu", version="v1"),
        ]
        first = control.plan(mesh, [target_pool], old)
        scale = next(a for a in first.actions if a.kind == CapacityActionKind.SCALE)
        assert scale.version == "v2"
        assert scale.desired_replicas >= 2
        assert not any(a.kind == CapacityActionKind.DRAIN for a in first.actions)

        mixed = old + [
            WorkerInstance("new-1", "gpu", version="v2"),
            WorkerInstance("new-2", "gpu", version="v2"),
        ]
        second = control.plan(mesh, [target_pool], mixed)
        drains = {a.worker_id for a in second.actions if a.kind == CapacityActionKind.DRAIN}
        assert drains == {"old-1", "old-2"}
    finally:
        session.close()


def test_missing_pool_yields_explicit_provision_action_not_fake_scaling(tmp_path):
    session, _store, control = controller(tmp_path)
    try:
        mesh = ExecutionMesh(
            [
                task(
                    "proof",
                    "lean",
                    resources={"memory_gb": 8},
                    version="lean-v1",
                    topology={"region": "west"},
                )
            ]
        )
        plan = control.plan(mesh, [], [])
        assert plan.unroutable_tasks == ("proof",)
        action = plan.actions[0]
        assert action.kind == CapacityActionKind.PROVISION
        assert action.required_capabilities == ("lean",)
        assert action.resource_shape == {"memory_gb": 8.0}
        assert action.version == "lean-v1"
        assert action.topology == {"region": "west"}
    finally:
        session.close()


class FakeActuator:
    def __init__(self, fail_pool=""):
        self.calls = []
        self.fail_pool = fail_pool

    async def scale_pool(self, pool_id, *, desired_replicas, version):
        self.calls.append(("scale", pool_id, desired_replicas, version))
        if pool_id == self.fail_pool:
            raise RuntimeError("scale failed")
        return {"desired": desired_replicas, "version": version}

    async def drain_worker(self, worker_id):
        self.calls.append(("drain", worker_id))
        return {"draining": worker_id}

    async def provision_pool(
        self,
        *,
        pool_id,
        capabilities,
        resources_per_replica,
        topology,
        version,
    ):
        self.calls.append(("provision", pool_id, tuple(capabilities), version))
        return {"pool_id": pool_id, "created": True}


def test_control_loop_executes_scale_before_drain_and_hash_chains_receipts(tmp_path):
    session, store, control = controller(
        tmp_path,
        policy=CapacityPolicy(target_utilization=1.0, scale_down_stabilization_seconds=0),
    )
    try:
        mesh = ExecutionMesh([task("queued", "gpu")])
        plan = control.plan(mesh, [pool()], [])
        actuator = FakeActuator()
        result = asyncio.run(CapacityControlLoop(store, actuator).apply(plan))
        assert result[0]["kind"] == "scale"
        rows = session.scalars(
            select(CapacityControlReceiptORM).order_by(CapacityControlReceiptORM.id)
        ).all()
        assert rows
        assert rows[0].previous_hash == ""
        assert rows[0].content_hash
    finally:
        session.close()


def test_failed_external_capacity_action_persists_failure_receipt(tmp_path):
    session, store, control = controller(
        tmp_path,
        policy=CapacityPolicy(target_utilization=1.0),
    )
    try:
        mesh = ExecutionMesh([task("queued", "gpu")])
        plan = control.plan(mesh, [pool()], [])
        with pytest.raises(RuntimeError, match="scale failed"):
            asyncio.run(CapacityControlLoop(store, FakeActuator("gpu")).apply(plan))
        row = session.scalar(select(CapacityControlReceiptORM))
        assert row.status == "failed"
        assert "scale failed" in row.error
    finally:
        session.close()
