"""Closed-loop capacity control for ECHO execution pools.

The controller is intentionally provider-neutral. It counterengineers useful
control mechanics from distributed runtimes without making any vendor runtime
an authority over ECHO:

* queued logical resource demand drives capacity, not host CPU alone;
* multidimensional task shapes are packed into replica-sized resource bins;
* scale-up reacts immediately to schedulable demand while scale-down uses a
  stabilization window and never drains a worker with durable active ownership;
* worker generations are replaced before old versions are drained;
* scale/drain actions execute through a small actuator interface and produce
  durable control receipts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from math import ceil, isfinite
from typing import Any, Mapping, Protocol, Sequence

from sqlalchemy import JSON, DateTime, Integer, String, Text, select
from sqlalchemy.orm import Mapped, Session, mapped_column

from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState, WorkerBackend
from echo.models import Base, utcnow
from echo.resource_scheduler import SchedulingIntent, WorkerRuntimeProfile


class CapacityActionKind(str, Enum):
    SCALE = "scale"
    DRAIN = "drain"
    HOLD = "hold"
    PROVISION = "provision"


@dataclass(frozen=True)
class CapacityPolicy:
    target_utilization: float = 0.75
    scale_down_stabilization_seconds: float = 300.0
    max_scale_up_step: int = 128
    max_scale_down_step: int = 32

    def validate(self) -> None:
        if not 0 < self.target_utilization <= 1:
            raise ValueError("target_utilization must be in (0, 1]")
        if self.scale_down_stabilization_seconds < 0:
            raise ValueError("scale_down_stabilization_seconds must be non-negative")
        if self.max_scale_up_step < 1:
            raise ValueError("max_scale_up_step must be positive")
        if self.max_scale_down_step < 1:
            raise ValueError("max_scale_down_step must be positive")


@dataclass(frozen=True)
class CapacityPool:
    pool_id: str
    capabilities: frozenset[str]
    resources_per_replica: Mapping[str, float] = field(default_factory=dict)
    topology: Mapping[str, str] = field(default_factory=dict)
    fitness: Mapping[str, float] = field(default_factory=dict)
    target_version: str = ""
    min_replicas: int = 0
    max_replicas: int = 1000
    max_inflight_per_replica: int = 1

    def validate(self) -> None:
        if not self.pool_id.strip():
            raise ValueError("pool_id must not be empty")
        if self.min_replicas < 0 or self.max_replicas < self.min_replicas:
            raise ValueError("invalid replica bounds")
        if self.max_inflight_per_replica < 1:
            raise ValueError("max_inflight_per_replica must be positive")
        for name, amount in self.resources_per_replica.items():
            if not str(name).strip():
                raise ValueError("pool resource name must not be empty")
            value = float(amount)
            if not isfinite(value) or value < 0:
                raise ValueError("pool resources must be finite and non-negative")
        for key, value in self.topology.items():
            if not str(key).strip() or not str(value).strip():
                raise ValueError("pool topology keys and values must not be empty")


@dataclass(frozen=True)
class WorkerInstance:
    worker_id: str
    pool_id: str
    version: str = ""
    draining: bool = False
    inflight: int = 0

    @classmethod
    def from_backend(cls, backend: WorkerBackend) -> "WorkerInstance":
        profile = WorkerRuntimeProfile.from_backend(backend)
        pool_id = str(getattr(backend, "capacity_pool", backend.worker_id)).strip()
        if not pool_id:
            raise ValueError(f"worker {backend.worker_id} capacity_pool must not be empty")
        return cls(
            worker_id=backend.worker_id,
            pool_id=pool_id,
            version=profile.version,
            draining=profile.draining,
            inflight=profile.inflight,
        )


@dataclass(frozen=True)
class PoolDemand:
    pool_id: str
    pending_tasks: tuple[str, ...]
    active_tasks: tuple[str, ...]
    required_replicas: int
    desired_replicas: int
    current_replicas: int
    current_target_version_replicas: int


@dataclass(frozen=True)
class CapacityAction:
    kind: CapacityActionKind
    pool_id: str
    desired_replicas: int | None = None
    version: str = ""
    worker_id: str = ""
    reason: str = ""
    task_ids: tuple[str, ...] = ()
    resource_shape: Mapping[str, float] = field(default_factory=dict)
    required_capabilities: tuple[str, ...] = ()
    topology: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CapacityPlan:
    actions: tuple[CapacityAction, ...]
    demand: tuple[PoolDemand, ...]
    unroutable_tasks: tuple[str, ...]
    observed_at: datetime


class CapacityPoolStateORM(Base):
    __tablename__ = "execution_capacity_pool_state"

    pool_id: Mapped[str] = mapped_column(String(192), primary_key=True)
    desired_replicas: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    target_version: Mapped[str] = mapped_column(String(128), default="", nullable=False)
    under_target_since: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    receipt_head: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class CapacityControlReceiptORM(Base):
    __tablename__ = "execution_capacity_receipts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pool_id: Mapped[str] = mapped_column(String(192), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    previous_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    error: Mapped[str] = mapped_column(Text, default="", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )


CAPACITY_TABLES = (
    CapacityPoolStateORM.__table__,
    CapacityControlReceiptORM.__table__,
)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _hash(value: Any) -> str:
    rendered = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


class CapacityActuator(Protocol):
    async def scale_pool(
        self,
        pool_id: str,
        *,
        desired_replicas: int,
        version: str,
    ) -> Mapping[str, Any]: ...

    async def drain_worker(self, worker_id: str) -> Mapping[str, Any]: ...

    async def provision_pool(
        self,
        *,
        pool_id: str,
        capabilities: Sequence[str],
        resources_per_replica: Mapping[str, float],
        topology: Mapping[str, str],
        version: str,
    ) -> Mapping[str, Any]: ...


class CapacityControlStore:
    def __init__(self, session: Session, *, ensure_schema: bool = True) -> None:
        self.session = session
        if ensure_schema:
            Base.metadata.create_all(bind=session.get_bind(), tables=CAPACITY_TABLES)

    def state(self, pool: CapacityPool) -> CapacityPoolStateORM:
        row = self.session.get(CapacityPoolStateORM, pool.pool_id)
        if row is None:
            row = CapacityPoolStateORM(
                pool_id=pool.pool_id,
                desired_replicas=pool.min_replicas,
                target_version=pool.target_version,
            )
            self.session.add(row)
            self.session.flush()
        elif row.target_version != pool.target_version:
            row.target_version = pool.target_version
            row.under_target_since = None
            self.session.flush()
        return row

    def receipt(
        self,
        action: CapacityAction,
        *,
        status: str,
        details: Mapping[str, Any],
        error: str = "",
    ) -> CapacityControlReceiptORM:
        pool_id = action.pool_id
        state = self.session.get(CapacityPoolStateORM, pool_id)
        previous_hash = state.receipt_head if state is not None else ""
        clean_details = json.loads(json.dumps(details, default=str))
        payload = {
            "pool_id": pool_id,
            "action": action.kind.value,
            "status": status,
            "details": clean_details,
            "previous_hash": previous_hash,
            "error": error,
        }
        digest = _hash(payload)
        row = CapacityControlReceiptORM(
            pool_id=pool_id,
            action=action.kind.value,
            status=status,
            details=clean_details,
            previous_hash=previous_hash,
            content_hash=digest,
            error=error,
        )
        self.session.add(row)
        if state is not None:
            state.receipt_head = digest
        self.session.flush()
        return row


@dataclass
class _Bin:
    resources: dict[str, float]
    slots: int = 0


class CapacityController:
    """Compute desired worker capacity from durable execution demand."""

    def __init__(
        self,
        store: CapacityControlStore,
        *,
        policy: CapacityPolicy | None = None,
    ) -> None:
        self.store = store
        self.policy = policy or CapacityPolicy()
        self.policy.validate()

    @staticmethod
    def _pool_eligible(task: ExecutionTask, pool: CapacityPool) -> bool:
        intent = SchedulingIntent.from_task(task)
        if not set(task.required_capabilities).issubset(pool.capabilities):
            return False
        if intent.required_worker_version and pool.target_version != intent.required_worker_version:
            return False
        if any(pool.topology.get(key) != value for key, value in intent.topology.items()):
            return False
        for name, amount in intent.resources.items():
            if name not in pool.resources_per_replica:
                return False
            if amount > float(pool.resources_per_replica[name]) + 1e-12:
                return False
        return True

    @staticmethod
    def _pool_score(task: ExecutionTask, pool: CapacityPool) -> tuple[float, str]:
        capabilities = task.required_capabilities
        specialist = (
            sum(float(pool.fitness.get(capability, 0.0)) for capability in capabilities)
            / len(capabilities)
            if capabilities
            else 0.0
        )
        intent = SchedulingIntent.from_task(task)
        waste = 0.0
        for name, amount in intent.resources.items():
            capacity = float(pool.resources_per_replica[name])
            if capacity > 0:
                waste += max(0.0, capacity - amount) / capacity
        return specialist - 0.01 * waste, pool.pool_id

    def _select_pool(
        self,
        task: ExecutionTask,
        pools: Sequence[CapacityPool],
    ) -> CapacityPool | None:
        eligible = [pool for pool in pools if self._pool_eligible(task, pool)]
        if not eligible:
            return None
        return max(eligible, key=lambda pool: self._pool_score(task, pool))

    @staticmethod
    def _dominant_fraction(task: ExecutionTask, pool: CapacityPool) -> float:
        intent = SchedulingIntent.from_task(task)
        fractions = [
            amount / float(pool.resources_per_replica[name])
            for name, amount in intent.resources.items()
            if float(pool.resources_per_replica.get(name, 0.0)) > 0
        ]
        fractions.append(1.0 / pool.max_inflight_per_replica)
        return max(fractions)

    @staticmethod
    def _fits_bin(task: ExecutionTask, pool: CapacityPool, bin_: _Bin) -> bool:
        intent = SchedulingIntent.from_task(task)
        if bin_.slots >= pool.max_inflight_per_replica:
            return False
        return all(
            bin_.resources.get(name, 0.0) + amount
            <= float(pool.resources_per_replica[name]) + 1e-12
            for name, amount in intent.resources.items()
        )

    @staticmethod
    def _place_bin(task: ExecutionTask, bin_: _Bin) -> None:
        intent = SchedulingIntent.from_task(task)
        for name, amount in intent.resources.items():
            bin_.resources[name] = bin_.resources.get(name, 0.0) + amount
        bin_.slots += 1

    def _replicas_for_tasks(
        self,
        tasks: Sequence[ExecutionTask],
        pool: CapacityPool,
    ) -> int:
        if not tasks:
            return 0
        bins: list[_Bin] = []
        ordered = sorted(
            tasks,
            key=lambda task: (-self._dominant_fraction(task, pool), task.task_id),
        )
        for task in ordered:
            for bin_ in bins:
                if self._fits_bin(task, pool, bin_):
                    self._place_bin(task, bin_)
                    break
            else:
                bin_ = _Bin(resources={})
                self._place_bin(task, bin_)
                bins.append(bin_)
        return len(bins)

    @staticmethod
    def _active_owner_ids(mesh: ExecutionMesh) -> set[str]:
        return {
            runtime.lease_owner
            for runtime in mesh.runtime.values()
            if runtime.state in {TaskState.LEASED, TaskState.RUNNING}
            and runtime.lease_owner
        }

    @staticmethod
    def _worker_maps(
        workers: Sequence[WorkerInstance],
    ) -> tuple[dict[str, WorkerInstance], dict[str, list[WorkerInstance]]]:
        by_id: dict[str, WorkerInstance] = {}
        by_pool: dict[str, list[WorkerInstance]] = {}
        for worker in workers:
            if worker.worker_id in by_id:
                raise ValueError(f"duplicate worker_id: {worker.worker_id}")
            by_id[worker.worker_id] = worker
            by_pool.setdefault(worker.pool_id, []).append(worker)
        return by_id, by_pool

    @staticmethod
    def _provision_action(task: ExecutionTask) -> CapacityAction:
        intent = SchedulingIntent.from_task(task)
        return CapacityAction(
            kind=CapacityActionKind.PROVISION,
            pool_id=f"dynamic:{task.task_id}",
            version=intent.required_worker_version,
            reason="no configured capacity pool can satisfy task placement",
            task_ids=(task.task_id,),
            resource_shape=dict(intent.resources),
            required_capabilities=tuple(task.required_capabilities),
            topology=dict(intent.topology),
        )

    def plan(
        self,
        mesh: ExecutionMesh,
        pools: Sequence[CapacityPool],
        workers: Sequence[WorkerInstance],
        *,
        now: datetime | None = None,
    ) -> CapacityPlan:
        now = _aware(now or utcnow())
        for pool in pools:
            pool.validate()
        if len({pool.pool_id for pool in pools}) != len(pools):
            raise ValueError("pool_id values must be unique")
        pool_by_id = {pool.pool_id: pool for pool in pools}
        by_worker, workers_by_pool = self._worker_maps(workers)

        pending_by_pool: dict[str, list[ExecutionTask]] = {
            pool.pool_id: [] for pool in pools
        }
        active_by_pool: dict[str, list[ExecutionTask]] = {
            pool.pool_id: [] for pool in pools
        }
        unroutable: list[ExecutionTask] = []

        for task_id, task in mesh.tasks.items():
            runtime = mesh.runtime[task_id]
            if runtime.state == TaskState.PENDING:
                selected = self._select_pool(task, pools)
                if selected is None:
                    unroutable.append(task)
                else:
                    pending_by_pool[selected.pool_id].append(task)
            elif runtime.state in {TaskState.LEASED, TaskState.RUNNING}:
                worker = by_worker.get(runtime.lease_owner)
                if worker is not None and worker.pool_id in active_by_pool:
                    active_by_pool[worker.pool_id].append(task)

        actions: list[CapacityAction] = [
            self._provision_action(task) for task in sorted(unroutable, key=lambda item: item.task_id)
        ]
        demand_rows: list[PoolDemand] = []
        active_owner_ids = self._active_owner_ids(mesh)

        for pool_id in sorted(pool_by_id):
            pool = pool_by_id[pool_id]
            state = self.store.state(pool)
            pool_workers = sorted(
                workers_by_pool.get(pool_id, []), key=lambda item: item.worker_id
            )
            current = len(pool_workers)
            active_tasks = active_by_pool[pool_id]
            pending_tasks = pending_by_pool[pool_id]
            all_demand = tuple(active_tasks + pending_tasks)
            required = self._replicas_for_tasks(all_demand, pool)
            buffered = (
                ceil(required / self.policy.target_utilization) if required else 0
            )
            base_desired = max(pool.min_replicas, min(pool.max_replicas, buffered))
            target_workers = [
                worker
                for worker in pool_workers
                if not pool.target_version or worker.version == pool.target_version
            ]
            old_workers = [
                worker
                for worker in pool_workers
                if pool.target_version and worker.version != pool.target_version
            ]

            # During a generation rollout, replacement capacity is established
            # before old workers are drained. The desired target generation is
            # at least the normal capacity target, capped by pool policy.
            target_generation_desired = base_desired
            if pool.target_version and old_workers:
                target_generation_desired = max(base_desired, min(current, pool.max_replicas))

            desired = base_desired
            if pool.target_version and old_workers:
                desired = max(base_desired, target_generation_desired + len(old_workers))
                desired = min(pool.max_replicas, desired)

            demand_rows.append(
                PoolDemand(
                    pool_id=pool_id,
                    pending_tasks=tuple(sorted(task.task_id for task in pending_tasks)),
                    active_tasks=tuple(sorted(task.task_id for task in active_tasks)),
                    required_replicas=required,
                    desired_replicas=desired,
                    current_replicas=current,
                    current_target_version_replicas=len(target_workers),
                )
            )

            if pool.target_version and len(target_workers) < target_generation_desired:
                scale_target = min(
                    pool.max_replicas,
                    max(
                        current,
                        min(
                            current + self.policy.max_scale_up_step,
                            current + (target_generation_desired - len(target_workers)),
                        ),
                    ),
                )
                actions.append(
                    CapacityAction(
                        kind=CapacityActionKind.SCALE,
                        pool_id=pool_id,
                        desired_replicas=scale_target,
                        version=pool.target_version,
                        reason="establish target worker generation before drain",
                        task_ids=tuple(sorted(task.task_id for task in all_demand)),
                    )
                )
                state.desired_replicas = scale_target
                state.under_target_since = None
                continue

            if pool.target_version and old_workers:
                idle_old = [
                    worker
                    for worker in old_workers
                    if worker.worker_id not in active_owner_ids and worker.inflight == 0
                ]
                if idle_old:
                    for worker in idle_old:
                        actions.append(
                            CapacityAction(
                                kind=CapacityActionKind.DRAIN,
                                pool_id=pool_id,
                                version=worker.version,
                                worker_id=worker.worker_id,
                                reason="retire superseded worker generation",
                            )
                        )
                busy_old = [worker for worker in old_workers if worker not in idle_old]
                if busy_old:
                    actions.append(
                        CapacityAction(
                            kind=CapacityActionKind.HOLD,
                            pool_id=pool_id,
                            version=pool.target_version,
                            reason="old generation still owns in-flight work",
                        )
                    )
                state.desired_replicas = max(base_desired, len(target_workers))
                state.under_target_since = None
                continue

            if desired > current:
                next_desired = min(
                    desired,
                    current + self.policy.max_scale_up_step,
                    pool.max_replicas,
                )
                actions.append(
                    CapacityAction(
                        kind=CapacityActionKind.SCALE,
                        pool_id=pool_id,
                        desired_replicas=next_desired,
                        version=pool.target_version,
                        reason="queued resource demand exceeds current capacity",
                        task_ids=tuple(sorted(task.task_id for task in all_demand)),
                    )
                )
                state.desired_replicas = next_desired
                state.under_target_since = None
            elif desired < current:
                if state.under_target_since is None:
                    state.under_target_since = now
                    actions.append(
                        CapacityAction(
                            kind=CapacityActionKind.HOLD,
                            pool_id=pool_id,
                            desired_replicas=current,
                            reason="scale-down stabilization window started",
                        )
                    )
                elif (
                    now - _aware(state.under_target_since)
                    >= timedelta(seconds=self.policy.scale_down_stabilization_seconds)
                ):
                    idle_workers = [
                        worker
                        for worker in pool_workers
                        if worker.worker_id not in active_owner_ids
                        and worker.inflight == 0
                        and not worker.draining
                    ]
                    removable = min(
                        current - desired,
                        self.policy.max_scale_down_step,
                        len(idle_workers),
                    )
                    if removable:
                        for worker in idle_workers[:removable]:
                            actions.append(
                                CapacityAction(
                                    kind=CapacityActionKind.DRAIN,
                                    pool_id=pool_id,
                                    version=worker.version,
                                    worker_id=worker.worker_id,
                                    reason="stabilized excess idle capacity",
                                )
                            )
                        state.desired_replicas = current - removable
                    else:
                        actions.append(
                            CapacityAction(
                                kind=CapacityActionKind.HOLD,
                                pool_id=pool_id,
                                desired_replicas=current,
                                reason="excess capacity still owns in-flight work",
                            )
                        )
                else:
                    actions.append(
                        CapacityAction(
                            kind=CapacityActionKind.HOLD,
                            pool_id=pool_id,
                            desired_replicas=current,
                            reason="waiting for scale-down stabilization window",
                        )
                    )
            else:
                state.desired_replicas = current
                state.under_target_since = None

        self.store.session.flush()
        return CapacityPlan(
            actions=tuple(actions),
            demand=tuple(demand_rows),
            unroutable_tasks=tuple(sorted(task.task_id for task in unroutable)),
            observed_at=now,
        )


class CapacityControlLoop:
    """Apply a capacity plan and persist evidence for every control action."""

    def __init__(
        self,
        store: CapacityControlStore,
        actuator: CapacityActuator,
    ) -> None:
        self.store = store
        self.actuator = actuator

    async def apply(self, plan: CapacityPlan) -> tuple[Mapping[str, Any], ...]:
        results: list[Mapping[str, Any]] = []
        ordered = sorted(
            plan.actions,
            key=lambda action: (
                {
                    CapacityActionKind.PROVISION: 0,
                    CapacityActionKind.SCALE: 1,
                    CapacityActionKind.HOLD: 2,
                    CapacityActionKind.DRAIN: 3,
                }[action.kind],
                action.pool_id,
                action.worker_id,
            ),
        )
        for action in ordered:
            try:
                if action.kind == CapacityActionKind.SCALE:
                    output = await self.actuator.scale_pool(
                        action.pool_id,
                        desired_replicas=int(action.desired_replicas or 0),
                        version=action.version,
                    )
                elif action.kind == CapacityActionKind.DRAIN:
                    output = await self.actuator.drain_worker(action.worker_id)
                elif action.kind == CapacityActionKind.PROVISION:
                    output = await self.actuator.provision_pool(
                        pool_id=action.pool_id,
                        capabilities=action.required_capabilities,
                        resources_per_replica=action.resource_shape,
                        topology=action.topology,
                        version=action.version,
                    )
                else:
                    output = {"held": True, "reason": action.reason}
                clean = json.loads(json.dumps(dict(output), default=str))
                self.store.receipt(
                    action,
                    status="executed" if action.kind != CapacityActionKind.HOLD else "held",
                    details=clean,
                )
                self.store.session.commit()
                results.append(
                    {
                        "pool_id": action.pool_id,
                        "kind": action.kind.value,
                        "status": "executed"
                        if action.kind != CapacityActionKind.HOLD
                        else "held",
                        "output": clean,
                    }
                )
            except Exception as exc:
                self.store.session.rollback()
                # Re-load the state before writing the failed receipt because a
                # provider failure must not erase the control-plane evidence.
                if action.pool_id and self.store.session.get(
                    CapacityPoolStateORM, action.pool_id
                ) is None:
                    self.store.session.add(
                        CapacityPoolStateORM(pool_id=action.pool_id)
                    )
                    self.store.session.flush()
                self.store.receipt(
                    action,
                    status="failed",
                    details={},
                    error=f"{type(exc).__name__}: {exc}",
                )
                self.store.session.commit()
                raise
        return tuple(results)
