"""Resource-aware fair scheduling for ECHO's heterogeneous execution mesh.

This module recombines transferable mechanics from modern distributed runtimes:
- logical CPU/GPU/custom resource vectors and atomic placement bundles;
- weighted fairness across independent flows;
- concurrency seats so expensive tasks consume proportional scheduler capacity;
- pull-style backpressure from worker in-flight limits;
- version/drain/topology-aware worker routing;
- specialist fitness preserved as the final placement discriminator.

The scheduler remains ECHO-owned and provider-neutral. Scheduling intent is
embedded inside the durable task payload under a reserved ECHO namespace, so
snapshot/replay and database persistence carry the placement contract without
requiring a second source of execution truth.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from math import isfinite
from typing import Any, Mapping, Sequence

from echo.execution_mesh import ExecutionMesh, ExecutionTask, TaskState, WorkerBackend
from echo.federation import FederatedExecutor, WorkerAssignment


SCHEDULING_KEY = "__echo_scheduling__"


def _finite_nonnegative(name: str, value: float) -> float:
    value = float(value)
    if not isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return value


@dataclass(frozen=True)
class SchedulingIntent:
    """Durable per-task scheduling contract.

    ``fairness_key`` identifies an independent flow/tenant/project lane.
    ``fairness_weight`` is relative entitlement, not a hard quota.
    ``seats`` is scheduler concurrency cost; expensive work can consume more
    than one seat even when it is one task.
    """

    fairness_key: str = "default"
    fairness_weight: float = 1.0
    seats: int = 1
    resources: Mapping[str, float] = field(default_factory=dict)
    placement_group: str = ""
    placement_strategy: str = "none"  # none | pack | spread
    gang: bool = False
    topology: Mapping[str, str] = field(default_factory=dict)
    required_worker_version: str = ""

    def validate(self) -> None:
        if not self.fairness_key.strip():
            raise ValueError("fairness_key must not be empty")
        if not isfinite(float(self.fairness_weight)) or self.fairness_weight <= 0:
            raise ValueError("fairness_weight must be finite and positive")
        if self.seats < 1:
            raise ValueError("seats must be positive")
        if self.placement_strategy not in {"none", "pack", "spread"}:
            raise ValueError("placement_strategy must be none, pack, or spread")
        if self.placement_strategy != "none" and not self.placement_group.strip():
            raise ValueError("placement strategy requires placement_group")
        if self.gang and not self.placement_group.strip():
            raise ValueError("gang scheduling requires placement_group")
        for resource, amount in self.resources.items():
            if not str(resource).strip():
                raise ValueError("resource name must not be empty")
            _finite_nonnegative(f"resource {resource}", float(amount))
        for key, value in self.topology.items():
            if not str(key).strip() or not str(value).strip():
                raise ValueError("topology keys and values must not be empty")

    def as_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "fairness_key": self.fairness_key,
            "fairness_weight": float(self.fairness_weight),
            "seats": int(self.seats),
            "resources": {
                str(key): float(value)
                for key, value in sorted(self.resources.items())
            },
            "placement_group": self.placement_group,
            "placement_strategy": self.placement_strategy,
            "gang": bool(self.gang),
            "topology": {
                str(key): str(value)
                for key, value in sorted(self.topology.items())
            },
            "required_worker_version": self.required_worker_version,
        }

    @classmethod
    def from_task(cls, task: ExecutionTask) -> "SchedulingIntent":
        raw = task.payload.get(SCHEDULING_KEY, {})
        if raw is None:
            raw = {}
        if not isinstance(raw, Mapping):
            raise ValueError(f"{SCHEDULING_KEY} must be an object")
        intent = cls(
            fairness_key=str(raw.get("fairness_key", "default")),
            fairness_weight=float(raw.get("fairness_weight", 1.0)),
            seats=int(raw.get("seats", 1)),
            resources={
                str(key): float(value)
                for key, value in dict(raw.get("resources", {})).items()
            },
            placement_group=str(raw.get("placement_group", "")),
            placement_strategy=str(raw.get("placement_strategy", "none")),
            gang=bool(raw.get("gang", False)),
            topology={
                str(key): str(value)
                for key, value in dict(raw.get("topology", {})).items()
            },
            required_worker_version=str(raw.get("required_worker_version", "")),
        )
        intent.validate()
        return intent


def schedule_task(task: ExecutionTask, intent: SchedulingIntent) -> ExecutionTask:
    """Attach durable scheduling intent without mutating the task's user payload."""
    payload = dict(task.payload)
    if SCHEDULING_KEY in payload:
        raise ValueError(f"task payload already contains reserved key {SCHEDULING_KEY}")
    payload[SCHEDULING_KEY] = intent.as_dict()
    return replace(task, payload=payload)


@dataclass(frozen=True)
class WorkerRuntimeProfile:
    worker_id: str
    resources: Mapping[str, float]
    topology: Mapping[str, str]
    version: str
    draining: bool
    inflight: int
    max_inflight: int | None

    @classmethod
    def from_backend(cls, backend: WorkerBackend) -> "WorkerRuntimeProfile":
        raw_resources = getattr(backend, "resources", {})
        if not isinstance(raw_resources, Mapping):
            raise ValueError(f"worker {backend.worker_id} resources must be a mapping")
        resources = {
            str(key): _finite_nonnegative(
                f"worker {backend.worker_id} resource {key}", float(value)
            )
            for key, value in raw_resources.items()
        }
        raw_topology = getattr(backend, "topology", {})
        if not isinstance(raw_topology, Mapping):
            raise ValueError(f"worker {backend.worker_id} topology must be a mapping")
        inflight = int(getattr(backend, "inflight", 0))
        if inflight < 0:
            raise ValueError(f"worker {backend.worker_id} inflight must be non-negative")
        raw_max = getattr(backend, "max_inflight", None)
        max_inflight = None if raw_max is None else int(raw_max)
        if max_inflight is not None and max_inflight < 1:
            raise ValueError(
                f"worker {backend.worker_id} max_inflight must be positive"
            )
        return cls(
            worker_id=backend.worker_id,
            resources=resources,
            topology={str(key): str(value) for key, value in raw_topology.items()},
            version=str(getattr(backend, "worker_version", "")),
            draining=bool(getattr(backend, "draining", False)),
            inflight=inflight,
            max_inflight=max_inflight,
        )


@dataclass
class _PlanningState:
    resources_used: dict[str, dict[str, float]]
    assignments: dict[str, int]
    group_workers: dict[str, list[str]]
    fairness_assignments: dict[str, int]
    seats_used: int = 0

    def clone(self) -> "_PlanningState":
        return _PlanningState(
            resources_used={
                worker: dict(values) for worker, values in self.resources_used.items()
            },
            assignments=dict(self.assignments),
            group_workers={group: list(workers) for group, workers in self.group_workers.items()},
            fairness_assignments=dict(self.fairness_assignments),
            seats_used=self.seats_used,
        )


@dataclass(frozen=True)
class SchedulingDecision:
    assignments: tuple[WorkerAssignment, ...]
    deferred: tuple[str, ...]
    unroutable: tuple[str, ...]
    backpressured: tuple[str, ...]
    seats_used: int
    fairness_service: Mapping[str, float]


class ResourceFairScheduler:
    """Stateless-replayable weighted fair resource scheduler.

    Fairness is derived from durable task progress, not an in-memory deficit
    counter. A replacement process therefore reconstructs the same service debt
    from the task graph and current states. Lower normalized service receives
    the next scheduling opportunity; priority orders work inside a flow.
    """

    def __init__(
        self,
        *,
        max_inflight_by_fairness: Mapping[str, int] | None = None,
    ) -> None:
        self.max_inflight_by_fairness = {
            str(key): int(value)
            for key, value in (max_inflight_by_fairness or {}).items()
        }
        for key, value in self.max_inflight_by_fairness.items():
            if value < 1:
                raise ValueError(f"fairness backpressure for {key} must be positive")

    @staticmethod
    def _service(mesh: ExecutionMesh) -> dict[str, float]:
        served: dict[str, float] = {}
        weights: dict[str, float] = {}
        for task_id, task in mesh.tasks.items():
            intent = SchedulingIntent.from_task(task)
            weights[intent.fairness_key] = intent.fairness_weight
            state = mesh.runtime[task_id].state
            if state in {
                TaskState.LEASED,
                TaskState.RUNNING,
                TaskState.SUCCEEDED,
                TaskState.FAILED,
            }:
                served[intent.fairness_key] = (
                    served.get(intent.fairness_key, 0.0) + intent.seats
                )
        keys = set(weights) | set(served)
        return {
            key: served.get(key, 0.0) / weights.get(key, 1.0)
            for key in keys
        }

    @staticmethod
    def _active_by_fairness(mesh: ExecutionMesh) -> dict[str, int]:
        active: dict[str, int] = {}
        for task_id, task in mesh.tasks.items():
            if mesh.runtime[task_id].state not in {TaskState.LEASED, TaskState.RUNNING}:
                continue
            key = SchedulingIntent.from_task(task).fairness_key
            active[key] = active.get(key, 0) + 1
        return active

    @staticmethod
    def _resource_fits(
        profile: WorkerRuntimeProfile,
        intent: SchedulingIntent,
        used: Mapping[str, float],
    ) -> bool:
        for name, amount in intent.resources.items():
            # Declaring a resource request requires the worker to expose that
            # capacity explicitly. Missing capacity is not treated as infinite.
            if name not in profile.resources:
                return False
            if used.get(name, 0.0) + amount > profile.resources[name] + 1e-12:
                return False
        return True

    @staticmethod
    def _topology_fits(
        profile: WorkerRuntimeProfile,
        intent: SchedulingIntent,
    ) -> bool:
        return all(profile.topology.get(key) == value for key, value in intent.topology.items())

    @staticmethod
    def _version_fits(profile: WorkerRuntimeProfile, intent: SchedulingIntent) -> bool:
        return (
            not intent.required_worker_version
            or profile.version == intent.required_worker_version
        )

    @staticmethod
    def _backpressure_fits(
        profile: WorkerRuntimeProfile,
        planned_assignments: int,
    ) -> bool:
        return (
            profile.max_inflight is None
            or profile.inflight + planned_assignments < profile.max_inflight
        )

    @staticmethod
    def _placement_candidates(
        candidates: Sequence[WorkerBackend],
        profiles: Mapping[str, WorkerRuntimeProfile],
        intent: SchedulingIntent,
        state: _PlanningState,
    ) -> list[WorkerBackend]:
        if not intent.placement_group or intent.placement_strategy == "none":
            return list(candidates)
        previous = state.group_workers.get(intent.placement_group, [])
        if intent.placement_strategy == "pack" and previous:
            packed = [worker for worker in candidates if worker.worker_id == previous[0]]
            return packed
        if intent.placement_strategy == "spread" and previous:
            unused = [worker for worker in candidates if worker.worker_id not in set(previous)]
            if unused:
                return unused
        return list(candidates)

    def _candidate_workers(
        self,
        task: ExecutionTask,
        intent: SchedulingIntent,
        backends: Sequence[WorkerBackend],
        profiles: Mapping[str, WorkerRuntimeProfile],
        state: _PlanningState,
    ) -> tuple[list[WorkerBackend], bool]:
        compatible: list[WorkerBackend] = []
        backpressure_only = False
        for backend in backends:
            profile = profiles[backend.worker_id]
            if profile.draining:
                continue
            if not set(task.required_capabilities).issubset(backend.capabilities):
                continue
            if not self._version_fits(profile, intent):
                continue
            if not self._topology_fits(profile, intent):
                continue
            if not self._resource_fits(
                profile,
                intent,
                state.resources_used[backend.worker_id],
            ):
                continue
            if not self._backpressure_fits(
                profile,
                state.assignments[backend.worker_id],
            ):
                backpressure_only = True
                continue
            compatible.append(backend)
        return (
            self._placement_candidates(
                compatible,
                profiles,
                intent,
                state,
            ),
            backpressure_only,
        )

    def _assign_one(
        self,
        task: ExecutionTask,
        intent: SchedulingIntent,
        backends: Sequence[WorkerBackend],
        profiles: Mapping[str, WorkerRuntimeProfile],
        state: _PlanningState,
    ) -> tuple[WorkerAssignment | None, str | None]:
        if state.seats_used + intent.seats > self._seat_limit:
            return None, "seat_backpressure"
        flow_limit = self.max_inflight_by_fairness.get(intent.fairness_key)
        if (
            flow_limit is not None
            and self._active_flows.get(intent.fairness_key, 0)
            + state.fairness_assignments.get(intent.fairness_key, 0)
            >= flow_limit
        ):
            return None, "flow_backpressure"

        candidates, worker_backpressure = self._candidate_workers(
            task,
            intent,
            backends,
            profiles,
            state,
        )
        if not candidates:
            return None, "worker_backpressure" if worker_backpressure else "unroutable"

        candidates = sorted(
            candidates,
            key=lambda backend: (
                -FederatedExecutor._worker_fitness(backend, task),
                state.assignments[backend.worker_id],
                backend.worker_id,
            ),
        )
        selected = candidates[0]
        for name, amount in intent.resources.items():
            state.resources_used[selected.worker_id][name] = (
                state.resources_used[selected.worker_id].get(name, 0.0) + amount
            )
        state.assignments[selected.worker_id] += 1
        state.fairness_assignments[intent.fairness_key] = (
            state.fairness_assignments.get(intent.fairness_key, 0) + 1
        )
        state.seats_used += intent.seats
        if intent.placement_group:
            state.group_workers.setdefault(intent.placement_group, []).append(
                selected.worker_id
            )
        return (
            WorkerAssignment(
                task=task,
                backend=selected,
                fitness=FederatedExecutor._worker_fitness(selected, task),
            ),
            None,
        )

    @staticmethod
    def _gang_units(tasks: Sequence[ExecutionTask]) -> list[tuple[ExecutionTask, ...]]:
        groups: dict[str, list[ExecutionTask]] = {}
        singles: list[tuple[ExecutionTask, ...]] = []
        for task in tasks:
            intent = SchedulingIntent.from_task(task)
            if intent.gang:
                groups.setdefault(intent.placement_group, []).append(task)
            else:
                singles.append((task,))
        units = singles + [tuple(items) for _, items in sorted(groups.items())]
        return units

    def plan(
        self,
        mesh: ExecutionMesh,
        backends: Sequence[WorkerBackend],
    ) -> SchedulingDecision:
        if not backends:
            ready = mesh.ready(frozenset())
            return SchedulingDecision(
                assignments=(),
                deferred=(),
                unroutable=tuple(task.task_id for task in ready),
                backpressured=(),
                seats_used=0,
                fairness_service=self._service(mesh),
            )

        profiles = {
            backend.worker_id: WorkerRuntimeProfile.from_backend(backend)
            for backend in backends
        }
        if len(profiles) != len(backends):
            raise ValueError("worker_id values must be unique")
        union_capabilities = frozenset().union(
            *(backend.capabilities for backend in backends)
        )
        ready = list(mesh.ready(union_capabilities))
        service = self._service(mesh)
        self._active_flows = self._active_by_fairness(mesh)
        self._seat_limit = mesh.max_concurrency
        state = _PlanningState(
            resources_used={backend.worker_id: {} for backend in backends},
            assignments={backend.worker_id: 0 for backend in backends},
            group_workers={},
            fairness_assignments={},
        )

        # Lower normalized service wins across flows; priority wins inside a flow.
        ready.sort(
            key=lambda task: (
                service.get(SchedulingIntent.from_task(task).fairness_key, 0.0),
                -task.priority,
                task.task_id,
            )
        )
        units = self._gang_units(ready)
        units.sort(
            key=lambda unit: (
                min(
                    service.get(
                        SchedulingIntent.from_task(task).fairness_key,
                        0.0,
                    )
                    for task in unit
                ),
                -max(task.priority for task in unit),
                tuple(task.task_id for task in unit),
            )
        )

        assignments: list[WorkerAssignment] = []
        deferred: list[str] = []
        unroutable: list[str] = []
        backpressured: list[str] = []

        for unit in units:
            tentative = state.clone()
            unit_assignments: list[WorkerAssignment] = []
            failures: list[tuple[str, str]] = []
            for task in unit:
                intent = SchedulingIntent.from_task(task)
                assignment, reason = self._assign_one(
                    task,
                    intent,
                    backends,
                    profiles,
                    tentative,
                )
                if assignment is None:
                    failures.append((task.task_id, reason or "deferred"))
                    if SchedulingIntent.from_task(task).gang:
                        break
                else:
                    unit_assignments.append(assignment)

            if failures and any(SchedulingIntent.from_task(task).gang for task in unit):
                # Atomic placement group: all concurrently-ready members reserve
                # capacity or none of them do.
                for task in unit:
                    reason = failures[0][1]
                    if reason == "unroutable":
                        unroutable.append(task.task_id)
                    elif "backpressure" in reason:
                        backpressured.append(task.task_id)
                    else:
                        deferred.append(task.task_id)
                continue

            state = tentative
            assignments.extend(unit_assignments)
            assigned_ids = {item.task.task_id for item in unit_assignments}
            for task_id, reason in failures:
                if task_id in assigned_ids:
                    continue
                if reason == "unroutable":
                    unroutable.append(task_id)
                elif "backpressure" in reason:
                    backpressured.append(task_id)
                else:
                    deferred.append(task_id)

        return SchedulingDecision(
            assignments=tuple(assignments),
            deferred=tuple(sorted(set(deferred))),
            unroutable=tuple(sorted(set(unroutable))),
            backpressured=tuple(sorted(set(backpressured))),
            seats_used=state.seats_used,
            fairness_service=service,
        )

    def plan_wave(
        self,
        mesh: ExecutionMesh,
        backends: Sequence[WorkerBackend],
    ) -> tuple[WorkerAssignment, ...]:
        return self.plan(mesh, backends).assignments
