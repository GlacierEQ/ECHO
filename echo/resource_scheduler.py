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
    placement_strategy: str = "none"
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
        resources = raw.get("resources", {})
        topology = raw.get("topology", {})
        if not isinstance(resources, Mapping):
            raise ValueError("scheduling resources must be an object")
        if not isinstance(topology, Mapping):
            raise ValueError("scheduling topology must be an object")
        intent = cls(
            fairness_key=str(raw.get("fairness_key", "default")),
            fairness_weight=float(raw.get("fairness_weight", 1.0)),
            seats=int(raw.get("seats", 1)),
            resources={str(key): float(value) for key, value in resources.items()},
            placement_group=str(raw.get("placement_group", "")),
            placement_strategy=str(raw.get("placement_strategy", "none")),
            gang=bool(raw.get("gang", False)),
            topology={str(key): str(value) for key, value in topology.items()},
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
    used_resources: Mapping[str, float]
    topology: Mapping[str, str]
    version: str
    draining: bool
    inflight: int
    max_inflight: int | None

    @classmethod
    def from_backend(cls, backend: WorkerBackend) -> "WorkerRuntimeProfile":
        raw_resources = getattr(backend, "resources", {})
        raw_used = getattr(backend, "used_resources", {})
        raw_topology = getattr(backend, "topology", {})
        if not isinstance(raw_resources, Mapping):
            raise ValueError(f"worker {backend.worker_id} resources must be a mapping")
        if not isinstance(raw_used, Mapping):
            raise ValueError(
                f"worker {backend.worker_id} used_resources must be a mapping"
            )
        if not isinstance(raw_topology, Mapping):
            raise ValueError(f"worker {backend.worker_id} topology must be a mapping")
        resources = {
            str(key): _finite_nonnegative(
                f"worker {backend.worker_id} resource {key}", float(value)
            )
            for key, value in raw_resources.items()
        }
        used = {
            str(key): _finite_nonnegative(
                f"worker {backend.worker_id} used resource {key}", float(value)
            )
            for key, value in raw_used.items()
        }
        for name, amount in used.items():
            if name not in resources:
                raise ValueError(
                    f"worker {backend.worker_id} uses undeclared resource {name}"
                )
            if amount > resources[name] + 1e-12:
                raise ValueError(
                    f"worker {backend.worker_id} used {name} exceeds capacity"
                )
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
            used_resources=used,
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
            group_workers={
                group: list(workers) for group, workers in self.group_workers.items()
            },
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
    """Replayable weighted-fair resource scheduler.

    Fairness is reconstructed from durable attempt counts rather than an
    in-memory deficit counter. A replacement process therefore retains service
    history including retries. Lower normalized service receives the next
    scheduling opportunity; priority orders work inside a flow.
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
    def _fairness_weights(mesh: ExecutionMesh) -> dict[str, float]:
        observed: dict[str, set[float]] = {}
        for task in mesh.tasks.values():
            intent = SchedulingIntent.from_task(task)
            observed.setdefault(intent.fairness_key, set()).add(intent.fairness_weight)
        conflicts = {
            key: sorted(values)
            for key, values in observed.items()
            if len(values) > 1
        }
        if conflicts:
            raise ValueError(
                "fairness_weight must be consistent within each fairness_key: "
                f"{conflicts}"
            )
        return {key: next(iter(values)) for key, values in observed.items()}

    @classmethod
    def _service(
        cls,
        mesh: ExecutionMesh,
        weights: Mapping[str, float],
    ) -> dict[str, float]:
        served: dict[str, float] = {}
        for task_id, task in mesh.tasks.items():
            intent = SchedulingIntent.from_task(task)
            attempts = mesh.runtime[task_id].attempts
            if attempts:
                served[intent.fairness_key] = (
                    served.get(intent.fairness_key, 0.0) + attempts * intent.seats
                )
        return {
            key: served.get(key, 0.0) / weight
            for key, weight in weights.items()
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
    def _resource_fits_total(
        profile: WorkerRuntimeProfile,
        intent: SchedulingIntent,
    ) -> bool:
        return all(
            name in profile.resources
            and amount <= profile.resources[name] + 1e-12
            for name, amount in intent.resources.items()
        )

    @classmethod
    def _resource_fits_available(
        cls,
        profile: WorkerRuntimeProfile,
        intent: SchedulingIntent,
        used: Mapping[str, float],
    ) -> bool:
        if not cls._resource_fits_total(profile, intent):
            return False
        return all(
            used.get(name, 0.0) + amount <= profile.resources[name] + 1e-12
            for name, amount in intent.resources.items()
        )

    @staticmethod
    def _topology_fits(
        profile: WorkerRuntimeProfile,
        intent: SchedulingIntent,
    ) -> bool:
        return all(
            profile.topology.get(key) == value
            for key, value in intent.topology.items()
        )

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
        intent: SchedulingIntent,
        state: _PlanningState,
    ) -> tuple[list[WorkerBackend], bool]:
        if not intent.placement_group or intent.placement_strategy == "none":
            return list(candidates), False
        previous = state.group_workers.get(intent.placement_group, [])
        if intent.placement_strategy == "pack" and previous:
            packed = [
                worker for worker in candidates if worker.worker_id == previous[0]
            ]
            return packed, not packed and bool(candidates)
        if intent.placement_strategy == "spread" and previous:
            unused = [
                worker
                for worker in candidates
                if worker.worker_id not in set(previous)
            ]
            if unused:
                return unused, False
            return list(candidates), False
        return list(candidates), False

    def _candidate_workers(
        self,
        task: ExecutionTask,
        intent: SchedulingIntent,
        backends: Sequence[WorkerBackend],
        profiles: Mapping[str, WorkerRuntimeProfile],
        state: _PlanningState,
    ) -> tuple[list[WorkerBackend], str | None]:
        hard_eligible: list[WorkerBackend] = []
        total_resource_fit: list[WorkerBackend] = []
        available_resource_fit: list[WorkerBackend] = []
        unsaturated: list[WorkerBackend] = []
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
            hard_eligible.append(backend)
            if not self._resource_fits_total(profile, intent):
                continue
            total_resource_fit.append(backend)
            if not self._resource_fits_available(
                profile,
                intent,
                state.resources_used[backend.worker_id],
            ):
                continue
            available_resource_fit.append(backend)
            if not self._backpressure_fits(
                profile,
                state.assignments[backend.worker_id],
            ):
                continue
            unsaturated.append(backend)

        if not hard_eligible or not total_resource_fit:
            return [], "unroutable"
        if not available_resource_fit:
            return [], "resource_backpressure"
        if not unsaturated:
            return [], "worker_backpressure"
        placed, placement_blocked = self._placement_candidates(
            unsaturated,
            intent,
            state,
        )
        if placement_blocked:
            return [], "placement_backpressure"
        return placed, None if placed else "unroutable"

    def _assign_one(
        self,
        task: ExecutionTask,
        intent: SchedulingIntent,
        backends: Sequence[WorkerBackend],
        profiles: Mapping[str, WorkerRuntimeProfile],
        state: _PlanningState,
        *,
        seat_limit: int,
        active_flows: Mapping[str, int],
    ) -> tuple[WorkerAssignment | None, str | None]:
        if state.seats_used + intent.seats > seat_limit:
            return None, "seat_backpressure"
        flow_limit = self.max_inflight_by_fairness.get(intent.fairness_key)
        if (
            flow_limit is not None
            and active_flows.get(intent.fairness_key, 0)
            + state.fairness_assignments.get(intent.fairness_key, 0)
            >= flow_limit
        ):
            return None, "flow_backpressure"

        candidates, reason = self._candidate_workers(
            task,
            intent,
            backends,
            profiles,
            state,
        )
        if not candidates:
            return None, reason or "unroutable"

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
    def _classify_failure(
        task_ids: Sequence[str],
        reason: str,
        *,
        deferred: list[str],
        unroutable: list[str],
        backpressured: list[str],
    ) -> None:
        if reason == "unroutable":
            unroutable.extend(task_ids)
        elif "backpressure" in reason:
            backpressured.extend(task_ids)
        else:
            deferred.extend(task_ids)

    @staticmethod
    def _ready_units(
        mesh: ExecutionMesh,
        ready: Sequence[ExecutionTask],
    ) -> tuple[list[tuple[ExecutionTask, ...]], list[str]]:
        ready_by_id = {task.task_id: task for task in ready}
        gang_members: dict[str, list[ExecutionTask]] = {}
        for task_id, task in mesh.tasks.items():
            intent = SchedulingIntent.from_task(task)
            if not intent.gang:
                continue
            if mesh.runtime[task_id].state == TaskState.PENDING:
                gang_members.setdefault(intent.placement_group, []).append(task)

        blocked_gangs: set[str] = set()
        for group, members in gang_members.items():
            if any(member.task_id not in ready_by_id for member in members):
                blocked_gangs.add(group)

        units: list[tuple[ExecutionTask, ...]] = []
        grouped: dict[str, list[ExecutionTask]] = {}
        deferred: list[str] = []
        for task in ready:
            intent = SchedulingIntent.from_task(task)
            if intent.gang:
                if intent.placement_group in blocked_gangs:
                    deferred.append(task.task_id)
                    continue
                grouped.setdefault(intent.placement_group, []).append(task)
            else:
                units.append((task,))
        units.extend(tuple(items) for _, items in sorted(grouped.items()))
        return units, deferred

    @staticmethod
    def _unit_fairness(
        unit: Sequence[ExecutionTask],
    ) -> tuple[str, float, int]:
        intents = [SchedulingIntent.from_task(task) for task in unit]
        keys = {intent.fairness_key for intent in intents}
        weights = {intent.fairness_weight for intent in intents}
        if len(keys) != 1 or len(weights) != 1:
            raise ValueError(
                "gang members must share fairness_key and fairness_weight"
            )
        return (
            intents[0].fairness_key,
            intents[0].fairness_weight,
            sum(intent.seats for intent in intents),
        )

    def plan(
        self,
        mesh: ExecutionMesh,
        backends: Sequence[WorkerBackend],
    ) -> SchedulingDecision:
        weights = self._fairness_weights(mesh)
        service = self._service(mesh, weights)
        declared_capabilities = (
            frozenset().union(
                *(task.required_capabilities for task in mesh.tasks.values())
            )
            if mesh.tasks
            else frozenset()
        )
        ready = list(mesh.ready(declared_capabilities))
        units, readiness_deferred = self._ready_units(mesh, ready)
        if not backends:
            return SchedulingDecision(
                assignments=(),
                deferred=tuple(sorted(readiness_deferred)),
                unroutable=tuple(sorted(task.task_id for task in ready)),
                backpressured=(),
                seats_used=0,
                fairness_service=service,
            )

        profiles = {
            backend.worker_id: WorkerRuntimeProfile.from_backend(backend)
            for backend in backends
        }
        if len(profiles) != len(backends):
            raise ValueError("worker_id values must be unique")
        active_flows = self._active_by_fairness(mesh)
        state = _PlanningState(
            resources_used={
                backend.worker_id: dict(profiles[backend.worker_id].used_resources)
                for backend in backends
            },
            assignments={backend.worker_id: 0 for backend in backends},
            group_workers={},
            fairness_assignments={},
        )

        queues: dict[str, list[tuple[ExecutionTask, ...]]] = {}
        unit_weights: dict[str, float] = {}
        for unit in units:
            key, weight, _ = self._unit_fairness(unit)
            queues.setdefault(key, []).append(unit)
            unit_weights[key] = weight
        for key in queues:
            queues[key].sort(
                key=lambda unit: (
                    -max(task.priority for task in unit),
                    tuple(task.task_id for task in unit),
                )
            )

        assignments: list[WorkerAssignment] = []
        deferred: list[str] = list(readiness_deferred)
        unroutable: list[str] = []
        backpressured: list[str] = []
        planned_seats: dict[str, int] = {key: 0 for key in queues}

        while any(queues.values()):
            available_keys = [key for key, queue in queues.items() if queue]
            key = min(
                available_keys,
                key=lambda flow: (
                    service.get(flow, 0.0)
                    + planned_seats.get(flow, 0) / unit_weights[flow],
                    -max(task.priority for task in queues[flow][0]),
                    flow,
                ),
            )
            unit = queues[key].pop(0)
            tentative = state.clone()
            unit_assignments: list[WorkerAssignment] = []
            failure_reason: str | None = None
            for task in unit:
                intent = SchedulingIntent.from_task(task)
                assignment, reason = self._assign_one(
                    task,
                    intent,
                    backends,
                    profiles,
                    tentative,
                    seat_limit=mesh.max_concurrency,
                    active_flows=active_flows,
                )
                if assignment is None:
                    failure_reason = reason or "deferred"
                    break
                unit_assignments.append(assignment)

            if failure_reason is not None:
                self._classify_failure(
                    [task.task_id for task in unit],
                    failure_reason,
                    deferred=deferred,
                    unroutable=unroutable,
                    backpressured=backpressured,
                )
                continue

            state = tentative
            assignments.extend(unit_assignments)
            planned_seats[key] = planned_seats.get(key, 0) + sum(
                SchedulingIntent.from_task(task).seats for task in unit
            )

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
