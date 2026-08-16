from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


AKOS_ASCENT_SCHEMA = "glaciereq.akos.apex-ascent-portfolio.v1"
AKOS_FRONTIER_SCHEMA = "glaciereq.akos.apex-frontier-motion.v1"


def _digest(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _nonempty(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _strings(value: object, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field_name} must be a sequence of strings")
    result: list[str] = []
    for item in value:
        result.append(_nonempty(item, field_name))
    return tuple(result)


def _finite(value: object, field_name: str, *, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be a finite number")
    return number


@dataclass(frozen=True)
class ExperimentSeed:
    experiment_id: str
    target: str
    members: tuple[str, ...]
    priority: float
    obligations: tuple[str, ...]
    source_refs: tuple[str, ...] = ()
    objective_signals: tuple[str, ...] = ()

    def validate(self) -> None:
        _nonempty(self.experiment_id, "experiment_id")
        _nonempty(self.target, "target")
        if not self.members:
            raise ValueError("experiment members must not be empty")
        if len(self.members) != len(set(self.members)):
            raise ValueError("experiment members must be unique")
        if not math.isfinite(self.priority):
            raise ValueError("experiment priority must be finite")
        for item in (*self.members, *self.obligations, *self.source_refs):
            _nonempty(item, "experiment seed value")


@dataclass(frozen=True)
class IncumbentSubject:
    subject_id: str
    target: str
    source_ref: str
    capabilities: tuple[str, ...] = ()

    def validate(self) -> None:
        _nonempty(self.subject_id, "incumbent.subject_id")
        _nonempty(self.target, "incumbent.target")
        _nonempty(self.source_ref, "incumbent.source_ref")


class RaceTaskKind(str, Enum):
    DISCOVER_BASELINE = "discover_baseline"
    PREPARE = "prepare"
    EXECUTE = "execute"
    COMPARE = "compare"
    PRESERVE = "preserve"


class RaceRole(str, Enum):
    INCUMBENT = "incumbent"
    CHALLENGER = "challenger"
    CONTROL = "control"


@dataclass(frozen=True)
class RaceTask:
    task_id: str
    kind: RaceTaskKind
    comparison_group: str
    target: str
    subject_id: str | None
    role: RaceRole
    dependencies: tuple[str, ...]
    isolation_scope: str
    idempotency_key: str
    metadata: Mapping[str, object] = field(default_factory=dict)
    mutates_production: bool = False


@dataclass(frozen=True)
class RacePlan:
    plan_id: str
    source_schema: str
    source_digest: str
    tasks: tuple[RaceTask, ...]
    max_parallel: int
    preserve_incumbents: bool = True

    def validate(self) -> None:
        _nonempty(self.plan_id, "plan_id")
        if self.source_schema not in {AKOS_ASCENT_SCHEMA, AKOS_FRONTIER_SCHEMA}:
            raise ValueError(f"unsupported ascent source schema: {self.source_schema}")
        if self.max_parallel <= 0:
            raise ValueError("max_parallel must be positive")
        if not self.preserve_incumbents:
            raise ValueError("APEX race plans must preserve incumbents during evaluation")

        task_ids = [task.task_id for task in self.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("race task IDs must be unique")
        known = set(task_ids)
        for task in self.tasks:
            if task.mutates_production:
                raise ValueError("race tasks cannot mutate production")
            if task.task_id in task.dependencies:
                raise ValueError("race task cannot depend on itself")
            missing = set(task.dependencies) - known
            if missing:
                raise ValueError(
                    f"race task {task.task_id} has unknown dependencies: {sorted(missing)}"
                )
        self._assert_acyclic()

    def _assert_acyclic(self) -> None:
        dependencies = {task.task_id: set(task.dependencies) for task in self.tasks}
        ready = [task_id for task_id, deps in dependencies.items() if not deps]
        visited = 0
        while ready:
            task_id = ready.pop()
            visited += 1
            for other_id, deps in dependencies.items():
                if task_id in deps:
                    deps.remove(task_id)
                    if not deps:
                        ready.append(other_id)
        if visited != len(dependencies):
            raise ValueError("race plan contains a dependency cycle")

    def ready_tasks(
        self,
        *,
        completed_task_ids: Iterable[str] = (),
        inflight_task_ids: Iterable[str] = (),
    ) -> tuple[RaceTask, ...]:
        completed = set(completed_task_ids)
        inflight = set(inflight_task_ids)
        slots = max(0, self.max_parallel - len(inflight))
        if slots == 0:
            return ()
        ready = [
            task
            for task in self.tasks
            if task.task_id not in completed
            and task.task_id not in inflight
            and set(task.dependencies).issubset(completed)
        ]
        ready.sort(
            key=lambda task: (
                self._kind_order(task.kind),
                task.comparison_group,
                task.subject_id or "",
                task.task_id,
            )
        )
        return tuple(ready[:slots])

    @staticmethod
    def _kind_order(kind: RaceTaskKind) -> int:
        return {
            RaceTaskKind.DISCOVER_BASELINE: 0,
            RaceTaskKind.PREPARE: 1,
            RaceTaskKind.EXECUTE: 2,
            RaceTaskKind.COMPARE: 3,
            RaceTaskKind.PRESERVE: 4,
        }[kind]


@dataclass(frozen=True)
class DispatchBatch:
    plan_id: str
    task_ids: tuple[str, ...]
    submission_receipts: tuple[str, ...]


def dispatch_ready(
    plan: RacePlan,
    submit: Callable[[RaceTask], str],
    *,
    completed_task_ids: Iterable[str] = (),
    inflight_task_ids: Iterable[str] = (),
) -> DispatchBatch:
    """Submit only the currently-ready bounded slice through a durable backend.

    ECHO's durable executor can use ``RaceTask.idempotency_key`` as its external
    idempotency key. This adapter deliberately does not invent another storage
    engine; it compiles ascent motion into deterministic work for the durable
    execution plane that already exists.
    """

    plan.validate()
    tasks = plan.ready_tasks(
        completed_task_ids=completed_task_ids,
        inflight_task_ids=inflight_task_ids,
    )
    receipts = tuple(_nonempty(submit(task), "submission receipt") for task in tasks)
    return DispatchBatch(
        plan_id=plan.plan_id,
        task_ids=tuple(task.task_id for task in tasks),
        submission_receipts=receipts,
    )


class AscentRaceCompiler:
    """Compile AKOS ascent/frontier portfolios into fair, isolated ECHO races.

    Missing incumbents create baseline-discovery work but do not stop challenger
    preparation or execution. Comparison waits for the baseline; experimentation
    does not. This converts missing evidence into parallel acquisition instead of
    another gate.
    """

    def compile(
        self,
        payload: Mapping[str, object],
        *,
        incumbents: Iterable[IncumbentSubject] = (),
        max_parallel: int = 4,
    ) -> RacePlan:
        if isinstance(max_parallel, bool) or not isinstance(max_parallel, int):
            raise ValueError("max_parallel must be an integer")
        if max_parallel <= 0:
            raise ValueError("max_parallel must be positive")

        schema = _nonempty(payload.get("schema"), "schema")
        seeds = self._seeds(payload, schema)
        if not seeds:
            raise ValueError("ascent payload contains no executable experiments")
        seed_ids = [seed.experiment_id for seed in seeds]
        if len(seed_ids) != len(set(seed_ids)):
            raise ValueError("ascent experiment identities must be unique")

        incumbent_map: dict[str, list[IncumbentSubject]] = {}
        for incumbent in incumbents:
            incumbent.validate()
            incumbent_map.setdefault(incumbent.target, []).append(incumbent)
        for values in incumbent_map.values():
            ids = [item.subject_id for item in values]
            if len(ids) != len(set(ids)):
                raise ValueError("incumbent subject identities must be unique per target")

        source_digest = _digest(payload)
        plan_id = f"apex-race-{source_digest[:20]}"
        groups: dict[str, list[ExperimentSeed]] = {}
        for seed in seeds:
            seed.validate()
            groups.setdefault(seed.target, []).append(seed)

        tasks: list[RaceTask] = []
        for target in sorted(groups):
            group_seeds = sorted(
                groups[target],
                key=lambda item: (-item.priority, item.experiment_id),
            )
            group_id = f"race-{_digest({'plan': plan_id, 'target': target})[:18]}"
            incumbents_for_target = tuple(
                sorted(incumbent_map.get(target, ()), key=lambda item: item.subject_id)
            )
            baseline_exec_ids: list[str] = []

            if incumbents_for_target:
                for incumbent in incumbents_for_target:
                    prepare, execute = self._subject_tasks(
                        plan_id=plan_id,
                        group_id=group_id,
                        target=target,
                        subject_id=incumbent.subject_id,
                        role=RaceRole.INCUMBENT,
                        metadata={
                            "source_ref": incumbent.source_ref,
                            "capabilities": list(incumbent.capabilities),
                        },
                    )
                    tasks.extend((prepare, execute))
                    baseline_exec_ids.append(execute.task_id)
            else:
                discover = self._task(
                    plan_id=plan_id,
                    group_id=group_id,
                    target=target,
                    kind=RaceTaskKind.DISCOVER_BASELINE,
                    subject_id=None,
                    role=RaceRole.CONTROL,
                    dependencies=(),
                    metadata={
                        "required_outcome": (
                            "identify the strongest current implementation or explicit "
                            "reference baseline for fair comparison"
                        )
                    },
                )
                tasks.append(discover)
                baseline_exec_ids.append(discover.task_id)

            challenger_exec_ids: list[str] = []
            challenger_ids: list[str] = []
            for seed in group_seeds:
                subject_id = seed.experiment_id
                prepare, execute = self._subject_tasks(
                    plan_id=plan_id,
                    group_id=group_id,
                    target=target,
                    subject_id=subject_id,
                    role=RaceRole.CHALLENGER,
                    metadata={
                        "members": list(seed.members),
                        "obligations": list(seed.obligations),
                        "source_refs": list(seed.source_refs),
                        "objective_signals": list(seed.objective_signals),
                        "priority": seed.priority,
                    },
                )
                tasks.extend((prepare, execute))
                challenger_exec_ids.append(execute.task_id)
                challenger_ids.append(subject_id)

            compare = self._task(
                plan_id=plan_id,
                group_id=group_id,
                target=target,
                kind=RaceTaskKind.COMPARE,
                subject_id=None,
                role=RaceRole.CONTROL,
                dependencies=tuple(sorted((*baseline_exec_ids, *challenger_exec_ids))),
                metadata={
                    "challengers": challenger_ids,
                    "incumbents": [item.subject_id for item in incumbents_for_target],
                    "baseline_discovery_required": not incumbents_for_target,
                    "comparison_rule": "vector_pareto_not_scalar_winner",
                },
            )
            preserve = self._task(
                plan_id=plan_id,
                group_id=group_id,
                target=target,
                kind=RaceTaskKind.PRESERVE,
                subject_id=None,
                role=RaceRole.CONTROL,
                dependencies=(compare.task_id,),
                metadata={
                    "required_outcome": (
                        "preserve every non-dominated incumbent and challenger; emit "
                        "promotion advice without deletion or production cutover authority"
                    )
                },
            )
            tasks.extend((compare, preserve))

        plan = RacePlan(
            plan_id=plan_id,
            source_schema=schema,
            source_digest=source_digest,
            tasks=tuple(tasks),
            max_parallel=max_parallel,
            preserve_incumbents=True,
        )
        plan.validate()
        return plan

    def _seeds(
        self,
        payload: Mapping[str, object],
        schema: str,
    ) -> tuple[ExperimentSeed, ...]:
        if schema == AKOS_ASCENT_SCHEMA:
            return self._ascent_seeds(payload)
        if schema == AKOS_FRONTIER_SCHEMA:
            return self._frontier_seeds(payload)
        raise ValueError(f"unsupported ascent source schema: {schema}")

    def _ascent_seeds(
        self,
        payload: Mapping[str, object],
    ) -> tuple[ExperimentSeed, ...]:
        experiments = payload.get("experiments")
        if not isinstance(experiments, list):
            raise ValueError("ascent portfolio experiments must be a list")
        seeds: list[ExperimentSeed] = []
        for index, raw in enumerate(experiments):
            if not isinstance(raw, Mapping):
                raise ValueError("ascent experiment must be an object")
            target = _nonempty(raw.get("target"), f"experiments[{index}].target")
            composition = raw.get("composition")
            if not isinstance(composition, Mapping):
                raise ValueError("ascent experiment composition must be an object")
            members = _strings(
                composition.get("node_ids"),
                f"experiments[{index}].composition.node_ids",
            )
            priority = _finite(
                raw.get("exploration_priority"),
                f"experiments[{index}].exploration_priority",
                default=_finite(
                    raw.get("target_priority"),
                    f"experiments[{index}].target_priority",
                ),
            )
            obligations = _strings(
                raw.get("obligations"),
                f"experiments[{index}].obligations",
            )
            source_refs = _strings(
                composition.get("source_refs"),
                f"experiments[{index}].composition.source_refs",
            )
            objective_signals = _strings(
                composition.get("objective_capabilities")
                or composition.get("unlocked_capabilities"),
                f"experiments[{index}].composition.objective_capabilities",
            )
            identity_payload = {
                "schema": AKOS_ASCENT_SCHEMA,
                "target": target,
                "members": members,
                "source_refs": source_refs,
            }
            experiment_id = f"challenger-{_digest(identity_payload)[:20]}"
            seeds.append(
                ExperimentSeed(
                    experiment_id=experiment_id,
                    target=target,
                    members=members,
                    priority=priority,
                    obligations=obligations,
                    source_refs=source_refs,
                    objective_signals=objective_signals,
                )
            )
        return tuple(seeds)

    def _frontier_seeds(
        self,
        payload: Mapping[str, object],
    ) -> tuple[ExperimentSeed, ...]:
        experiments = payload.get("experiments")
        if not isinstance(experiments, list):
            raise ValueError("frontier motion experiments must be a list")
        seeds: list[ExperimentSeed] = []
        for index, raw in enumerate(experiments):
            if not isinstance(raw, Mapping):
                raise ValueError("frontier experiment must be an object")
            if raw.get("production_mutation_allowed") is True:
                raise ValueError("frontier race experiments cannot mutate production")
            candidate_id = _nonempty(
                raw.get("candidate_id"),
                f"experiments[{index}].candidate_id",
            )
            priority = _finite(
                raw.get("potential_power"),
                f"experiments[{index}].potential_power",
                default=_finite(raw.get("score"), f"experiments[{index}].score"),
            )
            objectives = _strings(
                raw.get("objectives"),
                f"experiments[{index}].objectives",
            )
            blockers = _strings(
                raw.get("blockers"),
                f"experiments[{index}].blockers",
            )
            seeds.append(
                ExperimentSeed(
                    experiment_id=candidate_id,
                    target="frontier",
                    members=(candidate_id,),
                    priority=priority,
                    obligations=tuple(dict.fromkeys((*objectives, *blockers))),
                    objective_signals=objectives,
                )
            )
        return tuple(seeds)

    def _subject_tasks(
        self,
        *,
        plan_id: str,
        group_id: str,
        target: str,
        subject_id: str,
        role: RaceRole,
        metadata: Mapping[str, object],
    ) -> tuple[RaceTask, RaceTask]:
        prepare = self._task(
            plan_id=plan_id,
            group_id=group_id,
            target=target,
            kind=RaceTaskKind.PREPARE,
            subject_id=subject_id,
            role=role,
            dependencies=(),
            metadata=metadata,
        )
        execute = self._task(
            plan_id=plan_id,
            group_id=group_id,
            target=target,
            kind=RaceTaskKind.EXECUTE,
            subject_id=subject_id,
            role=role,
            dependencies=(prepare.task_id,),
            metadata=metadata,
        )
        return prepare, execute

    def _task(
        self,
        *,
        plan_id: str,
        group_id: str,
        target: str,
        kind: RaceTaskKind,
        subject_id: str | None,
        role: RaceRole,
        dependencies: tuple[str, ...],
        metadata: Mapping[str, object],
    ) -> RaceTask:
        identity = {
            "plan": plan_id,
            "group": group_id,
            "target": target,
            "kind": kind.value,
            "subject": subject_id,
            "role": role.value,
        }
        task_id = f"race-task-{_digest(identity)[:22]}"
        return RaceTask(
            task_id=task_id,
            kind=kind,
            comparison_group=group_id,
            target=target,
            subject_id=subject_id,
            role=role,
            dependencies=dependencies,
            isolation_scope=f"apex-race/{plan_id}/{group_id}/{subject_id or kind.value}",
            idempotency_key=_digest({"identity": identity, "metadata": metadata}),
            metadata=dict(metadata),
            mutates_production=False,
        )


class MetricDirection(str, Enum):
    HIGHER = "higher"
    LOWER = "lower"


@dataclass(frozen=True)
class MetricSpec:
    name: str
    direction: MetricDirection = MetricDirection.HIGHER
    tolerance: float = 0.0
    required: bool = True

    def validate(self) -> None:
        _nonempty(self.name, "metric.name")
        if not math.isfinite(self.tolerance) or self.tolerance < 0:
            raise ValueError("metric tolerance must be finite and non-negative")


@dataclass(frozen=True)
class PowerMeasurement:
    comparison_group: str
    subject_id: str
    values: Mapping[str, float]
    workload_digest: str
    environment_fingerprint: str
    evidence_refs: tuple[str, ...] = ()
    complete: bool = True

    def validate(self) -> None:
        _nonempty(self.comparison_group, "measurement.comparison_group")
        _nonempty(self.subject_id, "measurement.subject_id")
        _nonempty(self.workload_digest, "measurement.workload_digest")
        _nonempty(self.environment_fingerprint, "measurement.environment_fingerprint")
        if not self.values:
            raise ValueError("measurement values must not be empty")
        for name, value in self.values.items():
            _nonempty(name, "measurement metric")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("measurement values must be finite numbers")
            if not math.isfinite(float(value)):
                raise ValueError("measurement values must be finite numbers")


class RecommendationState(str, Enum):
    PROMOTION_CANDIDATE = "promotion_candidate"
    COEXIST = "coexist"
    DOMINATED = "dominated"
    RE_MEASURE = "re_measure"
    BASELINE_REQUIRED = "baseline_required"


@dataclass(frozen=True)
class SubjectRecommendation:
    comparison_group: str
    subject_id: str
    role: RaceRole
    state: RecommendationState
    reason: str
    production_cutover_allowed: bool = False
    provider_deletion_allowed: bool = False


@dataclass(frozen=True)
class RaceOutcome:
    frontier_by_group: Mapping[str, tuple[str, ...]]
    recommendations: tuple[SubjectRecommendation, ...]
    inconclusive_groups: tuple[str, ...]


class RaceEvaluator:
    """Evaluate fair comparison vectors without collapsing tradeoffs to one scalar."""

    def evaluate(
        self,
        plan: RacePlan,
        measurements: Iterable[PowerMeasurement],
        metrics: Iterable[MetricSpec],
    ) -> RaceOutcome:
        plan.validate()
        specs = tuple(metrics)
        if not specs:
            raise ValueError("race evaluation requires at least one metric")
        for spec in specs:
            spec.validate()
        names = [spec.name for spec in specs]
        if len(names) != len(set(names)):
            raise ValueError("metric names must be unique")

        by_key: dict[tuple[str, str], PowerMeasurement] = {}
        for measurement in measurements:
            measurement.validate()
            key = (measurement.comparison_group, measurement.subject_id)
            if key in by_key:
                raise ValueError("duplicate measurement for race subject")
            by_key[key] = measurement

        execute_tasks = [task for task in plan.tasks if task.kind is RaceTaskKind.EXECUTE]
        groups: dict[str, list[RaceTask]] = {}
        for task in execute_tasks:
            groups.setdefault(task.comparison_group, []).append(task)

        frontier_by_group: dict[str, tuple[str, ...]] = {}
        recommendations: list[SubjectRecommendation] = []
        inconclusive: list[str] = []

        for group_id in sorted(groups):
            subjects = groups[group_id]
            measurements_for_group = [
                by_key.get((group_id, task.subject_id or "")) for task in subjects
            ]
            incumbent_tasks = [task for task in subjects if task.role is RaceRole.INCUMBENT]
            if not incumbent_tasks:
                inconclusive.append(group_id)
                for task in subjects:
                    recommendations.append(
                        SubjectRecommendation(
                            comparison_group=group_id,
                            subject_id=task.subject_id or "",
                            role=task.role,
                            state=RecommendationState.BASELINE_REQUIRED,
                            reason="no incumbent/reference baseline measurement is bound",
                        )
                    )
                continue

            if not self._fair_and_complete(measurements_for_group, specs):
                inconclusive.append(group_id)
                for task in subjects:
                    recommendations.append(
                        SubjectRecommendation(
                            comparison_group=group_id,
                            subject_id=task.subject_id or "",
                            role=task.role,
                            state=RecommendationState.RE_MEASURE,
                            reason=(
                                "measurements are missing, incomplete, use different "
                                "workloads/environments, or omit required metrics"
                            ),
                        )
                    )
                continue

            typed_measurements = tuple(
                measurement
                for measurement in measurements_for_group
                if measurement is not None
            )
            frontier = tuple(
                sorted(
                    measurement.subject_id
                    for measurement in typed_measurements
                    if not any(
                        self._dominates(other, measurement, specs)
                        for other in typed_measurements
                        if other.subject_id != measurement.subject_id
                    )
                )
            )
            frontier_by_group[group_id] = frontier
            incumbent_ids = {
                task.subject_id for task in incumbent_tasks if task.subject_id is not None
            }
            measurements_by_subject = {
                measurement.subject_id: measurement for measurement in typed_measurements
            }

            for task in sorted(subjects, key=lambda item: item.subject_id or ""):
                subject_id = task.subject_id or ""
                measurement = measurements_by_subject[subject_id]
                if task.role is RaceRole.CHALLENGER:
                    incumbent_measurements = [
                        measurements_by_subject[item]
                        for item in incumbent_ids
                        if item in measurements_by_subject
                    ]
                    dominates_all_incumbents = bool(incumbent_measurements) and all(
                        self._dominates(measurement, incumbent, specs)
                        for incumbent in incumbent_measurements
                    )
                    if dominates_all_incumbents:
                        state = RecommendationState.PROMOTION_CANDIDATE
                        reason = (
                            "challenger dominates every measured incumbent on the "
                            "declared vector; production promotion remains a separate act"
                        )
                    elif subject_id in frontier:
                        state = RecommendationState.COEXIST
                        reason = (
                            "challenger is non-dominated and preserves a distinct "
                            "measured strength on the Pareto frontier"
                        )
                    else:
                        state = RecommendationState.DOMINATED
                        reason = (
                            "challenger is dominated in this race; retain provenance "
                            "and reject only this measured configuration"
                        )
                elif subject_id in frontier:
                    state = RecommendationState.COEXIST
                    reason = "incumbent remains non-dominated and must be preserved"
                else:
                    state = RecommendationState.DOMINATED
                    reason = (
                        "incumbent is dominated in this race, but ECHO has no provider "
                        "deletion or automatic cutover authority"
                    )
                recommendations.append(
                    SubjectRecommendation(
                        comparison_group=group_id,
                        subject_id=subject_id,
                        role=task.role,
                        state=state,
                        reason=reason,
                        production_cutover_allowed=False,
                        provider_deletion_allowed=False,
                    )
                )

        return RaceOutcome(
            frontier_by_group=frontier_by_group,
            recommendations=tuple(recommendations),
            inconclusive_groups=tuple(inconclusive),
        )

    @staticmethod
    def _fair_and_complete(
        measurements: Iterable[PowerMeasurement | None],
        specs: tuple[MetricSpec, ...],
    ) -> bool:
        items = tuple(measurements)
        if not items or any(item is None or not item.complete for item in items):
            return False
        typed = tuple(item for item in items if item is not None)
        if len({item.workload_digest for item in typed}) != 1:
            return False
        if len({item.environment_fingerprint for item in typed}) != 1:
            return False
        required = {spec.name for spec in specs if spec.required}
        return all(required.issubset(item.values) for item in typed)

    @staticmethod
    def _dominates(
        left: PowerMeasurement,
        right: PowerMeasurement,
        specs: tuple[MetricSpec, ...],
    ) -> bool:
        no_worse = True
        strictly_better = False
        for spec in specs:
            if spec.name not in left.values or spec.name not in right.values:
                if spec.required:
                    return False
                continue
            left_value = float(left.values[spec.name])
            right_value = float(right.values[spec.name])
            if spec.direction is MetricDirection.HIGHER:
                if left_value + spec.tolerance < right_value:
                    no_worse = False
                    break
                if left_value > right_value + spec.tolerance:
                    strictly_better = True
            else:
                if left_value - spec.tolerance > right_value:
                    no_worse = False
                    break
                if left_value < right_value - spec.tolerance:
                    strictly_better = True
        return no_worse and strictly_better
