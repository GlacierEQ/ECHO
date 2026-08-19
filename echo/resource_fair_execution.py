"""Durable resource/fair execution plane for ECHO.

The resource scheduler decides *which* specialist should receive capacity; the
durable federation then atomically claims that exact task before compute. This
keeps fairness, resource placement, version/drain routing, and backpressure on
the same fenced execution path as crash recovery and receipts.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from echo.durable_federation import (
    DurableAssignment,
    DurableFederatedExecutor,
    DurableOutcome,
)
from echo.execution_mesh import ExecutionContext, TaskState, WorkerBackend
from echo.models import utcnow
from echo.resource_scheduler import (
    ResourceFairScheduler,
    SchedulingDecision,
    SchedulingIntent,
)


class _GangClaimConflict(RuntimeError):
    def __init__(self, task_ids: Sequence[str]) -> None:
        self.task_ids = tuple(sorted(set(task_ids)))
        super().__init__(
            "gang claim lost atomicity race: " + ",".join(self.task_ids)
        )


class ResourceFairDurableExecutor(DurableFederatedExecutor):
    def __init__(
        self,
        *args: Any,
        scheduler: ResourceFairScheduler | None = None,
        max_claim_replans: int = 3,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if max_claim_replans < 1:
            raise ValueError("max_claim_replans must be positive")
        self.scheduler = scheduler or ResourceFairScheduler()
        self.max_claim_replans = max_claim_replans
        self.last_scheduling_decision: SchedulingDecision | None = None

    @staticmethod
    def _gang_members(
        decision: SchedulingDecision,
    ) -> dict[str, set[str]]:
        groups: dict[str, set[str]] = {}
        for assignment in decision.assignments:
            intent = SchedulingIntent.from_task(assignment.task)
            if intent.gang:
                groups.setdefault(intent.placement_group, set()).add(
                    assignment.task.task_id
                )
        return groups

    def _claim_decision(
        self,
        mesh,
        decision: SchedulingDecision,
        *,
        now: datetime | None,
    ) -> tuple[DurableAssignment, ...]:
        gang_members = self._gang_members(decision)
        claimed: list[DurableAssignment] = []
        for assignment in decision.assignments:
            intent = SchedulingIntent.from_task(assignment.task)
            claimed_state = self._claim_exact(
                assignment.task,
                assignment.backend,
                now=now,
            )
            if claimed_state is None:
                if intent.gang:
                    raise _GangClaimConflict(
                        gang_members.get(
                            intent.placement_group,
                            {assignment.task.task_id},
                        )
                    )
                continue
            token, attempt = claimed_state
            self.store.mark_running(token, now=now or utcnow())
            self.store._append_event(
                self.run_id,
                assignment.task.task_id,
                "resource_fair_scheduled",
                token.epoch,
                {
                    "worker_id": assignment.backend.worker_id,
                    "fitness": assignment.fitness,
                    "fairness_key": intent.fairness_key,
                    "fairness_weight": intent.fairness_weight,
                    "seats": intent.seats,
                    "resources": dict(intent.resources),
                    "placement_group": intent.placement_group,
                    "placement_strategy": intent.placement_strategy,
                    "gang": intent.gang,
                    "topology": dict(intent.topology),
                    "required_worker_version": intent.required_worker_version,
                },
            )
            context = ExecutionContext(
                workspace_id=(
                    assignment.task.workspace_id
                    or f"echo:{assignment.task.task_id}"
                ),
                dependency_outputs={
                    dependency: mesh.results[dependency].output
                    for dependency in assignment.task.dependencies
                },
                dependency_terminals={
                    dependency: mesh.results[dependency].terminal
                    for dependency in assignment.task.dependencies
                },
                attempt=attempt,
            )
            claimed.append(
                DurableAssignment(
                    task=assignment.task,
                    backend=assignment.backend,
                    token=token,
                    attempt=attempt,
                    fitness=assignment.fitness,
                    context=context,
                )
            )
        return tuple(claimed)

    def plan_and_claim(
        self,
        backends: Sequence[WorkerBackend],
        *,
        now: datetime | None = None,
    ) -> tuple[DurableAssignment, ...]:
        last_conflict: _GangClaimConflict | None = None
        for attempt in range(self.max_claim_replans):
            mesh = self.store.restore_mesh(self.run_id)
            decision = self.scheduler.plan(mesh, backends)
            self.last_scheduling_decision = decision
            try:
                claimed = self._claim_decision(mesh, decision, now=now)
                self.store.session.commit()
                return claimed
            except _GangClaimConflict as exc:
                self.store.session.rollback()
                last_conflict = exc
                if attempt + 1 < self.max_claim_replans:
                    continue
            except Exception:
                self.store.session.rollback()
                raise

        if last_conflict is not None and self.last_scheduling_decision is not None:
            prior = self.last_scheduling_decision
            self.last_scheduling_decision = SchedulingDecision(
                assignments=(),
                deferred=tuple(sorted(set(prior.deferred) | set(last_conflict.task_ids))),
                unroutable=prior.unroutable,
                backpressured=prior.backpressured,
                seats_used=0,
                fairness_service=prior.fairness_service,
            )
        return ()

    async def run_to_completion(
        self,
        backends: Sequence[WorkerBackend],
    ) -> Mapping[str, Any]:
        waves = 0
        assignment_count = 0
        outcomes: list[DurableOutcome] = []
        scheduler_backpressured: set[str] = set()
        scheduler_unroutable: set[str] = set()
        scheduler_deferred: set[str] = set()

        while True:
            wave = await self.run_wave(backends)
            decision = self.last_scheduling_decision
            if decision is not None:
                scheduler_backpressured.update(decision.backpressured)
                scheduler_unroutable.update(decision.unroutable)
                scheduler_deferred.update(decision.deferred)
            if not wave:
                break
            waves += 1
            assignment_count += len(wave)
            outcomes.extend(wave)

        mesh = self.store.restore_mesh(self.run_id)
        incomplete = sorted(
            task_id
            for task_id, state in mesh.runtime.items()
            if state.state in {TaskState.PENDING, TaskState.LEASED, TaskState.RUNNING}
        )
        return {
            "schema": "glaciereq.echo.resource-fair-durable-execution-run.v1",
            "waves": waves,
            "assignments": assignment_count,
            "workers": sorted(backend.worker_id for backend in backends),
            "succeeded": sorted(
                task_id
                for task_id, state in mesh.runtime.items()
                if state.state == TaskState.SUCCEEDED
            ),
            "failed": sorted(
                task_id
                for task_id, state in mesh.runtime.items()
                if state.state == TaskState.FAILED
            ),
            "blocked": sorted(
                task_id
                for task_id, state in mesh.runtime.items()
                if state.state == TaskState.BLOCKED
            ),
            "incomplete": incomplete,
            "scheduler_backpressured": sorted(
                task_id for task_id in scheduler_backpressured if task_id in incomplete
            ),
            "scheduler_unroutable": sorted(
                task_id for task_id in scheduler_unroutable if task_id in incomplete
            ),
            "scheduler_deferred": sorted(
                task_id for task_id in scheduler_deferred if task_id in incomplete
            ),
            "outcomes": [
                {
                    "task_id": item.task_id,
                    "worker_id": item.worker_id,
                    "outcome": item.outcome,
                    "error": item.error,
                }
                for item in outcomes
            ],
            "receipt_head": (
                mesh.receipts[-1].content_hash if mesh.receipts else ""
            ),
        }
