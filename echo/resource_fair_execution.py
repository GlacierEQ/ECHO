"""Durable resource/fair execution plane for ECHO.

The resource scheduler decides *which* specialist should receive capacity; the
durable federation then atomically claims that exact task before compute. This
keeps fairness, resource placement, version/drain routing, and backpressure on
the same fenced execution path as crash recovery and receipts.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from echo.durable_federation import (
    DurableAssignment,
    DurableFederatedExecutor,
    DurableOutcome,
)
from echo.execution_mesh import ExecutionContext, TaskState, WorkerBackend
from echo.models import utcnow
from echo.resource_scheduler import ResourceFairScheduler, SchedulingDecision, SchedulingIntent


class ResourceFairDurableExecutor(DurableFederatedExecutor):
    def __init__(
        self,
        *args: Any,
        scheduler: ResourceFairScheduler | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.scheduler = scheduler or ResourceFairScheduler()
        self.last_scheduling_decision: SchedulingDecision | None = None

    def plan_and_claim(
        self,
        backends: Sequence[WorkerBackend],
        *,
        now=None,
    ) -> tuple[DurableAssignment, ...]:
        mesh = self.store.restore_mesh(self.run_id)
        decision = self.scheduler.plan(mesh, backends)
        self.last_scheduling_decision = decision
        claimed: list[DurableAssignment] = []
        try:
            for assignment in decision.assignments:
                claimed_state = self._claim_exact(
                    assignment.task,
                    assignment.backend,
                    now=now,
                )
                if claimed_state is None:
                    continue
                token, attempt = claimed_state
                mark_now = now or utcnow()
                self.store.mark_running(token, now=mark_now)
                intent = SchedulingIntent.from_task(assignment.task)
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
            self.store.session.commit()
        except Exception:
            self.store.session.rollback()
            raise
        return tuple(claimed)

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
