"""Durable heterogeneous execution for ECHO.

This module composes the source-counterengineered execution mesh, specialist
federation, and durable fencing plane into one execution path:

    restore -> specialist plan -> atomic claim -> mark running -> commit lease
    -> concurrent compute -> heartbeat -> fenced result commit -> checkpoint

Worker compute remains provider-neutral. Database state remains ECHO-owned.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from echo.durable_execution import (
    DurableExecutionStore,
    DurableRunORM,
    LeaseToken,
    StaleLeaseError,
)
from echo.execution_mesh import (
    ExecutionContext,
    ExecutionMesh,
    ExecutionTask,
    TaskState,
    WorkerBackend,
    WorkerResult,
)
from echo.federation import FederatedExecutor
from echo.models import utcnow


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class DurableAssignment:
    task: ExecutionTask
    backend: WorkerBackend
    token: LeaseToken
    attempt: int
    fitness: float
    context: ExecutionContext


@dataclass(frozen=True)
class DurableOutcome:
    task_id: str
    worker_id: str
    outcome: str
    error: str = ""


class DurableFederatedExecutor:
    """Execute one durable DAG across heterogeneous detachable workers.

    Planning uses the existing ``FederatedExecutor`` fitness policy. Ownership
    is then revalidated and atomically claimed in the durable store before any
    compute starts. A worker can therefore be selected for its specialized
    boundary fitness without weakening the database fencing guarantees.
    """

    def __init__(
        self,
        store: DurableExecutionStore,
        run_id: str,
        *,
        lease_seconds: float | None = None,
        heartbeat_interval: float | None = None,
    ) -> None:
        self.store = store
        self.run_id = run_id
        run = store.session.get(DurableRunORM, run_id)
        if run is None:
            raise ValueError(f"durable run not found: {run_id}")
        self.lease_seconds = float(
            run.lease_seconds if lease_seconds is None else lease_seconds
        )
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        default_heartbeat = self.lease_seconds / 3.0
        self.heartbeat_interval = float(
            heartbeat_interval if heartbeat_interval is not None else default_heartbeat
        )
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        if self.heartbeat_interval >= self.lease_seconds:
            raise ValueError("heartbeat_interval must be below lease_seconds")

    def _claim_exact(
        self,
        task: ExecutionTask,
        backend: WorkerBackend,
        *,
        now: datetime | None = None,
    ) -> tuple[LeaseToken, int] | None:
        """Atomically claim the exact task selected by specialist planning."""
        observation_time = _aware(now or utcnow())
        self.store.recover_expired(self.run_id, now=observation_time)
        rows = self.store._task_rows(self.run_id)
        by_id = {row.task_id: row for row in rows}
        candidate = by_id.get(task.task_id)
        if candidate is None or candidate.status != TaskState.PENDING.value:
            return None
        if candidate.attempts >= int(candidate.definition.get("max_attempts", 1)):
            return None
        if not set(candidate.definition.get("dependencies", [])).issubset(
            {row.task_id for row in rows if row.status == TaskState.SUCCEEDED.value}
        ):
            return None
        if not set(candidate.definition.get("required_capabilities", [])).issubset(
            backend.capabilities
        ):
            return None

        dialect = self.store.session.get_bind().dialect.name
        row = self.store._locked_task(
            self.run_id,
            task.task_id,
            skip_locked=dialect == "postgresql",
        )
        if row is None or row.status != TaskState.PENDING.value:
            return None
        if row.attempts >= int(row.definition.get("max_attempts", 1)):
            return None

        claim_now = _aware(now or utcnow())
        expires_at = claim_now + timedelta(seconds=self.lease_seconds)
        if dialect == "sqlite":
            row = self.store._claim_sqlite(
                row,
                worker_id=backend.worker_id,
                expires_at=expires_at,
            )
            if row is None:
                return None
        else:
            row.lease_epoch += 1
            row.lease_owner = backend.worker_id
            row.lease_expires_at = expires_at
            row.status = TaskState.LEASED.value
            row.attempts += 1
            row.last_error = ""

        self.store._append_event(
            self.run_id,
            task.task_id,
            "task_leased",
            row.lease_epoch,
            {
                "worker_id": backend.worker_id,
                "expires_at": expires_at.isoformat(),
                "routing_fitness": FederatedExecutor._worker_fitness(backend, task),
            },
        )
        self.store.session.flush()
        return (
            LeaseToken(
                run_id=self.run_id,
                task_id=task.task_id,
                worker_id=backend.worker_id,
                epoch=row.lease_epoch,
                expires_at=expires_at,
            ),
            row.attempts,
        )

    def plan_and_claim(
        self,
        backends: Sequence[WorkerBackend],
        *,
        now: datetime | None = None,
    ) -> tuple[DurableAssignment, ...]:
        """Restore current state, choose specialists, and durably reserve a wave."""
        mesh = self.store.restore_mesh(self.run_id)
        planned = FederatedExecutor(mesh).plan_wave(backends)
        claimed: list[DurableAssignment] = []
        try:
            for assignment in planned:
                claimed_state = self._claim_exact(
                    assignment.task,
                    assignment.backend,
                    now=now,
                )
                if claimed_state is None:
                    continue
                token, attempt = claimed_state
                self.store.mark_running(token, now=_aware(now or utcnow()))
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

    async def _compute(
        self,
        assignment: DurableAssignment,
    ) -> WorkerResult | Exception:
        try:
            result = await asyncio.wait_for(
                assignment.backend.execute(assignment.task, assignment.context),
                timeout=assignment.task.timeout_seconds,
            )
            ExecutionMesh._validate_result(assignment.task, result)
            return result
        except Exception as exc:
            return exc

    def _heartbeat(
        self,
        assignments: Sequence[DurableAssignment],
        pending_task_ids: set[str],
    ) -> set[str]:
        lost: set[str] = set()
        try:
            for assignment in assignments:
                if assignment.task.task_id not in pending_task_ids:
                    continue
                try:
                    self.store.heartbeat(
                        assignment.token,
                        lease_seconds=self.lease_seconds,
                        now=utcnow(),
                    )
                except StaleLeaseError:
                    lost.add(assignment.task.task_id)
            self.store.session.commit()
        except Exception:
            self.store.session.rollback()
            raise
        return lost

    @staticmethod
    async def _cancel_tasks(
        running: dict[str, asyncio.Task[WorkerResult | Exception]],
        task_ids: set[str],
    ) -> None:
        cancelled: list[asyncio.Task[WorkerResult | Exception]] = []
        for task_id in sorted(task_ids):
            task = running.pop(task_id, None)
            if task is None:
                continue
            task.cancel()
            cancelled.append(task)
        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)

    async def _abort_wave(
        self,
        assignments: Mapping[str, DurableAssignment],
        running: dict[str, asyncio.Task[WorkerResult | Exception]],
        *,
        reason: str,
        extra_task_ids: set[str] | None = None,
    ) -> None:
        task_ids = set(running)
        task_ids.update(extra_task_ids or set())
        await self._cancel_tasks(running, set(running))
        self.store.session.rollback()
        for task_id in sorted(task_ids):
            assignment = assignments.get(task_id)
            if assignment is None:
                continue
            try:
                self.store.fail(
                    assignment.token,
                    reason,
                    retry=True,
                )
                self.store.session.commit()
            except StaleLeaseError:
                self.store.session.rollback()
            except Exception:
                self.store.session.rollback()

    async def run_wave(
        self,
        backends: Sequence[WorkerBackend],
    ) -> tuple[DurableOutcome, ...]:
        assignments = self.plan_and_claim(backends)
        if not assignments:
            return ()

        running: dict[str, asyncio.Task[WorkerResult | Exception]] = {
            assignment.task.task_id: asyncio.create_task(self._compute(assignment))
            for assignment in assignments
        }
        by_task = {assignment.task.task_id: assignment for assignment in assignments}
        outcomes: list[DurableOutcome] = []

        try:
            while running:
                done, _ = await asyncio.wait(
                    set(running.values()),
                    timeout=self.heartbeat_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    try:
                        lost = self._heartbeat(assignments, set(running))
                    except Exception as exc:
                        await self._abort_wave(
                            by_task,
                            running,
                            reason=(
                                "heartbeat persistence failure: "
                                f"{type(exc).__name__}: {exc}"
                            ),
                        )
                        raise RuntimeError(
                            "durable heartbeat persistence failed; wave compute cancelled"
                        ) from exc
                    await self._cancel_tasks(running, lost)
                    for task_id in sorted(lost):
                        outcomes.append(
                            DurableOutcome(
                                task_id=task_id,
                                worker_id=by_task[task_id].backend.worker_id,
                                outcome="stale_rejected",
                                error="durable lease ownership lost during execution",
                            )
                        )
                    continue

                completed_ids = sorted(
                    task_id for task_id, future in running.items() if future in done
                )
                for task_id in completed_ids:
                    future = running.pop(task_id)
                    assignment = by_task[task_id]
                    value = await future
                    try:
                        if isinstance(value, Exception):
                            status = self.store.fail(
                                assignment.token,
                                f"{type(value).__name__}: {value}",
                                retry=True,
                            )
                            outcomes.append(
                                DurableOutcome(
                                    task_id=task_id,
                                    worker_id=assignment.backend.worker_id,
                                    outcome=(
                                        "retry"
                                        if status == TaskState.PENDING.value
                                        else "failed"
                                    ),
                                    error=f"{type(value).__name__}: {value}",
                                )
                            )
                        else:
                            self.store.complete(assignment.token, value)
                            outcomes.append(
                                DurableOutcome(
                                    task_id=task_id,
                                    worker_id=assignment.backend.worker_id,
                                    outcome="success",
                                )
                            )
                        self.store.session.commit()
                    except StaleLeaseError as exc:
                        self.store.session.rollback()
                        outcomes.append(
                            DurableOutcome(
                                task_id=task_id,
                                worker_id=assignment.backend.worker_id,
                                outcome="stale_rejected",
                                error=str(exc),
                            )
                        )
                    except Exception as exc:
                        self.store.session.rollback()
                        await self._abort_wave(
                            by_task,
                            running,
                            reason=(
                                "result persistence failure: "
                                f"{type(exc).__name__}: {exc}"
                            ),
                            extra_task_ids={task_id},
                        )
                        raise RuntimeError(
                            "durable result persistence failed; wave compute cancelled"
                        ) from exc

                if running:
                    try:
                        lost = self._heartbeat(assignments, set(running))
                    except Exception as exc:
                        await self._abort_wave(
                            by_task,
                            running,
                            reason=(
                                "heartbeat persistence failure: "
                                f"{type(exc).__name__}: {exc}"
                            ),
                        )
                        raise RuntimeError(
                            "durable heartbeat persistence failed; wave compute cancelled"
                        ) from exc
                    await self._cancel_tasks(running, lost)
                    for task_id in sorted(lost):
                        outcomes.append(
                            DurableOutcome(
                                task_id=task_id,
                                worker_id=by_task[task_id].backend.worker_id,
                                outcome="stale_rejected",
                                error="durable lease ownership lost during execution",
                            )
                        )
        except asyncio.CancelledError:
            await self._abort_wave(
                by_task,
                running,
                reason="CancelledError: durable federated wave cancelled",
            )
            raise

        restored = self.store.restore_mesh(self.run_id)
        self.store.save_snapshot(self.run_id, restored.snapshot(), commit=True)
        return tuple(outcomes)

    async def run_to_completion(
        self,
        backends: Sequence[WorkerBackend],
    ) -> Mapping[str, Any]:
        waves = 0
        assignment_count = 0
        outcomes: list[DurableOutcome] = []
        while True:
            wave = await self.run_wave(backends)
            if not wave:
                break
            waves += 1
            assignment_count += len(wave)
            outcomes.extend(wave)

        mesh = self.store.restore_mesh(self.run_id)
        available_capabilities = (
            frozenset().union(*(backend.capabilities for backend in backends))
            if backends
            else frozenset()
        )
        incomplete = sorted(
            task_id
            for task_id, state in mesh.runtime.items()
            if state.state in {TaskState.PENDING, TaskState.LEASED, TaskState.RUNNING}
        )
        unroutable = sorted(
            task_id
            for task_id in incomplete
            if not set(mesh.tasks[task_id].required_capabilities).issubset(
                available_capabilities
            )
        )
        return {
            "schema": "glaciereq.echo.durable-federated-execution-run.v1",
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
            "unroutable": unroutable,
            "outcomes": [
                {
                    "task_id": item.task_id,
                    "worker_id": item.worker_id,
                    "outcome": item.outcome,
                    "error": item.error,
                }
                for item in outcomes
            ],
            "receipt_head": (mesh.receipts[-1].content_hash if mesh.receipts else ""),
        }
