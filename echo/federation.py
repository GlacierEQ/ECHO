"""Heterogeneous worker federation for the ECHO execution mesh.

One execution graph can span different model providers, local code workers,
accelerators, containers, Wasm components, or future runtimes.  The federation
routes each ready task to a worker that actually owns the required capability,
uses optional per-capability fitness to prefer specialists, and checkpoints the
shared ECHO state after each completed wave.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Mapping, Sequence

from echo.execution_mesh import (
    ExecutionMesh,
    ExecutionSnapshot,
    ExecutionTask,
    TaskState,
    WorkerBackend,
)


@dataclass(frozen=True)
class WorkerAssignment:
    task: ExecutionTask
    backend: WorkerBackend
    fitness: float


class FederatedExecutor:
    """Route one durable DAG across heterogeneous detachable workers."""

    def __init__(self, mesh: ExecutionMesh) -> None:
        self.mesh = mesh

    @staticmethod
    def _worker_fitness(backend: WorkerBackend, task: ExecutionTask) -> float:
        raw = getattr(backend, "fitness", {})
        fitness: Mapping[str, float] = raw if isinstance(raw, Mapping) else {}
        if task.required_capabilities:
            return sum(float(fitness.get(capability, 1.0)) for capability in task.required_capabilities)
        return float(fitness.get("*", 1.0))

    def plan_wave(
        self,
        backends: Sequence[WorkerBackend],
    ) -> tuple[WorkerAssignment, ...]:
        """Assign ready tasks to the strongest compatible workers.

        Selection is deterministic.  Capability fitness wins first, then load
        balancing within the wave, then worker identity.  A uniform default is
        never preferred over a measurably stronger specialist.
        """
        if not backends:
            return ()
        union_capabilities = frozenset().union(
            *(backend.capabilities for backend in backends)
        )
        ready = self.mesh.ready(union_capabilities)[: self.mesh.max_concurrency]
        load = {backend.worker_id: 0 for backend in backends}
        assignments: list[WorkerAssignment] = []
        for task in ready:
            candidates = [
                backend
                for backend in backends
                if set(task.required_capabilities).issubset(backend.capabilities)
            ]
            if not candidates:
                continue
            ranked = sorted(
                candidates,
                key=lambda backend: (
                    -self._worker_fitness(backend, task),
                    load[backend.worker_id],
                    backend.worker_id,
                ),
            )
            selected = ranked[0]
            load[selected.worker_id] += 1
            assignments.append(
                WorkerAssignment(
                    task=task,
                    backend=selected,
                    fitness=self._worker_fitness(selected, task),
                )
            )
        return tuple(assignments)

    async def run_wave(
        self,
        backends: Sequence[WorkerBackend],
        *,
        checkpoint: Callable[[ExecutionSnapshot], None] | None = None,
    ) -> tuple[WorkerAssignment, ...]:
        assignments = self.plan_wave(backends)
        if not assignments:
            return ()
        for assignment in assignments:
            self.mesh._lease(assignment.task, assignment.backend.worker_id)
        await asyncio.gather(
            *(
                self.mesh._execute_one(assignment.backend, assignment.task)
                for assignment in assignments
            )
        )
        self.mesh._propagate_blocked()
        if checkpoint is not None:
            checkpoint(self.mesh.snapshot())
        return assignments

    async def run_to_completion(
        self,
        backends: Sequence[WorkerBackend],
        *,
        checkpoint: Callable[[ExecutionSnapshot], None] | None = None,
    ) -> Mapping[str, object]:
        waves = 0
        assignment_count = 0
        while True:
            assignments = await self.run_wave(backends, checkpoint=checkpoint)
            if not assignments:
                self.mesh._propagate_blocked()
                break
            waves += 1
            assignment_count += len(assignments)

        incomplete = sorted(
            task_id
            for task_id, state in self.mesh.runtime.items()
            if state.state in {TaskState.PENDING, TaskState.LEASED, TaskState.RUNNING}
        )
        available_capabilities = frozenset().union(
            *(backend.capabilities for backend in backends)
        ) if backends else frozenset()
        unroutable = sorted(
            task_id
            for task_id in incomplete
            if not set(self.mesh.tasks[task_id].required_capabilities).issubset(
                available_capabilities
            )
        )
        return {
            "schema": "glaciereq.echo.federated-execution-run.v1",
            "waves": waves,
            "assignments": assignment_count,
            "workers": sorted(backend.worker_id for backend in backends),
            "succeeded": sorted(
                task_id
                for task_id, state in self.mesh.runtime.items()
                if state.state == TaskState.SUCCEEDED
            ),
            "failed": sorted(
                task_id
                for task_id, state in self.mesh.runtime.items()
                if state.state == TaskState.FAILED
            ),
            "blocked": sorted(
                task_id
                for task_id, state in self.mesh.runtime.items()
                if state.state == TaskState.BLOCKED
            ),
            "incomplete": incomplete,
            "unroutable": unroutable,
            "receipt_head": (
                self.mesh.receipts[-1].content_hash if self.mesh.receipts else ""
            ),
        }
