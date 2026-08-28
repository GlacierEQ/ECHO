"""Counter-engineered long-horizon execution mesh for ECHO.

The mesh copies execution mechanics, not vendor APIs: host-owned scheduling,
detachable workers, bounded parallel DAG execution, failure isolation, leases,
resource envelopes, stream/terminal separation, resumable snapshots, and
hash-chained receipts. Durable state belongs to ECHO, not to a worker process.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Protocol, Sequence


class TaskState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class ResourceEnvelope:
    max_agent_steps: int = 32
    max_tool_calls: int = 64
    max_output_bytes: int = 4_000_000

    def validate(self) -> None:
        if self.max_agent_steps < 1:
            raise ValueError("max_agent_steps must be positive")
        if self.max_tool_calls < 0:
            raise ValueError("max_tool_calls must be non-negative")
        if self.max_output_bytes < 1:
            raise ValueError("max_output_bytes must be positive")


@dataclass(frozen=True)
class ExecutionTask:
    task_id: str
    operation: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    dependencies: tuple[str, ...] = ()
    required_capabilities: tuple[str, ...] = ()
    workspace_id: str = ""
    priority: int = 0
    max_attempts: int = 3
    timeout_seconds: float = 300.0
    resources: ResourceEnvelope = field(default_factory=ResourceEnvelope)

    def validate(self) -> None:
        if not self.task_id.strip() or not self.operation.strip():
            raise ValueError("task_id and operation must not be empty")
        if self.task_id in self.dependencies:
            raise ValueError(f"task {self.task_id} cannot depend on itself")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.resources.validate()


@dataclass(frozen=True)
class WorkerResult:
    output: Mapping[str, Any] = field(default_factory=dict)
    stream: tuple[Any, ...] = ()
    terminal: Mapping[str, Any] = field(default_factory=dict)
    agent_steps: int = 0
    tool_calls: int = 0

    @property
    def output_bytes(self) -> int:
        return len(
            json.dumps(
                {
                    "output": self.output,
                    "stream": self.stream,
                    "terminal": self.terminal,
                },
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        )


@dataclass(frozen=True)
class ExecutionContext:
    workspace_id: str
    dependency_outputs: Mapping[str, Mapping[str, Any]]
    dependency_terminals: Mapping[str, Mapping[str, Any]]
    attempt: int


class WorkerBackend(Protocol):
    worker_id: str
    capabilities: frozenset[str]

    async def execute(
        self, task: ExecutionTask, context: ExecutionContext
    ) -> WorkerResult: ...


@dataclass
class TaskRuntime:
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    lease_owner: str = ""
    lease_expires_at: float = 0.0
    last_error: str = ""


@dataclass(frozen=True)
class ExecutionReceipt:
    task_id: str
    attempt: int
    outcome: str
    worker_id: str
    workspace_id: str
    details: Mapping[str, Any]
    previous_hash: str
    content_hash: str


@dataclass(frozen=True)
class ExecutionSnapshot:
    payload: Mapping[str, Any]
    digest: str

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "schema": "glaciereq.echo.execution-snapshot.v2",
            "digest": self.digest,
            "payload": self.payload,
        }


class ExecutionMesh:
    SNAPSHOT_CLOCK = "relative-lease-remaining-seconds-v1"

    def __init__(
        self,
        tasks: Sequence[ExecutionTask],
        *,
        max_concurrency: int = 8,
        lease_seconds: float = 120.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.max_concurrency = max_concurrency
        self.lease_seconds = lease_seconds
        self._clock = clock
        self.tasks = {task.task_id: task for task in tasks}
        if len(self.tasks) != len(tasks):
            raise ValueError("task_id values must be unique")
        for task in tasks:
            task.validate()
            missing = set(task.dependencies) - self.tasks.keys()
            if missing:
                raise ValueError(
                    f"task {task.task_id} has missing dependencies: {sorted(missing)}"
                )
        self._assert_acyclic()
        self.runtime = {task_id: TaskRuntime() for task_id in self.tasks}
        self.results: dict[str, WorkerResult] = {}
        self.receipts: list[ExecutionReceipt] = []

    def _assert_acyclic(self) -> None:
        indegree = {task_id: 0 for task_id in self.tasks}
        children: dict[str, list[str]] = {task_id: [] for task_id in self.tasks}
        for task in self.tasks.values():
            for dependency in task.dependencies:
                indegree[task.task_id] += 1
                children[dependency].append(task.task_id)
        queue = [task_id for task_id, degree in indegree.items() if degree == 0]
        seen = 0
        while queue:
            current = queue.pop()
            seen += 1
            for child in children[current]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if seen != len(self.tasks):
            raise ValueError("execution graph must be acyclic")

    def recover_stale_leases(self) -> tuple[str, ...]:
        now = self._clock()
        recovered: list[str] = []
        for task_id, state in self.runtime.items():
            if (
                state.state not in {TaskState.LEASED, TaskState.RUNNING}
                or state.lease_expires_at > now
            ):
                continue
            task = self.tasks[task_id]
            state.lease_owner = ""
            state.lease_expires_at = 0.0
            state.last_error = "stale lease recovered"
            if state.attempts >= task.max_attempts:
                state.state = TaskState.FAILED
                self._append_receipt(
                    task,
                    state,
                    "failed",
                    "lease-recovery",
                    {"error": state.last_error, "reason": "attempt_budget_exhausted"},
                )
            else:
                state.state = TaskState.PENDING
                self._append_receipt(
                    task, state, "retry", "lease-recovery", {"error": state.last_error}
                )
            recovered.append(task_id)
        self._propagate_blocked()
        return tuple(sorted(recovered))

    def _propagate_blocked(self) -> None:
        changed = True
        while changed:
            changed = False
            for task_id, task in self.tasks.items():
                state = self.runtime[task_id]
                if state.state != TaskState.PENDING:
                    continue
                blockers = [
                    dependency
                    for dependency in task.dependencies
                    if self.runtime[dependency].state
                    in {TaskState.FAILED, TaskState.BLOCKED}
                ]
                if blockers:
                    state.state = TaskState.BLOCKED
                    state.last_error = "blocked by failed dependency: " + ",".join(
                        sorted(blockers)
                    )
                    self._append_receipt(
                        task,
                        state,
                        "blocked",
                        "scheduler",
                        {"blockers": sorted(blockers)},
                    )
                    changed = True

    def ready(self, capabilities: frozenset[str]) -> tuple[ExecutionTask, ...]:
        self.recover_stale_leases()
        self._propagate_blocked()
        ready = [
            task
            for task in self.tasks.values()
            if self.runtime[task.task_id].state == TaskState.PENDING
            and set(task.required_capabilities).issubset(capabilities)
            and all(
                self.runtime[dependency].state == TaskState.SUCCEEDED
                for dependency in task.dependencies
            )
        ]
        return tuple(sorted(ready, key=lambda task: (-task.priority, task.task_id)))

    def _lease(self, task: ExecutionTask, worker_id: str) -> None:
        state = self.runtime[task.task_id]
        if state.state != TaskState.PENDING:
            raise RuntimeError(f"task {task.task_id} is not pending")
        if state.attempts >= task.max_attempts:
            state.state = TaskState.FAILED
            state.last_error = "max attempts exceeded before lease"
            self._append_receipt(
                task, state, "failed", "scheduler", {"error": state.last_error}
            )
            raise RuntimeError(f"task {task.task_id} exhausted attempts")
        state.state = TaskState.LEASED
        state.lease_owner = worker_id
        state.lease_expires_at = self._clock() + self.lease_seconds

    def _owns_attempt(
        self, task_id: str, worker_id: str, attempt: int, *, require_live_lease: bool
    ) -> bool:
        state = self.runtime[task_id]
        if (
            state.state != TaskState.RUNNING
            or state.lease_owner != worker_id
            or state.attempts != attempt
        ):
            return False
        return not require_live_lease or state.lease_expires_at > self._clock()

    def _release_after_interruption(
        self,
        task: ExecutionTask,
        state: TaskRuntime,
        *,
        worker_id: str,
        outcome: str,
        error: str,
    ) -> None:
        state.last_error = error
        state.lease_owner = ""
        state.lease_expires_at = 0.0
        if state.attempts < task.max_attempts:
            state.state = TaskState.PENDING
            receipt_outcome = outcome
        else:
            state.state = TaskState.FAILED
            receipt_outcome = "failed"
        self._append_receipt(task, state, receipt_outcome, worker_id, {"error": error})
        self._propagate_blocked()

    async def _execute_one(self, backend: WorkerBackend, task: ExecutionTask) -> None:
        state = self.runtime[task.task_id]
        state.state = TaskState.RUNNING
        state.attempts += 1
        attempt = state.attempts
        worker_id = backend.worker_id
        state.lease_expires_at = self._clock() + self.lease_seconds
        context = ExecutionContext(
            workspace_id=task.workspace_id or f"echo:{task.task_id}",
            dependency_outputs={
                dependency: self.results[dependency].output
                for dependency in task.dependencies
            },
            dependency_terminals={
                dependency: self.results[dependency].terminal
                for dependency in task.dependencies
            },
            attempt=attempt,
        )
        try:
            result = await asyncio.wait_for(
                backend.execute(task, context), timeout=task.timeout_seconds
            )
            self._validate_result(task, result)
        except asyncio.CancelledError:
            if self._owns_attempt(
                task.task_id, worker_id, attempt, require_live_lease=False
            ):
                self._release_after_interruption(
                    task,
                    state,
                    worker_id=worker_id,
                    outcome="cancelled-retry",
                    error="CancelledError: execution cancelled",
                )
            raise
        except Exception as exc:
            if self._owns_attempt(
                task.task_id, worker_id, attempt, require_live_lease=False
            ):
                self._release_after_interruption(
                    task,
                    state,
                    worker_id=worker_id,
                    outcome="retry",
                    error=f"{type(exc).__name__}: {exc}",
                )
            return
        if not self._owns_attempt(
            task.task_id, worker_id, attempt, require_live_lease=True
        ):
            if self._owns_attempt(
                task.task_id, worker_id, attempt, require_live_lease=False
            ):
                self._release_after_interruption(
                    task,
                    state,
                    worker_id=worker_id,
                    outcome="retry",
                    error="stale execution result rejected after lease expiry",
                )
            return
        self.results[task.task_id] = result
        state.state = TaskState.SUCCEEDED
        state.last_error = ""
        state.lease_owner = ""
        state.lease_expires_at = 0.0
        self._append_receipt(
            task,
            state,
            "success",
            worker_id,
            {
                "agent_steps": result.agent_steps,
                "tool_calls": result.tool_calls,
                "output_bytes": result.output_bytes,
                "stream_items": len(result.stream),
                "terminal": dict(result.terminal),
            },
        )

    @staticmethod
    def _validate_result(task: ExecutionTask, result: WorkerResult) -> None:
        if result.agent_steps < 0 or result.tool_calls < 0:
            raise ValueError("worker resource usage must be non-negative")
        if result.agent_steps > task.resources.max_agent_steps:
            raise RuntimeError("agent-step budget exceeded")
        if result.tool_calls > task.resources.max_tool_calls:
            raise RuntimeError("tool-call budget exceeded")
        if result.output_bytes > task.resources.max_output_bytes:
            raise RuntimeError("output-byte budget exceeded")

    @staticmethod
    def _receipt_digest(payload: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), default=str
            ).encode("utf-8")
        ).hexdigest()

    def _append_receipt(
        self,
        task: ExecutionTask,
        state: TaskRuntime,
        outcome: str,
        worker_id: str,
        details: Mapping[str, Any],
    ) -> None:
        previous_hash = self.receipts[-1].content_hash if self.receipts else ""
        payload = {
            "task_id": task.task_id,
            "attempt": state.attempts,
            "outcome": outcome,
            "worker_id": worker_id,
            "workspace_id": task.workspace_id or f"echo:{task.task_id}",
            "details": details,
            "previous_hash": previous_hash,
        }
        self.receipts.append(
            ExecutionReceipt(
                task_id=task.task_id,
                attempt=state.attempts,
                outcome=outcome,
                worker_id=worker_id,
                workspace_id=task.workspace_id or f"echo:{task.task_id}",
                details=dict(details),
                previous_hash=previous_hash,
                content_hash=self._receipt_digest(payload),
            )
        )

    async def run_wave(self, backend: WorkerBackend) -> tuple[str, ...]:
        ready = self.ready(backend.capabilities)[: self.max_concurrency]
        if not ready:
            return ()
        for task in ready:
            self._lease(task, backend.worker_id)
        await asyncio.gather(*(self._execute_one(backend, task) for task in ready))
        self._propagate_blocked()
        return tuple(task.task_id for task in ready)

    async def run_to_completion(self, backend: WorkerBackend) -> Mapping[str, Any]:
        waves = 0
        while True:
            scheduled = await self.run_wave(backend)
            if scheduled:
                waves += 1
                continue
            self._propagate_blocked()
            break
        incomplete = sorted(
            task_id
            for task_id, state in self.runtime.items()
            if state.state in {TaskState.PENDING, TaskState.LEASED, TaskState.RUNNING}
        )
        return {
            "schema": "glaciereq.echo.execution-run.v2",
            "waves": waves,
            "succeeded": sorted(
                task_id
                for task_id, state in self.runtime.items()
                if state.state == TaskState.SUCCEEDED
            ),
            "failed": sorted(
                task_id
                for task_id, state in self.runtime.items()
                if state.state == TaskState.FAILED
            ),
            "blocked": sorted(
                task_id
                for task_id, state in self.runtime.items()
                if state.state == TaskState.BLOCKED
            ),
            "incomplete": incomplete,
            "receipt_head": self.receipts[-1].content_hash if self.receipts else "",
        }

    def snapshot(self) -> ExecutionSnapshot:
        now = self._clock()
        payload = {
            "snapshot_version": 2,
            "lease_clock": self.SNAPSHOT_CLOCK,
            "max_concurrency": self.max_concurrency,
            "lease_seconds": self.lease_seconds,
            "tasks": [
                self._task_dict(self.tasks[task_id]) for task_id in sorted(self.tasks)
            ],
            "runtime": {
                task_id: {
                    "state": state.state.value,
                    "attempts": state.attempts,
                    "lease_owner": state.lease_owner,
                    "lease_remaining": max(0.0, state.lease_expires_at - now)
                    if state.state in {TaskState.LEASED, TaskState.RUNNING}
                    else 0.0,
                    "last_error": state.last_error,
                }
                for task_id, state in sorted(self.runtime.items())
            },
            "results": {
                task_id: self._result_dict(result)
                for task_id, result in sorted(self.results.items())
            },
            "receipts": [asdict(receipt) for receipt in self.receipts],
        }
        digest = hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), default=str
            ).encode("utf-8")
        ).hexdigest()
        return ExecutionSnapshot(payload=payload, digest=digest)

    @classmethod
    def from_snapshot(
        cls,
        snapshot: ExecutionSnapshot | Mapping[str, Any],
        *,
        clock: Callable[[], float] = time.time,
    ) -> "ExecutionMesh":
        if isinstance(snapshot, ExecutionSnapshot):
            payload = dict(snapshot.payload)
            expected = snapshot.digest
        else:
            payload = dict(snapshot["payload"])
            expected = str(snapshot["digest"])
        actual = hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), default=str
            ).encode("utf-8")
        ).hexdigest()
        if actual != expected:
            raise ValueError("execution snapshot digest mismatch")
        if payload.get("lease_clock") != cls.SNAPSHOT_CLOCK:
            raise ValueError("unsupported snapshot lease clock semantics")
        mesh = cls(
            [cls._task_from_dict(item) for item in payload["tasks"]],
            max_concurrency=int(payload["max_concurrency"]),
            lease_seconds=float(payload["lease_seconds"]),
            clock=clock,
        )
        task_ids = set(mesh.tasks)
        runtime_payload = payload.get("runtime", {})
        result_payload = payload.get("results", {})
        if set(runtime_payload) != task_ids:
            raise ValueError("snapshot runtime keys do not match task graph")
        if not set(result_payload).issubset(task_ids):
            raise ValueError("snapshot contains results for unknown tasks")
        now = clock()
        for task_id, raw in runtime_payload.items():
            state = TaskState(raw["state"])
            remaining = float(raw.get("lease_remaining", 0.0))
            if remaining < 0:
                raise ValueError("snapshot lease_remaining must be non-negative")
            mesh.runtime[task_id] = TaskRuntime(
                state=state,
                attempts=int(raw["attempts"]),
                lease_owner=str(raw["lease_owner"]),
                lease_expires_at=now + remaining
                if state in {TaskState.LEASED, TaskState.RUNNING}
                else 0.0,
                last_error=str(raw["last_error"]),
            )
        mesh.results = {
            task_id: WorkerResult(
                output=dict(raw["output"]),
                stream=tuple(raw["stream"]),
                terminal=dict(raw["terminal"]),
                agent_steps=int(raw["agent_steps"]),
                tool_calls=int(raw["tool_calls"]),
            )
            for task_id, raw in result_payload.items()
        }
        for task_id, state in mesh.runtime.items():
            if state.attempts < 0 or state.attempts > mesh.tasks[task_id].max_attempts:
                raise ValueError("snapshot attempt count violates task budget")
            if state.state == TaskState.SUCCEEDED and task_id not in mesh.results:
                raise ValueError("succeeded snapshot task is missing its result")
            if task_id in mesh.results and state.state != TaskState.SUCCEEDED:
                raise ValueError("snapshot result exists for non-succeeded task")
            if (
                state.state in {TaskState.LEASED, TaskState.RUNNING}
                and not state.lease_owner
            ):
                raise ValueError("active snapshot task is missing lease owner")
        mesh.receipts = []
        previous_hash = ""
        for raw in payload.get("receipts", []):
            receipt_payload = {
                "task_id": str(raw["task_id"]),
                "attempt": int(raw["attempt"]),
                "outcome": str(raw["outcome"]),
                "worker_id": str(raw["worker_id"]),
                "workspace_id": str(raw["workspace_id"]),
                "details": dict(raw["details"]),
                "previous_hash": str(raw["previous_hash"]),
            }
            if receipt_payload["task_id"] not in task_ids:
                raise ValueError("snapshot receipt references unknown task")
            if receipt_payload["previous_hash"] != previous_hash:
                raise ValueError("snapshot receipt chain previous_hash mismatch")
            computed = cls._receipt_digest(receipt_payload)
            if computed != str(raw["content_hash"]):
                raise ValueError("snapshot receipt content_hash mismatch")
            mesh.receipts.append(
                ExecutionReceipt(**receipt_payload, content_hash=computed)
            )
            previous_hash = computed
        mesh.recover_stale_leases()
        return mesh

    @staticmethod
    def _task_dict(task: ExecutionTask) -> Mapping[str, Any]:
        return {
            "task_id": task.task_id,
            "operation": task.operation,
            "payload": dict(task.payload),
            "dependencies": list(task.dependencies),
            "required_capabilities": list(task.required_capabilities),
            "workspace_id": task.workspace_id,
            "priority": task.priority,
            "max_attempts": task.max_attempts,
            "timeout_seconds": task.timeout_seconds,
            "resources": asdict(task.resources),
        }

    @staticmethod
    def _task_from_dict(raw: Mapping[str, Any]) -> ExecutionTask:
        return ExecutionTask(
            task_id=str(raw["task_id"]),
            operation=str(raw["operation"]),
            payload=dict(raw["payload"]),
            dependencies=tuple(raw["dependencies"]),
            required_capabilities=tuple(raw["required_capabilities"]),
            workspace_id=str(raw["workspace_id"]),
            priority=int(raw["priority"]),
            max_attempts=int(raw["max_attempts"]),
            timeout_seconds=float(raw["timeout_seconds"]),
            resources=ResourceEnvelope(**raw["resources"]),
        )

    @staticmethod
    def _result_dict(result: WorkerResult) -> Mapping[str, Any]:
        return {
            "output": dict(result.output),
            "stream": list(result.stream),
            "terminal": dict(result.terminal),
            "agent_steps": result.agent_steps,
            "tool_calls": result.tool_calls,
        }
