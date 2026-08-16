"""Stateless, self-describing dispatch for ECHO's durable execution mesh.

The transport mechanics are source-counterengineered from stateless agent/protocol
runtimes, then strengthened with ECHO's lease fencing and durable continuation.
A dispatch envelope contains everything a compatible worker needs to validate and
execute one leased task without coordinator-affine in-memory state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

from echo.durable_execution import LeaseToken
from echo.execution_mesh import ExecutionContext, ExecutionTask, WorkerBackend


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DispatchEnvelope:
    """Portable execution packet with lease fencing and deterministic identity."""

    schema: str
    run_id: str
    task_id: str
    operation: str
    payload: Mapping[str, Any]
    workspace_id: str
    attempt: int
    lease_epoch: int
    lease_expires_at: str
    worker_id: str
    required_capabilities: tuple[str, ...]
    worker_capabilities: tuple[str, ...]
    dependency_outputs: Mapping[str, Mapping[str, Any]]
    dependency_terminals: Mapping[str, Mapping[str, Any]]
    timeout_seconds: float
    resources: Mapping[str, int]

    def validate(self) -> None:
        if self.schema != "glaciereq.echo.stateless-dispatch.v1":
            raise ValueError("unsupported dispatch schema")
        for name in ("run_id", "task_id", "operation", "workspace_id", "worker_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must not be empty")
        if self.attempt < 1:
            raise ValueError("attempt must be positive")
        if self.lease_epoch < 1:
            raise ValueError("lease_epoch must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not set(self.required_capabilities).issubset(self.worker_capabilities):
            raise ValueError("worker does not satisfy required capabilities")
        required_resource_keys = {"max_agent_steps", "max_tool_calls", "max_output_bytes"}
        if set(self.resources) != required_resource_keys:
            raise ValueError("resource envelope keys are incomplete")
        if int(self.resources["max_agent_steps"]) < 1:
            raise ValueError("max_agent_steps must be positive")
        if int(self.resources["max_tool_calls"]) < 0:
            raise ValueError("max_tool_calls must be non-negative")
        if int(self.resources["max_output_bytes"]) < 1:
            raise ValueError("max_output_bytes must be positive")

    def as_dict(self) -> Mapping[str, Any]:
        self.validate()
        return asdict(self)

    @property
    def content_digest(self) -> str:
        return _digest(self.as_dict())

    @property
    def dispatch_key(self) -> str:
        """Stable routing/idempotency identity independent of transport instance."""
        return _digest(
            {
                "schema": self.schema,
                "run_id": self.run_id,
                "task_id": self.task_id,
                "attempt": self.attempt,
                "lease_epoch": self.lease_epoch,
                "operation": self.operation,
            }
        )

    def route_headers(self) -> Mapping[str, str]:
        """Header-friendly routing metadata for gateways and load balancers."""
        return {
            "Echo-Schema": self.schema,
            "Echo-Method": "execute",
            "Echo-Operation": self.operation,
            "Echo-Run-Id": self.run_id,
            "Echo-Task-Id": self.task_id,
            "Echo-Lease-Epoch": str(self.lease_epoch),
            "Echo-Dispatch-Key": self.dispatch_key,
            "Echo-Envelope-Digest": self.content_digest,
            "Echo-Capability-Digest": _digest(self.worker_capabilities),
        }


@dataclass(frozen=True)
class ContinuationCursor:
    """Multi-round-trip continuation fenced to one durable lease epoch."""

    run_id: str
    task_id: str
    lease_epoch: int
    round_index: int
    previous_digest: str

    def validate_for(self, envelope: DispatchEnvelope) -> None:
        envelope.validate()
        if self.run_id != envelope.run_id or self.task_id != envelope.task_id:
            raise ValueError("continuation does not belong to dispatch envelope")
        if self.lease_epoch != envelope.lease_epoch:
            raise ValueError("stale continuation lease epoch")
        if self.round_index < 0:
            raise ValueError("round_index must be non-negative")
        if self.previous_digest and len(self.previous_digest) != 64:
            raise ValueError("previous_digest must be a SHA-256 hex digest")

    def advance(self, payload: Mapping[str, Any]) -> "ContinuationCursor":
        return ContinuationCursor(
            run_id=self.run_id,
            task_id=self.task_id,
            lease_epoch=self.lease_epoch,
            round_index=self.round_index + 1,
            previous_digest=_digest(
                {
                    "round_index": self.round_index,
                    "previous_digest": self.previous_digest,
                    "payload": payload,
                }
            ),
        )


@dataclass(frozen=True)
class WorkerCatalogSnapshot:
    """Deterministic, cacheable worker-capability catalog."""

    workers: tuple[Mapping[str, Any], ...]
    digest: str

    @classmethod
    def from_backends(cls, backends: Iterable[WorkerBackend]) -> "WorkerCatalogSnapshot":
        entries = tuple(
            sorted(
                (
                    {
                        "worker_id": backend.worker_id,
                        "capabilities": sorted(backend.capabilities),
                    }
                    for backend in backends
                ),
                key=lambda item: str(item["worker_id"]),
            )
        )
        return cls(workers=entries, digest=_digest(entries))

    @property
    def cache_hint(self) -> str:
        return f'"{self.digest}"'


def build_dispatch_envelope(
    task: ExecutionTask,
    context: ExecutionContext,
    token: LeaseToken,
    backend: WorkerBackend,
) -> DispatchEnvelope:
    """Build a self-contained packet from an already-fenced durable assignment."""
    task.validate()
    if token.task_id != task.task_id:
        raise ValueError("lease token task does not match task")
    if token.worker_id != backend.worker_id:
        raise ValueError("lease token worker does not match backend")
    if context.attempt < 1:
        raise ValueError("execution context attempt must be positive")
    if not set(task.required_capabilities).issubset(backend.capabilities):
        raise ValueError("backend lacks required task capabilities")

    envelope = DispatchEnvelope(
        schema="glaciereq.echo.stateless-dispatch.v1",
        run_id=token.run_id,
        task_id=task.task_id,
        operation=task.operation,
        payload=dict(task.payload),
        workspace_id=context.workspace_id,
        attempt=context.attempt,
        lease_epoch=token.epoch,
        lease_expires_at=token.expires_at.isoformat(),
        worker_id=backend.worker_id,
        required_capabilities=tuple(sorted(task.required_capabilities)),
        worker_capabilities=tuple(sorted(backend.capabilities)),
        dependency_outputs={key: dict(value) for key, value in sorted(context.dependency_outputs.items())},
        dependency_terminals={key: dict(value) for key, value in sorted(context.dependency_terminals.items())},
        timeout_seconds=task.timeout_seconds,
        resources={
            "max_agent_steps": task.resources.max_agent_steps,
            "max_tool_calls": task.resources.max_tool_calls,
            "max_output_bytes": task.resources.max_output_bytes,
        },
    )
    envelope.validate()
    return envelope


def build_dispatch_wave(assignments: Sequence[object]) -> tuple[DispatchEnvelope, ...]:
    """Serialize a durable assignment wave without binding to one coordinator instance."""
    envelopes: list[DispatchEnvelope] = []
    for assignment in assignments:
        task = getattr(assignment, "task")
        context = getattr(assignment, "context")
        token = getattr(assignment, "token")
        backend = getattr(assignment, "backend")
        envelopes.append(build_dispatch_envelope(task, context, token, backend))
    return tuple(envelopes)
