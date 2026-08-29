"""Bridge WorkEnvelope contracts into ECHO mesh and durable receipts."""
from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence

from echo.execution_mesh import ExecutionMesh, ExecutionTask, WorkerBackend, WorkerResult
from echo.work_envelope import ExecutionReceipt, ReceiptChain, WorkEnvelope

ENVELOPE_PAYLOAD_KEY = "__echo_work_envelope__"


def task_for_envelope(
    envelope: WorkEnvelope,
    *,
    operation: str | None = None,
    dependencies: Sequence[str] = (),
    required_capabilities: Sequence[str] = (),
    workspace_id: str = "",
    priority: int = 0,
    max_attempts: int = 3,
    timeout_seconds: float = 300.0,
    resources: Any = None,
) -> ExecutionTask:
    """Create a mesh task carrying an integrity-checked envelope binding."""
    if ENVELOPE_PAYLOAD_KEY in envelope.payload:
        raise ValueError(f"payload key is reserved: {ENVELOPE_PAYLOAD_KEY}")
    payload = dict(envelope.payload)
    payload[ENVELOPE_PAYLOAD_KEY] = envelope.as_dict()
    kwargs = {
        "task_id": envelope.work_id,
        "operation": operation or envelope.capability,
        "payload": payload,
        "dependencies": tuple(dependencies),
        "required_capabilities": tuple(required_capabilities),
        "workspace_id": workspace_id,
        "priority": priority,
        "max_attempts": max_attempts,
        "timeout_seconds": timeout_seconds,
    }
    if resources is not None:
        kwargs["resources"] = resources
    return ExecutionTask(**kwargs)


def envelope_for_task(task: ExecutionTask) -> WorkEnvelope:
    """Recover and validate the envelope bound to a mesh task."""
    raw = task.payload.get(ENVELOPE_PAYLOAD_KEY)
    if not isinstance(raw, Mapping):
        raise ValueError("mesh task is missing its work-envelope binding")
    envelope = WorkEnvelope.from_dict(raw)
    if envelope.work_id != task.task_id:
        raise ValueError("mesh task id does not match work envelope")
    return envelope


class BoundWorkerBackend:
    """Validate the exact envelope before delegating to an existing worker."""

    def __init__(self, backend: WorkerBackend, envelope: WorkEnvelope) -> None:
        self._backend = backend
        self._envelope = envelope
        self.worker_id = backend.worker_id
        self.capabilities = backend.capabilities

    async def execute(self, task: ExecutionTask, context: Any) -> WorkerResult:
        actual = envelope_for_task(task)
        if actual.as_dict() != self._envelope.as_dict():
            raise ValueError("worker task envelope does not match expected envelope")
        return await self._backend.execute(task, context)


def _contract_status(outcome: str) -> str:
    return {
        "success": "succeeded",
        "failed": "failed",
        "blocked": "blocked",
        "rejected": "rejected",
    }.get(outcome, "failed")


def receipt_from_mesh(
    envelope: WorkEnvelope,
    mesh: ExecutionMesh,
    *,
    created_at: str,
    verification_method: str = "echo-mesh-readback",
    previous_receipt_hash: str = "",
    details: Mapping[str, Any] | None = None,
) -> ExecutionReceipt:
    """Project the authoritative mesh result into the portable receipt contract."""
    task = mesh.tasks.get(envelope.work_id)
    if task is None:
        raise ValueError(f"mesh has no task for work envelope: {envelope.work_id}")
    bound = envelope_for_task(task)
    if bound.as_dict() != envelope.as_dict():
        raise ValueError("mesh task envelope binding mismatch")
    candidates = [receipt for receipt in mesh.receipts if receipt.task_id == envelope.work_id]
    if not candidates:
        raise ValueError("mesh has no receipt for work envelope")
    mesh_receipt = candidates[-1]
    state = mesh.runtime[envelope.work_id].state.value
    output = mesh.results[envelope.work_id].output if envelope.work_id in mesh.results else None
    verified = state == "succeeded" and mesh_receipt.outcome == "success"
    receipt_details = {
        "mesh_receipt_hash": mesh_receipt.content_hash,
        "mesh_outcome": mesh_receipt.outcome,
        "mesh_state": state,
        "exact_target": envelope.exact_target,
        "source_repository": envelope.source_repository,
        "source_revision": envelope.source_revision,
        **dict(details or {}),
    }
    return ExecutionReceipt.from_output(
        envelope,
        status=_contract_status(mesh_receipt.outcome),
        output=output,
        verified=verified,
        verification_method=verification_method,
        created_at=created_at,
        previous_receipt_hash=previous_receipt_hash,
        details=receipt_details,
    )


def receipts_from_durable_records(
    envelope: WorkEnvelope,
    records: Sequence[Any],
    *,
    job_id: str,
    verification_method: str = "echo-durable-receipt-readback",
) -> ReceiptChain:
    """Project ECHO's persisted receipt rows into the portable receipt chain."""
    chain = ReceiptChain(envelope)
    previous_attempt = 0
    for record in records:
        if getattr(record, "job_id", "") != job_id:
            raise ValueError("durable receipt belongs to a different job")
        attempt = int(record.attempt)
        if attempt <= previous_attempt:
            raise ValueError("durable receipt attempts must be strictly increasing")
        outcome = str(record.outcome)
        status = {
            "success": "succeeded",
            "failure": "failed",
            "retry": "failed",
            "blocked": "blocked",
        }.get(outcome, "failed")
        stored_details = dict(record.details or {})
        receipt_details = {
            "durable_job_id": job_id,
            "durable_attempt": attempt,
            "durable_outcome": outcome,
            "durable_receipt_hash": str(record.content_hash),
            "durable_previous_hash": str(record.previous_hash or ""),
            "stored_details": stored_details,
        }
        created_at = record.created_at.isoformat()
        chain.append(
            status=status,
            output=stored_details,
            verified=True,
            verification_method=verification_method,
            created_at=created_at,
            details=receipt_details,
        )
        previous_attempt = attempt
    return chain


class DurableMeshStore(Protocol):
    def restore_mesh(self, run_id: str) -> ExecutionMesh: ...


def receipt_from_durable(
    store: DurableMeshStore,
    run_id: str,
    envelope: WorkEnvelope,
    *,
    created_at: str,
    verification_method: str = "echo-durable-readback",
    previous_receipt_hash: str = "",
) -> ExecutionReceipt:
    """Read a durable run through its public restore surface and project its receipt."""
    mesh = store.restore_mesh(run_id)
    return receipt_from_mesh(
        envelope,
        mesh,
        created_at=created_at,
        verification_method=verification_method,
        previous_receipt_hash=previous_receipt_hash,
        details={"durable_run_id": run_id},
    )
