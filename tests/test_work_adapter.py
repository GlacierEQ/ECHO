from __future__ import annotations

import asyncio

from echo.execution_mesh import ExecutionMesh, WorkerResult
from echo.work_adapter import (
    BoundWorkerBackend,
    receipt_from_durable,
    receipt_from_mesh,
    task_for_envelope,
)
from echo.work_envelope import WorkEnvelope, sha256_hex, verify_receipt_chain


class Worker:
    worker_id = "adapter-worker"
    capabilities = frozenset()

    async def execute(self, task, context):
        return WorkerResult(output={"target": task.payload["target"]}, terminal={"read": True})


def envelope(target="README.md"):
    return WorkEnvelope.create(
        work_id="w-adapter",
        idempotency_key="idem-adapter",
        producer="echo-test",
        source_repository="GlacierEQ/ECHO",
        source_revision="abc123",
        capability="inspect",
        authority_scope="casey-approved-read-only",
        exact_target=target,
        created_at="2026-08-28T23:50:00Z",
        payload={"target": target},
    )


def test_task_binding_preserves_payload_and_exact_target():
    e = envelope()
    task = task_for_envelope(e)
    assert task.task_id == e.work_id
    assert task.payload["target"] == e.exact_target
    from echo.work_adapter import envelope_for_task
    assert envelope_for_task(task) == e


def test_bound_backend_rejects_a_different_exact_target():
    e = envelope("README.md")
    task = task_for_envelope(envelope("other.md"))
    mesh = ExecutionMesh([task])
    asyncio.run(mesh.run_to_completion(BoundWorkerBackend(Worker(), e)))
    assert mesh.runtime[e.work_id].state.value == "failed"
    assert "does not match" in mesh.runtime[e.work_id].last_error


def test_mesh_result_projects_to_portable_receipt_chain():
    e = envelope()
    mesh = ExecutionMesh([task_for_envelope(e)])
    asyncio.run(mesh.run_to_completion(BoundWorkerBackend(Worker(), e)))
    receipt = receipt_from_mesh(e, mesh, created_at="2026-08-28T23:51:00Z")
    assert receipt.verified is True
    assert receipt.output_sha256 == sha256_hex({"target": "README.md"})
    assert receipt.details["mesh_receipt_hash"] == mesh.receipts[-1].content_hash
    assert verify_receipt_chain(e, [receipt]) is True


def test_durable_projection_uses_restored_mesh_receipt():
    e = envelope()
    mesh = ExecutionMesh([task_for_envelope(e)])
    asyncio.run(mesh.run_to_completion(BoundWorkerBackend(Worker(), e)))

    class Store:
        def restore_mesh(self, run_id):
            assert run_id == "run-adapter"
            return mesh

    receipt = receipt_from_durable(
        Store(), "run-adapter", e, created_at="2026-08-28T23:52:00Z"
    )
    assert receipt.verified is True
    assert receipt.details["durable_run_id"] == "run-adapter"
