"""API proof view for envelope-bound durable jobs."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from echo.db import init_db
from echo.main import create_app
from echo.work_envelope import WorkEnvelope


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("data") / "portable-receipts.db"
    return TestClient(create_app(init_db(db_path)))


def make_envelope(*, idempotency_key: str = "portable-job-001") -> WorkEnvelope:
    return WorkEnvelope.create(
        work_id="portable-work-001",
        idempotency_key=idempotency_key,
        producer="integration-test",
        source_repository="GlacierEQ/ECHO",
        source_revision="main",
        capability="echo.ping",
        authority_scope="echo:execute",
        exact_target="job:portable-work-001",
        created_at="2026-08-29T00:00:00+00:00",
        payload={"value": "read-only"},
    )


def enqueue_and_run(client: TestClient, envelope: WorkEnvelope) -> str:
    response = client.post(
        "/jobs",
        json={
            "job_type": "echo.ping",
            "idempotency_key": envelope.idempotency_key,
            "payload": {
                **envelope.payload,
                "__echo_work_envelope__": envelope.as_dict(),
            },
        },
    )
    assert response.status_code == 200, response.text
    job_id = response.json()["id"]
    run = client.post(f"/jobs/{job_id}/run")
    assert run.status_code == 200, run.text
    assert run.json()["status"] == "succeeded"
    return job_id


def test_portable_receipt_endpoint_binds_durable_job(client):
    envelope = make_envelope()
    job_id = enqueue_and_run(client, envelope)

    response = client.get(f"/jobs/{job_id}/portable-receipts")

    assert response.status_code == 200, response.text
    proof = response.json()
    assert proof["verified"] is True
    assert proof["portable_chain_valid"] is True
    assert proof["durable_chain"]["valid"] is True
    assert proof["work_id"] == envelope.work_id
    assert proof["envelope"]["envelope_sha256"] == envelope.envelope_sha256
    assert len(proof["portable_receipts"]) == 1
    assert proof["portable_receipts"][0]["status"] == "succeeded"


def test_envelope_idempotency_mismatch_is_rejected(client):
    envelope = make_envelope(idempotency_key="envelope-key")
    response = client.post(
        "/jobs",
        json={
            "job_type": "echo.ping",
            "idempotency_key": "different-key",
            "payload": {"__echo_work_envelope__": envelope.as_dict()},
        },
    )
    assert response.status_code == 422
    assert "idempotency" in response.json()["detail"]


def test_envelope_capability_mismatch_is_rejected(client):
    envelope = make_envelope()
    response = client.post(
        "/jobs",
        json={
            "job_type": "echo.summarize",
            "idempotency_key": envelope.idempotency_key,
            "payload": {"__echo_work_envelope__": envelope.as_dict()},
        },
    )
    assert response.status_code == 422
    assert "capability" in response.json()["detail"]


def test_unbound_job_has_no_portable_proof(client):
    response = client.post(
        "/jobs",
        json={
            "job_type": "echo.ping",
            "idempotency_key": "legacy-job-001",
            "payload": {},
        },
    )
    assert response.status_code == 200
    job_id = response.json()["id"]
    client.post(f"/jobs/{job_id}/run")

    proof = client.get(f"/jobs/{job_id}/portable-receipts")
    assert proof.status_code == 422
    assert "work-envelope" in proof.json()["detail"]
