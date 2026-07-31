"""End-to-end AKOS ↔ ECHO trust-loop verification."""

from __future__ import annotations

import time
import uuid

from fastapi.testclient import TestClient

from echo.auth import sign_authority
from echo.main import app


def headers(secret: str, scope: str, actor: str = "akos:integration") -> dict[str, str]:
    timestamp = str(int(time.time()))
    nonce = str(uuid.uuid4())
    return {
        "X-AKOS-Actor": actor,
        "X-AKOS-Scope": scope,
        "X-AKOS-Timestamp": timestamp,
        "X-AKOS-Nonce": nonce,
        "X-AKOS-Signature": sign_authority(
            secret, actor, scope, timestamp, nonce
        ),
    }


def test_full_authority_execution_receipt_loop(monkeypatch, tmp_path):
    secret = "integration-secret"
    monkeypatch.setenv("ECHO_AKOS_SHARED_SECRET", secret)
    monkeypatch.setenv("ECHO_DB", str(tmp_path / "integration.db"))

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/stats").status_code == 422
        assert client.get(
            "/stats", headers=headers("wrong-secret", "echo:read")
        ).status_code == 403

        conversation = client.post(
            "/conversations",
            headers=headers(secret, "echo:write"),
            json={
                "source": "grok-test",
                "external_id": "trust-loop-1",
                "title": "Trust Loop",
                "messages": [
                    {"role": "system", "content": "Prove the trust loop."}
                ],
            },
        )
        assert conversation.status_code == 201

        enqueue = client.post(
            "/jobs",
            headers=headers(secret, "echo:execute"),
            json={
                "job_type": "echo.ping",
                "payload": {"integration": True},
                "idempotency_key": "trust-loop-job-1",
                "max_attempts": 2,
            },
        )
        assert enqueue.status_code == 201
        job_id = enqueue.json()["id"]

        executed = client.post(
            f"/jobs/{job_id}/run",
            headers=headers(secret, "echo:execute"),
        )
        assert executed.status_code == 200
        assert executed.json()["status"] == "succeeded"

        trust = client.get(
            f"/jobs/{job_id}/trust",
            headers=headers(secret, "echo:verify"),
        )
        assert trust.status_code == 200
        proof = trust.json()
        assert proof["verified"] is True
        assert proof["receipt_count"] == 1
        assert proof["trust_loop"] == {
            "authority_recorded": True,
            "execution_terminal": True,
            "receipt_chain_verified": True,
        }

        blocked = client.post(
            "/jobs",
            headers=headers(secret, "echo:execute"),
            json={
                "job_type": "echo.not-registered",
                "payload": {},
                "idempotency_key": "blocked-job-1",
                "max_attempts": 1,
            },
        )
        assert blocked.status_code == 422


def test_trust_endpoint_requires_verify_scope(monkeypatch, tmp_path):
    secret = "integration-secret"
    monkeypatch.setenv("ECHO_AKOS_SHARED_SECRET", secret)
    monkeypatch.setenv("ECHO_DB", str(tmp_path / "scope.db"))

    with TestClient(app) as client:
        enqueue = client.post(
            "/jobs",
            headers=headers(secret, "echo:execute"),
            json={
                "job_type": "echo.ping",
                "payload": {},
                "idempotency_key": "scope-job-1",
                "max_attempts": 1,
            },
        )
        job_id = enqueue.json()["id"]
        client.post(
            f"/jobs/{job_id}/run",
            headers=headers(secret, "echo:execute"),
        )
        denied = client.get(
            f"/jobs/{job_id}/trust",
            headers=headers(secret, "echo:read"),
        )
        assert denied.status_code == 403
