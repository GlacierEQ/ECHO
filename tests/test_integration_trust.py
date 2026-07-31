"""End-to-end direct-access ECHO trust-loop verification."""

from __future__ import annotations

from fastapi.testclient import TestClient

from echo.db import init_db
from echo.main import create_app


def test_full_direct_execution_receipt_loop(tmp_path):
    db_path = tmp_path / "integration.db"
    client_app = create_app(init_db(db_path))

    with TestClient(client_app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/stats").status_code == 200

        conversation = client.post(
            "/conversations",
            json={
                "source": "grok-test",
                "external_id": "trust-loop-1",
                "title": "Trust Loop",
                "messages": [
                    {"role": "system", "content": "Prove the trust loop."}
                ],
            },
        )
        assert conversation.status_code == 200

        enqueue = client.post(
            "/jobs",
            json={
                "job_type": "echo.ping",
                "payload": {"integration": True},
                "idempotency_key": "trust-loop-job-1",
                "max_attempts": 2,
            },
        )
        assert enqueue.status_code == 200
        assert enqueue.json()["authority_actor"] == "direct-api"
        assert enqueue.json()["authority_scope"] == "echo:*"
        job_id = enqueue.json()["id"]

        executed = client.post(f"/jobs/{job_id}/run")
        assert executed.status_code == 200
        assert executed.json()["status"] == "succeeded"

        trust = client.get(f"/jobs/{job_id}/trust")
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
            json={
                "job_type": "echo.not-registered",
                "payload": {},
                "idempotency_key": "blocked-job-1",
                "max_attempts": 1,
            },
        )
        assert blocked.status_code == 422


def test_all_operational_endpoints_work_without_auth_headers(tmp_path):
    client_app = create_app(init_db(tmp_path / "direct.db"))

    with TestClient(client_app) as client:
        created = client.post(
            "/conversations",
            json={
                "source": "direct-test",
                "external_id": "no-key-1",
                "title": "No Key Required",
                "messages": [{"role": "user", "content": "Direct access works."}],
            },
        )
        assert created.status_code == 200
        conv_id = created.json()["id"]

        assert client.get("/conversations").status_code == 200
        assert client.get(f"/conversations/{conv_id}").status_code == 200
        assert client.get(f"/conversations/{conv_id}/messages").status_code == 200
        assert client.get(f"/conversations/{conv_id}/integrity").status_code == 200
        assert client.get(f"/conversations/{conv_id}/export.json").status_code == 200
        assert client.get(f"/conversations/{conv_id}/export.md").status_code == 200
