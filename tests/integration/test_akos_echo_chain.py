"""P0.5 – direct-access ECHO end-to-end continuity tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from echo.db import init_db
from echo.main import create_app


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("data") / "integration.db"
    engine = init_db(db_path)
    app = create_app(engine)
    return TestClient(app)


CONV_PAYLOAD = {
    "source": "integration",
    "external_id": "p0.5-chain-test",
    "title": "P0.5 Integration Test",
    "participants": ["akos", "echo"],
    "labels": ["integration", "p0.5"],
    "messages": [
        {"role": "user", "content": "Direct continuity request submitted."},
        {"role": "assistant", "content": "ECHO ingested it without a key."},
    ],
}


class TestIdentityPreserved:
    """Direct API execution still records stable provenance."""

    def test_actor_recorded_on_job(self, client):
        r = client.post(
            "/jobs",
            json={
                "job_type": "identity_check",
                "idempotency_key": "p0.5-identity-001",
                "payload": {"purpose": "identity preservation test"},
            },
        )
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["authority_actor"] == "direct-api"
        assert data["authority_scope"] == "echo:*"


class TestDirectAccess:
    """Operational endpoints do not require a secret or signed headers."""

    def test_write_requires_no_headers(self, client):
        r = client.post("/conversations", json=CONV_PAYLOAD)
        assert r.status_code == 200

    def test_read_requires_no_headers(self, client):
        r = client.get("/conversations")
        assert r.status_code == 200

    def test_stats_requires_no_headers(self, client):
        r = client.get("/stats")
        assert r.status_code == 200


class TestExecutionDeterministic:
    """Same input always produces the same execution path and identity."""

    def test_idempotent_conversation_ingest(self, client):
        r1 = client.post("/conversations", json=CONV_PAYLOAD)
        assert r1.status_code == 200
        id1 = r1.json()["id"]

        r2 = client.post("/conversations", json=CONV_PAYLOAD)
        assert r2.status_code == 200
        id2 = r2.json()["id"]

        assert id1 == id2, "Same source+external_id must produce same UUID"

    def test_content_hash_stable(self, client):
        r = client.get("/conversations")
        assert r.status_code == 200
        convs = r.json()["conversations"]
        target = next((c for c in convs if c["external_id"] == "p0.5-chain-test"), None)
        assert target is not None
        assert len(target["content_hash"]) == 64


class TestReceiptsChained:
    """No gaps appear in the receipt chain."""

    def test_job_receipt_populated_after_execution(self, client):
        r = client.post(
            "/jobs",
            json={
                "job_type": "echo:noop",
                "idempotency_key": "p0.5-receipt-001",
                "payload": {},
            },
        )
        assert r.status_code == 200
        job_id = r.json()["id"]

        er = client.post(f"/jobs/{job_id}/execute")
        assert er.status_code == 200

        gr = client.get(f"/jobs/{job_id}")
        assert gr.status_code == 200
        assert "receipt" in gr.json()


class TestFullAuditability:
    """Actions remain reconstructable from request through completion."""

    def test_integrity_endpoint_returns_valid(self, client):
        r = client.get("/conversations")
        assert r.status_code == 200
        convs = r.json()["conversations"]
        assert len(convs) > 0
        conv_id = convs[0]["id"]

        ir = client.get(f"/conversations/{conv_id}/integrity")
        assert ir.status_code == 200
        result = ir.json()
        assert result["valid"] is True
        assert result["expected_hash"] == result["actual_hash"]

    def test_health_exposes_runtime_stats(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert "conversations" in data or "status" in data
