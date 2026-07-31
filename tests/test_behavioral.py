"""Behavioral tests for ECHO v0.1.0 — three core invariants."""

from __future__ import annotations

import pytest
from echo.db import get_engine, get_session, init_db
from echo.models import ConversationIn, JobIn, MessageIn
from echo.service import ContinuityService


@pytest.fixture()
def svc(tmp_path):
    engine = init_db(tmp_path / "test.db")
    with get_session(engine) as session:
        yield ContinuityService(session)


def test_idempotent_ingest(svc: ContinuityService):
    """Same content produces same stable identity and hash."""
    body = ConversationIn(
        title="Idempotency Check",
        participants=["a", "b"],
        messages=[MessageIn(role="user", content="hello continuity")],
    )
    first = svc.ingest_conversation(body)
    second = svc.ingest_conversation(body)
    assert first.id == second.id
    assert first.content_hash == second.content_hash
    assert first.message_count == 1


def test_job_receipt_and_retry_bound(svc: ContinuityService):
    """Jobs produce receipts and respect max_attempts."""
    job = svc.enqueue_job(JobIn(job_type="echo.ping", payload={"n": 1}, max_attempts=2))
    assert job.status == "pending"
    ran = svc.run_job(job.id)
    assert ran.status == "succeeded"
    assert ran.receipt.get("pong") is True

    # re-run succeeds immediately (already finished)
    again = svc.run_job(job.id)
    assert again.status == "succeeded"


def test_search_and_export(svc: ContinuityService):
    """Search surfaces ingested content; export is deterministic."""
    body = ConversationIn(
        title="Searchable Continuity Pulse",
        labels=["test", "v0.1"],
        messages=[
            MessageIn(role="user", content="find me later"),
            MessageIn(role="assistant", content="found"),
        ],
    )
    conv = svc.ingest_conversation(body)
    hits = svc.search(q="Searchable")
    assert any(h.id == conv.id for h in hits)
    hits2 = svc.search(label="v0.1")
    assert any(h.id == conv.id for h in hits2)

    data = svc.export_json(conv.id)
    assert data["content_hash"] == conv.content_hash
    md = svc.export_markdown(conv.id)
    assert conv.title in md
    assert "find me later" in md
