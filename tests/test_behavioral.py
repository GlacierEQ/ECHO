"""Behavioral invariants for the governed ECHO piston."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from echo.db import get_session, init_db
from echo.models import ConversationIn, JobIn, MessageIn, ReceiptORM
from echo.service import ContinuityService


@pytest.fixture()
def svc(tmp_path):
    engine = init_db(tmp_path / "test.db")
    with get_session(engine) as session:
        yield ContinuityService(session)


def conversation(external_id: str = "thread-1", tail: str = "continuity established"):
    return ConversationIn(
        source="grok",
        external_id=external_id,
        title="Governed Continuity Pulse",
        participants=["operator", "echo"],
        labels=["test", "p0"],
        metadata={"provider": "grok-app"},
        messages=[
            MessageIn(role="user", content="begin orchestration"),
            MessageIn(role="assistant", content=tail),
        ],
    )


def test_source_identity_is_stable_and_content_updates(svc: ContinuityService):
    first = svc.ingest_conversation(conversation())
    second = svc.ingest_conversation(conversation(tail="continuity upgraded"))
    assert first.id == second.id
    assert first.content_hash != second.content_hash
    assert second.message_count == 2
    assert "upgraded" in svc.list_messages(second.id)[1]["content"]


def test_distinct_source_ids_do_not_collide(svc: ContinuityService):
    first = svc.ingest_conversation(conversation("thread-a"))
    second = svc.ingest_conversation(conversation("thread-b"))
    assert first.id != second.id


def test_integrity_recomputation_and_quarantine(svc: ContinuityService):
    conv = svc.ingest_conversation(conversation())
    valid = svc.verify_integrity(conv.id)
    assert valid.valid is True

    # Simulate out-of-band database tampering.
    message = svc.session.execute(select(__import__("echo.models", fromlist=["MessageORM"]).MessageORM)).scalars().first()
    message.content = "tampered"
    svc.session.flush()
    invalid = svc.verify_integrity(conv.id)
    assert invalid.valid is False
    assert invalid.message_failures
    assert svc.get_conversation(conv.id).integrity_status == "quarantined"


def test_search_reads_message_history(svc: ContinuityService):
    conv = svc.ingest_conversation(conversation(tail="unique deep history phrase"))
    assert any(hit.id == conv.id for hit in svc.search(q="deep history"))


def test_job_receipt_and_explicit_idempotency(svc: ContinuityService):
    job = svc.enqueue_job(
        JobIn(
            job_type="echo.ping",
            payload={"n": 1},
            idempotency_key="pulse-001",
            max_attempts=2,
        ),
        actor="akos:test",
        scope="echo:execute",
    )
    duplicate = svc.enqueue_job(
        JobIn(
            job_type="echo.ping",
            payload={"n": 999},
            idempotency_key="pulse-001",
            max_attempts=2,
        )
    )
    assert duplicate.id == job.id
    ran = svc.run_job(job.id)
    assert ran.status == "succeeded"
    assert ran.receipt["pong"] is True
    receipt = svc.session.scalar(select(ReceiptORM).where(ReceiptORM.job_id == job.id))
    assert receipt.content_hash
    assert receipt.attempt == 1


def test_unsupported_job_fails_closed(svc: ContinuityService):
    with pytest.raises(ValueError, match="unsupported capability"):
        svc.enqueue_job(
            JobIn(
                job_type="echo.pretend-success",
                payload={},
                idempotency_key="unsupported-001",
            )
        )


def test_export_contains_provenance(svc: ContinuityService):
    conv = svc.ingest_conversation(conversation())
    data = svc.export_json(conv.id)
    assert data["source"] == "grok"
    assert data["external_id"] == "thread-1"
    assert "begin orchestration" in svc.export_markdown(conv.id)
