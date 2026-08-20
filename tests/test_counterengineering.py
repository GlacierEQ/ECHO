"""Ceiling-first continuity regression tests for ECHO."""
from __future__ import annotations

import pytest
from sqlalchemy import select

from echo.counterengineering import build_recovery_continuity
from echo.db import get_session, init_db
from echo.models import JobIn, ReceiptORM
from echo.service import ContinuityService


@pytest.fixture()
def svc(tmp_path):
    engine = init_db(tmp_path / "counterengineering.db")
    with get_session(engine) as session:
        yield ContinuityService(session)


def recovery_payload(**overrides):
    payload = {
        "target": "Restore the full capability family and build beyond the pre-contraction ceiling.",
        "current_floor": "Current implementation preserves only a reduced subset.",
        "contraction_detected": True,
        "displaced_capabilities": ["deep analysis", "autonomous orchestration"],
        "lineage_sources": ["pre-contraction branch", "operator intent record"],
        "verified_strengths": ["receipt chain", "idempotent execution"],
        "real_constraints": ["external provider authorization"],
        "local_blocks": ["one CI runner unavailable"],
        "compatible_later_gains": ["stronger integrity receipts"],
        "surpass_opportunities": ["compose recovered analysis with newer execution mesh"],
        "verification_obligations": ["execute recovered capability tests"],
    }
    payload.update(overrides)
    return payload


def test_recovery_continuity_preserves_ceiling_and_separates_floor():
    result = build_recovery_continuity(recovery_payload())
    assert result["orientation"] == "CEILING_FIRST"
    assert result["target_preserved"] is True
    assert result["target"] != result["current_floor"]
    assert result["recovery_posture"] == "RESTORE_AND_SURPASS"
    assert result["unresolved_recovery_obligation_count"] == 5


def test_local_block_is_contained_not_globalized():
    result = build_recovery_continuity(recovery_payload())
    assert result["local_blocks"] == [
        {"boundary": "one CI runner unavailable", "scope": "LOCAL_UNLESS_PROVEN_GLOBAL"}
    ]


def test_detected_contraction_requires_displaced_capability_or_lineage():
    with pytest.raises(ValueError, match="displaced capability or lineage evidence"):
        build_recovery_continuity(
            recovery_payload(displaced_capabilities=[], lineage_sources=[])
        )


def test_counterengineering_job_is_idempotent_and_receipted(svc: ContinuityService):
    request = JobIn(
        job_type="echo.counterengineering.continuity",
        payload=recovery_payload(),
        idempotency_key="estate-recovery-001",
        max_attempts=2,
    )
    first = svc.enqueue_job(request, actor="akos:counterengineering", scope="echo:continuity")
    duplicate = svc.enqueue_job(request)
    assert duplicate.id == first.id

    completed = svc.run_job(first.id)
    assert completed.status == "succeeded"
    assert completed.receipt["orientation"] == "CEILING_FIRST"
    assert completed.receipt["recovery_posture"] == "RESTORE_AND_SURPASS"
    assert "deep analysis" in completed.receipt["displaced_capabilities"]

    receipt = svc.session.scalar(select(ReceiptORM).where(ReceiptORM.job_id == first.id))
    assert receipt is not None
    assert receipt.outcome == "success"
    assert receipt.content_hash


def test_no_contraction_still_advances_from_ceiling():
    result = build_recovery_continuity(
        recovery_payload(
            contraction_detected=False,
            displaced_capabilities=[],
            lineage_sources=[],
            surpass_opportunities=[],
        )
    )
    assert result["recovery_posture"] == "ADVANCE_FROM_CEILING"
