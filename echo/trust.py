"""AKOS ↔ ECHO trust-loop verification primitives."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from echo.models import JobORM, ReceiptORM, canonical_json, content_sha256


def verify_receipt_chain(session: Session, job_id: str) -> dict[str, Any]:
    """Recompute and verify the complete receipt chain for a job."""
    job = session.get(JobORM, job_id)
    if not job:
        raise ValueError("job not found")

    receipts = list(
        session.scalars(
            select(ReceiptORM)
            .where(ReceiptORM.job_id == job_id)
            .order_by(ReceiptORM.attempt.asc())
        ).all()
    )
    failures: list[dict[str, Any]] = []
    previous_hash = ""

    for receipt in receipts:
        payload = {
            "job_id": receipt.job_id,
            "attempt": receipt.attempt,
            "outcome": receipt.outcome,
            "details": receipt.details or {},
            "previous_hash": previous_hash,
        }
        actual_hash = content_sha256(canonical_json(payload))
        if receipt.previous_hash != previous_hash:
            failures.append(
                {
                    "attempt": receipt.attempt,
                    "reason": "previous_hash_mismatch",
                    "expected": previous_hash,
                    "actual": receipt.previous_hash,
                }
            )
        if receipt.content_hash != actual_hash:
            failures.append(
                {
                    "attempt": receipt.attempt,
                    "reason": "content_hash_mismatch",
                    "expected": receipt.content_hash,
                    "actual": actual_hash,
                }
            )
        previous_hash = receipt.content_hash

    valid = bool(receipts) and not failures
    return {
        "job_id": job_id,
        "valid": valid,
        "receipt_count": len(receipts),
        "head_hash": previous_hash,
        "failures": failures,
        "authority_actor": job.authority_actor,
        "authority_scope": job.authority_scope,
        "job_status": job.status,
    }


def trust_loop_report(session: Session, job_id: str) -> dict[str, Any]:
    """Return the complete authority → execution → receipt proof record."""
    result = verify_receipt_chain(session, job_id)
    result["trust_loop"] = {
        "authority_recorded": bool(result["authority_actor"]),
        "execution_terminal": result["job_status"] in {"succeeded", "failed"},
        "receipt_chain_verified": result["valid"],
    }
    result["verified"] = all(result["trust_loop"].values())
    return result
