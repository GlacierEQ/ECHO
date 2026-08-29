from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from echo.db import get_session, init_db
from echo.models import JobIn, JobORM, ReceiptORM
from echo.service import ContinuityService, JobLeaseConflictError


def make_job(service: ContinuityService, key: str = "lease-key"):
    return service.enqueue_job(JobIn(job_type="echo.ping", idempotency_key=key))


def test_two_workers_cannot_claim_the_same_job(tmp_path):
    engine = init_db(tmp_path / "leases.db")
    with get_session(engine) as session:
        service = ContinuityService(session)
        job = make_job(service)
        now = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
        claimed = service.claim_job(job.id, "worker-a", lease_seconds=30, now=now)
        assert claimed.status == "running"
        assert claimed.lease_epoch == 1
        with pytest.raises(JobLeaseConflictError, match="live lease"):
            service.claim_job(job.id, "worker-b", lease_seconds=30, now=now)


def test_expired_job_is_recovered_and_reclaimed_with_new_fence(tmp_path):
    engine = init_db(tmp_path / "recovery.db")
    with get_session(engine) as session:
        service = ContinuityService(session)
        job = make_job(service, "recovery-key")
        start = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
        first = service.claim_job(job.id, "dead-worker", lease_seconds=5, now=start)
        recovered = service.recover_stale_jobs(now=start + timedelta(seconds=6))
        assert recovered == [job.id]
        row = session.get(JobORM, job.id)
        assert row.status == "retrying"
        assert row.lease_owner == ""
        assert row.lease_expires_at is None
        assert row.last_error == "expired lease recovered"
        assert row.lease_epoch == 2
        recovery_receipt = session.scalar(
            select(ReceiptORM).where(
                ReceiptORM.job_id == job.id,
                ReceiptORM.action == "lease_recovery",
            )
        )
        assert recovery_receipt is not None
        assert recovery_receipt.details["worker"] == "dead-worker"
        second = service.claim_job(
            job.id,
            "live-worker",
            lease_seconds=30,
            now=start + timedelta(seconds=6),
        )
        assert second.attempts == first.attempts + 1
        assert second.lease_epoch == 3


def test_heartbeat_requires_current_worker_and_extends_expiry(tmp_path):
    engine = init_db(tmp_path / "heartbeat.db")
    with get_session(engine) as session:
        service = ContinuityService(session)
        job = make_job(service, "heartbeat-key")
        start = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
        claimed = service.claim_job(job.id, "worker-a", lease_seconds=5, now=start)
        renewed = service.heartbeat_job(
            job.id,
            "worker-a",
            lease_seconds=30,
            now=start + timedelta(seconds=2),
        )
        assert renewed.lease_epoch == claimed.lease_epoch
        assert renewed.lease_expires_at == start + timedelta(seconds=32)
        with pytest.raises(JobLeaseConflictError, match="stale or owned"):
            service.heartbeat_job(
                job.id,
                "worker-b",
                lease_seconds=30,
                now=start + timedelta(seconds=3),
            )


def test_run_job_claims_and_releases_lease_on_success(tmp_path):
    engine = init_db(tmp_path / "run.db")
    with get_session(engine) as session:
        service = ContinuityService(session)
        job = make_job(service, "run-key")
        result = service.run_job(job.id, worker_id="worker-a")
        assert result.status == "succeeded"
        row = session.get(JobORM, job.id)
        assert row.lease_owner == ""
        assert row.lease_expires_at is None
        assert row.lease_epoch == 1
