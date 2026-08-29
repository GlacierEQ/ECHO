"""Continuity and orchestration service."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session

from echo.models import (
    ConversationIn,
    ConversationORM,
    ConversationOut,
    IntegrityResult,
    JobIn,
    JobORM,
    JobOut,
    MessageORM,
    ReceiptORM,
    canonical_json,
    content_sha256,
    stable_uuid,
    utcnow,
)
from echo.trust import verify_receipt_chain
from echo.work_adapter import ENVELOPE_PAYLOAD_KEY, receipts_from_durable_records
from echo.work_envelope import WorkEnvelope

SUPPORTED_JOBS = {"echo.ping", "echo.summarize", "echo.integrity.verify"}
DEFAULT_LEASE_SECONDS = 300.0


class JobLeaseConflictError(ValueError):
    """Raised when a worker cannot acquire or renew a live job lease."""


class ContinuityService:
    def __init__(self, session: Session):
        self.session = session
        self._start = time.monotonic()

    @staticmethod
    def _canonical_conversation(data: ConversationIn) -> dict[str, Any]:
        return {
            "source": data.source,
            "external_id": data.external_id,
            "title": data.title,
            "participants": data.participants,
            "labels": data.labels,
            "metadata": data.metadata,
            "messages": [m.model_dump(mode="json") for m in data.messages],
        }

    def ingest_conversation(self, data: ConversationIn) -> ConversationOut:
        conv_id = stable_uuid(f"echo:conv:{data.source}:{data.external_id}")
        canonical = self._canonical_conversation(data)
        content_hash = content_sha256(canonical_json(canonical))
        existing = self.session.get(ConversationORM, conv_id)
        if existing and existing.content_hash == content_hash:
            existing.integrity_status = "verified"
            return self._to_out(existing)
        if existing:
            self.session.query(MessageORM).filter(
                MessageORM.conversation_id == conv_id
            ).delete(synchronize_session=False)
            conv = existing
            conv.title = data.title
            conv.participants = data.participants
            conv.labels = data.labels
            conv.metadata_ = data.metadata
            conv.summary = self._deterministic_summary(data.title, data.messages)
            conv.content_hash = content_hash
            conv.integrity_status = "verified"
            conv.message_count = len(data.messages)
            conv.updated_at = utcnow()
        else:
            conv = ConversationORM(
                id=conv_id,
                source=data.source,
                external_id=data.external_id,
                title=data.title,
                participants=data.participants,
                labels=data.labels,
                metadata_=data.metadata,
                summary=self._deterministic_summary(data.title, data.messages),
                content_hash=content_hash,
                integrity_status="verified",
                message_count=len(data.messages),
            )
            self.session.add(conv)
        for sequence, message in enumerate(data.messages):
            message_payload = message.model_dump(mode="json")
            self.session.add(
                MessageORM(
                    id=stable_uuid(f"echo:msg:{conv_id}:{sequence}"),
                    conversation_id=conv_id,
                    role=message.role,
                    content=message.content,
                    content_hash=content_sha256(canonical_json(message_payload)),
                    sequence=sequence,
                    metadata_=message.metadata,
                )
            )
        self.session.flush()
        return self._to_out(conv)

    def get_conversation(self, conv_id: str) -> Optional[ConversationOut]:
        conv = self.session.get(ConversationORM, conv_id)
        return self._to_out(conv) if conv else None

    def search(
        self, q: str = "", label: Optional[str] = None, limit: int = 50
    ) -> list[ConversationOut]:
        stmt = select(ConversationORM).distinct()
        if q:
            like = f"%{q}%"
            stmt = stmt.outerjoin(
                MessageORM, MessageORM.conversation_id == ConversationORM.id
            )
            stmt = stmt.where(
                or_(
                    ConversationORM.title.ilike(like),
                    ConversationORM.summary.ilike(like),
                    MessageORM.content.ilike(like),
                )
            )
        stmt = stmt.order_by(ConversationORM.updated_at.desc()).limit(
            limit * 3 if label else limit
        )
        results = [
            self._to_out(row) for row in self.session.scalars(stmt).unique().all()
        ]
        if label:
            results = [item for item in results if label in item.labels]
        return results[:limit]

    def list_messages(self, conv_id: str) -> list[dict[str, Any]]:
        stmt = (
            select(MessageORM)
            .where(MessageORM.conversation_id == conv_id)
            .order_by(MessageORM.sequence)
        )
        return [
            {
                "id": row.id,
                "role": row.role,
                "content": row.content,
                "content_hash": row.content_hash,
                "sequence": row.sequence,
                "metadata": row.metadata_ or {},
                "created_at": row.created_at.isoformat(),
            }
            for row in self.session.scalars(stmt).all()
        ]

    def verify_integrity(self, conv_id: str) -> IntegrityResult:
        conv = self.session.get(ConversationORM, conv_id)
        if not conv:
            raise ValueError("conversation not found")
        messages = self.list_messages(conv_id)
        failures: list[str] = []
        canonical_messages = []
        for message in messages:
            payload = {
                "role": message["role"],
                "content": message["content"],
                "metadata": message["metadata"],
            }
            actual_message_hash = content_sha256(canonical_json(payload))
            if actual_message_hash != message["content_hash"]:
                failures.append(message["id"])
            canonical_messages.append(payload)
        canonical = {
            "source": conv.source,
            "external_id": conv.external_id,
            "title": conv.title,
            "participants": conv.participants or [],
            "labels": conv.labels or [],
            "metadata": conv.metadata_ or {},
            "messages": canonical_messages,
        }
        actual = content_sha256(canonical_json(canonical))
        valid = actual == conv.content_hash and not failures
        conv.integrity_status = "verified" if valid else "quarantined"
        self.session.flush()
        return IntegrityResult(
            conversation_id=conv_id,
            valid=valid,
            expected_hash=conv.content_hash,
            actual_hash=actual,
            message_failures=failures,
        )

    def enqueue_job(self, data: JobIn, actor: str = "", scope: str = "") -> JobOut:
        if data.job_type not in SUPPORTED_JOBS:
            raise ValueError(f"unsupported capability: {data.job_type}")
        raw_envelope = data.payload.get(ENVELOPE_PAYLOAD_KEY)
        if raw_envelope is not None:
            if not isinstance(raw_envelope, Mapping):
                raise ValueError("work-envelope binding must be an object")
            envelope = WorkEnvelope.from_dict(raw_envelope)
            if envelope.idempotency_key != data.idempotency_key:
                raise ValueError("job idempotency key does not match work envelope")
            if envelope.capability != data.job_type:
                raise ValueError("job type does not match work-envelope capability")
        existing = self.session.scalar(
            select(JobORM).where(JobORM.idempotency_key == data.idempotency_key)
        )
        if existing:
            return self._job_out(existing)
        job = JobORM(
            id=stable_uuid(f"echo:job:{data.idempotency_key}"),
            job_type=data.job_type,
            payload=data.payload,
            idempotency_key=data.idempotency_key,
            status="pending",
            max_attempts=data.max_attempts,
            authority_actor=actor,
            authority_scope=scope,
        )
        self.session.add(job)
        self.session.flush()
        return self._job_out(job)

    @staticmethod
    def _aware(value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    @classmethod
    def _lease_live(cls, job: JobORM, now: datetime) -> bool:
        return bool(
            job.status == "running"
            and job.lease_expires_at is not None
            and cls._aware(job.lease_expires_at) > now
        )

    def recover_stale_jobs(self, now: datetime | None = None) -> list[str]:
        """Release expired jobs and record the recovery before another claim."""
        now = self._aware(now or utcnow())
        rows = list(
            self.session.scalars(
                select(JobORM).where(
                    JobORM.status == "running",
                    JobORM.lease_expires_at.is_not(None),
                    JobORM.lease_expires_at <= now,
                )
            ).all()
        )
        recovered: list[str] = []
        for candidate in rows:
            prior_owner = candidate.lease_owner
            result = self.session.execute(
                update(JobORM)
                .execution_options(synchronize_session=False)
                .where(
                    JobORM.id == candidate.id,
                    JobORM.status == "running",
                    JobORM.lease_epoch == candidate.lease_epoch,
                    JobORM.lease_expires_at <= now,
                )
                .values(
                    status=(
                        "failed"
                        if candidate.attempts >= candidate.max_attempts
                        else "retrying"
                    ),
                    lease_owner="",
                    lease_expires_at=None,
                    lease_epoch=candidate.lease_epoch + 1,
                    last_error="expired lease recovered",
                    finished_at=(
                        now if candidate.attempts >= candidate.max_attempts else None
                    ),
                )
            )
            if result.rowcount != 1:
                continue
            candidate.status = (
                "failed" if candidate.attempts >= candidate.max_attempts else "retrying"
            )
            candidate.lease_owner = ""
            candidate.lease_expires_at = None
            candidate.lease_epoch += 1
            candidate.last_error = "expired lease recovered"
            candidate.finished_at = (
                now if candidate.attempts >= candidate.max_attempts else None
            )
            self._write_receipt(
                candidate,
                "lease_expired",
                {"reason": "expired lease recovered", "worker": prior_owner},
                action="lease_recovery",
            )
            recovered.append(candidate.id)
        self.session.flush()
        return recovered

    def claim_job(
        self,
        job_id: str,
        worker_id: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> JobOut:
        """Atomically claim a pending/retrying job with a fencing epoch."""
        worker_id = worker_id.strip()
        if not worker_id:
            raise ValueError("worker_id is required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = self._aware(now or utcnow())
        self.recover_stale_jobs(now=now)
        job = self.session.get(JobORM, job_id)
        if not job:
            raise ValueError(f"job {job_id} not found")
        if job.status in {"succeeded", "failed"}:
            return self._job_out(job)
        if job.status == "running":
            if job.lease_owner == worker_id and self._lease_live(job, now):
                return self._job_out(job)
            raise JobLeaseConflictError("job has a live lease owned by another worker")
        if job.status not in {"pending", "retrying"}:
            raise JobLeaseConflictError(f"job is not claimable in state {job.status}")
        if job.attempts >= job.max_attempts:
            job.status = "failed"
            job.last_error = "max attempts exceeded"
            job.finished_at = now
            self._write_receipt(job, "failure", {"reason": "max_attempts"})
            self.session.flush()
            return self._job_out(job)
        expires = now + timedelta(seconds=lease_seconds)
        result = self.session.execute(
            update(JobORM)
            .execution_options(synchronize_session=False)
            .where(
                JobORM.id == job.id,
                JobORM.status == job.status,
                JobORM.attempts == job.attempts,
                JobORM.lease_epoch == job.lease_epoch,
            )
            .values(
                status="running",
                attempts=job.attempts + 1,
                lease_owner=worker_id,
                lease_expires_at=expires,
                lease_epoch=job.lease_epoch + 1,
                last_error="",
            )
        )
        if result.rowcount != 1:
            raise JobLeaseConflictError("job claim lost a concurrent race")
        self.session.refresh(job)
        return self._job_out(job)

    def heartbeat_job(
        self,
        job_id: str,
        worker_id: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> JobOut:
        """Extend a live lease without changing its fencing epoch."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = self._aware(now or utcnow())
        job = self.session.get(JobORM, job_id)
        if not job:
            raise ValueError(f"job {job_id} not found")
        if job.lease_owner != worker_id or not self._lease_live(job, now):
            raise JobLeaseConflictError("lease is stale or owned by another worker")
        expires = now + timedelta(seconds=lease_seconds)
        result = self.session.execute(
            update(JobORM)
            .execution_options(synchronize_session=False)
            .where(
                JobORM.id == job.id,
                JobORM.status == "running",
                JobORM.lease_owner == worker_id,
                JobORM.lease_epoch == job.lease_epoch,
                JobORM.lease_expires_at > now,
            )
            .values(lease_expires_at=expires)
        )
        if result.rowcount != 1:
            raise JobLeaseConflictError("lease heartbeat lost a concurrent race")
        self.session.refresh(job)
        return self._job_out(job)

    def run_job(self, job_id: str, worker_id: str = "direct") -> JobOut:
        job_state = self.claim_job(job_id, worker_id)
        if job_state.status in {"succeeded", "failed"}:
            return job_state
        job = self.session.get(JobORM, job_id)
        assert job is not None
        try:
            result = self._dispatch(job)
            if not self._lease_live(job, self._aware(utcnow())):
                raise JobLeaseConflictError("lease expired before result commit")
            job.status = "succeeded"
            job.receipt = result
            job.finished_at = utcnow()
            self._write_receipt(job, "success", result)
            job.lease_owner = ""
            job.lease_expires_at = None
        except JobLeaseConflictError:
            raise
        except Exception as exc:
            if not self._lease_live(job, self._aware(utcnow())):
                raise JobLeaseConflictError(
                    "lease expired before failure commit"
                ) from exc
            job.status = "retrying" if job.attempts < job.max_attempts else "failed"
            job.last_error = str(exc)
            if job.status == "failed":
                job.finished_at = utcnow()
            self._write_receipt(job, "failure", {"error": str(exc)})
            job.lease_owner = ""
            job.lease_expires_at = None
        self.session.flush()
        return self._job_out(job)

    def _dispatch(self, job: JobORM) -> dict[str, Any]:
        if job.job_type == "echo.ping":
            return {"pong": True, "payload": job.payload}
        if job.job_type == "echo.summarize":
            conv_id = str(job.payload.get("conversation_id", ""))
            conv = self.session.get(ConversationORM, conv_id)
            if not conv:
                raise ValueError("conversation not found")
            return {"conversation_id": conv_id, "summary": conv.summary}
        if job.job_type == "echo.integrity.verify":
            conv_id = str(job.payload.get("conversation_id", ""))
            return self.verify_integrity(conv_id).model_dump(mode="json")
        raise ValueError(f"unsupported capability: {job.job_type}")

    def _write_receipt(
        self,
        job: JobORM,
        outcome: str,
        details: dict[str, Any],
        action: str = "execute",
    ) -> None:
        previous = self.session.scalar(
            select(ReceiptORM)
            .where(ReceiptORM.job_id == job.id)
            .order_by(ReceiptORM.attempt.desc())
        )
        previous_hash = previous.content_hash if previous else ""
        payload = {
            "job_id": job.id,
            "attempt": job.attempts,
            "outcome": outcome,
            "details": details,
            "previous_hash": previous_hash,
        }
        receipt_hash = content_sha256(canonical_json(payload))
        self.session.add(
            ReceiptORM(
                id=stable_uuid(f"echo:receipt:{job.id}:{job.attempts}"),
                job_id=job.id,
                attempt=job.attempts,
                action=action,
                outcome=outcome,
                details=details,
                previous_hash=previous_hash,
                content_hash=receipt_hash,
            )
        )

    def portable_receipts(self, job_id: str) -> dict[str, Any]:
        """Return a read-only portable proof view for an envelope-bound job."""
        job = self.session.get(JobORM, job_id)
        if not job:
            raise ValueError("job not found")
        raw_envelope = job.payload.get(ENVELOPE_PAYLOAD_KEY)
        if not isinstance(raw_envelope, Mapping):
            raise ValueError("job has no work-envelope binding")
        envelope = WorkEnvelope.from_dict(raw_envelope)
        if envelope.idempotency_key != job.idempotency_key:
            raise ValueError("stored job and work envelope idempotency keys differ")
        durable_report = verify_receipt_chain(self.session, job_id)
        records = list(
            self.session.scalars(
                select(ReceiptORM)
                .where(ReceiptORM.job_id == job_id)
                .order_by(ReceiptORM.attempt.asc())
            ).all()
        )
        chain = receipts_from_durable_records(envelope, records, job_id=job_id)
        portable_valid = chain.verify()
        return {
            "job_id": job_id,
            "work_id": envelope.work_id,
            "envelope": envelope.as_dict(),
            "portable_receipts": [receipt.as_dict() for receipt in chain.receipts],
            "portable_head_hash": chain.head,
            "portable_chain_valid": portable_valid,
            "durable_chain": durable_report,
            "verified": bool(durable_report["valid"] and portable_valid),
        }

    def health(self) -> dict[str, Any]:
        count = lambda model: (
            self.session.scalar(select(func.count()).select_from(model)) or 0
        )
        return {
            "status": "ok",
            "version": "0.2.1-direct",
            "conversations": count(ConversationORM),
            "messages": count(MessageORM),
            "jobs": count(JobORM),
            "receipts": count(ReceiptORM),
            "uptime_seconds": round(time.monotonic() - self._start, 2),
            "pillar": "AKOS",
            "role": "piston",
            "authority_mode": "direct_access",
        }

    def recommendations(self) -> list[dict[str, str]]:
        return [
            {
                "area": "workers",
                "action": "Add transactional leases and stale-worker recovery",
            },
            {
                "area": "providers",
                "action": "Bind ChatGPT, Claude, Gemini, and Grok adapters behind capability contracts",
            },
            {
                "area": "observability",
                "action": "Publish queue, latency, integrity, and retry metrics",
            },
        ]

    def export_json(self, conv_id: str) -> dict[str, Any]:
        conv = self.get_conversation(conv_id)
        if not conv:
            raise ValueError("conversation not found")
        return {
            **conv.model_dump(mode="json"),
            "messages": self.list_messages(conv_id),
            "exported_at": utcnow().isoformat(),
        }

    def export_markdown(self, conv_id: str) -> str:
        data = self.export_json(conv_id)
        lines = [
            f"# {data['title']}",
            "",
            f"**Source:** `{data['source']}`",
            f"**External ID:** `{data['external_id']}`",
            f"**Hash:** `{data['content_hash']}`",
            "",
            "## Summary",
            data["summary"],
            "",
            "## Messages",
            "",
        ]
        for message in data["messages"]:
            lines.extend(
                [
                    f"### [{message['sequence']}] {message['role']}",
                    "",
                    message["content"],
                    "",
                ]
            )
        return "\n".join(lines)

    @staticmethod
    def _deterministic_summary(title: str, messages: list[Any]) -> str:
        first = messages[0].content[:160].replace("\n", " ")
        last = messages[-1].content[:120].replace("\n", " ")
        return f"{title} — {len(messages)} messages. Opens: {first}… Ends: {last}…"

    @staticmethod
    def _to_out(conv: ConversationORM) -> ConversationOut:
        return ConversationOut(
            id=conv.id,
            source=conv.source,
            external_id=conv.external_id,
            title=conv.title,
            participants=conv.participants or [],
            labels=conv.labels or [],
            metadata=conv.metadata_ or {},
            summary=conv.summary,
            content_hash=conv.content_hash,
            integrity_status=conv.integrity_status,
            message_count=conv.message_count,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
        )

    @staticmethod
    def _job_out(job: JobORM) -> JobOut:
        return JobOut(
            id=job.id,
            job_type=job.job_type,
            idempotency_key=job.idempotency_key,
            status=job.status,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            last_error=job.last_error or "",
            receipt=job.receipt or {},
            authority_actor=job.authority_actor or "",
            authority_scope=job.authority_scope or "",
            lease_epoch=job.lease_epoch,
            lease_expires_at=(
                ContinuityService._aware(job.lease_expires_at)
                if job.lease_expires_at is not None
                else None
            ),
            created_at=job.created_at,
            updated_at=job.updated_at,
            finished_at=job.finished_at,
        )
