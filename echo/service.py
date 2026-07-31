"""Core continuity + orchestration service."""

from __future__ import annotations

import json
import time
from typing import Any, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from echo.models import (
    ConversationIn,
    ConversationORM,
    ConversationOut,
    JobIn,
    JobORM,
    JobOut,
    MessageORM,
    ReceiptORM,
    content_sha256,
    stable_uuid,
    utcnow,
)


class ContinuityService:
    def __init__(self, session: Session):
        self.session = session
        self._start = time.monotonic()

    def ingest_conversation(self, data: ConversationIn) -> ConversationOut:
        seed = data.title + "|" + (data.messages[0].content if data.messages else "")
        conv_id = stable_uuid(f"echo:conv:{seed}")

        existing = self.session.get(ConversationORM, conv_id)
        if existing:
            return self._to_out(existing)

        messages_payload = [
            {"role": m.role, "content": m.content, "metadata": m.metadata}
            for m in data.messages
        ]
        full_content = json.dumps({"title": data.title, "messages": messages_payload}, sort_keys=True)
        c_hash = content_sha256(full_content)
        summary = self._deterministic_summary(data.title, data.messages)

        conv = ConversationORM(
            id=conv_id,
            title=data.title,
            participants=data.participants,
            labels=data.labels,
            metadata_=data.metadata,
            summary=summary,
            content_hash=c_hash,
            message_count=len(data.messages),
        )
        self.session.add(conv)

        for idx, msg in enumerate(data.messages):
            msg_id = stable_uuid(f"echo:msg:{conv_id}:{idx}:{msg.content[:64]}")
            self.session.add(
                MessageORM(
                    id=msg_id,
                    conversation_id=conv_id,
                    role=msg.role,
                    content=msg.content,
                    content_hash=content_sha256(msg.content),
                    sequence=idx,
                    metadata_=msg.metadata,
                )
            )

        self.session.flush()
        return self._to_out(conv)

    def get_conversation(self, conv_id: str) -> Optional[ConversationOut]:
        conv = self.session.get(ConversationORM, conv_id)
        return self._to_out(conv) if conv else None

    def search(self, q: str = "", label: Optional[str] = None, limit: int = 50) -> list[ConversationOut]:
        stmt = select(ConversationORM)
        if q:
            like = f"%{q}%"
            stmt = stmt.where(
                or_(
                    ConversationORM.title.ilike(like),
                    ConversationORM.summary.ilike(like),
                )
            )
        stmt = stmt.order_by(ConversationORM.updated_at.desc()).limit(limit * 3 if label else limit)
        rows = self.session.scalars(stmt).all()
        results = [self._to_out(r) for r in rows]
        if label:
            results = [r for r in results if label in (r.labels or [])]
        return results[:limit]

    def list_messages(self, conv_id: str) -> list[dict]:
        stmt = select(MessageORM).where(MessageORM.conversation_id == conv_id).order_by(MessageORM.sequence)
        return [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "content_hash": m.content_hash,
                "sequence": m.sequence,
                "metadata": m.metadata_ or {},
                "created_at": m.created_at.isoformat() if m.created_at else None,
            }
            for m in self.session.scalars(stmt).all()
        ]

    def enqueue_job(self, data: JobIn) -> JobOut:
        payload_str = json.dumps(data.payload, sort_keys=True)
        job_id = stable_uuid(f"echo:job:{data.job_type}:{payload_str}")
        existing = self.session.get(JobORM, job_id)
        if existing:
            return self._job_out(existing)

        job = JobORM(
            id=job_id,
            job_type=data.job_type,
            payload=data.payload,
            status="pending",
            max_attempts=data.max_attempts,
        )
        self.session.add(job)
        self.session.flush()
        return self._job_out(job)

    def run_job(self, job_id: str) -> JobOut:
        job = self.session.get(JobORM, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        if job.status == "succeeded":
            return self._job_out(job)

        if job.attempts >= job.max_attempts:
            job.status = "failed"
            job.last_error = "max attempts exceeded"
            job.finished_at = utcnow()
            self._write_receipt(job, "execute", "failure", {"reason": "max_attempts"})
            self.session.flush()
            return self._job_out(job)

        job.status = "running"
        job.attempts += 1
        self.session.flush()

        try:
            result = self._dispatch(job)
            job.status = "succeeded"
            job.receipt = result
            job.finished_at = utcnow()
            self._write_receipt(job, "execute", "success", result)
        except Exception as exc:
            job.status = "retrying" if job.attempts < job.max_attempts else "failed"
            job.last_error = str(exc)
            if job.status == "failed":
                job.finished_at = utcnow()
            self._write_receipt(job, "execute", "failure", {"error": str(exc)})
        self.session.flush()
        return self._job_out(job)

    def _dispatch(self, job: JobORM) -> dict[str, Any]:
        t = job.job_type
        if t == "echo.ping":
            return {"pong": True, "payload": job.payload}
        if t == "echo.summarize":
            conv_id = job.payload.get("conversation_id")
            conv = self.session.get(ConversationORM, conv_id) if conv_id else None
            return {"summary": conv.summary if conv else "not found"}
        if t == "echo.export":
            return {"exported": True, "conversation_id": job.payload.get("conversation_id"), "format": job.payload.get("format", "json")}
        return {"acknowledged": True, "job_type": t, "payload": job.payload}

    def _write_receipt(self, job: JobORM, action: str, outcome: str, details: dict):
        rid = stable_uuid(f"echo:receipt:{job.id}:{action}:{job.attempts}")
        rec = ReceiptORM(
            id=rid,
            job_id=job.id,
            action=action,
            outcome=outcome,
            details=details,
            content_hash=content_sha256(json.dumps(details, sort_keys=True)),
        )
        self.session.add(rec)

    def health(self) -> dict[str, Any]:
        convs = self.session.scalar(select(func.count()).select_from(ConversationORM)) or 0
        msgs = self.session.scalar(select(func.count()).select_from(MessageORM)) or 0
        jobs = self.session.scalar(select(func.count()).select_from(JobORM)) or 0
        receipts = self.session.scalar(select(func.count()).select_from(ReceiptORM)) or 0
        return {
            "status": "ok",
            "version": "0.1.0",
            "conversations": convs,
            "messages": msgs,
            "jobs": jobs,
            "receipts": receipts,
            "uptime_seconds": round(time.monotonic() - self._start, 2),
            "pillar": "AKOS",
            "role": "piston",
        }

    def recommendations(self) -> list[dict[str, str]]:
        stats = self.health()
        recs = []
        if stats["conversations"] == 0:
            recs.append({"area": "ingestion", "action": "Ingest first conversation to seed continuity"})
        if stats["jobs"] > 0 and stats["receipts"] == 0:
            recs.append({"area": "receipts", "action": "Ensure every job produces an execution receipt"})
        recs.append({"area": "observability", "action": "Wire Prometheus metrics endpoint"})
        recs.append({"area": "authority", "action": "Require AKOS authority envelope on privileged routes"})
        return recs

    def export_json(self, conv_id: str) -> dict:
        conv = self.get_conversation(conv_id)
        if not conv:
            raise ValueError("conversation not found")
        messages = self.list_messages(conv_id)
        return {
            "id": conv.id,
            "title": conv.title,
            "participants": conv.participants,
            "labels": conv.labels,
            "metadata": conv.metadata,
            "summary": conv.summary,
            "content_hash": conv.content_hash,
            "messages": messages,
            "exported_at": utcnow().isoformat(),
        }

    def export_markdown(self, conv_id: str) -> str:
        data = self.export_json(conv_id)
        lines = [
            f"# {data['title']}",
            "",
            f"**ID:** `{data['id']}`  ",
            f"**Hash:** `{data['content_hash']}`  ",
            f"**Participants:** {', '.join(data['participants']) or '—'}  ",
            f"**Labels:** {', '.join(data['labels']) or '—'}  ",
            "",
            "## Summary",
            data["summary"] or "_none_",
            "",
            "## Messages",
            "",
        ]
        for m in data["messages"]:
            lines.append(f"### [{m['sequence']}] {m['role']}")
            lines.append("")
            lines.append(m["content"])
            lines.append("")
        return "\n".join(lines)

    def _deterministic_summary(self, title: str, messages: list) -> str:
        if not messages:
            return f"Empty conversation: {title}"
        first = messages[0].content[:120].replace("\n", " ")
        last = messages[-1].content[:80].replace("\n", " ") if len(messages) > 1 else ""
        return f"{title} — {len(messages)} msgs. Opens: {first}… Ends: {last}…"

    def _to_out(self, conv: ConversationORM) -> ConversationOut:
        return ConversationOut(
            id=conv.id,
            title=conv.title,
            participants=conv.participants or [],
            labels=conv.labels or [],
            metadata=conv.metadata_ or {},
            summary=conv.summary or "",
            content_hash=conv.content_hash,
            message_count=conv.message_count or 0,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
        )

    def _job_out(self, job: JobORM) -> JobOut:
        return JobOut(
            id=job.id,
            job_type=job.job_type,
            status=job.status,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            last_error=job.last_error or "",
            receipt=job.receipt or {},
            created_at=job.created_at,
            updated_at=job.updated_at,
            finished_at=job.finished_at,
        )
