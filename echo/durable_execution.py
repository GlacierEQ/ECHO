"""Durable transactional execution state for ECHO.

This is a source-counterengineered synthesis rather than a Temporal or database
wrapper.  It combines durable execution history/replay mechanics with
transactional queue ownership and fencing tokens so ECHO's provider-neutral
execution mesh survives process death and rejects stale workers.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text, or_, select
from sqlalchemy.orm import Mapped, Session, mapped_column

from echo.execution_mesh import (
    ExecutionMesh,
    ExecutionSnapshot,
    ExecutionTask,
    ResourceEnvelope,
    TaskRuntime,
    TaskState,
    WorkerResult,
)
from echo.models import Base, canonical_json, utcnow


TERMINAL_STATES = {TaskState.SUCCEEDED.value, TaskState.FAILED.value, TaskState.BLOCKED.value}
CLAIMABLE_STATES = {TaskState.PENDING.value}


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


class DurableRunORM(Base):
    __tablename__ = "execution_runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False, index=True)
    event_head_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class DurableTaskORM(Base):
    __tablename__ = "execution_tasks"

    id: Mapped[str] = mapped_column(String(320), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("execution_runs.run_id", ondelete="CASCADE"), index=True
    )
    task_id: Mapped[str] = mapped_column(String(192), index=True)
    definition: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default=TaskState.PENDING.value, nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_owner: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    lease_epoch: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    last_error: Mapped[str] = mapped_column(Text, default="", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class DurableSnapshotORM(Base):
    __tablename__ = "execution_snapshots"

    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("execution_runs.run_id", ondelete="CASCADE"), primary_key=True
    )
    digest: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    receipt_head: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class DurableEventORM(Base):
    __tablename__ = "execution_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("execution_runs.run_id", ondelete="CASCADE"), index=True
    )
    task_id: Mapped[str] = mapped_column(String(192), default="", nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    lease_epoch: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    previous_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


DURABLE_TABLES = (
    DurableRunORM.__table__,
    DurableTaskORM.__table__,
    DurableSnapshotORM.__table__,
    DurableEventORM.__table__,
)


@dataclass(frozen=True)
class LeaseToken:
    run_id: str
    task_id: str
    worker_id: str
    epoch: int
    expires_at: datetime


class StaleLeaseError(RuntimeError):
    """Raised when an old, expired, or superseded worker tries to mutate state."""


class DurableExecutionStore:
    """Transactional durability plane for ``ExecutionMesh``.

    PostgreSQL claims use ``FOR UPDATE SKIP LOCKED`` so competing consumers can
    select queue-like work without waiting on rows already owned by another
    transaction.  Every claim increments a fencing epoch.  Completion, failure,
    and heartbeat calls must present the current epoch, preventing a recovered
    stale worker from committing after another worker has taken ownership.
    """

    def __init__(self, session: Session, *, ensure_schema: bool = True) -> None:
        self.session = session
        if ensure_schema:
            bind = session.get_bind()
            Base.metadata.create_all(bind=bind, tables=DURABLE_TABLES)

    @staticmethod
    def _definition(task: ExecutionTask) -> dict[str, Any]:
        return {
            "task_id": task.task_id,
            "operation": task.operation,
            "payload": dict(task.payload),
            "dependencies": list(task.dependencies),
            "required_capabilities": list(task.required_capabilities),
            "workspace_id": task.workspace_id,
            "priority": task.priority,
            "max_attempts": task.max_attempts,
            "timeout_seconds": task.timeout_seconds,
            "resources": asdict(task.resources),
        }

    @classmethod
    def definition_hash(cls, tasks: Sequence[ExecutionTask]) -> str:
        payload = [cls._definition(task) for task in sorted(tasks, key=lambda item: item.task_id)]
        return _hash({"tasks": payload})

    def ensure_run(self, run_id: str, mesh: ExecutionMesh) -> None:
        run_id = run_id.strip()
        if not run_id:
            raise ValueError("run_id must not be empty")
        expected_hash = self.definition_hash(tuple(mesh.tasks.values()))
        run = self.session.get(DurableRunORM, run_id)
        if run is not None:
            if run.definition_hash != expected_hash:
                raise ValueError("durable run definition hash mismatch")
            return

        run = DurableRunORM(run_id=run_id, definition_hash=expected_hash)
        self.session.add(run)
        for task in sorted(mesh.tasks.values(), key=lambda item: item.task_id):
            self.session.add(
                DurableTaskORM(
                    id=f"{run_id}:{task.task_id}",
                    run_id=run_id,
                    task_id=task.task_id,
                    definition=self._definition(task),
                    status=TaskState.PENDING.value,
                )
            )
        self.session.flush()
        self._append_event(run_id, "", "run_created", 0, {"definition_hash": expected_hash})

    def _lock_run(self, run_id: str) -> DurableRunORM:
        stmt = select(DurableRunORM).where(DurableRunORM.run_id == run_id)
        if self.session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        run = self.session.scalar(stmt)
        if run is None:
            raise ValueError(f"durable run not found: {run_id}")
        return run

    def _append_event(
        self,
        run_id: str,
        task_id: str,
        event_type: str,
        lease_epoch: int,
        details: Mapping[str, Any],
    ) -> DurableEventORM:
        run = self._lock_run(run_id)
        previous_hash = run.event_head_hash or ""
        payload = {
            "run_id": run_id,
            "task_id": task_id,
            "event_type": event_type,
            "lease_epoch": lease_epoch,
            "details": dict(details),
            "previous_hash": previous_hash,
        }
        digest = _hash(payload)
        event = DurableEventORM(
            run_id=run_id,
            task_id=task_id,
            event_type=event_type,
            lease_epoch=lease_epoch,
            details=dict(details),
            previous_hash=previous_hash,
            content_hash=digest,
        )
        self.session.add(event)
        run.event_head_hash = digest
        self.session.flush()
        return event

    def history(self, run_id: str) -> tuple[Mapping[str, Any], ...]:
        rows = self.session.scalars(
            select(DurableEventORM)
            .where(DurableEventORM.run_id == run_id)
            .order_by(DurableEventORM.id)
        ).all()
        return tuple(
            {
                "id": row.id,
                "task_id": row.task_id,
                "event_type": row.event_type,
                "lease_epoch": row.lease_epoch,
                "details": row.details,
                "previous_hash": row.previous_hash,
                "content_hash": row.content_hash,
                "created_at": row.created_at.isoformat(),
            }
            for row in rows
        )

    def verify_history(self, run_id: str) -> bool:
        previous_hash = ""
        for row in self.session.scalars(
            select(DurableEventORM)
            .where(DurableEventORM.run_id == run_id)
            .order_by(DurableEventORM.id)
        ).all():
            payload = {
                "run_id": run_id,
                "task_id": row.task_id,
                "event_type": row.event_type,
                "lease_epoch": row.lease_epoch,
                "details": row.details,
                "previous_hash": previous_hash,
            }
            if row.previous_hash != previous_hash or row.content_hash != _hash(payload):
                return False
            previous_hash = row.content_hash
        run = self.session.get(DurableRunORM, run_id)
        return run is not None and (run.event_head_hash or "") == previous_hash

    def _task_rows(self, run_id: str) -> list[DurableTaskORM]:
        return list(
            self.session.scalars(
                select(DurableTaskORM)
                .where(DurableTaskORM.run_id == run_id)
                .order_by(DurableTaskORM.task_id)
            ).all()
        )

    def recover_expired(self, run_id: str, *, now: datetime | None = None) -> tuple[str, ...]:
        now = _aware(now or utcnow())
        recovered: list[str] = []
        for row in self._task_rows(run_id):
            expires = _aware(row.lease_expires_at)
            if (
                row.status in {TaskState.LEASED.value, TaskState.RUNNING.value}
                and expires is not None
                and expires <= now
            ):
                prior_owner = row.lease_owner
                row.status = TaskState.PENDING.value
                row.lease_owner = ""
                row.lease_expires_at = None
                row.last_error = "expired durable lease recovered"
                self._append_event(
                    run_id,
                    row.task_id,
                    "lease_expired",
                    row.lease_epoch,
                    {"prior_owner": prior_owner},
                )
                recovered.append(row.task_id)
        self.session.flush()
        return tuple(sorted(recovered))

    def _dependency_success(self, rows: Sequence[DurableTaskORM]) -> set[str]:
        return {row.task_id for row in rows if row.status == TaskState.SUCCEEDED.value}

    def _claimable_ids(
        self,
        run_id: str,
        capabilities: frozenset[str],
        *,
        now: datetime,
    ) -> list[str]:
        rows = self._task_rows(run_id)
        succeeded = self._dependency_success(rows)
        candidates: list[tuple[int, str]] = []
        for row in rows:
            expires = _aware(row.lease_expires_at)
            expired = expires is not None and expires <= now
            if row.status not in CLAIMABLE_STATES and not (
                row.status in {TaskState.LEASED.value, TaskState.RUNNING.value} and expired
            ):
                continue
            definition = row.definition
            if not set(definition.get("dependencies", [])).issubset(succeeded):
                continue
            if not set(definition.get("required_capabilities", [])).issubset(capabilities):
                continue
            candidates.append((-int(definition.get("priority", 0)), row.task_id))
        candidates.sort()
        return [task_id for _, task_id in candidates]

    def _locked_task(self, run_id: str, task_id: str) -> DurableTaskORM | None:
        stmt = select(DurableTaskORM).where(
            DurableTaskORM.run_id == run_id,
            DurableTaskORM.task_id == task_id,
        )
        if self.session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)
        return self.session.scalar(stmt)

    def claim_next(
        self,
        run_id: str,
        worker_id: str,
        capabilities: frozenset[str],
        *,
        lease_seconds: float = 330.0,
        now: datetime | None = None,
    ) -> LeaseToken | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        worker_id = worker_id.strip()
        if not worker_id:
            raise ValueError("worker_id must not be empty")
        now = _aware(now or utcnow())
        self.recover_expired(run_id, now=now)

        for task_id in self._claimable_ids(run_id, capabilities, now=now):
            row = self._locked_task(run_id, task_id)
            if row is None:
                continue
            expires = _aware(row.lease_expires_at)
            if row.status != TaskState.PENDING.value and not (
                row.status in {TaskState.LEASED.value, TaskState.RUNNING.value}
                and expires is not None
                and expires <= now
            ):
                continue
            row.lease_epoch += 1
            row.lease_owner = worker_id
            row.lease_expires_at = now + timedelta(seconds=lease_seconds)
            row.status = TaskState.LEASED.value
            row.attempts += 1
            row.last_error = ""
            self._append_event(
                run_id,
                task_id,
                "task_leased",
                row.lease_epoch,
                {"worker_id": worker_id, "expires_at": row.lease_expires_at.isoformat()},
            )
            self.session.flush()
            return LeaseToken(
                run_id=run_id,
                task_id=task_id,
                worker_id=worker_id,
                epoch=row.lease_epoch,
                expires_at=_aware(row.lease_expires_at),
            )
        return None

    def _validate_token(
        self,
        token: LeaseToken,
        *,
        now: datetime | None = None,
        require_live: bool = True,
    ) -> DurableTaskORM:
        row = self._locked_task(token.run_id, token.task_id)
        if row is None:
            raise StaleLeaseError("leased task no longer exists")
        if row.lease_owner != token.worker_id or row.lease_epoch != token.epoch:
            raise StaleLeaseError("lease fencing token is stale")
        expires = _aware(row.lease_expires_at)
        if require_live and (expires is None or expires <= _aware(now or utcnow())):
            raise StaleLeaseError("lease has expired")
        return row

    def mark_running(self, token: LeaseToken, *, now: datetime | None = None) -> None:
        row = self._validate_token(token, now=now)
        row.status = TaskState.RUNNING.value
        self._append_event(
            token.run_id,
            token.task_id,
            "task_running",
            token.epoch,
            {"worker_id": token.worker_id},
        )
        self.session.flush()

    def heartbeat(
        self,
        token: LeaseToken,
        *,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> LeaseToken:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = _aware(now or utcnow())
        row = self._validate_token(token, now=now)
        row.lease_expires_at = now + timedelta(seconds=lease_seconds)
        self._append_event(
            token.run_id,
            token.task_id,
            "lease_heartbeat",
            token.epoch,
            {"worker_id": token.worker_id, "expires_at": row.lease_expires_at.isoformat()},
        )
        self.session.flush()
        return LeaseToken(
            run_id=token.run_id,
            task_id=token.task_id,
            worker_id=token.worker_id,
            epoch=token.epoch,
            expires_at=_aware(row.lease_expires_at),
        )

    @staticmethod
    def _serialize_result(result: WorkerResult) -> dict[str, Any]:
        return {
            "output": dict(result.output),
            "stream": list(result.stream),
            "terminal": dict(result.terminal),
            "agent_steps": result.agent_steps,
            "tool_calls": result.tool_calls,
        }

    @staticmethod
    def _deserialize_result(raw: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(
            output=dict(raw.get("output", {})),
            stream=tuple(raw.get("stream", [])),
            terminal=dict(raw.get("terminal", {})),
            agent_steps=int(raw.get("agent_steps", 0)),
            tool_calls=int(raw.get("tool_calls", 0)),
        )

    def complete(
        self,
        token: LeaseToken,
        result: WorkerResult,
        *,
        now: datetime | None = None,
    ) -> None:
        row = self._validate_token(token, now=now)
        row.status = TaskState.SUCCEEDED.value
        row.result = self._serialize_result(result)
        row.last_error = ""
        row.lease_owner = ""
        row.lease_expires_at = None
        self._append_event(
            token.run_id,
            token.task_id,
            "task_succeeded",
            token.epoch,
            {
                "worker_id": token.worker_id,
                "result_hash": _hash(row.result),
            },
        )
        self._update_run_terminal_state(token.run_id)
        self.session.flush()

    def fail(
        self,
        token: LeaseToken,
        error: str,
        *,
        retry: bool = True,
        now: datetime | None = None,
    ) -> str:
        row = self._validate_token(token, now=now)
        max_attempts = int(row.definition.get("max_attempts", 1))
        should_retry = retry and row.attempts < max_attempts
        row.status = TaskState.PENDING.value if should_retry else TaskState.FAILED.value
        row.last_error = error
        row.lease_owner = ""
        row.lease_expires_at = None
        self._append_event(
            token.run_id,
            token.task_id,
            "task_retry" if should_retry else "task_failed",
            token.epoch,
            {"worker_id": token.worker_id, "error": error},
        )
        self._update_run_terminal_state(token.run_id)
        self.session.flush()
        return row.status

    def _update_run_terminal_state(self, run_id: str) -> None:
        rows = self._task_rows(run_id)
        run = self._lock_run(run_id)
        if rows and all(row.status in TERMINAL_STATES for row in rows):
            run.status = "failed" if any(row.status == TaskState.FAILED.value for row in rows) else "succeeded"
        else:
            run.status = "active"

    def save_snapshot(
        self,
        run_id: str,
        snapshot: ExecutionSnapshot,
        *,
        commit: bool = False,
    ) -> None:
        recalculated = _hash(snapshot.payload)
        if recalculated != snapshot.digest:
            raise ValueError("execution snapshot digest mismatch")
        row = self.session.get(DurableSnapshotORM, run_id)
        receipt_head = str(snapshot.payload.get("receipts", [{}])[-1].get("content_hash", "")) if snapshot.payload.get("receipts") else ""
        if row is None:
            row = DurableSnapshotORM(
                run_id=run_id,
                digest=snapshot.digest,
                payload=dict(snapshot.payload),
                receipt_head=receipt_head,
            )
            self.session.add(row)
        else:
            row.digest = snapshot.digest
            row.payload = dict(snapshot.payload)
            row.receipt_head = receipt_head
        self._append_event(
            run_id,
            "",
            "snapshot_saved",
            0,
            {"digest": snapshot.digest, "receipt_head": receipt_head},
        )
        self.session.flush()
        if commit:
            self.session.commit()

    def checkpoint_callback(self, run_id: str):
        def checkpoint(snapshot: ExecutionSnapshot) -> None:
            self.save_snapshot(run_id, snapshot, commit=True)

        return checkpoint

    def load_snapshot(self, run_id: str) -> ExecutionSnapshot | None:
        row = self.session.get(DurableSnapshotORM, run_id)
        if row is None:
            return None
        snapshot = ExecutionSnapshot(payload=dict(row.payload), digest=row.digest)
        # Reuse ECHO's digest validator before returning persisted state.
        ExecutionMesh.from_snapshot(snapshot)
        return snapshot

    @staticmethod
    def _task_from_definition(raw: Mapping[str, Any]) -> ExecutionTask:
        return ExecutionTask(
            task_id=str(raw["task_id"]),
            operation=str(raw["operation"]),
            payload=dict(raw.get("payload", {})),
            dependencies=tuple(raw.get("dependencies", [])),
            required_capabilities=tuple(raw.get("required_capabilities", [])),
            workspace_id=str(raw.get("workspace_id", "")),
            priority=int(raw.get("priority", 0)),
            max_attempts=int(raw.get("max_attempts", 3)),
            timeout_seconds=float(raw.get("timeout_seconds", 300.0)),
            resources=ResourceEnvelope(**dict(raw.get("resources", {}))),
        )

    def restore_mesh(self, run_id: str) -> ExecutionMesh:
        """Rehydrate execution from snapshot, then overlay newer durable task state.

        The overlay matters when a process dies after a task commit but before
        the next wave snapshot.  Persisted task state is therefore authoritative
        for execution progress; snapshots accelerate reconstruction rather than
        becoming a second truth source.
        """
        self.recover_expired(run_id)
        rows = self._task_rows(run_id)
        if not rows:
            raise ValueError(f"durable run has no tasks: {run_id}")
        snapshot = self.load_snapshot(run_id)
        if snapshot is not None:
            mesh = ExecutionMesh.from_snapshot(snapshot)
        else:
            mesh = ExecutionMesh(
                [self._task_from_definition(row.definition) for row in rows]
            )

        for row in rows:
            state = mesh.runtime[row.task_id]
            state.state = TaskState(row.status)
            state.attempts = row.attempts
            state.lease_owner = row.lease_owner
            state.lease_expires_at = (
                _aware(row.lease_expires_at).timestamp() if row.lease_expires_at else 0.0
            )
            state.last_error = row.last_error
            if row.status == TaskState.SUCCEEDED.value and row.result:
                mesh.results[row.task_id] = self._deserialize_result(row.result)
            elif row.task_id in mesh.results and row.status != TaskState.SUCCEEDED.value:
                del mesh.results[row.task_id]
        return mesh

    @staticmethod
    def postgres_claim_sql() -> str:
        """Expose the PostgreSQL ownership primitive for contract verification."""
        from sqlalchemy.dialects import postgresql

        stmt = (
            select(DurableTaskORM)
            .where(DurableTaskORM.status == TaskState.PENDING.value)
            .order_by(DurableTaskORM.task_id)
            .with_for_update(skip_locked=True)
        )
        return str(stmt.compile(dialect=postgresql.dialect()))
