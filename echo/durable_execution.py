"""Durable transactional execution state for ECHO.

This is a source-counterengineered synthesis rather than a Temporal or database
wrapper. It combines durable execution history/replay mechanics with
transactional queue ownership and fencing tokens so ECHO's provider-neutral
execution mesh survives process death and rejects stale workers.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    select,
    update,
)
from sqlalchemy.orm import Mapped, Session, mapped_column

from echo.execution_mesh import (
    ExecutionMesh,
    ExecutionReceipt,
    ExecutionSnapshot,
    ExecutionTask,
    ResourceEnvelope,
    TaskState,
    WorkerResult,
)
from echo.models import Base, utcnow


TERMINAL_STATES = {
    TaskState.SUCCEEDED.value,
    TaskState.FAILED.value,
    TaskState.BLOCKED.value,
}
CLAIMABLE_STATES = {TaskState.PENDING.value}


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _canonical_text(value: Any) -> str:
    """Match the execution mesh's deterministic rich-value serialization."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _jsonable(value: Any) -> Any:
    """Normalize rich Python values into the exact representation persisted as JSON."""
    return json.loads(_canonical_text(value))


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_text(value).encode("utf-8")).hexdigest()


def _task_row_id(run_id: str, task_id: str) -> str:
    return _hash({"run_id": run_id, "task_id": task_id})


class DurableRunORM(Base):
    __tablename__ = "execution_runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    max_concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default="active", nullable=False, index=True
    )
    event_head_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    mesh_receipt_head: Mapped[str] = mapped_column(
        String(64), default="", nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class DurableTaskORM(Base):
    __tablename__ = "execution_tasks"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("execution_runs.run_id", ondelete="CASCADE"),
        index=True,
    )
    task_id: Mapped[str] = mapped_column(String(192), index=True)
    definition: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default=TaskState.PENDING.value, nullable=False, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_owner: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    lease_epoch: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    last_error: Mapped[str] = mapped_column(Text, default="", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class DurableSnapshotORM(Base):
    __tablename__ = "execution_snapshots"

    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("execution_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
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
        String(128),
        ForeignKey("execution_runs.run_id", ondelete="CASCADE"),
        index=True,
    )
    task_id: Mapped[str] = mapped_column(
        String(192), default="", nullable=False, index=True
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    lease_epoch: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    previous_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )


class DurableReceiptORM(Base):
    __tablename__ = "execution_mesh_receipts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("execution_runs.run_id", ondelete="CASCADE"),
        index=True,
    )
    task_id: Mapped[str] = mapped_column(String(192), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workspace_id: Mapped[str] = mapped_column(String(255), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    previous_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )


DURABLE_TABLES = (
    DurableRunORM.__table__,
    DurableTaskORM.__table__,
    DurableSnapshotORM.__table__,
    DurableEventORM.__table__,
    DurableReceiptORM.__table__,
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

    PostgreSQL claims use ``FOR UPDATE SKIP LOCKED``. SQLite claims use an
    optimistic compare-and-swap update, so stale readers cannot create duplicate
    valid leases. Every successful claim increments a fencing epoch; completion,
    failure, and heartbeat calls must present that current epoch.
    """

    def __init__(self, session: Session, *, ensure_schema: bool = True) -> None:
        self.session = session
        if ensure_schema:
            Base.metadata.create_all(bind=session.get_bind(), tables=DURABLE_TABLES)

    @staticmethod
    def _definition(task: ExecutionTask) -> dict[str, Any]:
        return _jsonable(
            {
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
        )

    @classmethod
    def mesh_definition_hash(cls, mesh: ExecutionMesh) -> str:
        return _hash(
            {
                "max_concurrency": mesh.max_concurrency,
                "lease_seconds": mesh.lease_seconds,
                "tasks": [
                    cls._definition(task)
                    for task in sorted(
                        mesh.tasks.values(), key=lambda item: item.task_id
                    )
                ],
            }
        )

    def ensure_run(self, run_id: str, mesh: ExecutionMesh) -> None:
        run_id = run_id.strip()
        if not run_id:
            raise ValueError("run_id must not be empty")
        expected_hash = self.mesh_definition_hash(mesh)
        run = self.session.get(DurableRunORM, run_id)
        if run is not None:
            if run.definition_hash != expected_hash:
                raise ValueError("durable run definition hash mismatch")
            return

        run = DurableRunORM(
            run_id=run_id,
            definition_hash=expected_hash,
            max_concurrency=mesh.max_concurrency,
            lease_seconds=mesh.lease_seconds,
        )
        self.session.add(run)
        for task in sorted(mesh.tasks.values(), key=lambda item: item.task_id):
            self.session.add(
                DurableTaskORM(
                    id=_task_row_id(run_id, task.task_id),
                    run_id=run_id,
                    task_id=task.task_id,
                    definition=self._definition(task),
                    status=TaskState.PENDING.value,
                )
            )
        self.session.flush()
        self._append_event(
            run_id,
            "",
            "run_created",
            0,
            {
                "definition_hash": expected_hash,
                "max_concurrency": mesh.max_concurrency,
                "lease_seconds": mesh.lease_seconds,
            },
        )
        self._sync_mesh_receipts(run_id, [asdict(item) for item in mesh.receipts])

    def _lock_run(self, run_id: str) -> DurableRunORM:
        stmt = select(DurableRunORM).where(DurableRunORM.run_id == run_id)
        if self.session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        run = self.session.scalar(stmt.execution_options(populate_existing=True))
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
        clean_details = _jsonable(details)
        payload = {
            "run_id": run_id,
            "task_id": task_id,
            "event_type": event_type,
            "lease_epoch": lease_epoch,
            "details": clean_details,
            "previous_hash": previous_hash,
        }
        digest = _hash(payload)
        event = DurableEventORM(
            run_id=run_id,
            task_id=task_id,
            event_type=event_type,
            lease_epoch=lease_epoch,
            details=clean_details,
            previous_hash=previous_hash,
            content_hash=digest,
        )
        self.session.add(event)
        run.event_head_hash = digest
        self.session.flush()
        return event

    @staticmethod
    def _receipt_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "task_id": str(raw["task_id"]),
            "attempt": int(raw["attempt"]),
            "outcome": str(raw["outcome"]),
            "worker_id": str(raw["worker_id"]),
            "workspace_id": str(raw["workspace_id"]),
            "details": _jsonable(raw.get("details", {})),
            "previous_hash": str(raw.get("previous_hash", "")),
        }

    def _receipt_rows(self, run_id: str) -> list[DurableReceiptORM]:
        return list(
            self.session.scalars(
                select(DurableReceiptORM)
                .where(DurableReceiptORM.run_id == run_id)
                .order_by(DurableReceiptORM.id)
            ).all()
        )

    def _sync_mesh_receipts(
        self,
        run_id: str,
        receipts: Sequence[Mapping[str, Any]],
    ) -> None:
        """Persist a mesh receipt chain without allowing a checkpoint to regress it."""
        run = self._lock_run(run_id)
        existing = self._receipt_rows(run_id)
        normalized: list[tuple[dict[str, Any], str]] = []
        previous = ""
        for raw in receipts:
            payload = self._receipt_payload(raw)
            if payload["previous_hash"] != previous:
                raise ValueError("mesh receipt chain previous_hash mismatch")
            digest = ExecutionMesh._receipt_digest(payload)
            if digest != str(raw["content_hash"]):
                raise ValueError("mesh receipt content_hash mismatch")
            normalized.append((payload, digest))
            previous = digest

        common = min(len(existing), len(normalized))
        for index in range(common):
            if existing[index].content_hash != normalized[index][1]:
                raise ValueError(
                    "snapshot receipt chain diverges from durable receipts"
                )
        if len(existing) > len(normalized):
            raise ValueError("snapshot receipt chain is behind durable receipt state")

        durable_previous = existing[-1].content_hash if existing else ""
        for payload, digest in normalized[len(existing) :]:
            if payload["previous_hash"] != durable_previous:
                raise ValueError("new receipt does not extend durable receipt head")
            self.session.add(
                DurableReceiptORM(
                    run_id=run_id,
                    task_id=payload["task_id"],
                    attempt=payload["attempt"],
                    outcome=payload["outcome"],
                    worker_id=payload["worker_id"],
                    workspace_id=payload["workspace_id"],
                    details=payload["details"],
                    previous_hash=payload["previous_hash"],
                    content_hash=digest,
                )
            )
            durable_previous = digest
        run.mesh_receipt_head = durable_previous
        self.session.flush()

    def _append_mesh_receipt(
        self,
        run_id: str,
        task: DurableTaskORM,
        outcome: str,
        worker_id: str,
        details: Mapping[str, Any],
    ) -> ExecutionReceipt:
        run = self._lock_run(run_id)
        payload = {
            "task_id": task.task_id,
            "attempt": task.attempts,
            "outcome": outcome,
            "worker_id": worker_id,
            "workspace_id": str(
                task.definition.get("workspace_id") or f"echo:{task.task_id}"
            ),
            "details": _jsonable(details),
            "previous_hash": run.mesh_receipt_head or "",
        }
        digest = ExecutionMesh._receipt_digest(payload)
        row = DurableReceiptORM(
            run_id=run_id,
            task_id=payload["task_id"],
            attempt=payload["attempt"],
            outcome=payload["outcome"],
            worker_id=payload["worker_id"],
            workspace_id=payload["workspace_id"],
            details=payload["details"],
            previous_hash=payload["previous_hash"],
            content_hash=digest,
        )
        self.session.add(row)
        run.mesh_receipt_head = digest
        self.session.flush()
        return ExecutionReceipt(**payload, content_hash=digest)

    def _load_mesh_receipts(self, run_id: str) -> list[ExecutionReceipt]:
        rows = self._receipt_rows(run_id)
        receipts: list[ExecutionReceipt] = []
        previous = ""
        for row in rows:
            payload = {
                "task_id": row.task_id,
                "attempt": row.attempt,
                "outcome": row.outcome,
                "worker_id": row.worker_id,
                "workspace_id": row.workspace_id,
                "details": dict(row.details),
                "previous_hash": row.previous_hash,
            }
            if row.previous_hash != previous:
                raise ValueError("durable mesh receipt previous_hash mismatch")
            digest = ExecutionMesh._receipt_digest(payload)
            if digest != row.content_hash:
                raise ValueError("durable mesh receipt content_hash mismatch")
            receipts.append(ExecutionReceipt(**payload, content_hash=digest))
            previous = digest
        run = self._lock_run(run_id)
        if (run.mesh_receipt_head or "") != previous:
            raise ValueError("durable mesh receipt head mismatch")
        return receipts

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
                .execution_options(populate_existing=True)
            ).all()
        )

    def _propagate_blocked(self, run_id: str) -> tuple[str, ...]:
        blocked: list[str] = []
        changed = True
        while changed:
            changed = False
            rows = self._task_rows(run_id)
            by_id = {row.task_id: row for row in rows}
            for row in rows:
                if row.status != TaskState.PENDING.value:
                    continue
                blockers = sorted(
                    dependency
                    for dependency in row.definition.get("dependencies", [])
                    if dependency in by_id
                    and by_id[dependency].status
                    in {TaskState.FAILED.value, TaskState.BLOCKED.value}
                )
                if not blockers:
                    continue
                row.status = TaskState.BLOCKED.value
                row.last_error = "blocked by failed dependency: " + ",".join(blockers)
                self._append_event(
                    run_id,
                    row.task_id,
                    "task_blocked",
                    row.lease_epoch,
                    {"blockers": blockers},
                )
                self._append_mesh_receipt(
                    run_id,
                    row,
                    "blocked",
                    "scheduler",
                    {"blockers": blockers},
                )
                blocked.append(row.task_id)
                changed = True
        self.session.flush()
        return tuple(sorted(set(blocked)))

    def recover_expired(
        self, run_id: str, *, now: datetime | None = None
    ) -> tuple[str, ...]:
        now = _aware(now or utcnow())
        recovered: list[str] = []
        for row in self._task_rows(run_id):
            expires = _aware(row.lease_expires_at)
            if not (
                row.status in {TaskState.LEASED.value, TaskState.RUNNING.value}
                and expires is not None
                and expires <= now
            ):
                continue
            prior_owner = row.lease_owner
            max_attempts = int(row.definition.get("max_attempts", 1))
            row.lease_owner = ""
            row.lease_expires_at = None
            row.last_error = "expired durable lease recovered"
            if row.attempts >= max_attempts:
                row.status = TaskState.FAILED.value
                event_type = "lease_expired_exhausted"
                receipt_outcome = "failed"
            else:
                row.status = TaskState.PENDING.value
                event_type = "lease_expired"
                receipt_outcome = "retry"
            details = {
                "prior_owner": prior_owner,
                "attempts": row.attempts,
                "max_attempts": max_attempts,
            }
            self._append_event(
                run_id,
                row.task_id,
                event_type,
                row.lease_epoch,
                details,
            )
            self._append_mesh_receipt(
                run_id,
                row,
                receipt_outcome,
                "lease-recovery",
                {"error": row.last_error, **details},
            )
            recovered.append(row.task_id)
        self._propagate_blocked(run_id)
        self._update_run_terminal_state(run_id)
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
            max_attempts = int(row.definition.get("max_attempts", 1))
            if row.attempts >= max_attempts:
                continue
            if row.status not in CLAIMABLE_STATES and not (
                row.status in {TaskState.LEASED.value, TaskState.RUNNING.value}
                and expired
            ):
                continue
            definition = row.definition
            if not set(definition.get("dependencies", [])).issubset(succeeded):
                continue
            if not set(definition.get("required_capabilities", [])).issubset(
                capabilities
            ):
                continue
            candidates.append((-int(definition.get("priority", 0)), row.task_id))
        candidates.sort()
        return [task_id for _, task_id in candidates]

    def _locked_task(
        self,
        run_id: str,
        task_id: str,
        *,
        skip_locked: bool = False,
    ) -> DurableTaskORM | None:
        stmt = select(DurableTaskORM).where(
            DurableTaskORM.run_id == run_id,
            DurableTaskORM.task_id == task_id,
        )
        if self.session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update(skip_locked=skip_locked)
        return self.session.scalar(stmt.execution_options(populate_existing=True))

    def _claim_sqlite(
        self,
        row: DurableTaskORM,
        *,
        worker_id: str,
        expires_at: datetime,
    ) -> DurableTaskORM | None:
        """Atomically reserve a stale-read candidate using compare-and-swap fields."""
        expected_attempts = row.attempts
        expected_epoch = row.lease_epoch
        statement = (
            update(DurableTaskORM)
            .where(
                DurableTaskORM.id == row.id,
                DurableTaskORM.status == TaskState.PENDING.value,
                DurableTaskORM.attempts == expected_attempts,
                DurableTaskORM.lease_epoch == expected_epoch,
            )
            .values(
                status=TaskState.LEASED.value,
                attempts=expected_attempts + 1,
                lease_epoch=expected_epoch + 1,
                lease_owner=worker_id,
                lease_expires_at=expires_at,
                last_error="",
            )
            .execution_options(synchronize_session=False)
        )
        result = self.session.execute(statement)
        if result.rowcount != 1:
            self.session.expire_all()
            return None
        self.session.flush()
        self.session.expire_all()
        return self._locked_task(row.run_id, row.task_id)

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
        backend = self.session.get_bind().dialect.name

        for task_id in self._claimable_ids(run_id, capabilities, now=now):
            row = self._locked_task(
                run_id,
                task_id,
                skip_locked=backend == "postgresql",
            )
            if row is None:
                continue
            expires = _aware(row.lease_expires_at)
            max_attempts = int(row.definition.get("max_attempts", 1))
            if row.attempts >= max_attempts:
                continue
            if row.status != TaskState.PENDING.value and not (
                row.status in {TaskState.LEASED.value, TaskState.RUNNING.value}
                and expires is not None
                and expires <= now
            ):
                continue

            expires_at = now + timedelta(seconds=lease_seconds)
            if backend == "sqlite":
                row = self._claim_sqlite(
                    row,
                    worker_id=worker_id,
                    expires_at=expires_at,
                )
                if row is None:
                    continue
            else:
                row.lease_epoch += 1
                row.lease_owner = worker_id
                row.lease_expires_at = expires_at
                row.status = TaskState.LEASED.value
                row.attempts += 1
                row.last_error = ""

            self._append_event(
                run_id,
                task_id,
                "task_leased",
                row.lease_epoch,
                {
                    "worker_id": worker_id,
                    "expires_at": expires_at.isoformat(),
                },
            )
            self.session.flush()
            return LeaseToken(
                run_id=run_id,
                task_id=task_id,
                worker_id=worker_id,
                epoch=row.lease_epoch,
                expires_at=expires_at,
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
            {
                "worker_id": token.worker_id,
                "expires_at": row.lease_expires_at.isoformat(),
            },
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
        return _jsonable(
            {
                "output": dict(result.output),
                "stream": list(result.stream),
                "terminal": dict(result.terminal),
                "agent_steps": result.agent_steps,
                "tool_calls": result.tool_calls,
            }
        )

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
        details = {
            "worker_id": token.worker_id,
            "result_hash": _hash(row.result),
        }
        self._append_event(
            token.run_id,
            token.task_id,
            "task_succeeded",
            token.epoch,
            details,
        )
        self._append_mesh_receipt(
            token.run_id,
            row,
            "success",
            token.worker_id,
            {
                "agent_steps": result.agent_steps,
                "tool_calls": result.tool_calls,
                "output_bytes": result.output_bytes,
                "stream_items": len(result.stream),
                "terminal": dict(result.terminal),
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
        outcome = "retry" if should_retry else "failed"
        self._append_event(
            token.run_id,
            token.task_id,
            "task_retry" if should_retry else "task_failed",
            token.epoch,
            {"worker_id": token.worker_id, "error": error},
        )
        self._append_mesh_receipt(
            token.run_id,
            row,
            outcome,
            token.worker_id,
            {"error": error},
        )
        if not should_retry:
            self._propagate_blocked(token.run_id)
        self._update_run_terminal_state(token.run_id)
        self.session.flush()
        return row.status

    def _update_run_terminal_state(self, run_id: str) -> None:
        """Serialize terminal-state calculation behind the run ownership lock."""
        run = self._lock_run(run_id)
        self.session.flush()
        rows = self._task_rows(run_id)
        if rows and all(row.status in TERMINAL_STATES for row in rows):
            run.status = (
                "failed"
                if any(row.status == TaskState.FAILED.value for row in rows)
                else "succeeded"
            )
        else:
            run.status = "active"
        self.session.flush()

    def save_snapshot(
        self,
        run_id: str,
        snapshot: ExecutionSnapshot,
        *,
        commit: bool = False,
    ) -> None:
        clean_payload = _jsonable(snapshot.payload)
        recalculated = _hash(clean_payload)
        if recalculated != snapshot.digest:
            raise ValueError("execution snapshot digest mismatch")
        self._sync_mesh_receipts(run_id, clean_payload.get("receipts", []))
        run = self._lock_run(run_id)
        row = self.session.get(DurableSnapshotORM, run_id)
        if row is None:
            row = DurableSnapshotORM(
                run_id=run_id,
                digest=snapshot.digest,
                payload=clean_payload,
                receipt_head=run.mesh_receipt_head or "",
            )
            self.session.add(row)
        else:
            row.digest = snapshot.digest
            row.payload = clean_payload
            row.receipt_head = run.mesh_receipt_head or ""
        self._append_event(
            run_id,
            "",
            "snapshot_saved",
            0,
            {
                "digest": snapshot.digest,
                "receipt_head": run.mesh_receipt_head or "",
            },
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
        """Rehydrate from snapshot, then overlay newer durable task and receipt state."""
        self.recover_expired(run_id)
        run = self._lock_run(run_id)
        rows = self._task_rows(run_id)
        if not rows:
            raise ValueError(f"durable run has no tasks: {run_id}")
        snapshot = self.load_snapshot(run_id)
        if snapshot is not None:
            mesh = ExecutionMesh.from_snapshot(snapshot)
            if (
                mesh.max_concurrency != run.max_concurrency
                or abs(mesh.lease_seconds - run.lease_seconds) > 1e-9
            ):
                raise ValueError(
                    "snapshot scheduling settings diverge from durable run"
                )
        else:
            mesh = ExecutionMesh(
                [self._task_from_definition(row.definition) for row in rows],
                max_concurrency=run.max_concurrency,
                lease_seconds=run.lease_seconds,
            )

        for row in rows:
            state = mesh.runtime[row.task_id]
            state.state = TaskState(row.status)
            state.attempts = row.attempts
            state.lease_owner = row.lease_owner
            state.lease_expires_at = (
                _aware(row.lease_expires_at).timestamp()
                if row.lease_expires_at
                else 0.0
            )
            state.last_error = row.last_error
            if row.status == TaskState.SUCCEEDED.value and row.result:
                mesh.results[row.task_id] = self._deserialize_result(row.result)
            elif (
                row.task_id in mesh.results and row.status != TaskState.SUCCEEDED.value
            ):
                del mesh.results[row.task_id]

        mesh.receipts = self._load_mesh_receipts(run_id)
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
