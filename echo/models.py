"""Core domain models for ECHO continuity and governed orchestration."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stable_uuid(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def canonical_json(payload: Any) -> str:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def content_sha256(payload: str | bytes) -> str:
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class Base(DeclarativeBase):
    pass


class ConversationORM(Base):
    __tablename__ = "conversations"
    __table_args__ = (
        UniqueConstraint(
            "source", "external_id", name="uq_conversation_source_external"
        ),
    )

    id = Column(String(36), primary_key=True)
    source = Column(String(128), nullable=False, index=True)
    external_id = Column(String(512), nullable=False)
    title = Column(String(512), nullable=False, index=True)
    participants = Column(JSON, default=list, nullable=False)
    labels = Column(JSON, default=list, nullable=False)
    metadata_ = Column("metadata", JSON, default=dict, nullable=False)
    summary = Column(Text, default="", nullable=False)
    content_hash = Column(String(64), nullable=False, index=True)
    integrity_status = Column(
        String(32), default="unverified", nullable=False, index=True
    )
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )
    message_count = Column(Integer, default=0, nullable=False)


class MessageORM(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence", name="uq_message_sequence"),
    )

    id = Column(String(36), primary_key=True)
    conversation_id = Column(
        String(36),
        ForeignKey("conversations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    role = Column(String(64), nullable=False)
    content = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False)
    sequence = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    metadata_ = Column("metadata", JSON, default=dict, nullable=False)


class JobORM(Base):
    __tablename__ = "orchestration_jobs"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_job_idempotency_key"),
    )

    id = Column(String(36), primary_key=True)
    job_type = Column(String(128), nullable=False, index=True)
    payload = Column(JSON, default=dict, nullable=False)
    idempotency_key = Column(String(255), nullable=False)
    status = Column(String(32), default="pending", nullable=False, index=True)
    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=3, nullable=False)
    last_error = Column(Text, default="", nullable=False)
    receipt = Column(JSON, default=dict, nullable=False)
    authority_actor = Column(String(255), default="", nullable=False)
    authority_scope = Column(String(255), default="", nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)


class ReceiptORM(Base):
    __tablename__ = "execution_receipts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt", name="uq_receipt_job_attempt"),
    )

    id = Column(String(36), primary_key=True)
    job_id = Column(
        String(36),
        ForeignKey("orchestration_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    attempt = Column(Integer, nullable=False)
    action = Column(String(128), nullable=False)
    outcome = Column(String(32), nullable=False)
    details = Column(JSON, default=dict, nullable=False)
    content_hash = Column(String(64), nullable=False)
    previous_hash = Column(String(64), default="", nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


class MessageIn(BaseModel):
    role: str = Field(min_length=1, max_length=64)
    content: str = Field(min_length=1, max_length=2_000_000)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("role")
    @classmethod
    def normalize_role(cls, value: str) -> str:
        return value.strip().lower()


class ConversationIn(BaseModel):
    source: str = Field(min_length=1, max_length=128)
    external_id: str = Field(min_length=1, max_length=512)
    title: str = Field(min_length=1, max_length=512)
    participants: list[str] = Field(default_factory=list, max_length=100)
    labels: list[str] = Field(default_factory=list, max_length=100)
    metadata: dict[str, Any] = Field(default_factory=dict)
    messages: list[MessageIn] = Field(min_length=1, max_length=100_000)

    @field_validator("source", "external_id", "title")
    @classmethod
    def strip_required(cls, value: str) -> str:
        return value.strip()


class ConversationOut(BaseModel):
    id: str
    source: str
    external_id: str
    title: str
    participants: list[str]
    labels: list[str]
    metadata: dict[str, Any]
    summary: str
    content_hash: str
    integrity_status: str
    message_count: int
    created_at: datetime
    updated_at: datetime


class JobIn(BaseModel):
    job_type: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(min_length=1, max_length=255)
    max_attempts: int = Field(default=3, ge=1, le=10)

    @field_validator("job_type", "idempotency_key")
    @classmethod
    def strip_job_fields(cls, value: str) -> str:
        return value.strip()


class JobOut(BaseModel):
    id: str
    job_type: str
    idempotency_key: str
    status: str
    attempts: int
    max_attempts: int
    last_error: str
    receipt: dict[str, Any]
    authority_actor: str
    authority_scope: str
    created_at: datetime
    updated_at: datetime
    finished_at: Optional[datetime] = None


class IntegrityResult(BaseModel):
    conversation_id: str
    valid: bool
    expected_hash: str
    actual_hash: str
    message_failures: list[str] = Field(default_factory=list)
