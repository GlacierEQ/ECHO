"""Core domain models for ECHO continuity store."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import JSON, Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stable_uuid(seed: str) -> str:
    """Deterministic UUID from seed (for stable identities)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def content_sha256(payload: str | bytes) -> str:
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class Base(DeclarativeBase):
    pass


class ConversationORM(Base):
    __tablename__ = "conversations"

    id = Column(String(36), primary_key=True)
    title = Column(String(512), nullable=False, index=True)
    participants = Column(JSON, default=list)
    labels = Column(JSON, default=list)
    metadata_ = Column("metadata", JSON, default=dict)
    summary = Column(Text, default="")
    content_hash = Column(String(64), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    message_count = Column(Integer, default=0)


class MessageORM(Base):
    __tablename__ = "messages"

    id = Column(String(36), primary_key=True)
    conversation_id = Column(String(36), nullable=False, index=True)
    role = Column(String(64), nullable=False)
    content = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False)
    sequence = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    metadata_ = Column("metadata", JSON, default=dict)


class JobORM(Base):
    __tablename__ = "orchestration_jobs"

    id = Column(String(36), primary_key=True)
    job_type = Column(String(128), nullable=False, index=True)
    payload = Column(JSON, default=dict)
    status = Column(String(32), default="pending", index=True)  # pending|running|succeeded|failed|retrying
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    last_error = Column(Text, default="")
    receipt = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    finished_at = Column(DateTime(timezone=True), nullable=True)


class ReceiptORM(Base):
    __tablename__ = "execution_receipts"

    id = Column(String(36), primary_key=True)
    job_id = Column(String(36), nullable=False, index=True)
    action = Column(String(128), nullable=False)
    outcome = Column(String(32), nullable=False)  # success|failure|partial
    details = Column(JSON, default=dict)
    content_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)


# Pydantic API models

class MessageIn(BaseModel):
    role: str
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ConversationIn(BaseModel):
    title: str
    participants: list[str] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    messages: list[MessageIn] = Field(default_factory=list)


class ConversationOut(BaseModel):
    id: str
    title: str
    participants: list[str]
    labels: list[str]
    metadata: dict[str, Any]
    summary: str
    content_hash: str
    message_count: int
    created_at: datetime
    updated_at: datetime


class JobIn(BaseModel):
    job_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = 3


class JobOut(BaseModel):
    id: str
    job_type: str
    status: str
    attempts: int
    max_attempts: int
    last_error: str
    receipt: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    finished_at: Optional[datetime] = None


class HealthOut(BaseModel):
    status: str
    version: str
    conversations: int
    messages: int
    jobs: int
    receipts: int
    uptime_seconds: float
