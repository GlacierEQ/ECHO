"""Portable read-only work and receipt contracts for long-horizon builds."""
from __future__ import annotations
import hashlib, json, re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

WORK_SCHEMA = "glaciereq.work-envelope.v1"
RECEIPT_SCHEMA = "glaciereq.execution-receipt.v1"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_STATUSES = frozenset({"succeeded", "failed", "rejected", "blocked"})

def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

def sha256_hex(value: Any) -> str:
    raw = value if isinstance(value, bytes) else value.encode() if isinstance(value, str) else canonical_json(value).encode()
    return hashlib.sha256(raw).hexdigest()

def _text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip(): raise ValueError(f"{name} must not be empty")

def _digest(name: str, value: str) -> None:
    if not _HEX64.fullmatch(value): raise ValueError(f"{name} must be a lowercase SHA-256 digest")

@dataclass(frozen=True)
class WorkEnvelope:
    work_id: str
    idempotency_key: str
    producer: str
    source_repository: str
    source_revision: str
    capability: str
    authority_scope: str
    exact_target: str
    input_sha256: str
    created_at: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    schema: str = WORK_SCHEMA
    action_mode: str = "read_only"

    def __post_init__(self) -> None:
        for name in ("work_id", "idempotency_key", "producer", "source_repository", "source_revision", "capability", "authority_scope", "exact_target", "created_at"):
            _text(name, getattr(self, name))
        if self.schema != WORK_SCHEMA: raise ValueError(f"unsupported work schema: {self.schema}")
        if self.action_mode != "read_only": raise ValueError("WorkEnvelope permits only read_only action_mode")
        _digest("input_sha256", self.input_sha256)
        canonical_json(self.payload)

    @classmethod
    def create(cls, *, work_id: str, idempotency_key: str, producer: str, source_repository: str, source_revision: str, capability: str, authority_scope: str, exact_target: str, created_at: str, payload: Mapping[str, Any] | None = None) -> "WorkEnvelope":
        payload = dict(payload or {})
        return cls(work_id, idempotency_key, producer, source_repository, source_revision, capability, authority_scope, exact_target, sha256_hex(payload), created_at, payload)

    def unsigned_dict(self) -> dict[str, Any]:
        return {"schema": self.schema, "work_id": self.work_id, "idempotency_key": self.idempotency_key, "producer": self.producer, "source_repository": self.source_repository, "source_revision": self.source_revision, "capability": self.capability, "authority_scope": self.authority_scope, "exact_target": self.exact_target, "input_sha256": self.input_sha256, "created_at": self.created_at, "payload": dict(self.payload), "action_mode": self.action_mode}

    @property
    def envelope_sha256(self) -> str: return sha256_hex(self.unsigned_dict())
    def as_dict(self) -> dict[str, Any]: return {**self.unsigned_dict(), "envelope_sha256": self.envelope_sha256}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "WorkEnvelope":
        fields = {k: raw[k] for k in cls.__dataclass_fields__ if k in raw}
        result = cls(**fields)
        if raw.get("envelope_sha256", result.envelope_sha256) != result.envelope_sha256: raise ValueError("work envelope digest mismatch")
        return result

@dataclass(frozen=True)
class ExecutionReceipt:
    work_id: str
    envelope_sha256: str
    status: str
    verified: bool
    verification_method: str
    output_sha256: str
    created_at: str
    previous_receipt_hash: str = ""
    external_actions_performed: int = 0
    details: Mapping[str, Any] = field(default_factory=dict)
    schema: str = RECEIPT_SCHEMA

    def __post_init__(self) -> None:
        _text("work_id", self.work_id); _digest("envelope_sha256", self.envelope_sha256); _text("verification_method", self.verification_method); _text("created_at", self.created_at)
        if self.schema != RECEIPT_SCHEMA: raise ValueError(f"unsupported receipt schema: {self.schema}")
        if self.status not in _STATUSES: raise ValueError(f"unsupported receipt status: {self.status}")
        if self.output_sha256: _digest("output_sha256", self.output_sha256)
        if self.previous_receipt_hash: _digest("previous_receipt_hash", self.previous_receipt_hash)
        if self.external_actions_performed != 0: raise ValueError("read-only receipt cannot report external actions")
        if not isinstance(self.verified, bool): raise ValueError("verified must be boolean")
        canonical_json(self.details)

    @classmethod
    def from_output(cls, envelope: WorkEnvelope, *, status: str, output: Any, verified: bool, verification_method: str, created_at: str, previous_receipt_hash: str = "", details: Mapping[str, Any] | None = None) -> "ExecutionReceipt":
        return cls(envelope.work_id, envelope.envelope_sha256, status, verified, verification_method, sha256_hex(output) if output is not None else "", created_at, previous_receipt_hash, 0, dict(details or {}))

    def unsigned_dict(self) -> dict[str, Any]:
        return {"schema": self.schema, "work_id": self.work_id, "envelope_sha256": self.envelope_sha256, "status": self.status, "verified": self.verified, "verification_method": self.verification_method, "output_sha256": self.output_sha256, "created_at": self.created_at, "previous_receipt_hash": self.previous_receipt_hash, "external_actions_performed": self.external_actions_performed, "details": dict(self.details)}

    @property
    def receipt_hash(self) -> str: return sha256_hex(self.unsigned_dict())
    def as_dict(self) -> dict[str, Any]: return {**self.unsigned_dict(), "receipt_hash": self.receipt_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionReceipt":
        values = {k: raw[k] for k in cls.__dataclass_fields__ if k in raw}
        result = cls(**values)
        if raw.get("receipt_hash", result.receipt_hash) != result.receipt_hash: raise ValueError("execution receipt digest mismatch")
        return result

class ReceiptChain:
    def __init__(self, envelope: WorkEnvelope): self.envelope, self._receipts = envelope, []
    @property
    def head(self) -> str: return self._receipts[-1].receipt_hash if self._receipts else ""
    @property
    def receipts(self) -> tuple[ExecutionReceipt, ...]: return tuple(self._receipts)
    def append(self, **kwargs: Any) -> ExecutionReceipt:
        receipt = ExecutionReceipt.from_output(self.envelope, previous_receipt_hash=self.head, **kwargs)
        self._receipts.append(receipt); return receipt
    def verify(self) -> bool: return verify_receipt_chain(self.envelope, self._receipts)

def verify_receipt_chain(envelope: WorkEnvelope, receipts: Sequence[ExecutionReceipt]) -> bool:
    previous = ""
    for receipt in receipts:
        if receipt.work_id != envelope.work_id or receipt.envelope_sha256 != envelope.envelope_sha256 or receipt.previous_receipt_hash != previous or receipt.receipt_hash != sha256_hex(receipt.unsigned_dict()): return False
        previous = receipt.receipt_hash
    return True
