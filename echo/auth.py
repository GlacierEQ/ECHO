"""AKOS authority-envelope verification for privileged ECHO operations."""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass

from fastapi import Header, HTTPException


@dataclass(frozen=True)
class AuthorityContext:
    actor: str
    scope: str
    nonce: str
    timestamp: int


def canonical_authority_message(actor: str, scope: str, timestamp: str, nonce: str) -> bytes:
    return f"{actor}\n{scope}\n{timestamp}\n{nonce}".encode("utf-8")


def sign_authority(secret: str, actor: str, scope: str, timestamp: str, nonce: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        canonical_authority_message(actor, scope, timestamp, nonce),
        hashlib.sha256,
    ).hexdigest()


def require_authority(
    x_akos_actor: str = Header(...),
    x_akos_scope: str = Header(...),
    x_akos_timestamp: str = Header(...),
    x_akos_nonce: str = Header(...),
    x_akos_signature: str = Header(...),
) -> AuthorityContext:
    secret = os.environ.get("ECHO_AKOS_SHARED_SECRET", "")
    if not secret:
        raise HTTPException(503, "AKOS authority verification is not configured")
    try:
        timestamp = int(x_akos_timestamp)
    except ValueError as exc:
        raise HTTPException(401, "invalid AKOS authority timestamp") from exc
    max_skew = int(os.environ.get("ECHO_AUTH_MAX_SKEW_SECONDS", "300"))
    if abs(int(time.time()) - timestamp) > max_skew:
        raise HTTPException(401, "expired AKOS authority envelope")
    expected = sign_authority(
        secret, x_akos_actor, x_akos_scope, x_akos_timestamp, x_akos_nonce
    )
    if not hmac.compare_digest(expected, x_akos_signature):
        raise HTTPException(403, "invalid AKOS authority signature")
    return AuthorityContext(
        actor=x_akos_actor.strip(),
        scope=x_akos_scope.strip(),
        nonce=x_akos_nonce.strip(),
        timestamp=timestamp,
    )


def require_scope(authority: AuthorityContext, required: str) -> None:
    scopes = {item.strip() for item in authority.scope.split(",") if item.strip()}
    if "echo:*" not in scopes and required not in scopes:
        raise HTTPException(403, f"AKOS authority scope required: {required}")
