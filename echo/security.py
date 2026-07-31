"""Staged OIDC bearer-token verification for ECHO.

The default mode is ``shadow``: tokens are verified and logged when present,
but requests are never blocked. Enforcement is enabled explicitly through
``ECHO_AUTH_MODE`` after production validation.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Annotated, Any, Callable

import jwt
from fastapi import Depends, Header, HTTPException, Request
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError

LOGGER = logging.getLogger("echo.auth")
VALID_MODES = {"off", "shadow", "enforce-writes", "enforce-all"}


@dataclass(frozen=True)
class AuthSettings:
    mode: str
    issuer: str
    audience: str

    @property
    def configured(self) -> bool:
        return bool(self.issuer and self.audience)

    @property
    def jwks_uri(self) -> str:
        return f"{self.issuer.rstrip('/')}/.well-known/jwks.json"


@dataclass(frozen=True)
class AuthContext:
    status: str
    subject: str = ""
    scopes: frozenset[str] = field(default_factory=frozenset)
    claims: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    @property
    def valid(self) -> bool:
        return self.status == "valid"


def get_auth_settings() -> AuthSettings:
    mode = os.environ.get("ECHO_AUTH_MODE", "shadow").strip().lower()
    if mode not in VALID_MODES:
        mode = "shadow"

    issuer = os.environ.get("ECHO_OIDC_ISSUER", "").strip()
    auth0_domain = os.environ.get("AUTH0_DOMAIN", "").strip()
    if not issuer and auth0_domain:
        issuer = (
            auth0_domain
            if auth0_domain.startswith(("https://", "http://"))
            else f"https://{auth0_domain}"
        )
    if issuer:
        issuer = f"{issuer.rstrip('/')}/"

    audience = (
        os.environ.get("ECHO_OIDC_AUDIENCE", "").strip()
        or os.environ.get("AUTH0_AUDIENCE", "").strip()
    )
    return AuthSettings(mode=mode, issuer=issuer, audience=audience)


@lru_cache(maxsize=8)
def _get_jwks_client(jwks_uri: str) -> PyJWKClient:
    return PyJWKClient(
        jwks_uri,
        cache_keys=True,
        cache_jwk_set=True,
        lifespan=300,
        timeout=5,
    )


def _extract_scopes(claims: dict[str, Any]) -> frozenset[str]:
    scopes: set[str] = set()
    raw_scope = claims.get("scope", "")
    if isinstance(raw_scope, str):
        scopes.update(item for item in raw_scope.split() if item)
    permissions = claims.get("permissions", [])
    if isinstance(permissions, list):
        scopes.update(str(item) for item in permissions if item)
    return frozenset(scopes)


def validate_authorization_header(authorization: str | None) -> AuthContext:
    settings = get_auth_settings()
    if settings.mode == "off":
        return AuthContext(status="off")
    if not settings.configured:
        return AuthContext(status="unconfigured")
    if not authorization:
        return AuthContext(status="missing")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return AuthContext(status="invalid", reason="malformed_authorization_header")

    try:
        signing_key = _get_jwks_client(settings.jwks_uri).get_signing_key_from_jwt(
            token.strip()
        )
        claims = jwt.decode(
            token.strip(),
            signing_key.key,
            algorithms=["RS256"],
            audience=settings.audience,
            issuer=settings.issuer,
            leeway=30,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
        return AuthContext(
            status="valid",
            subject=str(claims.get("sub", "")),
            scopes=_extract_scopes(claims),
            claims=claims,
        )
    except (PyJWTError, ValueError, OSError) as exc:
        return AuthContext(status="invalid", reason=exc.__class__.__name__)


def _log_auth_event(
    request: Request,
    context: AuthContext,
    required_scope: str,
    enforced: bool,
) -> None:
    payload = {
        "event": "echo_auth",
        "method": request.method,
        "path": request.url.path,
        "mode": get_auth_settings().mode,
        "status": context.status,
        "subject": context.subject,
        "scope_count": len(context.scopes),
        "required_scope": required_scope,
        "enforced": enforced,
        "reason": context.reason,
        "request_id": request.headers.get("x-request-id", ""),
    }
    level = logging.WARNING if context.status == "invalid" else logging.INFO
    LOGGER.log(level, json.dumps(payload, sort_keys=True))


def _should_enforce(mode: str, write_operation: bool) -> bool:
    return mode == "enforce-all" or (mode == "enforce-writes" and write_operation)


def auth_dependency(
    required_scope: str,
    *,
    write_operation: bool = False,
) -> Callable[..., AuthContext]:
    """Return a FastAPI dependency for staged authentication enforcement."""

    def dependency(
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ) -> AuthContext:
        settings = get_auth_settings()
        context = validate_authorization_header(authorization)
        enforce = _should_enforce(settings.mode, write_operation)
        _log_auth_event(request, context, required_scope, enforce)

        if not enforce:
            return context
        if not settings.configured:
            raise HTTPException(
                status_code=503,
                detail="OIDC authentication is not configured",
            )
        if not context.valid:
            raise HTTPException(
                status_code=401,
                detail="valid bearer token required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if required_scope not in context.scopes and "echo:*" not in context.scopes:
            raise HTTPException(
                status_code=403,
                detail=f"required scope: {required_scope}",
            )
        return context

    return dependency


def auth_runtime_status() -> dict[str, Any]:
    settings = get_auth_settings()
    return {
        "mode": settings.mode,
        "configured": settings.configured,
        "algorithm": "RS256",
        "shared_secret_required": False,
    }


def provenance(context: AuthContext) -> tuple[str, str]:
    if not context.valid:
        return "direct-api", "echo:*"
    scope = ",".join(sorted(context.scopes))
    return context.subject or "authenticated-api", scope
