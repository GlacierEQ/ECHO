"""Production-safe HTTP hardening for the ECHO API.

The controls in this module deliberately avoid changing authentication
semantics. They reduce browser, caching, framing, request-amplification, and
accidental-documentation exposure risks while the staged OIDC rollout remains
in shadow mode.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

ASGIApp = Callable[
    [
        dict[str, Any],
        Callable[[], Awaitable[dict[str, Any]]],
        Callable[[dict[str, Any]], Awaitable[None]],
    ],
    Awaitable[None],
]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]

_DEFAULT_MAX_REQUEST_BYTES = 4 * 1024 * 1024
_MAX_REQUEST_BYTES_CEILING = 64 * 1024 * 1024
_TRUE_VALUES = {"1", "true", "yes", "on"}
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


@dataclass(frozen=True)
class HardeningSettings:
    environment: str
    max_request_bytes: int
    docs_enabled: bool
    cors_origins: tuple[str, ...]
    allowed_hosts: tuple[str, ...]

    @property
    def production(self) -> bool:
        return self.environment == "production"


def _is_true(value: str) -> bool:
    return value.strip().lower() in _TRUE_VALUES


def _bounded_request_limit(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return _DEFAULT_MAX_REQUEST_BYTES
    return min(max(parsed, 1024), _MAX_REQUEST_BYTES_CEILING)


def _csv_values(value: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(item.strip() for item in value.split(",") if item.strip())
    )


def _safe_cors_origins(value: str) -> tuple[str, ...]:
    """Accept only explicit HTTP(S) origins; wildcard CORS is never enabled."""
    return tuple(
        origin
        for origin in _csv_values(value)
        if origin != "*" and origin.startswith(("https://", "http://"))
    )


def get_hardening_settings() -> HardeningSettings:
    environment = (
        os.environ.get("ECHO_ENV", "").strip().lower()
        or os.environ.get("VERCEL_ENV", "").strip().lower()
        or ("production" if os.environ.get("VERCEL") else "development")
    )
    production = environment == "production"
    docs_override = os.environ.get("ECHO_ENABLE_DOCS", "").strip()
    docs_enabled = _is_true(docs_override) if docs_override else not production

    return HardeningSettings(
        environment=environment,
        max_request_bytes=_bounded_request_limit(
            os.environ.get("ECHO_MAX_REQUEST_BYTES", str(_DEFAULT_MAX_REQUEST_BYTES))
        ),
        docs_enabled=docs_enabled,
        cors_origins=_safe_cors_origins(os.environ.get("ECHO_CORS_ORIGINS", "")),
        allowed_hosts=_csv_values(os.environ.get("ECHO_ALLOWED_HOSTS", "")),
    )


class _RequestTooLarge(Exception):
    pass


async def _send_json(send: Send, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class RequestSizeLimitMiddleware:
    """Reject oversized fixed-length and streamed request bodies."""

    def __init__(self, app: ASGIApp, max_request_bytes: int) -> None:
        self.app = app
        self.max_request_bytes = max_request_bytes

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                declared = int(content_length)
            except ValueError:
                await _send_json(send, 400, {"detail": "invalid Content-Length"})
                return
            if declared < 0:
                await _send_json(send, 400, {"detail": "invalid Content-Length"})
                return
            if declared > self.max_request_bytes:
                await _send_json(send, 413, {"detail": "request body too large"})
                return

        received = 0
        response_started = False

        async def limited_receive() -> dict[str, Any]:
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_request_bytes:
                    raise _RequestTooLarge
            return message

        async def tracked_send(message: dict[str, Any]) -> None:
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _RequestTooLarge:
            if response_started:
                raise
            await _send_json(send, 413, {"detail": "request body too large"})


class SecurityHeadersMiddleware:
    """Apply deterministic security headers and a bounded request identifier."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    @staticmethod
    def _request_id(scope: dict[str, Any]) -> str:
        for key, value in scope.get("headers", []):
            if key.lower() != b"x-request-id":
                continue
            candidate = value.decode("latin-1", errors="ignore")
            if _REQUEST_ID_RE.fullmatch(candidate):
                return candidate
        return uuid4().hex

    @staticmethod
    def _content_security_policy(path: str) -> str:
        if path.startswith("/docs"):
            return (
                "default-src 'self' https://cdn.jsdelivr.net; "
                "script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; "
                "style-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; "
                "img-src 'self' data: https://fastapi.tiangolo.com; "
                "frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
            )
        return (
            "default-src 'self'; script-src 'none'; "
            "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
            "connect-src 'self'; frame-ancestors 'none'; "
            "base-uri 'none'; form-action 'none'"
        )

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        request_id = self._request_id(scope)
        path = str(scope.get("path", ""))
        enforced = {
            b"cache-control": b"no-store",
            b"content-security-policy": self._content_security_policy(path).encode(
                "ascii"
            ),
            b"cross-origin-opener-policy": b"same-origin",
            b"cross-origin-resource-policy": b"same-origin",
            b"permissions-policy": b"camera=(), microphone=(), geolocation=(), payment=(), usb=()",
            b"referrer-policy": b"no-referrer",
            b"strict-transport-security": b"max-age=63072000; includeSubDomains; preload",
            b"x-content-type-options": b"nosniff",
            b"x-dns-prefetch-control": b"off",
            b"x-frame-options": b"DENY",
            b"x-permitted-cross-domain-policies": b"none",
            b"x-request-id": request_id.encode("ascii"),
        }

        async def send_with_headers(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                existing = [
                    (key, value)
                    for key, value in message.get("headers", [])
                    if key.lower() not in enforced
                ]
                message["headers"] = existing + list(enforced.items())
            await send(message)

        await self.app(scope, receive, send_with_headers)


def configure_hardening(
    app: FastAPI,
    settings: HardeningSettings | None = None,
) -> HardeningSettings:
    settings = settings or get_hardening_settings()

    if settings.allowed_hosts:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=list(settings.allowed_hosts),
        )

    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
            expose_headers=["X-Request-ID"],
            max_age=600,
        )

    app.add_middleware(
        RequestSizeLimitMiddleware,
        max_request_bytes=settings.max_request_bytes,
    )
    app.add_middleware(SecurityHeadersMiddleware)
    return settings


def hardening_runtime_status(settings: HardeningSettings) -> dict[str, Any]:
    """Return non-sensitive controls for health and deployment verification."""
    return {
        "environment": settings.environment,
        "docs_enabled": settings.docs_enabled,
        "max_request_bytes": settings.max_request_bytes,
        "cors_enabled": bool(settings.cors_origins),
        "cors_origin_count": len(settings.cors_origins),
        "trusted_hosts_enabled": bool(settings.allowed_hosts),
        "security_headers": True,
        "request_id": True,
    }
