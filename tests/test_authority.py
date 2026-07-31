"""AKOS authority-envelope verification tests."""

from __future__ import annotations

import time

import pytest
from fastapi import HTTPException

from echo.auth import require_authority, require_scope, sign_authority


def test_valid_authority_envelope(monkeypatch):
    monkeypatch.setenv("ECHO_AKOS_SHARED_SECRET", "test-secret")
    timestamp = str(int(time.time()))
    signature = sign_authority("test-secret", "akos:test", "echo:write,echo:execute", timestamp, "n-1")
    authority = require_authority(
        x_akos_actor="akos:test",
        x_akos_scope="echo:write,echo:execute",
        x_akos_timestamp=timestamp,
        x_akos_nonce="n-1",
        x_akos_signature=signature,
    )
    require_scope(authority, "echo:write")
    assert authority.actor == "akos:test"


def test_missing_server_secret_fails_closed(monkeypatch):
    monkeypatch.delenv("ECHO_AKOS_SHARED_SECRET", raising=False)
    with pytest.raises(HTTPException) as failure:
        require_authority(
            x_akos_actor="akos:test",
            x_akos_scope="echo:write",
            x_akos_timestamp=str(int(time.time())),
            x_akos_nonce="n-2",
            x_akos_signature="bad",
        )
    assert failure.value.status_code == 503


def test_invalid_signature_and_scope_are_rejected(monkeypatch):
    monkeypatch.setenv("ECHO_AKOS_SHARED_SECRET", "test-secret")
    timestamp = str(int(time.time()))
    with pytest.raises(HTTPException) as signature_failure:
        require_authority(
            x_akos_actor="akos:test",
            x_akos_scope="echo:read",
            x_akos_timestamp=timestamp,
            x_akos_nonce="n-3",
            x_akos_signature="invalid",
        )
    assert signature_failure.value.status_code == 403

    signature = sign_authority("test-secret", "akos:test", "echo:read", timestamp, "n-4")
    authority = require_authority(
        x_akos_actor="akos:test",
        x_akos_scope="echo:read",
        x_akos_timestamp=timestamp,
        x_akos_nonce="n-4",
        x_akos_signature=signature,
    )
    with pytest.raises(HTTPException) as scope_failure:
        require_scope(authority, "echo:execute")
    assert scope_failure.value.status_code == 403
