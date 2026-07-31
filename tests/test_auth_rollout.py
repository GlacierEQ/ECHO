"""Regression tests for staged OIDC authentication rollout."""

from __future__ import annotations

import time
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

import echo.security as security
from echo.db import init_db
from echo.main import create_app

ISSUER = "https://tenant.example/"
AUDIENCE = "https://echo-api"


class FakeJwksClient:
    def __init__(self, public_key):
        self.public_key = public_key

    def get_signing_key_from_jwt(self, token: str):
        assert token
        return SimpleNamespace(key=self.public_key)


@pytest.fixture
def keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def configure_auth(monkeypatch, public_key, mode: str) -> None:
    monkeypatch.setenv("ECHO_AUTH_MODE", mode)
    monkeypatch.setenv("AUTH0_DOMAIN", "tenant.example")
    monkeypatch.setenv("AUTH0_AUDIENCE", AUDIENCE)
    monkeypatch.delenv("ECHO_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("ECHO_OIDC_AUDIENCE", raising=False)
    monkeypatch.setattr(
        security,
        "_get_jwks_client",
        lambda _uri: FakeJwksClient(public_key),
    )


def make_token(private_key, scopes: str, subject: str = "svc:test") -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "iat": now,
            "exp": now + 300,
            "scope": scopes,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "test-key"},
    )


def job_payload(key: str) -> dict:
    return {
        "job_type": "echo.ping",
        "payload": {"auth_test": True},
        "idempotency_key": key,
        "max_attempts": 1,
    }


def test_shadow_mode_is_safe_when_unconfigured(monkeypatch, tmp_path):
    monkeypatch.delenv("ECHO_AUTH_MODE", raising=False)
    monkeypatch.delenv("AUTH0_DOMAIN", raising=False)
    monkeypatch.delenv("AUTH0_AUDIENCE", raising=False)
    monkeypatch.delenv("ECHO_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("ECHO_OIDC_AUDIENCE", raising=False)
    client = TestClient(create_app(init_db(tmp_path / "shadow-unconfigured.db")))

    response = client.post("/jobs", json=job_payload("shadow-unconfigured"))
    assert response.status_code == 200
    assert response.json()["authority_actor"] == "direct-api"

    authentication = client.get("/health").json()["authentication"]
    assert authentication == {
        "mode": "shadow",
        "configured": False,
        "algorithm": "RS256",
        "shared_secret_required": False,
    }


def test_valid_shadow_token_records_real_provenance(monkeypatch, tmp_path, keypair):
    private_key, public_key = keypair
    configure_auth(monkeypatch, public_key, "shadow")
    client = TestClient(create_app(init_db(tmp_path / "shadow-valid.db")))
    token = make_token(private_key, "echo:execute echo:read", "service:collector")

    response = client.post(
        "/jobs",
        json=job_payload("shadow-valid"),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["authority_actor"] == "service:collector"
    assert response.json()["authority_scope"] == "echo:execute,echo:read"


def test_invalid_shadow_token_never_causes_lockout(monkeypatch, tmp_path, keypair):
    _private_key, public_key = keypair
    configure_auth(monkeypatch, public_key, "shadow")
    client = TestClient(create_app(init_db(tmp_path / "shadow-invalid.db")))

    response = client.post(
        "/jobs",
        json=job_payload("shadow-invalid"),
        headers={"Authorization": "Bearer definitely-not-a-jwt"},
    )
    assert response.status_code == 200
    assert response.json()["authority_actor"] == "direct-api"


def test_enforce_writes_blocks_missing_token_but_not_reads(
    monkeypatch, tmp_path, keypair
):
    _private_key, public_key = keypair
    configure_auth(monkeypatch, public_key, "enforce-writes")
    client = TestClient(create_app(init_db(tmp_path / "enforce-writes.db")))

    assert client.get("/stats").status_code == 200
    blocked = client.post("/jobs", json=job_payload("missing-token"))
    assert blocked.status_code == 401
    assert blocked.headers["www-authenticate"] == "Bearer"


def test_enforce_writes_checks_scope(monkeypatch, tmp_path, keypair):
    private_key, public_key = keypair
    configure_auth(monkeypatch, public_key, "enforce-writes")
    client = TestClient(create_app(init_db(tmp_path / "scope.db")))

    read_token = make_token(private_key, "echo:read")
    denied = client.post(
        "/jobs",
        json=job_payload("wrong-scope"),
        headers={"Authorization": f"Bearer {read_token}"},
    )
    assert denied.status_code == 403

    execute_token = make_token(private_key, "echo:execute")
    allowed = client.post(
        "/jobs",
        json=job_payload("correct-scope"),
        headers={"Authorization": f"Bearer {execute_token}"},
    )
    assert allowed.status_code == 200


def test_enforce_all_keeps_health_public(monkeypatch, tmp_path, keypair):
    _private_key, public_key = keypair
    configure_auth(monkeypatch, public_key, "enforce-all")
    client = TestClient(create_app(init_db(tmp_path / "enforce-all.db")))

    assert client.get("/health").status_code == 200
    assert client.get("/stats").status_code == 401


def test_enforcement_fails_explicitly_when_provider_is_unconfigured(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("ECHO_AUTH_MODE", "enforce-writes")
    monkeypatch.delenv("AUTH0_DOMAIN", raising=False)
    monkeypatch.delenv("AUTH0_AUDIENCE", raising=False)
    monkeypatch.delenv("ECHO_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("ECHO_OIDC_AUDIENCE", raising=False)
    client = TestClient(create_app(init_db(tmp_path / "unconfigured.db")))

    response = client.post("/jobs", json=job_payload("unconfigured"))
    assert response.status_code == 503
