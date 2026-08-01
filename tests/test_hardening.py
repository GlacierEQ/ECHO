"""Regression coverage for HTTP runtime hardening."""

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from echo.hardening import (
    HardeningSettings,
    configure_hardening,
    get_hardening_settings,
    hardening_runtime_status,
)


def _settings(**overrides):
    values = {
        "environment": "test",
        "max_request_bytes": 1024,
        "docs_enabled": False,
        "cors_origins": (),
        "allowed_hosts": (),
    }
    values.update(overrides)
    return HardeningSettings(**values)


def _client(settings: HardeningSettings) -> TestClient:
    app = FastAPI(docs_url=None, openapi_url=None)

    @app.get("/ping")
    def ping():
        return {"ok": True}

    @app.post("/body")
    async def body(request: Request):
        payload = await request.body()
        return {"size": len(payload)}

    configure_hardening(app, settings)
    return TestClient(app)


def test_production_defaults_disable_docs_and_wildcard_cors(monkeypatch):
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.delenv("ECHO_ENV", raising=False)
    monkeypatch.delenv("ECHO_ENABLE_DOCS", raising=False)
    monkeypatch.setenv(
        "ECHO_CORS_ORIGINS",
        "*,https://console.example,https://console.example,invalid-origin",
    )

    settings = get_hardening_settings()

    assert settings.production is True
    assert settings.docs_enabled is False
    assert settings.cors_origins == ("https://console.example",)


def test_docs_can_be_explicitly_enabled_for_controlled_debugging(monkeypatch):
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("ECHO_ENABLE_DOCS", "true")

    assert get_hardening_settings().docs_enabled is True


def test_security_headers_and_valid_request_id_are_applied():
    response = _client(_settings()).get(
        "/ping",
        headers={"X-Request-ID": "echo-test-123"},
    )

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "echo-test-123"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["cross-origin-opener-policy"] == "same-origin"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


def test_untrusted_request_id_is_replaced():
    response = _client(_settings()).get(
        "/ping",
        headers={"X-Request-ID": "bad request id\nspoofed"},
    )

    assert response.status_code == 200
    assert response.headers["x-request-id"] != "bad request id\nspoofed"
    assert len(response.headers["x-request-id"]) == 32


def test_declared_oversized_body_is_rejected_before_route_execution():
    response = _client(_settings(max_request_bytes=1024)).post(
        "/body",
        content=b"x" * 1025,
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}
    assert response.headers["x-content-type-options"] == "nosniff"


def test_streamed_oversized_body_is_rejected():
    def chunks():
        yield b"x" * 700
        yield b"y" * 700

    response = _client(_settings(max_request_bytes=1024)).post(
        "/body",
        content=chunks(),
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}


def test_explicit_cors_origin_is_allowed_and_unknown_origin_is_not():
    client = _client(
        _settings(cors_origins=("https://console.example",))
    )

    allowed = client.options(
        "/ping",
        headers={
            "Origin": "https://console.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    denied = client.options(
        "/ping",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "https://console.example"
    assert denied.status_code == 400
    assert "access-control-allow-origin" not in denied.headers


def test_trusted_host_validation_is_opt_in_and_enforced():
    client = _client(_settings(allowed_hosts=("echo.example",)))

    allowed = client.get("/ping", headers={"Host": "echo.example"})
    denied = client.get("/ping", headers={"Host": "evil.example"})

    assert allowed.status_code == 200
    assert denied.status_code == 400


def test_runtime_status_contains_no_origin_or_host_values():
    status = hardening_runtime_status(
        _settings(
            environment="production",
            cors_origins=("https://private.example",),
            allowed_hosts=("echo.example",),
        )
    )

    assert status["cors_enabled"] is True
    assert status["cors_origin_count"] == 1
    assert status["trusted_hosts_enabled"] is True
    assert "https://private.example" not in str(status)
    assert "echo.example" not in str(status)
