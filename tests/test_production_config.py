"""Fresh-process smoke test for production-safe application defaults."""

from __future__ import annotations

import json
import os
import subprocess
import sys


def test_production_app_hides_docs_and_record_counts(tmp_path):
    environment = os.environ.copy()
    environment.update(
        {
            "VERCEL": "1",
            "VERCEL_ENV": "production",
            "ECHO_AUTH_MODE": "shadow",
            "ECHO_DB": str(tmp_path / "production-smoke.db"),
        }
    )
    environment.pop("ECHO_ENABLE_DOCS", None)
    environment.pop("ECHO_CORS_ORIGINS", None)
    environment.pop("ECHO_ALLOWED_HOSTS", None)

    script = """
import json
from fastapi.testclient import TestClient
from echo.main import app

with TestClient(app) as client:
    health = client.get('/health')
    docs = client.get('/docs')
    print(json.dumps({
        'health_status': health.status_code,
        'health': health.json(),
        'docs_status': docs.status_code,
        'headers': {
            'cache-control': health.headers.get('cache-control'),
            'content-security-policy': health.headers.get('content-security-policy'),
            'x-content-type-options': health.headers.get('x-content-type-options'),
            'x-frame-options': health.headers.get('x-frame-options'),
            'x-request-id': health.headers.get('x-request-id'),
        },
    }))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    result = json.loads(completed.stdout.strip().splitlines()[-1])

    assert result["health_status"] == 200
    assert result["docs_status"] == 404
    assert result["health"]["hardening"]["environment"] == "production"
    assert result["health"]["hardening"]["docs_enabled"] is False
    assert result["health"]["authentication"]["mode"] == "shadow"
    assert "conversations" not in result["health"]
    assert "messages" not in result["health"]
    assert "jobs" not in result["health"]
    assert "receipts" not in result["health"]
    assert result["headers"]["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in result["headers"]["content-security-policy"]
    assert result["headers"]["x-content-type-options"] == "nosniff"
    assert result["headers"]["x-frame-options"] == "DENY"
    assert len(result["headers"]["x-request-id"]) == 32
