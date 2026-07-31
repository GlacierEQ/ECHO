"""Regression tests for runtime-specific database path selection."""

from pathlib import Path

from echo.db import LOCAL_DEFAULT_DB, VERCEL_DEFAULT_DB, resolve_db_path


def test_vercel_uses_writable_tmp_path(monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.delenv("ECHO_DB", raising=False)
    assert resolve_db_path() == VERCEL_DEFAULT_DB == Path("/tmp/echo.db")


def test_explicit_database_path_wins_on_vercel(monkeypatch, tmp_path):
    monkeypatch.setenv("VERCEL", "1")
    configured = tmp_path / "configured.db"
    monkeypatch.setenv("ECHO_DB", str(configured))
    assert resolve_db_path() == configured


def test_local_runtime_keeps_durable_relative_default(monkeypatch):
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.delenv("ECHO_DB", raising=False)
    assert resolve_db_path() == LOCAL_DEFAULT_DB == Path("echo_data/echo.db")
