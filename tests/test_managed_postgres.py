"""Regression tests for managed Postgres selection and isolation."""

import pytest

from echo.db import (
    database_runtime_status,
    get_engine,
    normalize_database_url,
    resolve_database_url,
    resolve_postgres_schema,
)


def _clear_database_urls(monkeypatch):
    for key in (
        "ECHO_DATABASE_URL",
        "DATABASE_URL",
        "POSTGRES_URL",
        "POSTGRES_URL_NON_POOLING",
        "SUPABASE_DB_URL",
        "ECHO_DB",
    ):
        monkeypatch.delenv(key, raising=False)


def test_postgres_url_is_normalized_to_psycopg3():
    assert normalize_database_url("postgres://user:pass@db.example/echo") == (
        "postgresql+psycopg://user:pass@db.example/echo"
    )
    assert normalize_database_url("postgresql://user:pass@db.example/echo") == (
        "postgresql+psycopg://user:pass@db.example/echo"
    )


def test_echo_database_url_has_highest_environment_priority(monkeypatch):
    _clear_database_urls(monkeypatch)
    monkeypatch.setenv("POSTGRES_URL", "postgres://lower:priority@db/echo")
    monkeypatch.setenv("ECHO_DATABASE_URL", "postgres://echo:priority@db/echo")
    assert resolve_database_url() == "postgresql+psycopg://echo:priority@db/echo"


def test_postgres_engine_uses_psycopg_and_isolated_schema(monkeypatch):
    _clear_database_urls(monkeypatch)
    monkeypatch.setenv("ECHO_DB_SCHEMA", "echo_test")
    engine = get_engine("postgres://user:pass@localhost:5432/echo")
    assert engine.dialect.name == "postgresql"
    assert engine.url.drivername == "postgresql+psycopg"
    assert resolve_postgres_schema() == "echo_test"
    assert database_runtime_status(engine) == {
        "backend": "postgresql",
        "durable": True,
        "schema": "echo_test",
    }
    engine.dispose()


def test_invalid_schema_is_rejected(monkeypatch):
    monkeypatch.setenv("ECHO_DB_SCHEMA", "echo;drop schema public")
    with pytest.raises(ValueError, match="valid PostgreSQL identifier"):
        resolve_postgres_schema()


def test_sqlite_remains_the_safe_default(monkeypatch, tmp_path):
    _clear_database_urls(monkeypatch)
    engine = get_engine(tmp_path / "fallback.db")
    assert database_runtime_status(engine) == {
        "backend": "sqlite",
        "durable": False,
        "schema": "",
    }
    engine.dispose()
