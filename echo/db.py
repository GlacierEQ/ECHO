"""Database persistence for ECHO with managed Postgres and SQLite fallback."""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import sessionmaker

from echo.models import Base

LOCAL_DEFAULT_DB = Path("echo_data/echo.db")
VERCEL_DEFAULT_DB = Path("/tmp/echo.db")
DATABASE_URL_ENV_KEYS = (
    "ECHO_DATABASE_URL",
    "DATABASE_URL",
    "POSTGRES_URL",
    "POSTGRES_URL_NON_POOLING",
    "SUPABASE_DB_URL",
)
_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def resolve_db_path(db_path: Path | str | None = None) -> Path:
    """Resolve the SQLite fallback path for the current runtime."""
    if db_path is not None:
        return Path(db_path)
    configured = os.environ.get("ECHO_DB", "").strip()
    if configured and "://" not in configured:
        return Path(configured)
    if os.environ.get("VERCEL"):
        return VERCEL_DEFAULT_DB
    return LOCAL_DEFAULT_DB


def normalize_database_url(value: str) -> str:
    """Normalize Postgres URLs onto the psycopg 3 SQLAlchemy driver."""
    value = value.strip()
    if value.startswith("postgres://"):
        return "postgresql+psycopg://" + value.removeprefix("postgres://")
    if value.startswith("postgresql://"):
        return "postgresql+psycopg://" + value.removeprefix("postgresql://")
    return value


def resolve_database_url(db_path: Path | str | None = None) -> str:
    """Resolve the active database URL without logging credentials.

    Explicit function arguments win. Managed database variables are checked in
    a deterministic order, followed by the legacy ``ECHO_DB`` path and the
    runtime-specific SQLite fallback.
    """
    if db_path is not None:
        raw = str(db_path).strip()
        if "://" in raw:
            return normalize_database_url(raw)
        return f"sqlite:///{Path(raw)}"

    for key in DATABASE_URL_ENV_KEYS:
        configured = os.environ.get(key, "").strip()
        if configured:
            return normalize_database_url(configured)

    legacy = os.environ.get("ECHO_DB", "").strip()
    if legacy and "://" in legacy:
        return normalize_database_url(legacy)
    return f"sqlite:///{resolve_db_path()}"


def resolve_postgres_schema() -> str:
    """Return a validated isolated schema name for ECHO tables."""
    schema = os.environ.get("ECHO_DB_SCHEMA", "echo").strip() or "echo"
    if not _SCHEMA_RE.fullmatch(schema):
        raise ValueError("ECHO_DB_SCHEMA must be a valid PostgreSQL identifier")
    return schema


def get_engine(db_path: Path | str | None = None) -> Engine:
    url = resolve_database_url(db_path)
    parsed = make_url(url)
    backend = parsed.get_backend_name()

    if backend == "sqlite":
        path = Path(parsed.database or resolve_db_path())
        path.parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(
            url,
            connect_args={"check_same_thread": False},
            echo=False,
        )

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        return engine

    if backend != "postgresql":
        raise ValueError(f"unsupported database backend: {backend}")

    schema = resolve_postgres_schema()
    return create_engine(
        url,
        connect_args={
            "connect_timeout": 5,
            "options": f"-csearch_path={schema},public",
        },
        pool_pre_ping=True,
        pool_recycle=300,
        echo=False,
    )


def _ensure_job_lease_columns(engine: Engine) -> None:
    """Add lease columns when upgrading an existing pre-lease database."""
    columns = {
        item["name"] for item in inspect(engine).get_columns("orchestration_jobs")
    }
    missing = {
        "lease_owner": "VARCHAR(255) NOT NULL DEFAULT ''",
        "lease_epoch": "INTEGER NOT NULL DEFAULT 0",
        "lease_expires_at": (
            "TIMESTAMP WITH TIME ZONE NULL"
            if engine.dialect.name == "postgresql"
            else "TIMESTAMP NULL"
        ),
    }
    with engine.begin() as connection:
        for name, definition in missing.items():
            if name not in columns:
                connection.execute(
                    text(
                        f"ALTER TABLE orchestration_jobs ADD COLUMN {name} {definition}"
                    )
                )


def init_db(db_path: Path | str | None = None) -> Engine:
    engine = get_engine(db_path)
    if engine.dialect.name == "postgresql":
        schema = resolve_postgres_schema()
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    Base.metadata.create_all(engine)
    _ensure_job_lease_columns(engine)
    return engine


def database_runtime_status(engine: Engine | None) -> dict[str, Any]:
    """Expose non-secret database state for health and rollout checks."""
    if engine is None:
        return {"backend": "uninitialized", "durable": False, "schema": ""}
    backend = engine.dialect.name
    return {
        "backend": backend,
        "durable": backend == "postgresql",
        "schema": resolve_postgres_schema() if backend == "postgresql" else "",
    }


SessionLocal = sessionmaker(autocommit=False, autoflush=False)


@contextmanager
def get_session(engine: Engine | None = None):
    if engine is None:
        engine = get_engine()
    SessionLocal.configure(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
