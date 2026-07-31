"""SQLite persistence layer for ECHO."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from echo.models import Base

LOCAL_DEFAULT_DB = Path("echo_data/echo.db")
VERCEL_DEFAULT_DB = Path("/tmp/echo.db")


def resolve_db_path(db_path: Path | str | None = None) -> Path:
    """Resolve a writable database path for the current runtime.

    Explicit arguments and ``ECHO_DB`` always win. Vercel Functions expose a
    read-only application filesystem, so their safe SQLite fallback is /tmp.
    """
    if db_path is not None:
        return Path(db_path)
    configured = os.environ.get("ECHO_DB", "").strip()
    if configured:
        return Path(configured)
    if os.environ.get("VERCEL"):
        return VERCEL_DEFAULT_DB
    return LOCAL_DEFAULT_DB


def get_engine(db_path: Path | str | None = None):
    path = resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite:///{path}"
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


def init_db(db_path=None):
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    return engine


SessionLocal = sessionmaker(autocommit=False, autoflush=False)


@contextmanager
def get_session(engine=None):
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
