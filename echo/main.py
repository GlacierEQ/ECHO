"""ECHO FastAPI application — continuity console and REST API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from sqlalchemy.engine import Engine

from echo import __version__
from echo.db import database_runtime_status, get_session, init_db
from echo.hardening import (
    configure_hardening,
    get_hardening_settings,
    hardening_runtime_status,
)
from echo.models import ConversationIn, JobIn, JobORM
from echo.security import (
    AuthContext,
    auth_dependency,
    auth_runtime_status,
    get_auth_settings,
    provenance,
)
from echo.service import ContinuityService, JobLeaseConflictError
from echo.trust import trust_loop_report

ENGINE: Engine | None = None
AuthRead = Annotated[AuthContext, Depends(auth_dependency("echo:read"))]
AuthWrite = Annotated[
    AuthContext,
    Depends(auth_dependency("echo:write", write_operation=True)),
]
AuthVerify = Annotated[
    AuthContext,
    Depends(auth_dependency("echo:verify", write_operation=True)),
]
AuthExecute = Annotated[
    AuthContext,
    Depends(auth_dependency("echo:execute", write_operation=True)),
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ENGINE
    if ENGINE is None:
        ENGINE = init_db()
    yield


_IMPORT_AUTH_SETTINGS = get_auth_settings()
_HARDENING_SETTINGS = get_hardening_settings()
_DOCS_DISABLED = (
    _IMPORT_AUTH_SETTINGS.mode == "enforce-all" or not _HARDENING_SETTINGS.docs_enabled
)
app = FastAPI(
    title="ECHO",
    description=(
        "Engine for Continuity, History, and Orchestration — "
        "staged OIDC continuity service"
    ),
    version=__version__,
    lifespan=lifespan,
    docs_url=None if _DOCS_DISABLED else "/docs",
    redoc_url=None,
    openapi_url=None if _DOCS_DISABLED else "/openapi.json",
)
configure_hardening(app, _HARDENING_SETTINGS)


def create_app(engine: Engine | None = None) -> FastAPI:
    """Return the application, optionally bound to an injected test engine."""
    global ENGINE
    if engine is not None:
        ENGINE = engine
    return app


@app.get("/health")
def health():
    """Return a minimal public liveness response without record counts."""
    return {
        "status": "ok",
        "version": __version__,
        "pillar": "AKOS",
        "role": "piston",
        "authority_mode": "staged_oidc",
        "authentication": auth_runtime_status(),
        "database": database_runtime_status(ENGINE),
        "hardening": hardening_runtime_status(_HARDENING_SETTINGS),
    }


@app.get("/stats")
def stats(auth: AuthRead):
    """Return detailed counts only to a valid identity in production."""
    if _HARDENING_SETTINGS.production and not auth.valid:
        raise HTTPException(
            status_code=401,
            detail="valid bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    with get_session(ENGINE) as session:
        service_status = ContinuityService(session).health()
    return {
        **service_status,
        **health(),
    }


@app.get("/recommendations")
def recommendations(_auth: AuthRead):
    with get_session(ENGINE) as session:
        return {"recommendations": ContinuityService(session).recommendations()}


@app.post("/conversations")
def create_conversation(body: ConversationIn, _auth: AuthWrite):
    with get_session(ENGINE) as session:
        return ContinuityService(session).ingest_conversation(body)


@app.get("/conversations")
def list_conversations(
    _auth: AuthRead,
    q: str = Query("", description="Search titles, summaries, and message content"),
    label: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    with get_session(ENGINE) as session:
        results = ContinuityService(session).search(q=q, label=label, limit=limit)
        return {"conversations": results, "count": len(results)}


@app.get("/conversations/{conv_id}")
def get_conversation(conv_id: str, _auth: AuthRead):
    with get_session(ENGINE) as session:
        out = ContinuityService(session).get_conversation(conv_id)
        if not out:
            raise HTTPException(404, "conversation not found")
        return out


@app.get("/conversations/{conv_id}/messages")
def get_messages(conv_id: str, _auth: AuthRead):
    with get_session(ENGINE) as session:
        service = ContinuityService(session)
        if not service.get_conversation(conv_id):
            raise HTTPException(404, "conversation not found")
        return service.list_messages(conv_id)


def _integrity_result(conv_id: str):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).verify_integrity(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.post("/conversations/{conv_id}/integrity")
def verify_integrity(conv_id: str, _auth: AuthVerify):
    return _integrity_result(conv_id)


@app.get("/conversations/{conv_id}/integrity")
def verify_integrity_compat(conv_id: str, _auth: AuthVerify):
    return _integrity_result(conv_id)


@app.get("/conversations/{conv_id}/export.json")
def export_json(conv_id: str, _auth: AuthRead):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_json(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.get("/conversations/{conv_id}/export.md", response_class=PlainTextResponse)
def export_markdown(conv_id: str, _auth: AuthRead):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_markdown(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


def _canonical_job(body: JobIn) -> JobIn:
    aliases = {
        "identity_check": "echo.ping",
        "echo:noop": "echo.ping",
    }
    mapped = aliases.get(body.job_type)
    if not mapped:
        return body
    payload = {**body.payload, "requested_job_type": body.job_type}
    return body.model_copy(update={"job_type": mapped, "payload": payload})


@app.post("/jobs")
def enqueue_job(body: JobIn, auth: AuthExecute):
    actor, scope = provenance(auth)
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).enqueue_job(
                _canonical_job(body),
                actor=actor,
                scope=scope,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc


def _execute_job(job_id: str, worker_id: str = "direct"):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).run_job(job_id, worker_id=worker_id)
        except JobLeaseConflictError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.post("/jobs/{job_id}/run")
def run_job(
    job_id: str,
    _auth: AuthExecute,
    worker_id: str = Header("direct", alias="X-ECHO-Worker-ID"),
):
    return _execute_job(job_id, worker_id=worker_id)


@app.post("/jobs/{job_id}/execute")
def execute_job_compat(
    job_id: str,
    _auth: AuthExecute,
    worker_id: str = Header("direct", alias="X-ECHO-Worker-ID"),
):
    return _execute_job(job_id, worker_id=worker_id)


@app.post("/jobs/{job_id}/claim")
def claim_job(
    job_id: str,
    _auth: AuthExecute,
    worker_id: str = Header(..., alias="X-ECHO-Worker-ID"),
):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).claim_job(job_id, worker_id)
        except JobLeaseConflictError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.post("/jobs/{job_id}/heartbeat")
def heartbeat_job(
    job_id: str,
    _auth: AuthExecute,
    worker_id: str = Header(..., alias="X-ECHO-Worker-ID"),
):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).heartbeat_job(job_id, worker_id)
        except JobLeaseConflictError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.get("/jobs/{job_id}")
def get_job(job_id: str, _auth: AuthRead):
    with get_session(ENGINE) as session:
        job = session.get(JobORM, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return ContinuityService(session)._job_out(job)


@app.get("/jobs/{job_id}/portable-receipts")
def portable_receipts(job_id: str, _auth: AuthRead):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).portable_receipts(job_id)
        except ValueError as exc:
            status_code = 404 if str(exc) == "job not found" else 422
            raise HTTPException(status_code, str(exc)) from exc


@app.get("/jobs/{job_id}/trust")
def verify_job_trust(job_id: str, _auth: AuthRead):
    """Verify execution state and receipt-chain integrity."""
    with get_session(ENGINE) as session:
        try:
            return trust_loop_report(session, job_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


_DOCS_CARD = (
    """
<div class="card">
<h2>API documentation</h2>
<p><a href="/docs" style="color:#58a6ff">OpenAPI console</a></p>
</div>
"""
    if not _DOCS_DISABLED
    else """
<div class="card">
<h2>API documentation</h2>
<p>Interactive documentation is disabled in this environment.</p>
</div>
"""
)

CONSOLE_HTML = (
    """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ECHO Continuity Console</title>
<style>
body{font-family:system-ui;background:#0d1117;color:#e6edf3;margin:0}
.shell{max-width:900px;margin:auto;padding:32px}
.card{background:#161b22;border:1px solid #30363d;border-radius:12px;
padding:20px;margin:16px 0}
code{color:#58a6ff}
</style>
</head>
<body>
<div class="shell">
<h1>ECHO</h1>
<p>Engine for Continuity, History, and Orchestration</p>
<div class="card">
<h2>Staged OIDC security</h2>
<p>ECHO verifies RS256 bearer tokens against public signing keys. Shadow mode
observes authentication without blocking traffic; enforcement is enabled only
after production validation.</p>
</div>
<div class="card">
<h2>Runtime hardening</h2>
<p>Security headers, bounded request bodies, no-store responses, and explicit
browser-origin controls are enabled.</p>
</div>
<div class="card">
<h2>No application signing secret</h2>
<p>ECHO never stores the identity provider's private signing key. Validated
subjects and scopes are attached to job receipts for audit provenance.</p>
</div>
"""
    + _DOCS_CARD
    + """
</div>
</body>
</html>"""
)


@app.get("/", response_class=HTMLResponse)
def console():
    return CONSOLE_HTML


@app.get("/console", response_class=HTMLResponse)
def console_alias():
    return CONSOLE_HTML
