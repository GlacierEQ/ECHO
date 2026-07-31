"""ECHO FastAPI application — continuity console and REST API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from sqlalchemy.engine import Engine

from echo import __version__
from echo.db import get_session, init_db
from echo.models import ConversationIn, JobIn, JobORM
from echo.service import ContinuityService
from echo.trust import trust_loop_report

ENGINE: Engine | None = None
DIRECT_ACTOR = "direct-api"
DIRECT_SCOPE = "echo:*"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ENGINE
    if ENGINE is None:
        ENGINE = init_db()
    yield


app = FastAPI(
    title="ECHO",
    description=(
        "Engine for Continuity, History, and Orchestration — "
        "direct-access continuity service"
    ),
    version=__version__,
    lifespan=lifespan,
)


def create_app(engine: Engine | None = None) -> FastAPI:
    """Return the application, optionally bound to an injected test engine."""
    global ENGINE
    if engine is not None:
        ENGINE = engine
    return app


@app.get("/health")
def health():
    with get_session(ENGINE) as session:
        return ContinuityService(session).health()


@app.get("/stats")
def stats():
    return health()


@app.get("/recommendations")
def recommendations():
    with get_session(ENGINE) as session:
        return {"recommendations": ContinuityService(session).recommendations()}


@app.post("/conversations")
def create_conversation(body: ConversationIn):
    with get_session(ENGINE) as session:
        return ContinuityService(session).ingest_conversation(body)


@app.get("/conversations")
def list_conversations(
    q: str = Query("", description="Search titles, summaries, and message content"),
    label: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    with get_session(ENGINE) as session:
        results = ContinuityService(session).search(q=q, label=label, limit=limit)
        return {"conversations": results, "count": len(results)}


@app.get("/conversations/{conv_id}")
def get_conversation(conv_id: str):
    with get_session(ENGINE) as session:
        out = ContinuityService(session).get_conversation(conv_id)
        if not out:
            raise HTTPException(404, "conversation not found")
        return out


@app.get("/conversations/{conv_id}/messages")
def get_messages(conv_id: str):
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
def verify_integrity(conv_id: str):
    return _integrity_result(conv_id)


@app.get("/conversations/{conv_id}/integrity")
def verify_integrity_compat(conv_id: str):
    return _integrity_result(conv_id)


@app.get("/conversations/{conv_id}/export.json")
def export_json(conv_id: str):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_json(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.get("/conversations/{conv_id}/export.md", response_class=PlainTextResponse)
def export_markdown(conv_id: str):
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
def enqueue_job(body: JobIn):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).enqueue_job(
                _canonical_job(body),
                actor=DIRECT_ACTOR,
                scope=DIRECT_SCOPE,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc


def _execute_job(job_id: str):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).run_job(job_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.post("/jobs/{job_id}/run")
def run_job(job_id: str):
    return _execute_job(job_id)


@app.post("/jobs/{job_id}/execute")
def execute_job_compat(job_id: str):
    return _execute_job(job_id)


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with get_session(ENGINE) as session:
        job = session.get(JobORM, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return ContinuityService(session)._job_out(job)


@app.get("/jobs/{job_id}/trust")
def verify_job_trust(job_id: str):
    """Verify execution state and receipt-chain integrity."""
    with get_session(ENGINE) as session:
        try:
            return trust_loop_report(session, job_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


CONSOLE_HTML = """<!doctype html>
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
<h2>Direct access active</h2>
<p>The API no longer requires an AKOS shared secret, signature, or authority
headers. Requests can call continuity, export, verification, and job endpoints
directly.</p>
</div>
<div class="card">
<h2>Audit provenance</h2>
<p>Jobs are recorded with actor <code>direct-api</code> and scope
<code>echo:*</code> so receipt history remains attributable without a key.</p>
</div>
<div class="card">
<h2>API documentation</h2>
<p><a href="/docs" style="color:#58a6ff">OpenAPI console</a></p>
</div>
</div>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def console():
    return CONSOLE_HTML


@app.get("/console", response_class=HTMLResponse)
def console_alias():
    return CONSOLE_HTML
