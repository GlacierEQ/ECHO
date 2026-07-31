"""ECHO FastAPI application — governed continuity console and REST API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from echo import __version__
from echo.auth import AuthorityContext, require_authority, require_scope
from echo.db import get_session, init_db
from echo.models import ConversationIn, JobIn, JobORM
from echo.service import ContinuityService
from echo.trust import trust_loop_report

ENGINE = None
Authority = Annotated[AuthorityContext, Depends(require_authority)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ENGINE
    ENGINE = init_db()
    yield


app = FastAPI(
    title="ECHO",
    description=(
        "Engine for Continuity, History, and Orchestration — "
        "the governed piston to AKOS"
    ),
    version=__version__,
    lifespan=lifespan,
)


@app.get("/health")
def health():
    with get_session(ENGINE) as session:
        return ContinuityService(session).health()


@app.get("/stats")
def stats(authority: Authority):
    require_scope(authority, "echo:read")
    return health()


@app.get("/recommendations")
def recommendations(authority: Authority):
    require_scope(authority, "echo:read")
    with get_session(ENGINE) as session:
        return {"recommendations": ContinuityService(session).recommendations()}


@app.post("/conversations", status_code=201)
def create_conversation(body: ConversationIn, authority: Authority):
    require_scope(authority, "echo:write")
    with get_session(ENGINE) as session:
        return ContinuityService(session).ingest_conversation(body)


@app.get("/conversations")
def list_conversations(
    authority: Authority,
    q: str = Query("", description="Search titles, summaries, and message content"),
    label: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    require_scope(authority, "echo:read")
    with get_session(ENGINE) as session:
        return ContinuityService(session).search(q=q, label=label, limit=limit)


@app.get("/conversations/{conv_id}")
def get_conversation(conv_id: str, authority: Authority):
    require_scope(authority, "echo:read")
    with get_session(ENGINE) as session:
        out = ContinuityService(session).get_conversation(conv_id)
        if not out:
            raise HTTPException(404, "conversation not found")
        return out


@app.get("/conversations/{conv_id}/messages")
def get_messages(conv_id: str, authority: Authority):
    require_scope(authority, "echo:read")
    with get_session(ENGINE) as session:
        service = ContinuityService(session)
        if not service.get_conversation(conv_id):
            raise HTTPException(404, "conversation not found")
        return service.list_messages(conv_id)


@app.post("/conversations/{conv_id}/integrity")
def verify_integrity(conv_id: str, authority: Authority):
    require_scope(authority, "echo:verify")
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).verify_integrity(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.get("/conversations/{conv_id}/export.json")
def export_json(conv_id: str, authority: Authority):
    require_scope(authority, "echo:export")
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_json(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.get("/conversations/{conv_id}/export.md", response_class=PlainTextResponse)
def export_markdown(conv_id: str, authority: Authority):
    require_scope(authority, "echo:export")
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_markdown(conv_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.post("/jobs", status_code=201)
def enqueue_job(body: JobIn, authority: Authority):
    require_scope(authority, "echo:execute")
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).enqueue_job(
                body, actor=authority.actor, scope=authority.scope
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc


@app.post("/jobs/{job_id}/run")
def run_job(job_id: str, authority: Authority):
    require_scope(authority, "echo:execute")
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).run_job(job_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc


@app.get("/jobs/{job_id}")
def get_job(job_id: str, authority: Authority):
    require_scope(authority, "echo:read")
    with get_session(ENGINE) as session:
        job = session.get(JobORM, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return ContinuityService(session)._job_out(job)


@app.get("/jobs/{job_id}/trust")
def verify_job_trust(job_id: str, authority: Authority):
    """Verify authority attribution, terminal execution, and receipt chain."""
    require_scope(authority, "echo:verify")
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
<title>ECHO Governed Continuity Console</title>
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
<h2>Governed piston active</h2>
<p>Health remains public. Continuity reads, writes, exports, verification,
and execution require a signed AKOS authority envelope.</p>
</div>
<div class="card">
<h2>Required headers</h2>
<code>X-AKOS-Actor · X-AKOS-Scope · X-AKOS-Timestamp · X-AKOS-Nonce ·
X-AKOS-Signature</code>
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
