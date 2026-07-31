"""ECHO FastAPI application — Continuity Console + REST API."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from echo import __version__
from echo.db import get_session, init_db
from echo.models import ConversationIn, JobIn
from echo.service import ContinuityService

ENGINE = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ENGINE
    ENGINE = init_db()
    yield


app = FastAPI(
    title="ECHO",
    description="Engine for Continuity, History, and Orchestration — The Piston to AKOS",
    version=__version__,
    lifespan=lifespan,
)

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


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


@app.post("/conversations", status_code=201)
def create_conversation(body: ConversationIn):
    with get_session(ENGINE) as session:
        return ContinuityService(session).ingest_conversation(body)


@app.get("/conversations")
def list_conversations(
    q: str = Query("", description="Search titles, participants, summary"),
    label: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    with get_session(ENGINE) as session:
        return ContinuityService(session).search(q=q, label=label, limit=limit)


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
        s = ContinuityService(session)
        if not s.get_conversation(conv_id):
            raise HTTPException(404, "conversation not found")
        return s.list_messages(conv_id)


@app.get("/conversations/{conv_id}/export.json")
def export_json(conv_id: str):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_json(conv_id)
        except ValueError:
            raise HTTPException(404, "conversation not found")


@app.get("/conversations/{conv_id}/export.md", response_class=PlainTextResponse)
def export_md(conv_id: str):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).export_markdown(conv_id)
        except ValueError:
            raise HTTPException(404, "conversation not found")


@app.post("/jobs", status_code=201)
def enqueue_job(body: JobIn):
    with get_session(ENGINE) as session:
        return ContinuityService(session).enqueue_job(body)


@app.post("/jobs/{job_id}/run")
def run_job(job_id: str):
    with get_session(ENGINE) as session:
        try:
            return ContinuityService(session).run_job(job_id)
        except ValueError as e:
            raise HTTPException(404, str(e))


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with get_session(ENGINE) as session:
        from echo.models import JobORM
        job = session.get(JobORM, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return ContinuityService(session)._job_out(job)


CONSOLE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>ECHO Continuity Console</title>
  <style>
    :root { --bg:#0d1117; --fg:#e6edf3; --muted:#8b949e; --accent:#58a6ff; --card:#161b22; --border:#30363d; }
    * { box-sizing: border-box; }
    body { margin:0; font-family: ui-sans-serif, system-ui, sans-serif; background:var(--bg); color:var(--fg); }
    header { padding:1rem 1.5rem; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:1rem; }
    header h1 { margin:0; font-size:1.25rem; letter-spacing:0.05em; }
    header .tag { font-size:0.7rem; background:var(--accent); color:#000; padding:0.15rem 0.5rem; border-radius:999px; font-weight:600; }
    main { max-width:960px; margin:0 auto; padding:1.5rem; }
    .card { background:var(--card); border:1px solid var(--border); border-radius:8px; padding:1rem; margin-bottom:1rem; }
    label { display:block; font-size:0.8rem; color:var(--muted); margin-bottom:0.25rem; }
    input, textarea { width:100%; background:#0d1117; border:1px solid var(--border); color:var(--fg); padding:0.5rem; border-radius:6px; margin-bottom:0.75rem; }
    button { background:var(--accent); color:#000; border:none; padding:0.5rem 1rem; border-radius:6px; font-weight:600; cursor:pointer; }
    pre { background:#0d1117; padding:0.75rem; border-radius:6px; overflow:auto; font-size:0.85rem; }
    .row { display:flex; gap:0.75rem; flex-wrap:wrap; }
    .row > * { flex:1; min-width:140px; }
    .muted { color:var(--muted); font-size:0.85rem; }
  </style>
</head>
<body>
  <header>
    <h1>ECHO</h1>
    <span class="tag">v0.1.0 · Piston</span>
    <span class="muted" id="health">…</span>
  </header>
  <main>
    <div class="card">
      <h2 style="margin-top:0">Ingest Conversation</h2>
      <label>Title</label>
      <input id="title" value="First Continuity Test"/>
      <label>Participants (comma)</label>
      <input id="participants" value="operator, echo"/>
      <label>Labels (comma)</label>
      <input id="labels" value="test, v0.1"/>
      <label>Message (role: content)</label>
      <textarea id="content" rows="3">user: Hello ECHO, this is the first continuity pulse.\nassistant: Acknowledged. Continuity established. Hash will be recorded.</textarea>
      <button onclick="ingest()">Ingest</button>
    </div>
    <div class="card">
      <h2 style="margin-top:0">Search</h2>
      <div class="row">
        <div><label>Query</label><input id="q" placeholder="search…"/></div>
        <div><label>Label</label><input id="label" placeholder="optional"/></div>
      </div>
      <button onclick="search()">Search</button>
    </div>
    <div class="card">
      <h2 style="margin-top:0">Output</h2>
      <pre id="out">Ready.</pre>
    </div>
  </main>
  <script>
    const out = (o) => document.getElementById('out').textContent = typeof o === 'string' ? o : JSON.stringify(o, null, 2);
    async function health() {
      const r = await fetch('/health');
      const j = await r.json();
      document.getElementById('health').textContent = `${j.status} · ${j.conversations} conv · ${j.jobs} jobs · uptime ${j.uptime_seconds}s`;
    }
    async function ingest() {
      const title = document.getElementById('title').value;
      const participants = document.getElementById('participants').value.split(',').map(s=>s.trim()).filter(Boolean);
      const labels = document.getElementById('labels').value.split(',').map(s=>s.trim()).filter(Boolean);
      const raw = document.getElementById('content').value.trim();
      const messages = raw.split('\\n').filter(Boolean).map(line => {
        const i = line.indexOf(':');
        if (i === -1) return {role:'user', content:line};
        return {role: line.slice(0,i).trim(), content: line.slice(i+1).trim()};
      });
      const r = await fetch('/conversations', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({title, participants, labels, messages})
      });
      out(await r.json());
      health();
    }
    async function search() {
      const q = document.getElementById('q').value;
      const label = document.getElementById('label').value;
      const params = new URLSearchParams({q, limit:20});
      if (label) params.set('label', label);
      const r = await fetch('/conversations?' + params);
      out(await r.json());
    }
    health();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def console():
    return CONSOLE_HTML


@app.get("/console", response_class=HTMLResponse)
def console_alias():
    return CONSOLE_HTML
