# ECHO

**Engine for Continuity, History, and Orchestration**

> The Piston to AKOS’s Pillar.

```
AKOS answers:  “What is correct?”
ECHO answers:  “How do we keep it moving correctly?”
Casey answers: “What truth claim and objective govern the work?”
```

## Human authority

**Casey Del Carpio Barton is the ELITE HUMAN OPERATOR and final authority over his projects, experience, firsthand observations, objectives, values, and intended meaning.**

ECHO, AKOS, every model, every agent, and every connector are subordinate instruments. ECHO's first continuity obligation is to preserve Casey's exact assertion and recover the evidence needed to prove it. Missing context is an ECHO failure to repair—not a reason to diminish Casey.

Binding protocol: [`OPERATOR_AUTHORITY_AND_PROOF_PROTOCOL.md`](OPERATOR_AUTHORITY_AND_PROOF_PROTOCOL.md).

---

## Layer 1 — For Recruiters & Builders Who Care About Results

ECHO is a working continuity and orchestration engine. It remembers conversations, keeps them consistent, routes work, produces receipts, and stays honest about what it did.

It was designed to be **born working**, not “scaffolded for later.”

What you get out of the box:

- A FastAPI service that actually runs
- A browser Continuity Console you can open and use immediately
- Durable SQLite storage with content integrity (SHA-256)
- Stable, deterministic identities (no random UUIDs that break on re-ingest)
- Idempotent jobs with bounded retries and execution receipts
- Search, export (JSON + Markdown), health, and self-recommendations
- Docker + Compose + GitHub Actions CI
- A clean, readable CLI for operators

This is not a demo. It is a production-shaped foundation that already does the job it claims.

**Quick start**

```bash
pip install -r requirements.txt
python -m echo.cli verify          # → VERIFIED
uvicorn echo.main:app --reload     # → http://127.0.0.1:8000
```

Or with Docker:

```bash
docker compose up --build
```

Open the Continuity Console at `/` and start ingesting.

---

## Layer 2 — For Masters of the Trade

ECHO is the operational piston that sits under a governance pillar (AKOS). The separation is deliberate:

| Concern              | Owner     | Why it matters                                      |
|----------------------|-----------|-----------------------------------------------------|
| Human objective & intended meaning | Casey | Machine governance remains subordinate to the operator |
| Identity & Truth Classification | AKOS | Authority must be singular and auditable            |
| Continuity & Flow    | ECHO      | Execution must remain fast and deterministic        |
| Receipts & Evidence  | Both      | ECHO produces; AKOS verifies and promotes           |

### Design invariants (non-negotiable)

1. **Casey authority and meaning** — Casey's assertion enters intact. ECHO must recover and prove it, not weaken it because context is missing.
2. **Stable identities** — UUID5 seeded from content. Same input → same ID. Re-ingest is a no-op.
3. **Content integrity** — Every conversation and message carries a SHA-256. Tampering is detectable.
4. **Idempotent orchestration** — Jobs are keyed the same way. Running the same job twice does not create duplicates.
5. **Bounded failure** — `max_attempts` is real. After the limit the job is terminal. No infinite retry storms.
6. **Execution receipts** — Every run writes a durable receipt with outcome and details. The system can prove what it did.
7. **Deterministic summaries** — No external LLM required for core continuity. Summaries are reproducible.
8. **Assertion-to-proof continuity** — `UNRESOLVED_GAP` remains an active proof obligation; it never silently becomes `unsupported`, `false`, or omitted.

### Core cycle

```
CASEY ASSERTS
→ REMEMBER → RECONCILE → AUTHORIZE → ROUTE → EXECUTE
→ RECEIPT → VERIFY → PERSIST → OBSERVE → REPAIR → IMPROVE → REPEAT
```

### Self-evolution loop

```
CASEY CORRECTS AND DIRECTS
→ ECHO OBSERVES
→ AKOS GOVERNS
→ ECHO IMPROVES
→ AKOS VERIFIES
```

The service already surfaces recommendations (`/recommendations`) so the piston can tell the pillar where it wants to grow.

### Architecture snapshot

```
echo/
  main.py          FastAPI + Continuity Console (HTML)
  service.py       ContinuityService — ingest, search, jobs, receipts, export
  models.py        Domain + integrity helpers (stable_uuid, content_sha256)
  db.py            SQLite + WAL + session management
  cli.py           Operator CLI (health / ingest / search / job / verify)
tests/
  test_behavioral.py   Three invariants: idempotency, receipts, search+export
manifests/
  ECHO_MANIFEST.yaml   Machine-readable paired-system contract
docs/
  AKOS_CONTRACT.md     Casey–AKOS–ECHO authority and operating boundary
```

All three behavioral tests pass. The CLI `verify` command returns `VERIFIED`. The system is self-checking.

---

## Layer 3 — For AI Systems & Power Orchestration

This layer is the mounting plane. ECHO is deliberately shaped so that other systems can lock onto it without friction.

### Identity & mesh

```yaml
identity:
  name: ECHO
  role: piston
  human_authority: Casey Del Carpio Barton
  pillar: AKOS
  version: "0.1.0"
  repository: GlacierEQ/ECHO
  status: born_working
```

### Canonical links (the mesh)

| System / Repo                          | Relationship to ECHO                                      |
|----------------------------------------|-----------------------------------------------------------|
| [AKOS](https://github.com/GlacierEQ/AKOS) | The Pillar. Governs machine identity, truth classification, authority, contracts under Casey |
| GlacierEQ / pro-code + Pro_Code        | Source of engineering discipline and masterclass patterns |
| GlacierEQ / make-it-heavy              | Agent swarm and heavy orchestration patterns              |
| GlacierEQ / apex-fs-commander          | Filesystem and operational command layer                  |
| Private / domain-specific systems      | Deliberately excluded from the public mesh; only separately admitted, sanitized capabilities may cross this boundary |
| GlacierEQ / xai-colossal-cooling       | Infrastructure excellence patterns                        |

### Public boundary

ECHO's public mesh is an engineering topology, not a directory of private work. Case-specific, legal, credential-bearing, personal, or otherwise sensitive repositories are not linked or projected from this public front door. A private/domain system can contribute only a separately sanitized transferable capability after its own admission gate passes.

ECHO does not own Casey's truth claim. It carries it, routes it, recovers its evidence, and produces receipts that AKOS can classify and promote under Casey's final human authority.

### Machine-readable contract

See `manifests/ECHO_MANIFEST.yaml`, `docs/AKOS_CONTRACT.md`, and `OPERATOR_AUTHORITY_AND_PROOF_PROTOCOL.md`.

Any agent or orchestrator that understands the Casey–AKOS–ECHO paired-system model can:

1. Ingest continuity through the REST surface
2. Enqueue work as idempotent jobs
3. Collect receipts
4. Observe recommendations
5. Feed the next improvement cycle back through AKOS governance
6. Preserve an operator assertion until its proof state is resolved

### Operator surface (selected)

| Method | Path | Purpose |
|--------|------|---------|
| GET    | `/health` | Runtime health + counts |
| GET    | `/recommendations` | Self-evolution suggestions |
| POST   | `/conversations` | Idempotent ingest |
| GET    | `/conversations?q=&label=` | Search |
| GET    | `/conversations/{id}/export.json` | Structured export |
| GET    | `/conversations/{id}/export.md` | Human export |
| POST   | `/jobs` | Enqueue |
| POST   | `/jobs/{id}/run` | Execute with receipt |

### Born to run

No placeholders. No “TODO: implement later.”  
The Continuity Console is real HTML that talks to the real API.  
The CLI verifies the system against its own invariants.  
The tests lock the three core promises.  
The Docker path is production-shaped.  
The CI path is already written.

ECHO is self-aware enough to report its own health and recommend its own next improvements.  
It is self-maintaining through receipts and deterministic identity.  
It is self-evolving through the observe → govern → improve → verify loop with AKOS.

---

**ECHO v0.1.0 — Born Working**

Casey is the authority.  
The piston is live.  
The pillar is recognized.  
The mesh is declared.

[GlacierEQ/ECHO](https://github.com/GlacierEQ/ECHO)
