# ECHO — Engine for Continuity, History, and Orchestration

**The Piston to AKOS's Pillar.**

```
AKOS answers: "What is correct?"
ECHO answers:  "How do we keep it moving correctly?"
```

## Status — v0.1.0 Born Working

| Check | Result |
|-------|--------|
| Behavioral tests | 3 collected · 3 passed |
| Python compilation | passed |
| Verification receipt | **VERIFIED** |
| Repository | [GlacierEQ/ECHO](https://github.com/GlacierEQ/ECHO) (public) |

## What it is

ECHO is the operational piston paired with the AKOS governance pillar. It ensures the right work happens in the right place, at the right time, with the right context.

### Core responsibilities

- **Orchestration** → routing execution to the right system
- **Continuity** → carrying history forward into future decisions
- **Context** → maintaining state and relevance across calls
- **Synchronization** → keeping truth consistent across platforms
- **Recall** → surfacing the right memory at the right moment
- **Execution Flow** → timing, sequencing, delegation

## Architecture relationship

```
AKOS                          ECHO
The Pillar (Governance)       The Piston (Operation)
identity                      continuity
truth                         history
provenance                    normalization
authority                     synchronization
contracts                     recall
evidence                      routing
maturity                      execution flow
promotion                     retries
completion truth              operational receipts
```

Canonical cycle:

```
REMEMBER → RECONCILE → AUTHORIZE → ROUTE → EXECUTE
→ RECEIPT → VERIFY → PERSIST → OBSERVE → REPAIR → IMPROVE → REPEAT
```

Self-evolution:

```
ECHO OBSERVES → AKOS GOVERNS → ECHO IMPROVES → AKOS VERIFIES
```

## Quick start

```bash
# local
pip install -r requirements.txt
python -m echo.cli verify          # → VERIFIED
uvicorn echo.main:app --reload     # → http://127.0.0.1:8000

# docker
docker compose up --build
```

Browser Continuity Console lives at `/` and `/console`.

## API surface (selected)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Runtime health + counts |
| GET | `/recommendations` | Self-evolution suggestions |
| POST | `/conversations` | Ingest (idempotent) |
| GET | `/conversations?q=&label=` | Search |
| GET | `/conversations/{id}/export.json` | JSON export |
| GET | `/conversations/{id}/export.md` | Markdown export |
| POST | `/jobs` | Enqueue orchestration job |
| POST | `/jobs/{id}/run` | Execute (bounded retries + receipt) |

## Design invariants

1. **Stable identities** — UUID5 from content seed (idempotent ingest & jobs)
2. **Content integrity** — SHA-256 on every conversation and message
3. **Receipts** — every job execution produces a durable receipt
4. **Bounded retries** — max_attempts hard limit, failure state explicit
5. **Deterministic summaries** — no LLM required for core continuity

## Files

```
echo/           # core package
  main.py       # FastAPI + Continuity Console
  service.py    # Continuity + orchestration engine
  models.py     # domain + integrity helpers
  db.py         # SQLite + WAL
  cli.py        # operator CLI
tests/          # 3 behavioral tests
manifests/      # ECHO_MANIFEST.yaml
docs/           # AKOS_CONTRACT.md
Dockerfile + docker-compose.yml
.github/workflows/ci.yml
```

## AKOS integration

The pillar–piston contract and machine-readable manifest are live on AKOS `main`.  
ECHO is a first-class paired system: separately deployable, independently testable, operationally joined through authority envelopes, contracts, receipts, and verification.

---

*ECHO v0.1.0 — Born Working*
