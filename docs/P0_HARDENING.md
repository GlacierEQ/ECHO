# ECHO P0 Governed-Piston Hardening

This hardening pass converts the initial open prototype into a fail-closed continuity and orchestration core.

## Enforced invariants

1. Privileged API routes require a signed AKOS authority envelope.
2. Conversation identity derives from `(source, external_id)`, not mutable content.
3. Re-ingestion updates canonical state when content changes.
4. Canonical integrity covers provenance, title, participants, labels, metadata, and messages.
5. Integrity verification recomputes hashes and quarantines mismatches.
6. Message and receipt ownership is database constrained.
7. Jobs require caller-provided idempotency keys.
8. Unsupported job capabilities fail closed.
9. Receipts chain attempts through `previous_hash`.
10. CLI verification runs against an isolated temporary database.

## Authority envelope

Required headers:

- `X-AKOS-Actor`
- `X-AKOS-Scope`
- `X-AKOS-Timestamp`
- `X-AKOS-Nonce`
- `X-AKOS-Signature`

The signature is HMAC-SHA256 over:

```text
actor\nscope\ntimestamp\nnonce
```

The shared verification secret is supplied through `ECHO_AKOS_SHARED_SECRET`. A missing secret blocks privileged operations rather than silently degrading to open access.

## Supported v0.2 orchestration capabilities

- `echo.ping`
- `echo.summarize`
- `echo.integrity.verify`

Additional capabilities must be explicitly registered, implemented, tested, and governed before they can return success.

## Migration warning

The schema adds provenance, integrity, foreign-key, uniqueness, authority, and receipt-chain fields. Existing v0.1 SQLite stores require an explicit migration or a controlled export/rebuild before this branch is promoted to production.
