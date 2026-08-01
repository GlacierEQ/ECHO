# ECHO Runtime Hardening

This layer reduces exposure without changing the staged OIDC enforcement mode.
Authentication remains independently controlled by `ECHO_AUTH_MODE`.

## Production defaults

When `VERCEL_ENV=production` or `ECHO_ENV=production`:

- `/docs` and `/openapi.json` are disabled.
- Responses use `Cache-Control: no-store`.
- CSP, framing, content-type, referrer, permissions, cross-origin, and HSTS
  headers are applied in the application and reinforced at the Vercel edge.
- Request bodies larger than 4 MiB are rejected with HTTP 413 before route
  execution. Streamed bodies are counted as they are received.
- Browser CORS access is disabled unless explicit origins are configured.
- Wildcard CORS is ignored rather than accepted.
- A bounded `X-Request-ID` is returned on every response. Invalid caller IDs
  are replaced and are never reflected.
- Public `/health` omits conversation, message, job, and receipt counts.
  Detailed counts remain on the scoped `/stats` endpoint.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `ECHO_ENV` | Explicit runtime environment | `VERCEL_ENV`, then runtime detection |
| `ECHO_ENABLE_DOCS` | Controlled production docs override | disabled in production |
| `ECHO_MAX_REQUEST_BYTES` | Body-size ceiling, clamped to 1 KiB–64 MiB | 4 MiB |
| `ECHO_CORS_ORIGINS` | Comma-separated explicit HTTP(S) origins | none |
| `ECHO_ALLOWED_HOSTS` | Optional comma-separated Host allowlist | disabled |

Do not use `*` for CORS. The parser intentionally discards it.

## Database boundary

The Supabase `echo` schema is isolated from `public` and hardened as follows:

- only the PostgreSQL `postgres` login has schema and table privileges;
- `PUBLIC`, `anon`, `authenticated`, and `service_role` have no privileges;
- RLS is enabled and forced on all four ECHO tables;
- future tables, sequences, and functions inherit deny-by-default privileges;
- the planned direct pooled PostgreSQL connection uses the `postgres` login,
  which has `BYPASSRLS` in this Supabase project;
- production remains on SQLite fallback until `ECHO_DATABASE_URL` is attached.

## Rollback

The HTTP controls are isolated in `echo/hardening.py`. Reverting the hardening
commit restores the prior runtime behavior. Individual compatibility switches:

- set `ECHO_ENABLE_DOCS=true` to temporarily restore docs;
- increase `ECHO_MAX_REQUEST_BYTES` for a bounded ingestion exception;
- set explicit CORS origins rather than weakening the policy globally;
- leave `ECHO_ALLOWED_HOSTS` unset if a host allowlist causes routing issues.

Authentication enforcement and managed-database activation remain separate
rollout gates and must not be coupled to this hardening deployment.
