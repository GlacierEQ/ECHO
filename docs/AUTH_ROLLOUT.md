# ECHO OIDC Authentication Rollout

ECHO uses short-lived RS256 access tokens verified against an identity
provider's public JWKS. The application does not store the provider's private
signing key and does not use a shared HMAC secret.

## Configuration

```text
AUTH0_DOMAIN=your-tenant.us.auth0.com
AUTH0_AUDIENCE=https://echo-api
ECHO_AUTH_MODE=shadow
```

Generic OIDC names are also supported:

```text
ECHO_OIDC_ISSUER=https://issuer.example/
ECHO_OIDC_AUDIENCE=https://echo-api
```

`AUTH0_DOMAIN` is normalized to an HTTPS issuer with a trailing slash. Tokens
must use RS256 and contain matching `iss` and `aud` claims plus `sub`, `iat`,
and `exp`.

## Rollout modes

| Mode | Behavior |
|---|---|
| `off` | Authentication observation disabled. |
| `shadow` | Validate and audit tokens when present; never block traffic. This is the default. |
| `enforce-writes` | Require scoped tokens for conversation ingestion, integrity mutation, job creation, and job execution. Reads remain available. |
| `enforce-all` | Require scoped tokens for every operational route except `/health`; interactive API docs are disabled. |

Never move directly from `shadow` to `enforce-all`.

## Scopes

| Capability | Scope |
|---|---|
| Read stats, recommendations, conversations, exports, jobs, and trust reports | `echo:read` |
| Ingest conversations | `echo:write` |
| Verify conversation integrity | `echo:verify` |
| Create and execute jobs | `echo:execute` |
| Administrative wildcard | `echo:*` |

Auth0 can issue scopes through the `scope` claim or permissions through the
`permissions` claim. ECHO accepts both.

## Safe production sequence

1. Deploy with `ECHO_AUTH_MODE=shadow`.
2. Configure `AUTH0_DOMAIN` and `AUTH0_AUDIENCE`.
3. Send valid and invalid test tokens and inspect `echo_auth` runtime events.
4. Confirm `/health` reports `configured: true` and `mode: shadow`.
5. Switch to `enforce-writes` and smoke-test reads, ingestion, and job execution.
6. Observe production errors and authentication events.
7. Switch to `enforce-all` only after every legitimate caller sends a valid token.

Rollback is one environment-variable change back to `shadow`; no source-code
rollback or shared-secret restoration is required.

## Audit behavior

ECHO logs authentication status, method, path, required scope, subject, and an
optional request ID. It never logs the bearer token or full JWT claims. Valid
subjects and granted scopes are recorded on orchestration jobs and receipts.
