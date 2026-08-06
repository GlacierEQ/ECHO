# Secret and Identity Management

## Primary path (current main)

ECHO's live authority mode is **staged OIDC**.

- Default: `ECHO_AUTH_MODE=shadow`
- Algorithm: RS256 against the identity provider JWKS
- **No application shared secret is required for the primary path**
- Runtime reports `shared_secret_required: false`

Required only when you intentionally enforce OIDC:

```text
ECHO_OIDC_ISSUER=https://your-issuer.example/
ECHO_OIDC_AUDIENCE=https://echo-api
# or
AUTH0_DOMAIN=your-tenant.us.auth0.com
AUTH0_AUDIENCE=https://echo-api
ECHO_AUTH_MODE=shadow   # then enforce-writes, then enforce-all
```

Rollout sequence is documented in `docs/AUTH_ROLLOUT.md`.

## Live verification (no shared secret)

Canonical harness:

```bash
python scripts/live_smoke.py --base-url https://your-echo-host
```

Or trigger the reusable Action:

```text
.github/workflows/live-verify.yml
```

Set repository variable `ECHO_BASE_URL` once. No HMAC key is required.

## Optional advanced: AKOS HMAC authority envelope

`ECHO_AKOS_SHARED_SECRET` remains available for machine-to-machine AKOS
authority envelopes (`echo/auth.py`). It is **not** required for:

- process start
- `/health`
- staged OIDC operation
- `scripts/live_smoke.py`
- the live-verify GitHub Action

If you use the optional envelope:

```bash
openssl rand -hex 64
```

Place it only in the deployment environment (platform env / secret store).
Never commit it. Never log it. Never pass it as a CLI argument visible in `ps`.

Legacy script that still exercises the HMAC path:

```bash
# optional / advanced only
ECHO_URL=... ECHO_AKOS_SHARED_SECRET=... bash scripts/smoke_test.sh
```

## What is never acceptable

- Committing any secret to Git
- Treating the optional HMAC key as a startup requirement
- Leaving residual human secret placement as a blocker for basic live operation
