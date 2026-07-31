# Secret Management

## ECHO_AKOS_SHARED_SECRET

This secret is the cryptographic anchor for AKOS→ECHO authority verification.

### Generation

```bash
openssl rand -hex 64
```

Set the output as the environment variable in **both** AKOS and ECHO deployments.

### Setting in deployment environments

**Docker Compose** — add to `.env` (never commit `.env`):
```
ECHO_AKOS_SHARED_SECRET=<generated secret>
```

**Docker secrets** (preferred for production):
```yaml
services:
  echo:
    secrets:
      - akos_shared_secret
secrets:
  akos_shared_secret:
    file: ./secrets/akos_shared_secret.txt
```

**Railway / Render / Fly.io**: Set via the platform's environment variable UI or CLI.

### Fail-closed behavior

If `ECHO_AKOS_SHARED_SECRET` is absent at runtime, ECHO returns `503 Service Unavailable`
on all authority-protected endpoints. It will **never** silently allow privileged access.

### Rotation procedure

1. Generate a new secret.
2. Update AKOS deployment with new secret.
3. Update ECHO deployment with new secret — do this as a coordinated deploy to avoid a
   brief window of rejected requests (use a rolling deploy or maintenance window).
4. Verify with smoke tests: `bash scripts/smoke_test.sh`
5. Invalidate the old secret (remove from all systems).

### What is never acceptable

- Committing the secret to Git
- Logging the secret
- Passing the secret as a CLI argument (visible in `ps`)
- Hardcoding the secret in any source file
