#!/usr/bin/env python3
"""Exact-bound deploy approval tokens.

Python mirror of the APEX ApprovalVerifier
(``apex-enforcement-kernel/adapters/approval.go``): HMAC-SHA256, canonical
payload, constant-time compare, expiry, binding, nonce.

A token authorizes exactly one deployment: work unit + action + capability
+ route + config fingerprint, signed by a host-held secret, expiring, and
single-use in-process. This replaces the human-click GitHub environment
gate: no click-to-continue, no vague "governance approval" — a signed,
exact-bound authorization, or the deploy refuses (exit 78).

Token JSON schema (exact — the runner, the mint CLI, and the workflow all
depend on it)::

    {
        "request": {
            "work_unit_id": str, "action_id": str, "capability": str,
            "route": str, "config_sha": str,
            "expires_at": str,  # ISO-8601 UTC, "Z"
            "nonce": str,        # 32 hex chars
        },
        "granted_by": str,
        "granted_at": str,       # ISO-8601 UTC, "Z"
        "signature": str,        # hex HMAC-SHA256
    }

Canonical signing payload::

    json.dumps({"request": req, "granted_by": g, "granted_at": t},
               sort_keys=True, separators=(",", ":"))

Honest limitation (same as the Go verifier's host-side note): the nonce
replay guard is an in-process set. Cross-run replay-proof needs a host-side
nonce store. A replayed token inside its TTL can therefore only ever
re-run the *exact same bound config* — it cannot be transplanted onto a
different action, route, or config, because binding is checked against
the runner's own computed ``config_sha`` on every run.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

WORK_UNIT_ID = "GlacierEQ/ECHO"
ACTION_ID = "deploy"
CAPABILITY = "deploy"
ROUTE = "main"
MIN_SECRET_BYTES = 32
DEFAULT_TTL_SECONDS = 2592000  # 30 days

REQUEST_FIELDS = (
    "work_unit_id",
    "action_id",
    "capability",
    "route",
    "config_sha",
    "expires_at",
    "nonce",
)


def _parse_iso8601(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp ("Z" suffix); naive input is UTC."""
    if not isinstance(value, str):
        raise ValueError("not a string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_iso(dt: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with a "Z" suffix, second precision."""
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def canonical_payload(request: dict, granted_by: str, granted_at: str) -> str:
    """The exact bytes signed: canonical JSON of request + grant identity."""
    return json.dumps(
        {"request": request, "granted_by": granted_by, "granted_at": granted_at},
        sort_keys=True,
        separators=(",", ":"),
    )


def _sign(secret: bytes, payload: str) -> str:
    return hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()


class ApprovalVerifier:
    """Mint and verify exact-bound deploy approval tokens.

    The secret must be at least 32 bytes and comes from the host (env /
    secret store), never from a commit, a payload, or model output.
    """

    def __init__(self, secret: bytes | str):
        if isinstance(secret, str):
            secret_bytes = secret.encode("utf-8")
        else:
            secret_bytes = bytes(secret)
        if len(secret_bytes) < MIN_SECRET_BYTES:
            raise ValueError(
                "DEPLOY_APPROVAL secret too short: approval secret must be "
                "at least 32 bytes"
            )
        self._secret = secret_bytes
        # Consumed nonce -> expiry. In-process only; see module docstring.
        self._seen: dict[str, datetime] = {}

    def sign(self, request: dict, granted_by: str, granted_at: str) -> str:
        return _sign(self._secret, canonical_payload(request, granted_by, granted_at))

    def mint(
        self,
        *,
        config_sha: str,
        granted_by: str,
        work_unit_id: str = WORK_UNIT_ID,
        action_id: str = ACTION_ID,
        capability: str = CAPABILITY,
        route: str = ROUTE,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        now: datetime | None = None,
        expires_at: str | None = None,
        nonce: str | None = None,
    ) -> dict:
        """Mint a signed token bound to one exact config fingerprint."""
        if not granted_by:
            raise ValueError("DEPLOY_APPROVAL granted_by is required")
        if not config_sha:
            raise ValueError("DEPLOY_APPROVAL config_sha is required")
        now_dt = now or _now()
        request = {
            "work_unit_id": work_unit_id,
            "action_id": action_id,
            "capability": capability,
            "route": route,
            "config_sha": config_sha,
            "expires_at": expires_at
            or _format_iso(now_dt + timedelta(seconds=ttl_seconds)),
            "nonce": nonce or secrets.token_hex(16),  # 16 bytes -> 32 hex chars
        }
        granted_at = _format_iso(now_dt)
        return {
            "request": request,
            "granted_by": granted_by,
            "granted_at": granted_at,
            "signature": self.sign(request, granted_by, granted_at),
        }

    def verify(
        self,
        token: dict | None,
        expected_request: dict,
        now: datetime | None = None,
    ) -> dict:
        """Verify a token against the expected request, in this order:

        (1) signature, (2) binding (all six fields, exactly),
        (3) expiry against ``now`` (injectable; tests inject it),
        (4) nonce not seen before in this process.

        The expected request must come from the host's own computed values
        (work unit / action / capability / route / config_sha) — never from
        the token itself. ``expires_at``/``nonce`` are carried from the
        token into the expected request by the caller (see
        :func:`build_expected_request`): they are covered by the signature,
        expiry, and replay checks, while the host-bound fields are what
        stop a token being transplanted onto another deploy.
        """
        if token is None:
            raise ValueError("DEPLOY_APPROVAL token missing")
        if not isinstance(token, dict):
            raise ValueError(
                "DEPLOY_APPROVAL token malformed: token must be a JSON object"
            )
        request = token.get("request")
        if not isinstance(request, dict):
            raise ValueError("DEPLOY_APPROVAL token malformed: request missing")
        for field in ("granted_by", "granted_at", "signature"):
            if not isinstance(token.get(field), str) or not token[field]:
                raise ValueError(f"DEPLOY_APPROVAL token malformed: {field} missing")
        for field in REQUEST_FIELDS:
            if not isinstance(request.get(field), str) or not request[field]:
                raise ValueError(
                    f"DEPLOY_APPROVAL token malformed: request.{field} missing"
                )

        # (1) Signature over the presented values.
        expected_sig = self.sign(request, token["granted_by"], token["granted_at"])
        if not hmac.compare_digest(expected_sig, token["signature"]):
            raise ValueError("DEPLOY_APPROVAL signature invalid")

        # (2) Binding: the token must be for exactly the expected deploy.
        if not isinstance(expected_request, dict):
            raise ValueError(
                "DEPLOY_APPROVAL binding mismatch: expected request missing"
            )
        for field in REQUEST_FIELDS:
            if request.get(field) != expected_request.get(field):
                raise ValueError(
                    "DEPLOY_APPROVAL binding mismatch: "
                    f"{field} expected {expected_request.get(field)!r} "
                    f"got {request.get(field)!r}"
                )

        # (3) Freshness.
        try:
            expires = _parse_iso8601(request["expires_at"])
        except ValueError:
            raise ValueError(
                "DEPLOY_APPROVAL token malformed: expires_at is not ISO-8601 UTC"
            ) from None
        now_dt = now or _now()
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        if now_dt > expires:
            raise ValueError("DEPLOY_APPROVAL expired")

        # (4) Replay: each nonce authorizes exactly once in this process.
        # Cross-run replay-proof needs a host-side nonce store (see module
        # docstring); a replay inside the TTL can only re-run the exact
        # same bound config.
        nonce = request["nonce"]
        seen_exp = self._seen.get(nonce)
        if seen_exp is not None and now_dt < seen_exp:
            raise ValueError("DEPLOY_APPROVAL token already consumed")
        self._seen = {n: e for n, e in self._seen.items() if now_dt < e}
        self._seen[nonce] = expires
        return token


def build_expected_request(
    *,
    config_sha: str,
    expires_at: str | None,
    nonce: str | None,
    work_unit_id: str = WORK_UNIT_ID,
    action_id: str = ACTION_ID,
    capability: str = CAPABILITY,
    route: str = ROUTE,
) -> dict:
    """Build the expected request the runner verifies a token against.

    Host-bound fields come from the runner's own constants and its computed
    ``config_sha``; ``expires_at``/``nonce`` are carried from the presented
    token (they are covered by the signature, expiry, and replay checks).
    """
    return {
        "work_unit_id": work_unit_id,
        "action_id": action_id,
        "capability": capability,
        "route": route,
        "config_sha": config_sha,
        "expires_at": expires_at,
        "nonce": nonce,
    }


def resolve_spec_path(root: Path | str, spec_path: str | None) -> Path | None:
    """Resolve the spec source exactly like the runner: an explicit ``--spec``
    path wins, otherwise ``deploy.spec.json`` in the repo root, else None."""
    if spec_path:
        return Path(spec_path)
    candidate = Path(root) / "deploy.spec.json"
    return candidate if candidate.is_file() else None


def detect_project(root: Path | str) -> str:
    """Mirror of ``ops/ai_deploy.py::detect_project`` (same file checks, same
    order) so mint and runner agree on the project type without importing
    the runner — importing it here would be a circular import."""
    root = Path(root)
    if (root / "Dockerfile").exists() or (root / "docker-compose.yml").exists():
        return "docker"
    if (
        (root / "pyproject.toml").exists()
        or (root / "requirements.txt").exists()
        or (root / "setup.py").exists()
    ):
        return "python"
    if (root / "package-lock.json").exists():
        return "npm"
    if (root / "pnpm-lock.yaml").exists():
        return "pnpm"
    if (root / "yarn.lock").exists():
        return "yarn"
    if (root / "package.json").exists():
        return "npm"
    if (root / "go.mod").exists():
        return "go"
    if (root / "Cargo.toml").exists():
        return "rust"
    return "unknown"


def spec_project_type(root: Path | str, spec_path: str | None) -> str:
    """Project type for a checkout: the spec file's ``project_type`` if it
    sets one, else :func:`detect_project` — the same rule the runner uses."""
    resolved = resolve_spec_path(root, spec_path)
    if resolved is not None:
        try:
            raw = json.loads(resolved.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise ValueError(f"DEPLOY_APPROVAL spec unreadable: {exc}") from exc
        if not isinstance(raw, dict):
            raise ValueError(
                "DEPLOY_APPROVAL spec malformed: top level must be a JSON object"
            )
        return raw.get("project_type") or detect_project(root)
    return detect_project(root)


def config_fingerprint(
    root: Path | str, spec_path_or_None: Path | str | None, project_type: str
) -> str:
    """SHA-256 hex of canonical JSON ``{"spec_sha", "project_type"}``.

    ``spec_sha`` is the SHA-256 of the spec file bytes, or None when there
    is no spec file. Shared by mint and runner — same function, no
    duplication — so a token minted for a checkout matches what the runner
    computes for that checkout. ``root`` is accepted for call-site symmetry
    (callers resolve the spec path against it); the fingerprint itself is
    over the spec bytes + project type only.
    """
    if spec_path_or_None is None:
        spec_sha = None
    else:
        path = Path(spec_path_or_None)
        if not path.is_file():
            raise ValueError(
                f"DEPLOY_APPROVAL spec file not found: {spec_path_or_None}"
            )
        spec_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    canonical = json.dumps(
        {"spec_sha": spec_sha, "project_type": project_type},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _secret_from_env(secret_env: str) -> str:
    """Read the signing secret ONLY from the environment; fail closed."""
    secret = os.environ.get(secret_env)
    if not secret:
        raise ValueError("DEPLOY_APPROVAL secret missing")
    return secret


def _cmd_mint(args: argparse.Namespace) -> int:
    try:
        verifier = ApprovalVerifier(_secret_from_env(args.secret_env))
        root = Path(args.root)
        spec_path = resolve_spec_path(root, args.spec)
        project_type = spec_project_type(root, args.spec)
        config_sha = config_fingerprint(root, spec_path, project_type)
        token = verifier.mint(
            config_sha=config_sha,
            granted_by=args.granted_by,
            work_unit_id=args.work_unit,
            action_id=args.action,
            capability=args.capability,
            route=args.route,
            ttl_seconds=args.ttl_seconds,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(token, indent=2))
    return 0


def _cmd_fingerprint(args: argparse.Namespace) -> int:
    try:
        root = Path(args.root)
        spec_path = resolve_spec_path(root, args.spec)
        project_type = spec_project_type(root, args.spec)
        print(config_fingerprint(root, spec_path, project_type))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Mint / verify exact-bound deploy approval tokens"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    mint = sub.add_parser(
        "mint", help="Mint a signed deploy approval token (prints JSON)"
    )
    mint.add_argument("--root", default=".", help="Repository checkout root")
    mint.add_argument("--spec", help="Path to deploy.spec.json")
    mint.add_argument("--work-unit", default=WORK_UNIT_ID)
    mint.add_argument("--action", default=ACTION_ID)
    mint.add_argument("--capability", default=CAPABILITY)
    mint.add_argument("--route", default=ROUTE)
    mint.add_argument(
        "--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS,
        help="Token lifetime in seconds (default 2592000 = 30d)",
    )
    mint.add_argument("--granted-by", default="operator")
    mint.add_argument(
        "--secret-env", default="DEPLOY_APPROVAL_SECRET",
        help="Env var holding the signing secret (min 32 chars)",
    )
    mint.set_defaults(func=_cmd_mint)

    fp = sub.add_parser(
        "fingerprint", help="Print the config_sha for a checkout"
    )
    fp.add_argument("--root", default=".", help="Repository checkout root")
    fp.add_argument("--spec", help="Path to deploy.spec.json")
    fp.set_defaults(func=_cmd_fingerprint)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
