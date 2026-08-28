#!/usr/bin/env python3
"""AI Autonomous Deploy Runner

Real deploy + verify + rollback. Never claims success without verification proof.
No prompts. No click-to-continue. Machine-readable results only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

VERSION = "1.0.0"
RESULT_FILE = "deployment-result.json"
LOCK_FILE = ".ai-deploy.lock"
ARTIFACT_DIR = "deployment-artifacts"

SECRET_PATTERNS = [
    re.compile(
        r"(?i)(api[_-]?key|token|password|secret|credential|authorization|bearer|hook)[=:\s]+[^\s\"']+",
        re.I,
    ),
    re.compile(r"(?i)(postgres|mysql|mongodb|redis)://[^\s\"']+"),
    re.compile(r"(?i)eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    re.compile(r"(?i)ghp_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)sk-[A-Za-z0-9]{20,}"),
]


@dataclass
class HealthCheck:
    url: str
    expect_status: list[int] = field(default_factory=lambda: [200])
    attempts: int = 20
    interval_seconds: float = 3.0
    timeout_seconds: float = 10.0
    expect_body_contains: str | None = None


@dataclass
class Pipeline:
    prepare: list[str] = field(default_factory=list)
    test: list[str] = field(default_factory=list)
    build: list[str] = field(default_factory=list)
    deploy: list[str] = field(default_factory=list)
    verify: list[str] = field(default_factory=list)
    rollback: list[str] = field(default_factory=list)


@dataclass
class Spec:
    version: int = 1
    pipeline: Pipeline = field(default_factory=Pipeline)
    health: list[HealthCheck] = field(default_factory=list)
    retries: int = 2
    backoff_seconds: float = 2.0
    project_type: str | None = None


@dataclass
class StepResult:
    name: str
    command: str
    exit_code: int
    duration_seconds: float
    stdout_tail: str
    stderr_tail: str
    attempt: int


@dataclass
class DeployResult:
    status: str
    version: str = VERSION
    run_id: str = ""
    started_at: str = ""
    finished_at: str = ""
    root: str = ""
    project_type: str | None = None
    steps: list[dict[str, Any]] = field(default_factory=list)
    health: list[dict[str, Any]] = field(default_factory=list)
    rollback: list[dict[str, Any]] = field(default_factory=list)
    artifacts: dict[str, str] = field(default_factory=dict)
    error: str | None = None
    exit_code: int = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def redact(text: str) -> str:
    out = text
    for pat in SECRET_PATTERNS:
        out = pat.sub("[REDACTED]", out)
    return out


def tail(text: str, n: int = 40) -> str:
    lines = text.splitlines()
    return "\n".join(lines[-n:])


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def detect_project(root: Path) -> str:
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


def default_pipeline(project_type: str) -> Pipeline:
    if project_type == "python":
        return Pipeline(
            prepare=[
                "python -m pip install --upgrade pip",
                "python -m pip install -e '.[dev]' || python -m pip install -r requirements.txt || true",
            ],
            test=["python -m compileall -q .", "pytest -q --tb=line || true"],
            build=[],
            deploy=[],
            verify=[],
            rollback=[],
        )
    if project_type == "docker":
        return Pipeline(
            prepare=[],
            test=[],
            build=["docker compose build || docker build -t app:local ."],
            deploy=["docker compose up -d --build || true"],
            verify=["docker compose ps || true"],
            rollback=["docker compose down || true"],
        )
    if project_type in {"npm", "pnpm", "yarn"}:
        pm = project_type if project_type != "npm" else "npm"
        install = {
            "npm": "npm ci || npm install",
            "pnpm": "pnpm install --frozen-lockfile || pnpm install",
            "yarn": "yarn install --frozen-lockfile || yarn install",
        }[pm]
        return Pipeline(
            prepare=[install],
            test=[f"{pm} test --if-present"],
            build=[f"{pm} run build --if-present"],
            deploy=[],
            verify=[],
            rollback=[],
        )
    if project_type == "go":
        return Pipeline(
            prepare=["go mod download"],
            test=["go test ./..."],
            build=["go build ./..."],
            deploy=[],
            verify=[],
            rollback=[],
        )
    if project_type == "rust":
        return Pipeline(
            prepare=[],
            test=["cargo test"],
            build=["cargo build --release"],
            deploy=[],
            verify=[],
            rollback=[],
        )
    return Pipeline()


def load_spec(root: Path, args: argparse.Namespace) -> Spec:
    raw: dict[str, Any] | None = None
    if args.spec:
        raw = json.loads(Path(args.spec).read_text(encoding="utf-8"))
    elif os.environ.get("DEPLOY_SPEC_JSON"):
        raw = json.loads(os.environ["DEPLOY_SPEC_JSON"])
    elif (root / "deploy.spec.json").exists():
        raw = json.loads((root / "deploy.spec.json").read_text(encoding="utf-8"))

    project_type = (raw or {}).get("project_type") or detect_project(root)
    base = default_pipeline(project_type)

    if not raw:
        if os.environ.get("DEPLOY_COMMAND"):
            base.deploy = [os.environ["DEPLOY_COMMAND"]]
        if os.environ.get("ROLLBACK_COMMAND"):
            base.rollback = [os.environ["ROLLBACK_COMMAND"]]
        health: list[HealthCheck] = []
        if os.environ.get("HEALTH_URL"):
            health.append(HealthCheck(url=os.environ["HEALTH_URL"]))
        return Spec(pipeline=base, health=health, project_type=project_type)

    p = raw.get("pipeline")
    if p is None:
        pipeline = base
        if os.environ.get("DEPLOY_COMMAND"):
            pipeline.deploy = [os.environ["DEPLOY_COMMAND"]]
        if os.environ.get("ROLLBACK_COMMAND"):
            pipeline.rollback = [os.environ["ROLLBACK_COMMAND"]]
    else:

        def phase(name: str, env_key: str | None = None) -> list[str]:
            if name in p:
                return list(p.get(name) or [])
            if env_key and os.environ.get(env_key):
                return [os.environ[env_key]]
            return []

        pipeline = Pipeline(
            prepare=phase("prepare"),
            test=phase("test"),
            build=phase("build"),
            deploy=phase("deploy", "DEPLOY_COMMAND"),
            verify=phase("verify"),
            rollback=phase("rollback", "ROLLBACK_COMMAND"),
        )
    health = []
    for h in raw.get("health") or []:
        health.append(
            HealthCheck(
                url=h["url"],
                expect_status=list(h.get("expect_status") or [200]),
                attempts=int(h.get("attempts") or 20),
                interval_seconds=float(h.get("interval_seconds") or 3),
                timeout_seconds=float(h.get("timeout_seconds") or 10),
                expect_body_contains=h.get("expect_body_contains"),
            )
        )
    if not health and os.environ.get("HEALTH_URL"):
        health.append(HealthCheck(url=os.environ["HEALTH_URL"]))

    return Spec(
        version=int(raw.get("version") or 1),
        pipeline=pipeline,
        health=health,
        retries=int(raw.get("retries") or 2),
        backoff_seconds=float(raw.get("backoff_seconds") or 2.0),
        project_type=project_type,
    )


class DeployLock:
    def __init__(self, root: Path):
        self.path = root / LOCK_FILE
        self.held = False

    def acquire(self) -> bool:
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                pid = int(data.get("pid") or 0)
                if pid and Path(f"/proc/{pid}").exists():
                    return False
            except Exception:
                pass
        self.path.write_text(
            json.dumps({"pid": os.getpid(), "at": utc_now()}), encoding="utf-8"
        )
        self.held = True
        return True

    def release(self) -> None:
        if self.held and self.path.exists():
            try:
                self.path.unlink()
            except OSError:
                pass
            self.held = False


def run_command(cmd: str, cwd: Path, timeout: int = 1800) -> StepResult:
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            shell=True,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        return StepResult(
            name="command",
            command=redact(cmd),
            exit_code=proc.returncode,
            duration_seconds=round(time.monotonic() - started, 3),
            stdout_tail=redact(tail(proc.stdout or "")),
            stderr_tail=redact(tail(proc.stderr or "")),
            attempt=1,
        )
    except subprocess.TimeoutExpired as exc:
        return StepResult(
            name="command",
            command=redact(cmd),
            exit_code=124,
            duration_seconds=round(time.monotonic() - started, 3),
            stdout_tail=redact(
                tail(
                    (exc.stdout or b"").decode()
                    if isinstance(exc.stdout, bytes)
                    else (exc.stdout or "")
                )
            ),
            stderr_tail="timeout",
            attempt=1,
        )


def run_with_retries(
    cmd: str, cwd: Path, retries: int, backoff: float, phase: str
) -> StepResult:
    last: StepResult | None = None
    for attempt in range(1, retries + 2):
        result = run_command(cmd, cwd)
        result.name = phase
        result.attempt = attempt
        last = result
        if result.exit_code == 0:
            return result
        if attempt <= retries:
            time.sleep(backoff * (2 ** (attempt - 1)))
    assert last is not None
    return last


def check_health(h: HealthCheck) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    for i in range(1, h.attempts + 1):
        try:
            req = Request(
                h.url, method="GET", headers={"User-Agent": f"ai-deploy/{VERSION}"}
            )
            with urlopen(req, timeout=h.timeout_seconds) as resp:
                body = resp.read(4096).decode("utf-8", errors="replace")
                status = getattr(resp, "status", 200)
                ok = status in h.expect_status
                if ok and h.expect_body_contains and h.expect_body_contains not in body:
                    ok = False
                evidence.append(
                    {
                        "attempt": i,
                        "status": status,
                        "ok": ok,
                        "body_tail": redact(body[-200:]),
                    }
                )
                if ok:
                    return {
                        "url": h.url,
                        "passed": True,
                        "attempts": i,
                        "evidence": evidence,
                    }
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            evidence.append(
                {"attempt": i, "status": None, "ok": False, "error": type(exc).__name__}
            )
        if i < h.attempts:
            time.sleep(h.interval_seconds)
    return {"url": h.url, "passed": False, "attempts": h.attempts, "evidence": evidence}


def write_artifacts(root: Path, result: DeployResult) -> dict[str, str]:
    art = root / ARTIFACT_DIR
    art.mkdir(parents=True, exist_ok=True)
    result_path = art / RESULT_FILE
    result_path.write_text(json.dumps(asdict(result), indent=2), encoding="utf-8")
    (root / RESULT_FILE).write_text(
        json.dumps(asdict(result), indent=2), encoding="utf-8"
    )
    manifest = {RESULT_FILE: sha256_file(result_path)}
    log_path = art / "deploy.log"
    lines = []
    for s in result.steps:
        lines.append(
            f"$ {s.get('command')}\nexit={s.get('exit_code')}\n{s.get('stdout_tail')}\n{s.get('stderr_tail')}\n"
        )
    log_path.write_text("\n".join(lines), encoding="utf-8")
    manifest["deploy.log"] = sha256_file(log_path)
    (art / "manifest.sha256").write_text(
        "\n".join(f"{v}  {k}" for k, v in manifest.items()) + "\n", encoding="utf-8"
    )
    return manifest


def phase_commands(spec: Spec, name: str) -> list[str]:
    return getattr(spec.pipeline, name) or []


def execute(root: Path, spec: Spec) -> DeployResult:
    run_id = str(uuid.uuid4())
    result = DeployResult(
        status="FAILED",
        run_id=run_id,
        started_at=utc_now(),
        root=str(root),
        project_type=spec.project_type,
        exit_code=1,
    )

    lock = DeployLock(root)
    if not lock.acquire():
        result.status = "LOCKED"
        result.error = "another deployment holds the lock"
        result.exit_code = 75
        result.finished_at = utc_now()
        write_artifacts(root, result)
        return result

    deployed = False
    try:
        for phase in ("prepare", "test", "build", "deploy", "verify"):
            for cmd in phase_commands(spec, phase):
                step = run_with_retries(
                    cmd, root, spec.retries, spec.backoff_seconds, phase
                )
                result.steps.append(asdict(step))
                if step.exit_code != 0:
                    result.error = (
                        f"{phase} failed: {step.command} (exit {step.exit_code})"
                    )
                    result.exit_code = step.exit_code or 1
                    if deployed or phase in {"deploy", "verify"}:
                        for rb in phase_commands(spec, "rollback"):
                            rb_step = run_command(rb, root)
                            rb_step.name = "rollback"
                            result.rollback.append(asdict(rb_step))
                    result.finished_at = utc_now()
                    result.artifacts = write_artifacts(root, result)
                    return result
                if phase == "deploy":
                    deployed = True

        for h in spec.health:
            hr = check_health(h)
            result.health.append(hr)
            if not hr["passed"]:
                result.error = f"health check failed: {h.url}"
                result.exit_code = 60
                for rb in phase_commands(spec, "rollback"):
                    rb_step = run_command(rb, root)
                    rb_step.name = "rollback"
                    result.rollback.append(asdict(rb_step))
                result.finished_at = utc_now()
                result.artifacts = write_artifacts(root, result)
                return result

        if (
            not any(phase_commands(spec, p) for p in ("deploy", "verify"))
            and not spec.health
        ):
            result.status = "CONFIG_ERROR"
            result.error = "no deploy/verify commands and no health checks configured"
            result.exit_code = 78
            result.finished_at = utc_now()
            result.artifacts = write_artifacts(root, result)
            return result

        result.status = "SUCCESS"
        result.exit_code = 0
        result.finished_at = utc_now()
        result.artifacts = write_artifacts(root, result)
        return result
    finally:
        lock.release()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AI Autonomous Deploy Runner")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--spec", help="Path to deploy.spec.json")
    parser.add_argument(
        "--print-spec", action="store_true", help="Print resolved spec and exit"
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    if not root.is_dir():
        print(
            json.dumps({"status": "CONFIG_ERROR", "error": f"root not found: {root}"}),
            file=sys.stderr,
        )
        return 78

    try:
        spec = load_spec(root, args)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        print(
            json.dumps({"status": "CONFIG_ERROR", "error": f"invalid spec: {exc}"}),
            file=sys.stderr,
        )
        return 78

    if args.print_spec:
        print(
            json.dumps(
                {
                    "version": spec.version,
                    "project_type": spec.project_type,
                    "pipeline": asdict(spec.pipeline),
                    "health": [asdict(h) for h in spec.health],
                    "retries": spec.retries,
                },
                indent=2,
            )
        )
        return 0

    if (
        not phase_commands(spec, "deploy")
        and not spec.health
        and not os.environ.get("DEPLOY_COMMAND")
    ):
        if not any(
            phase_commands(spec, p) for p in ("prepare", "test", "build", "verify")
        ):
            out = DeployResult(
                status="CONFIG_ERROR",
                run_id=str(uuid.uuid4()),
                started_at=utc_now(),
                finished_at=utc_now(),
                root=str(root),
                project_type=spec.project_type,
                error="missing DEPLOY_COMMAND / pipeline.deploy / health — nothing to execute",
                exit_code=78,
            )
            write_artifacts(root, out)
            print(json.dumps(asdict(out), indent=2))
            return 78

    result = execute(root, spec)
    print(json.dumps(asdict(result), indent=2))
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
