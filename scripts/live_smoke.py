#!/usr/bin/env python3
"""Live AKOS ↔ ECHO deployment smoke harness."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from typing import Any

import httpx

from echo.auth import sign_authority


def signed_headers(secret: str, actor: str, scope: str) -> dict[str, str]:
    timestamp = str(int(time.time()))
    nonce = str(uuid.uuid4())
    return {
        "X-AKOS-Actor": actor,
        "X-AKOS-Scope": scope,
        "X-AKOS-Timestamp": timestamp,
        "X-AKOS-Nonce": nonce,
        "X-AKOS-Signature": sign_authority(
            secret, actor, scope, timestamp, nonce
        ),
    }


def check(response: httpx.Response, expected: int, name: str) -> dict[str, Any]:
    passed = response.status_code == expected
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text
    return {
        "name": name,
        "passed": passed,
        "expected_status": expected,
        "actual_status": response.status_code,
        "body": body,
    }


def run(base_url: str, secret: str, actor: str) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    base_url = base_url.rstrip("/")
    with httpx.Client(base_url=base_url, timeout=20.0) as client:
        results.append(check(client.get("/health"), 200, "public health"))

        bad_headers = signed_headers("incorrect-secret", actor, "echo:read")
        results.append(
            check(client.get("/stats", headers=bad_headers), 403, "bad signature rejected")
        )

        read_headers = signed_headers(secret, actor, "echo:read")
        results.append(
            check(client.get("/stats", headers=read_headers), 200, "authorized read")
        )

        write_headers = signed_headers(secret, actor, "echo:write")
        external_id = f"smoke-{uuid.uuid4()}"
        conversation = client.post(
            "/conversations",
            headers=write_headers,
            json={
                "source": "grok-live-smoke",
                "external_id": external_id,
                "title": "AKOS ECHO Trust Loop Smoke",
                "participants": [actor, "echo"],
                "labels": ["smoke", "p0.5"],
                "messages": [
                    {
                        "role": "system",
                        "content": "Verify the governed trust loop.",
                    }
                ],
            },
        )
        results.append(check(conversation, 200, "authorized conversation ingest"))

        execute_headers = signed_headers(secret, actor, "echo:execute")
        idempotency_key = f"smoke-job-{uuid.uuid4()}"
        job = client.post(
            "/jobs",
            headers=execute_headers,
            json={
                "job_type": "echo.ping",
                "payload": {"smoke": True},
                "idempotency_key": idempotency_key,
                "max_attempts": 2,
            },
        )
        results.append(check(job, 200, "authorized job enqueue"))
        job_id = job.json().get("id") if job.status_code == 200 else None

        if job_id:
            run_response = client.post(
                f"/jobs/{job_id}/run",
                headers=signed_headers(secret, actor, "echo:execute"),
            )
            results.append(check(run_response, 200, "authorized job execution"))

            trust_response = client.get(
                f"/jobs/{job_id}/trust",
                headers=signed_headers(secret, actor, "echo:verify"),
            )
            trust_result = check(trust_response, 200, "receipt trust-chain verification")
            if trust_response.status_code == 200:
                trust_result["passed"] = bool(trust_response.json().get("verified"))
            results.append(trust_result)

        unsupported = client.post(
            "/jobs",
            headers=signed_headers(secret, actor, "echo:execute"),
            json={
                "job_type": "echo.unsupported",
                "payload": {},
                "idempotency_key": f"unsupported-{uuid.uuid4()}",
                "max_attempts": 1,
            },
        )
        results.append(check(unsupported, 422, "unsupported capability blocked"))

    return {
        "base_url": base_url,
        "actor": actor,
        "passed": all(item["passed"] for item in results),
        "checks": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("ECHO_BASE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--secret",
        default=os.environ.get("ECHO_AKOS_SHARED_SECRET", ""),
    )
    parser.add_argument("--actor", default="akos:live-smoke")
    args = parser.parse_args()
    if not args.secret:
        raise SystemExit("ECHO_AKOS_SHARED_SECRET or --secret is required")
    report = run(args.base_url, args.secret, args.actor)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
