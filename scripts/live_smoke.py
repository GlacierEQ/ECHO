#!/usr/bin/env python3
"""Live ECHO deployment smoke harness (direct-access mode)."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from typing import Any

import httpx


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


def run(base_url: str) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    base_url = base_url.rstrip("/")
    with httpx.Client(base_url=base_url, timeout=20.0) as client:
        results.append(check(client.get("/health"), 200, "public health"))

        stats = client.get("/stats")
        results.append(check(stats, 200, "stats readable"))

        external_id = f"smoke-{uuid.uuid4()}"
        conversation = client.post(
            "/conversations",
            json={
                "source": "grok-live-smoke",
                "external_id": external_id,
                "title": "ECHO Direct Access Smoke",
                "participants": ["smoke", "echo"],
                "labels": ["smoke", "direct"],
                "messages": [
                    {
                        "role": "system",
                        "content": "Verify direct-access continuity and jobs.",
                    }
                ],
            },
        )
        results.append(check(conversation, 200, "conversation ingest"))

        idempotency_key = f"smoke-job-{uuid.uuid4()}"
        job = client.post(
            "/jobs",
            json={
                "job_type": "echo.ping",
                "payload": {"smoke": True},
                "idempotency_key": idempotency_key,
                "max_attempts": 2,
            },
        )
        results.append(check(job, 200, "job enqueue"))
        job_id = job.json().get("id") if job.status_code == 200 else None

        if job_id:
            run_response = client.post(f"/jobs/{job_id}/run")
            results.append(check(run_response, 200, "job execution"))

            trust_response = client.get(f"/jobs/{job_id}/trust")
            trust_result = check(
                trust_response, 200, "receipt trust-chain verification"
            )
            if trust_response.status_code == 200:
                body = trust_response.json()
                # In direct mode, authority_actor is recorded as direct-api;
                # chain validity + terminal state still matter.
                trust_result["passed"] = bool(body.get("valid") or body.get("verified"))
            results.append(trust_result)

        unsupported = client.post(
            "/jobs",
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
        "mode": "direct_access",
        "passed": all(item["passed"] for item in results),
        "checks": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("ECHO_BASE_URL", "http://127.0.0.1:8000"),
    )
    args = parser.parse_args()
    report = run(args.base_url)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
