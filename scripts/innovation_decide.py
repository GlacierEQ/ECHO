#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from echo.frontier import FrontierEvent
from echo.innovation import (
    InnovationConstraints,
    InnovationDecisionEngine,
    InnovationOutcome,
    InnovationPath,
)


def load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("input root must be an object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Select one evidence-bound ECHO innovation path.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument(
        "--preference",
        choices=("maximum_advance", "balanced", "fast_proof", "low_risk"),
        default="maximum_advance",
    )
    args = parser.parse_args()

    payload = load(args.input)
    event_payload = payload.get("event")
    path_payloads = payload.get("paths")
    constraint_payload = payload.get("constraints", {})
    if not isinstance(event_payload, dict):
        raise ValueError("event must be an object")
    if not isinstance(path_payloads, list) or not path_payloads:
        raise ValueError("paths must be a non-empty list")
    if not isinstance(constraint_payload, dict):
        raise ValueError("constraints must be an object")

    event = FrontierEvent(**event_payload)
    event_id = event.event_id
    paths: list[InnovationPath] = []
    for raw in path_payloads:
        if not isinstance(raw, dict):
            raise ValueError("each path must be an object")
        row = dict(raw)
        row.setdefault("event_id", event_id)
        row["outcome"] = InnovationOutcome(row["outcome"])
        if "evidence_refs" in row:
            row["evidence_refs"] = tuple(row["evidence_refs"])
        paths.append(InnovationPath(**row))

    constraints = InnovationConstraints(**constraint_payload)
    decision = InnovationDecisionEngine(event, paths).decide(
        constraints,
        preference=args.preference,
    )
    output = decision.as_dict()
    output["event"] = event.as_dict()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(output, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    artifact_sha = hashlib.sha256(args.output.read_bytes()).hexdigest()

    receipt = {
        "schema": "glaciereq.echo.innovation-receipt.v1",
        "artifact": str(args.output),
        "artifact_sha256": artifact_sha,
        "decision_sha256": decision.digest,
        "event_id": event_id,
        "selected_path": decision.selected.path_id,
        "selected_outcome": decision.selected.outcome.value,
        "evidence_state": decision.evidence_state,
        "verified_state": "DETERMINISTIC_DECISION_EXECUTED",
        "execution_claim": "DECISION_EXECUTED_SELECTED_ACTION_NOT_YET_EXECUTED",
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
