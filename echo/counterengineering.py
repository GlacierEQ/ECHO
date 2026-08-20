"""Ceiling-first recovery continuity for ECHO.

ECHO does not decide the operator's target. It preserves the target and the
unresolved recovery obligations long enough for AKOS and execution systems to
act on them without a narrower current floor silently replacing the objective.
"""
from __future__ import annotations

from typing import Any


def _string_list(payload: dict[str, Any], key: str) -> list[str]:
    raw = payload.get(key, [])
    if raw is None:
        return []
    if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
        raise ValueError(f"{key} must be a list of strings")
    return [item.strip() for item in raw if item.strip()]


def build_recovery_continuity(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a deterministic recovery-continuity receipt payload.

    The result preserves separate target/floor/recovery layers. A local block is
    recorded as local. Missing verification becomes an evidence obligation, not
    permission to erase a target or historical capability.
    """
    if not isinstance(payload, dict):
        raise ValueError("counterengineering payload must be an object")

    target = payload.get("target")
    current_floor = payload.get("current_floor")
    if not isinstance(target, str) or not target.strip():
        raise ValueError("target must be a non-empty string")
    if not isinstance(current_floor, str) or not current_floor.strip():
        raise ValueError("current_floor must be a non-empty string")

    contraction_detected = payload.get("contraction_detected", False)
    if not isinstance(contraction_detected, bool):
        raise ValueError("contraction_detected must be boolean")

    displaced = _string_list(payload, "displaced_capabilities")
    lineage = _string_list(payload, "lineage_sources")
    verified_strengths = _string_list(payload, "verified_strengths")
    real_constraints = _string_list(payload, "real_constraints")
    local_blocks = _string_list(payload, "local_blocks")
    compatible_later_gains = _string_list(payload, "compatible_later_gains")
    surpass_opportunities = _string_list(payload, "surpass_opportunities")
    verification_obligations = _string_list(payload, "verification_obligations")

    if contraction_detected and not displaced and not lineage:
        raise ValueError(
            "detected contraction must identify displaced capability or lineage evidence"
        )

    recovery_actions: list[dict[str, Any]] = []
    for capability in displaced:
        recovery_actions.append(
            {
                "action": "RECOVER_CAPABILITY",
                "subject": capability,
                "state": "OPEN",
            }
        )
    for gain in compatible_later_gains:
        recovery_actions.append(
            {
                "action": "PRESERVE_COMPATIBLE_GAIN",
                "subject": gain,
                "state": "OPEN",
            }
        )
    for opportunity in surpass_opportunities:
        recovery_actions.append(
            {
                "action": "EVALUATE_SURPASS_COMPOSITION",
                "subject": opportunity,
                "state": "OPEN",
            }
        )
    for obligation in verification_obligations:
        recovery_actions.append(
            {
                "action": "VERIFY_WITHOUT_SHRINKING_TARGET",
                "subject": obligation,
                "state": "OPEN",
            }
        )

    if contraction_detected:
        posture = "RESTORE_AND_SURPASS" if surpass_opportunities else "RESTORE_AND_RECOMPUTE_CEILING"
    else:
        posture = "ADVANCE_FROM_CEILING"

    return {
        "schema": "glaciereq.echo.counterengineering-continuity.v1",
        "orientation": "CEILING_FIRST",
        "target": target.strip(),
        "current_floor": current_floor.strip(),
        "target_preserved": True,
        "contraction_detected": contraction_detected,
        "recovery_posture": posture,
        "displaced_capabilities": displaced,
        "lineage_sources": lineage,
        "verified_strengths_to_preserve": verified_strengths,
        "real_constraints": real_constraints,
        "local_blocks": [
            {"boundary": block, "scope": "LOCAL_UNLESS_PROVEN_GLOBAL"}
            for block in local_blocks
        ],
        "compatible_later_gains": compatible_later_gains,
        "surpass_opportunities": surpass_opportunities,
        "verification_obligations": verification_obligations,
        "recovery_actions": recovery_actions,
        "unresolved_recovery_obligation_count": len(recovery_actions),
        "continuity_rule": (
            "Preserve the ceiling, displaced capability, lineage, compatible later gains, "
            "real constraints, and unresolved proof obligations across sessions. The floor "
            "measures the gap and may not silently replace the target."
        ),
    }
