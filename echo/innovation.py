"""Evidence-bound innovation decisions for ECHO frontier events.

ECHO already preserves fresh primary-source technology signals. This module closes
that loop: each signal can be given multiple concrete implementation/experiment
paths, Pareto-filtered without collapsing real tradeoffs, and converted into one
reproducible decision. The engine never treats a score as project authority and
never claims an implementation happened merely because a path was selected.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from hashlib import sha256
import json
from math import isfinite
from typing import Iterable, Literal

from .frontier import FrontierEvent

EVIDENCE_STATE = "DETERMINISTIC_ECHO_INNOVATION_DECISION"
Preference = Literal["maximum_advance", "balanced", "fast_proof", "low_risk"]


class InnovationOutcome(StrEnum):
    IMPLEMENT = "implement"
    EXPERIMENT = "experiment"
    HOLD = "hold_with_reason"
    REJECT = "reject_with_reason"


class NoFeasibleInnovationPath(ValueError):
    """Raised when explicit constraints remove every candidate path."""


@dataclass(frozen=True)
class InnovationConstraints:
    max_implementation_cost: float | None = None
    max_execution_risk: float | None = None
    max_time_to_proof: float | None = None
    minimum_evidence_confidence: float = 0.0
    require_executable_for_action: bool = True

    def __post_init__(self) -> None:
        for name in (
            "max_implementation_cost",
            "max_execution_risk",
            "max_time_to_proof",
            "minimum_evidence_confidence",
        ):
            value = getattr(self, name)
            if value is not None and (not isfinite(value) or value < 0):
                raise ValueError(f"{name} must be finite and non-negative")
        if self.max_execution_risk is not None and self.max_execution_risk > 1:
            raise ValueError("max_execution_risk must be <= 1")
        if self.minimum_evidence_confidence > 1:
            raise ValueError("minimum_evidence_confidence must be <= 1")


@dataclass(frozen=True)
class InnovationPath:
    event_id: str
    path_id: str
    outcome: InnovationOutcome
    capability_gain: float
    boundary_fit: float
    evidence_confidence: float
    composability: float
    reversibility: float
    implementation_cost: float
    execution_risk: float
    time_to_proof: float
    evidence_refs: tuple[str, ...] = ()
    executable_ref: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.path_id.strip():
            raise ValueError("event_id and path_id must be non-empty")
        for name in (
            "capability_gain",
            "boundary_fit",
            "evidence_confidence",
            "composability",
            "reversibility",
            "execution_risk",
        ):
            value = getattr(self, name)
            if not isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{name} must be finite and within [0, 1]")
        for name in ("implementation_cost", "time_to_proof"):
            value = getattr(self, name)
            if not isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if any(not ref.strip() for ref in self.evidence_refs):
            raise ValueError("evidence_refs may not contain empty references")
        if self.outcome in {InnovationOutcome.HOLD, InnovationOutcome.REJECT}:
            if not self.reason or not self.reason.strip():
                raise ValueError(f"{self.outcome.value} requires a reason")

    @property
    def actionable(self) -> bool:
        return self.outcome in {InnovationOutcome.IMPLEMENT, InnovationOutcome.EXPERIMENT}

    def accepted_by(self, constraints: InnovationConstraints) -> bool:
        if self.evidence_confidence < constraints.minimum_evidence_confidence:
            return False
        if (
            constraints.max_implementation_cost is not None
            and self.implementation_cost > constraints.max_implementation_cost
        ):
            return False
        if (
            constraints.max_execution_risk is not None
            and self.execution_risk > constraints.max_execution_risk
        ):
            return False
        if constraints.max_time_to_proof is not None and self.time_to_proof > constraints.max_time_to_proof:
            return False
        if constraints.require_executable_for_action and self.actionable and not self.executable_ref:
            return False
        return True

    def dominates(self, other: "InnovationPath") -> bool:
        gains = (
            self.capability_gain,
            self.boundary_fit,
            self.evidence_confidence,
            self.composability,
            self.reversibility,
        )
        other_gains = (
            other.capability_gain,
            other.boundary_fit,
            other.evidence_confidence,
            other.composability,
            other.reversibility,
        )
        costs = (self.implementation_cost, self.execution_risk, self.time_to_proof)
        other_costs = (other.implementation_cost, other.execution_risk, other.time_to_proof)
        no_worse = all(left >= right for left, right in zip(gains, other_gains))
        no_worse = no_worse and all(left <= right for left, right in zip(costs, other_costs))
        strictly_better = any(left > right for left, right in zip(gains, other_gains))
        strictly_better = strictly_better or any(left < right for left, right in zip(costs, other_costs))
        return no_worse and strictly_better

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["outcome"] = self.outcome.value
        return value


@dataclass(frozen=True)
class InnovationDecision:
    event_id: str
    preference: Preference
    selected: InnovationPath
    frontier: tuple[InnovationPath, ...]
    candidate_count: int
    evidence_state: str = EVIDENCE_STATE

    @property
    def digest(self) -> str:
        payload = json.dumps(self.as_dict(include_digest=False), sort_keys=True, separators=(",", ":"))
        return sha256(payload.encode("utf-8")).hexdigest()

    def as_dict(self, *, include_digest: bool = True) -> dict[str, object]:
        value: dict[str, object] = {
            "schema": "glaciereq.echo.innovation-decision.v1",
            "event_id": self.event_id,
            "preference": self.preference,
            "candidate_count": self.candidate_count,
            "frontier_count": len(self.frontier),
            "selected": self.selected.as_dict(),
            "frontier": [path.as_dict() for path in self.frontier],
            "evidence_state": self.evidence_state,
            "execution_claim": "SELECTION_ONLY_NOT_EXECUTION",
        }
        if include_digest:
            value["decision_sha256"] = self.digest
        return value


class InnovationDecisionEngine:
    """Pareto-first decision engine for one ECHO frontier event."""

    def __init__(self, event: FrontierEvent, paths: Iterable[InnovationPath]) -> None:
        event.validate()
        self.event = event
        self.paths = tuple(paths)
        if not self.paths:
            raise ValueError("at least one innovation path is required")
        if any(path.event_id != event.event_id for path in self.paths):
            raise ValueError("all innovation paths must bind to the supplied frontier event")
        ids = [path.path_id for path in self.paths]
        if len(ids) != len(set(ids)):
            raise ValueError("path_id values must be unique within an event")

    @staticmethod
    def pareto_frontier(paths: Iterable[InnovationPath]) -> tuple[InnovationPath, ...]:
        items = tuple(paths)
        frontier = tuple(
            path
            for path in items
            if not any(other.dominates(path) for other in items if other is not path)
        )
        return tuple(
            sorted(
                frontier,
                key=lambda path: (
                    -path.capability_gain,
                    -path.boundary_fit,
                    -path.evidence_confidence,
                    path.execution_risk,
                    path.time_to_proof,
                    path.path_id,
                ),
            )
        )

    def feasible_frontier(
        self, constraints: InnovationConstraints | None = None
    ) -> tuple[InnovationPath, ...]:
        active = constraints or InnovationConstraints()
        feasible = tuple(path for path in self.paths if path.accepted_by(active))
        if not feasible:
            raise NoFeasibleInnovationPath(
                "no innovation path satisfies the requested evidence, cost, risk, time, and executable constraints"
            )
        return self.pareto_frontier(feasible)

    def decide(
        self,
        constraints: InnovationConstraints | None = None,
        *,
        preference: Preference = "maximum_advance",
    ) -> InnovationDecision:
        frontier = self.feasible_frontier(constraints)
        if preference not in {"maximum_advance", "balanced", "fast_proof", "low_risk"}:
            raise ValueError("unsupported innovation preference")

        if preference == "maximum_advance":
            selected = max(
                frontier,
                key=lambda path: (
                    path.capability_gain,
                    path.boundary_fit,
                    path.composability,
                    path.evidence_confidence,
                    path.reversibility,
                    -path.execution_risk,
                    -path.time_to_proof,
                ),
            )
        elif preference == "fast_proof":
            selected = min(
                frontier,
                key=lambda path: (
                    path.time_to_proof,
                    -path.evidence_confidence,
                    -path.capability_gain,
                    path.execution_risk,
                ),
            )
        elif preference == "low_risk":
            selected = min(
                frontier,
                key=lambda path: (
                    path.execution_risk,
                    -path.reversibility,
                    -path.evidence_confidence,
                    -path.capability_gain,
                ),
            )
        else:
            dimensions = {
                "capability_gain": True,
                "boundary_fit": True,
                "evidence_confidence": True,
                "composability": True,
                "reversibility": True,
                "implementation_cost": False,
                "execution_risk": False,
                "time_to_proof": False,
            }
            bounds: dict[str, tuple[float, float]] = {}
            for name in dimensions:
                values = [float(getattr(path, name)) for path in frontier]
                bounds[name] = (min(values), max(values))

            def normalized(value: float, low: float, high: float) -> float:
                return 0.0 if high == low else (value - low) / (high - low)

            def score(path: InnovationPath) -> tuple[float, float, float, str]:
                total = 0.0
                for name, gain in dimensions.items():
                    low, high = bounds[name]
                    value = normalized(float(getattr(path, name)), low, high)
                    total += value if gain else 1.0 - value
                return (total, path.capability_gain, path.evidence_confidence, path.path_id)

            selected = max(frontier, key=score)

        return InnovationDecision(
            event_id=self.event.event_id,
            preference=preference,
            selected=selected,
            frontier=frontier,
            candidate_count=len(self.paths),
        )
