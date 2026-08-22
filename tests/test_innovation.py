from __future__ import annotations

import unittest

from echo.frontier import FrontierEvent
from echo.innovation import (
    EVIDENCE_STATE,
    InnovationConstraints,
    InnovationDecisionEngine,
    InnovationOutcome,
    InnovationPath,
    NoFeasibleInnovationPath,
)


class InnovationDecisionEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = FrontierEvent(
            title="Portable component runtime release",
            technology="Example Runtime",
            domain="portable_components",
            source_url="https://example.com/releases/runtime-2",
            published_at="2026-08-22T00:00:00Z",
            maturity="release_candidate",
            summary="Adds a portable component execution surface with explicit interface boundaries.",
        )
        event_id = self.event.event_id
        self.implement = InnovationPath(
            event_id=event_id,
            path_id="implement-adapter",
            outcome=InnovationOutcome.IMPLEMENT,
            capability_gain=0.95,
            boundary_fit=0.92,
            evidence_confidence=0.90,
            composability=0.96,
            reversibility=0.85,
            implementation_cost=0.55,
            execution_risk=0.25,
            time_to_proof=0.45,
            evidence_refs=("primary:release", "repo:adapter-proof"),
            executable_ref="experiments/runtime_adapter.py",
        )
        self.experiment = InnovationPath(
            event_id=event_id,
            path_id="bounded-experiment",
            outcome=InnovationOutcome.EXPERIMENT,
            capability_gain=0.75,
            boundary_fit=0.94,
            evidence_confidence=0.82,
            composability=0.88,
            reversibility=0.98,
            implementation_cost=0.20,
            execution_risk=0.08,
            time_to_proof=0.15,
            evidence_refs=("primary:release",),
            executable_ref="experiments/runtime_probe.py",
        )
        self.dominated = InnovationPath(
            event_id=event_id,
            path_id="weak-adapter",
            outcome=InnovationOutcome.IMPLEMENT,
            capability_gain=0.60,
            boundary_fit=0.70,
            evidence_confidence=0.70,
            composability=0.70,
            reversibility=0.70,
            implementation_cost=0.80,
            execution_risk=0.40,
            time_to_proof=0.80,
            evidence_refs=("primary:release",),
            executable_ref="experiments/weak.py",
        )
        self.engine = InnovationDecisionEngine(
            self.event, (self.implement, self.experiment, self.dominated)
        )

    def test_maximum_advance_selects_strongest_capability_path(self) -> None:
        decision = self.engine.decide(preference="maximum_advance")
        self.assertEqual(decision.selected.path_id, "implement-adapter")
        self.assertEqual(decision.evidence_state, EVIDENCE_STATE)
        self.assertEqual(decision.as_dict()["execution_claim"], "SELECTION_ONLY_NOT_EXECUTION")

    def test_fast_proof_preserves_experiment_as_real_frontier_choice(self) -> None:
        decision = self.engine.decide(preference="fast_proof")
        self.assertEqual(decision.selected.path_id, "bounded-experiment")
        self.assertEqual(decision.selected.outcome, InnovationOutcome.EXPERIMENT)

    def test_pareto_frontier_removes_strictly_dominated_path(self) -> None:
        frontier = self.engine.feasible_frontier()
        ids = {path.path_id for path in frontier}
        self.assertIn("implement-adapter", ids)
        self.assertIn("bounded-experiment", ids)
        self.assertNotIn("weak-adapter", ids)

    def test_constraints_can_force_safe_bounded_experiment(self) -> None:
        decision = self.engine.decide(
            InnovationConstraints(max_execution_risk=0.10, max_implementation_cost=0.30),
            preference="maximum_advance",
        )
        self.assertEqual(decision.selected.path_id, "bounded-experiment")

    def test_action_without_executable_reference_is_not_silently_actionable(self) -> None:
        path = InnovationPath(
            event_id=self.event.event_id,
            path_id="paper-only",
            outcome=InnovationOutcome.IMPLEMENT,
            capability_gain=1.0,
            boundary_fit=1.0,
            evidence_confidence=1.0,
            composability=1.0,
            reversibility=1.0,
            implementation_cost=0.0,
            execution_risk=0.0,
            time_to_proof=0.0,
            evidence_refs=("primary:release",),
            executable_ref=None,
        )
        engine = InnovationDecisionEngine(self.event, (path,))
        with self.assertRaises(NoFeasibleInnovationPath):
            engine.decide()

    def test_hold_and_reject_require_actual_reason(self) -> None:
        with self.assertRaises(ValueError):
            InnovationPath(
                event_id=self.event.event_id,
                path_id="hold",
                outcome=InnovationOutcome.HOLD,
                capability_gain=0,
                boundary_fit=0,
                evidence_confidence=0,
                composability=0,
                reversibility=1,
                implementation_cost=0,
                execution_risk=0,
                time_to_proof=0,
            )

    def test_decision_digest_is_deterministic(self) -> None:
        left = self.engine.decide(preference="balanced")
        right = self.engine.decide(preference="balanced")
        self.assertEqual(left.digest, right.digest)
        self.assertEqual(len(left.digest), 64)

    def test_event_binding_prevents_cross_signal_path_reuse(self) -> None:
        alien = InnovationPath(
            event_id="not-this-event",
            path_id="alien",
            outcome=InnovationOutcome.EXPERIMENT,
            capability_gain=0.5,
            boundary_fit=0.5,
            evidence_confidence=0.5,
            composability=0.5,
            reversibility=1.0,
            implementation_cost=0.1,
            execution_risk=0.1,
            time_to_proof=0.1,
            evidence_refs=("source",),
            executable_ref="probe.py",
        )
        with self.assertRaises(ValueError):
            InnovationDecisionEngine(self.event, (alien,))


if __name__ == "__main__":
    unittest.main()
