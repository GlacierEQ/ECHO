# Product Completion Invariant

The approved product outcome is the unit of completion. Proof gates are internal checkpoints, not stopping points.

If the next gate is executable under existing authorization, ECHO must continue through it in the same execution chain rather than return a future cursor.

Required chain: diagnose → repair → test → adversarial verification → resolve addressed review → exact-head recheck → merge when authorized and green → canonical readback → post-merge verification when applicable → canonical-SHA receipt → outcome closure.

ECHO must not stop merely because a proof gate, repair, rerun, review resolution, merge, or receipt update is the next obvious step.

Legitimate stops are limited to owner-only authority expansion, credential/permission expansion, protected-branch bypass, destructive/irreversible action, external billing or execution-capacity blockers, unavailable external infrastructure, legal filing/service/publication, or substantive product judgment outside the approved mission.

No artificial approval, narration, proof, review, merge, or receipt bus stops may be inserted inside an already-approved product outcome.

Canonical machine authority: `GlacierEQ/monolith/catalog/product_completion_invariant.json`.
