# AKOS ↔ ECHO Pillar–Piston Contract

## Roles

| System | Role | Responsibility |
|--------|------|----------------|
| **AKOS** | The Pillar (Governance) | Identity · Truth · Authority · Evidence · Structure · Contracts |
| **ECHO** | The Piston (Operation) | Orchestration · Continuity · Context · Synchronization · Recall · Routing · Execution Flow |

## Authority boundary

- ECHO never claims authority over truth or identity.
- Every privileged mutation that affects governed state must carry an AKOS authority envelope (future wire).
- ECHO produces operational receipts; AKOS verifies and promotes them into the evidence ledger.

## Data flow (canonical cycle)

```
REMEMBER → RECONCILE → AUTHORIZE → ROUTE → EXECUTE
→ RECEIPT → VERIFY → PERSIST → OBSERVE → REPAIR → IMPROVE → REPEAT
```

## Self-evolution relationship

```
ECHO OBSERVES
→ AKOS GOVERNS
→ ECHO IMPROVES
→ AKOS VERIFIES
```

This contract is deployed on AKOS `main` and is the binding operating model for both systems.
