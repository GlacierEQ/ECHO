#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR=".verification-artifacts"
DECISION="${ARTIFACT_DIR}/echo-innovation-decision.json"
RECEIPT="${ARTIFACT_DIR}/echo-innovation-decision.receipt.json"
mkdir -p "${ARTIFACT_DIR}"

python -m compileall -q echo tests scripts
ruff check --select F echo tests scripts
pytest tests/ -q --disable-warnings --maxfail=1 | tee "${ARTIFACT_DIR}/pytest.txt"
python -m echo.cli verify | tee "${ARTIFACT_DIR}/echo-cli-verify.txt"

python scripts/innovation_decide.py \
  --input frontier/innovation-demo.json \
  --preference maximum_advance \
  --output "${DECISION}" \
  --receipt "${RECEIPT}" \
  | tee "${ARTIFACT_DIR}/innovation-cli.txt"

python - <<'PY'
import hashlib
import json
from pathlib import Path

path = Path('.verification-artifacts/echo-innovation-decision.json')
receipt_path = Path('.verification-artifacts/echo-innovation-decision.receipt.json')
decision = json.loads(path.read_text(encoding='utf-8'))
receipt = json.loads(receipt_path.read_text(encoding='utf-8'))

assert decision['schema'] == 'glaciereq.echo.innovation-decision.v1'
assert decision['evidence_state'] == 'DETERMINISTIC_ECHO_INNOVATION_DECISION'
assert decision['execution_claim'] == 'SELECTION_ONLY_NOT_EXECUTION'
assert decision['selected']['path_id'] == 'implement-adapter'
assert decision['selected']['outcome'] == 'implement'
assert decision['frontier_count'] >= 2
assert all(row['path_id'] != 'weak-adapter' for row in decision['frontier'])
actual = hashlib.sha256(path.read_bytes()).hexdigest()
assert receipt['artifact_sha256'] == actual
assert receipt['decision_sha256'] == decision['decision_sha256']
assert receipt['verified_state'] == 'DETERMINISTIC_DECISION_EXECUTED'
assert receipt['execution_claim'] == 'DECISION_EXECUTED_SELECTED_ACTION_NOT_YET_EXECUTED'
print(json.dumps({
    'elite_core': 'PASS',
    'selected': decision['selected']['path_id'],
    'frontier_count': decision['frontier_count'],
    'artifact_sha256': actual,
}, indent=2))
PY
