#!/usr/bin/env bash
# Live smoke test for a deployed ECHO instance.
# Usage: ECHO_URL=http://localhost:8000 ECHO_AKOS_SHARED_SECRET=mysecret bash scripts/smoke_test.sh

set -euo pipefail

ECHO_URL="${ECHO_URL:-http://localhost:8000}"
SECRET="${ECHO_AKOS_SHARED_SECRET:-}"
PASS=0
FAIL=0

green() { printf '\033[32m[PASS]\033[0m %s\n' "$1"; }
red()   { printf '\033[31m[FAIL]\033[0m %s\n' "$1"; }

check() {
  local label="$1"; local expected="$2"; local actual="$3"
  if [[ "$actual" == *"$expected"* ]]; then
    green "$label"
    ((PASS+=1))
  else
    red "$label (expected '$expected', got '$actual')"
    ((FAIL+=1))
  fi
}

if [[ -z "$SECRET" ]]; then
  echo "ERROR: ECHO_AKOS_SHARED_SECRET is not set" && exit 1
fi

make_headers() {
  local actor="$1" scope="$2"
  local ts; ts=$(date +%s)
  local nonce; nonce=$(openssl rand -hex 16)
  local msg; msg=$(printf '%s\n%s\n%s\n%s' "$actor" "$scope" "$ts" "$nonce")
  local sig; sig=$(printf '%s' "$msg" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $2}')
  printf '%s %s %s %s %s' "$ts" "$nonce" "$sig" "$actor" "$scope"
}

# 1. Health
status=$(curl -s -o /dev/null -w "%{http_code}" "$ECHO_URL/health")
check "Health endpoint responds 200" "200" "$status"

# 2. Authorized request succeeds
read -r ts nonce sig actor scope <<< "$(make_headers akos echo:read)"
status=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-AKOS-Actor: $actor" -H "X-AKOS-Scope: $scope" \
  -H "X-AKOS-Timestamp: $ts" -H "X-AKOS-Nonce: $nonce" \
  -H "X-AKOS-Signature: $sig" \
  "$ECHO_URL/conversations")
check "Authorized request succeeds" "200" "$status"

# 3. Invalid signature rejected
status=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-AKOS-Actor: akos" -H "X-AKOS-Scope: echo:read" \
  -H "X-AKOS-Timestamp: $(date +%s)" -H "X-AKOS-Nonce: badfeed" \
  -H "X-AKOS-Signature: deadbeefdeadbeefdeadbeef" \
  "$ECHO_URL/conversations")
check "Invalid signature rejected" "403" "$status"

# 4. Missing scope rejected — request scope:read against a write-only endpoint
read -r ts nonce sig actor scope <<< "$(make_headers akos echo:read)"
status=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -H "X-AKOS-Actor: $actor" -H "X-AKOS-Scope: $scope" \
  -H "X-AKOS-Timestamp: $ts" -H "X-AKOS-Nonce: $nonce" \
  -H "X-AKOS-Signature: $sig" \
  -d '{"source":"s","external_id":"e","title":"t","messages":[{"role":"user","content":"hi"}]}' \
  "$ECHO_URL/conversations")
check "Missing write scope rejected" "403" "$status"

# 5. Conversation ingestion succeeds
read -r ts nonce sig actor scope <<< "$(make_headers akos echo:write)"
response=$(curl -s -w "\n%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -H "X-AKOS-Actor: $actor" -H "X-AKOS-Scope: $scope" \
  -H "X-AKOS-Timestamp: $ts" -H "X-AKOS-Nonce: $nonce" \
  -H "X-AKOS-Signature: $sig" \
  -d "{\"source\":\"smoke\",\"external_id\":\"smoke-$(date +%s)\",\"title\":\"Smoke Test\",\"messages\":[{\"role\":\"user\",\"content\":\"smoke test message\"}]}" \
  "$ECHO_URL/conversations")
body=$(echo "$response" | head -1)
status=$(echo "$response" | tail -1)
check "Conversation ingestion succeeds" "200" "$status"
CONV_ID=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")

# 6. Integrity verification
if [[ -n "$CONV_ID" ]]; then
  read -r ts nonce sig actor scope <<< "$(make_headers akos echo:read)"
  status=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-AKOS-Actor: $actor" -H "X-AKOS-Scope: $scope" \
    -H "X-AKOS-Timestamp: $ts" -H "X-AKOS-Nonce: $nonce" \
    -H "X-AKOS-Signature: $sig" \
    "$ECHO_URL/conversations/$CONV_ID/integrity")
  check "Integrity verification succeeds" "200" "$status"
else
  red "Integrity verification (skipped — no conv_id)"
  ((FAIL+=1))
fi

# 7. Health stats present
body=$(curl -s "$ECHO_URL/health")
check "Health includes stats" "conversations" "$body"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && echo "SMOKE TEST: PASSED" && exit 0 || echo "SMOKE TEST: FAILED" && exit 1
