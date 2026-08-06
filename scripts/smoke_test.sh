#!/usr/bin/env bash
# LEGACY optional HMAC authority envelope smoke.
# Canonical live verification is: python scripts/live_smoke.py --base-url ...
# and .github/workflows/live-verify.yml
#
# This script is retained only for advanced AKOS HMAC envelope testing.
# It is NOT required for primary staged-OIDC operation.

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
  echo "This is the LEGACY HMAC envelope smoke."
  echo "Primary live path does not require ECHO_AKOS_SHARED_SECRET."
  echo "Use: python scripts/live_smoke.py --base-url $ECHO_URL"
  exit 2
fi

make_headers() {
  local actor="$1" scope="$2"
  local ts; ts=$(date +%s)
  local nonce; nonce=$(openssl rand -hex 16)
  local msg; msg=$(printf '%s\n%s\n%s\n%s' "$actor" "$scope" "$ts" "$nonce")
  local sig; sig=$(printf '%s' "$msg" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $2}')
  printf '%s %s %s %s %s' "$ts" "$nonce" "$sig" "$actor" "$scope"
}

status=$(curl -s -o /dev/null -w "%{http_code}" "$ECHO_URL/health")
check "Health endpoint responds 200" "200" "$status"

read -r ts nonce sig actor scope <<< "$(make_headers akos echo:read)"
status=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-AKOS-Actor: $actor" -H "X-AKOS-Scope: $scope" \
  -H "X-AKOS-Timestamp: $ts" -H "X-AKOS-Nonce: $nonce" \
  -H "X-AKOS-Signature: $sig" \
  "$ECHO_URL/conversations")
check "Authorized request succeeds" "200" "$status"

status=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-AKOS-Actor: akos" -H "X-AKOS-Scope: echo:read" \
  -H "X-AKOS-Timestamp: $(date +%s)" -H "X-AKOS-Nonce: badfeed" \
  -H "X-AKOS-Signature: deadbeefdeadbeefdeadbeef" \
  "$ECHO_URL/conversations")
check "Invalid signature rejected" "403" "$status"

read -r ts nonce sig actor scope <<< "$(make_headers akos echo:read)"
status=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -H "X-AKOS-Actor: $actor" -H "X-AKOS-Scope: $scope" \
  -H "X-AKOS-Timestamp: $ts" -H "X-AKOS-Nonce: $nonce" \
  -H "X-AKOS-Signature: $sig" \
  -d '{"source":"s","external_id":"e","title":"t","messages":[{"role":"user","content":"hi"}]}' \
  "$ECHO_URL/conversations")
check "Missing write scope rejected" "403" "$status"

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

body=$(curl -s "$ECHO_URL/health")
check "Health responds" "status" "$body"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && echo "LEGACY HMAC SMOKE: PASSED" && exit 0 || echo "LEGACY HMAC SMOKE: FAILED" && exit 1
