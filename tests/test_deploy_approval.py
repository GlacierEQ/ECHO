"""Tests for the exact-bound deploy approval token (ops/approval.py).

Covers the APEX ApprovalVerifier mirror: mint -> verify round-trip,
expiry, binding, signature, in-process nonce replay, missing token/secret,
short secret, and the mint CLI end-to-end incl. fingerprint agreement
between mint and a runner-side computation.

Test-only secret: "test-secret-" * 4. Never a production secret.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ops"))

import ai_deploy  # noqa: E402  (runner-side fingerprint computation)
from approval import (  # noqa: E402
    ApprovalVerifier,
    build_expected_request,
    config_fingerprint,
)

TEST_SECRET = "test-secret-" * 4  # 48 chars, test-only
OTHER_SECRET = "other-secret-" * 4
CONFIG_SHA = "a" * 64


def _verifier(secret: str = TEST_SECRET) -> ApprovalVerifier:
    return ApprovalVerifier(secret)


def _expected(token: dict, config_sha: str = CONFIG_SHA) -> dict:
    req = token["request"]
    return build_expected_request(
        config_sha=config_sha, expires_at=req["expires_at"], nonce=req["nonce"]
    )


def _mint(verifier: ApprovalVerifier, config_sha: str = CONFIG_SHA, **kw) -> dict:
    return verifier.mint(config_sha=config_sha, granted_by="tester", **kw)


def test_mint_verify_round_trip():
    verifier = _verifier()
    token = _mint(verifier)
    assert verifier.verify(token, _expected(token)) == token
    assert token["request"]["work_unit_id"] == "GlacierEQ/ECHO"
    assert token["request"]["action_id"] == "deploy"
    assert token["request"]["capability"] == "deploy"
    assert token["request"]["route"] == "main"
    assert token["request"]["config_sha"] == CONFIG_SHA
    assert token["request"]["expires_at"].endswith("Z")
    assert len(token["request"]["nonce"]) == 32
    assert all(c in "0123456789abcdef" for c in token["request"]["nonce"])
    assert len(token["signature"]) == 64


def test_expired_token_refused():
    verifier = _verifier()
    token = _mint(verifier, ttl_seconds=-60)
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL expired"):
        verifier.verify(token, _expected(token))


def test_expired_token_refused_with_injected_now():
    verifier = _verifier()
    token = _mint(verifier, ttl_seconds=3600)
    future = datetime.now(timezone.utc) + timedelta(seconds=7200)
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL expired"):
        verifier.verify(token, _expected(token), now=future)


def test_wrong_binding_config_sha_refused():
    verifier = _verifier()
    token = _mint(verifier, config_sha="b" * 64)  # minted for another config
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL binding mismatch: config_sha"):
        verifier.verify(token, _expected(token, config_sha=CONFIG_SHA))


def test_wrong_binding_route_refused():
    verifier = _verifier()
    token = _mint(verifier, route="staging")
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL binding mismatch: route"):
        verifier.verify(token, _expected(token))


def test_wrong_binding_action_refused():
    verifier = _verifier()
    token = _mint(verifier, action_id="rollback")
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL binding mismatch: action_id"):
        verifier.verify(token, _expected(token))


def test_tampered_signature_refused():
    verifier = _verifier()
    token = _mint(verifier)
    bad = dict(token)
    bad["signature"] = ("0" if token["signature"][0] != "0" else "1") + token["signature"][1:]
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL signature invalid"):
        verifier.verify(bad, _expected(token))


def test_token_signed_with_other_secret_refused():
    other = _verifier(OTHER_SECRET)
    token = _mint(other)
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL signature invalid"):
        _verifier().verify(token, _expected(token))


def test_replayed_nonce_refused_in_process():
    verifier = _verifier()
    token = _mint(verifier)
    verifier.verify(token, _expected(token))
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL token already consumed"):
        verifier.verify(token, _expected(token))


def test_missing_token_refused():
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL token missing"):
        _verifier().verify(None, _expected(_mint(_verifier())))


def test_short_secret_refused_at_verifier_construction():
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL secret too short"):
        ApprovalVerifier("short")
    with pytest.raises(ValueError, match="DEPLOY_APPROVAL secret too short"):
        ApprovalVerifier(b"x" * 31)
    # Exactly 32 bytes is accepted.
    ApprovalVerifier("x" * 32)


def test_config_fingerprint_changes_with_spec_and_project_type(tmp_path):
    spec = tmp_path / "deploy.spec.json"
    spec.write_text(json.dumps({"pipeline": {"deploy": ["echo one"]}}))
    sha1 = config_fingerprint(tmp_path, spec, "unknown")
    spec.write_text(json.dumps({"pipeline": {"deploy": ["echo two"]}}))
    sha2 = config_fingerprint(tmp_path, spec, "unknown")
    assert sha1 != sha2
    assert config_fingerprint(tmp_path, spec, "python") != sha2
    assert config_fingerprint(tmp_path, None, "unknown") != sha1


def test_mint_cli_end_to_end_and_fingerprint_agreement(tmp_path):
    (tmp_path / "deploy.spec.json").write_text(
        json.dumps({"pipeline": {"deploy": ["echo hi"]}})
    )
    approval_py = Path(__file__).resolve().parents[1] / "ops" / "approval.py"
    env = {**os.environ, "DEPLOY_APPROVAL_SECRET": TEST_SECRET}
    minted = subprocess.run(
        [sys.executable, str(approval_py), "mint", "--root", str(tmp_path),
         "--granted-by", "tester"],
        capture_output=True, text=True, env=env, check=True,
    )
    token = json.loads(minted.stdout)

    # Runner-side computation (ops/ai_deploy.py) must agree with mint.
    runner_sha = ai_deploy.deploy_config_sha(tmp_path, None)
    assert token["request"]["config_sha"] == runner_sha

    # The fingerprint subcommand must agree with both.
    fingerprinted = subprocess.run(
        [sys.executable, str(approval_py), "fingerprint", "--root", str(tmp_path)],
        capture_output=True, text=True, env=env, check=True,
    )
    assert fingerprinted.stdout.strip() == runner_sha

    # And the minted token must verify against the runner-side fingerprint.
    verifier = _verifier()
    expected = build_expected_request(
        config_sha=runner_sha,
        expires_at=token["request"]["expires_at"],
        nonce=token["request"]["nonce"],
    )
    assert verifier.verify(token, expected) == token


def test_mint_cli_short_secret_refused(tmp_path):
    approval_py = Path(__file__).resolve().parents[1] / "ops" / "approval.py"
    env = {**os.environ, "DEPLOY_APPROVAL_SECRET": "short"}
    proc = subprocess.run(
        [sys.executable, str(approval_py), "mint", "--root", str(tmp_path)],
        capture_output=True, text=True, env=env,
    )
    assert proc.returncode != 0
    assert "DEPLOY_APPROVAL secret too short" in proc.stderr


def test_mint_cli_missing_secret_refused(tmp_path):
    approval_py = Path(__file__).resolve().parents[1] / "ops" / "approval.py"
    env = {k: v for k, v in os.environ.items() if k != "DEPLOY_APPROVAL_SECRET"}
    proc = subprocess.run(
        [sys.executable, str(approval_py), "mint", "--root", str(tmp_path)],
        capture_output=True, text=True, env=env,
    )
    assert proc.returncode != 0
    assert "DEPLOY_APPROVAL secret missing" in proc.stderr
