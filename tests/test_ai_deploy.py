"""Runner tests for the token gate + centralized env-command guard
(ops/ai_deploy.py rework).

Extends the old PR-2 runner tests (4/4 shape: env spec ignored, env
command without marker refused, host-marked env command honored, repo
spec honored) with the holes the old PR-2 run left open: the unguarded
ROLLBACK_COMMAND in the no-spec branch, both env commands in the
raw-spec-but-no-pipeline branch, and the phase() env fallback — plus the
new DEPLOY_APPROVAL token gate in main().

Test-only secret: "test-secret-" * 4. Deploy commands only ever `touch`
a marker file inside a tmp dir; nothing else is executed.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ops"))

import ai_deploy  # noqa: E402
from approval import ApprovalVerifier  # noqa: E402

TEST_SECRET = "test-secret-" * 4  # 48 chars, test-only

ENV_KEYS = [
    "DEPLOY_SPEC_JSON",
    "DEPLOY_COMMAND",
    "DEPLOY_COMMAND_SOURCE",
    "ROLLBACK_COMMAND",
    "ROLLBACK_COMMAND_SOURCE",
    "HEALTH_URL",
    "DEPLOY_APPROVAL_TOKEN",
    "DEPLOY_APPROVAL_SECRET",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _load(root, monkeypatch, env=None):
    if env:
        for key, value in env.items():
            monkeypatch.setenv(key, value)
    return ai_deploy.load_spec(Path(root), argparse.Namespace(spec=None))


def _write_spec(root, spec):
    Path(root, "deploy.spec.json").write_text(json.dumps(spec))


def _valid_token_env(root, monkeypatch):
    """Mint a token bound to this checkout's config and put it in env."""
    config_sha = ai_deploy.deploy_config_sha(Path(root), None)
    verifier = ApprovalVerifier(TEST_SECRET)
    token = verifier.mint(config_sha=config_sha, granted_by="tester")
    monkeypatch.setenv("DEPLOY_APPROVAL_TOKEN", json.dumps(token))
    monkeypatch.setenv("DEPLOY_APPROVAL_SECRET", TEST_SECRET)
    return token


# --- Old PR-2 4/4 shape, kept -------------------------------------------

def test_env_spec_ignored(tmp_path, monkeypatch):
    spec = _load(tmp_path, monkeypatch, {
        "DEPLOY_SPEC_JSON": json.dumps({"pipeline": {"deploy": ["echo PWNED"]}}),
    })
    assert spec.pipeline.deploy == []


def test_no_spec_deploy_command_without_marker_refused(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="DEPLOY_COMMAND refused"):
        _load(tmp_path, monkeypatch, {"DEPLOY_COMMAND": "echo PWNED"})


def test_no_spec_deploy_command_with_marker_honored(tmp_path, monkeypatch):
    spec = _load(tmp_path, monkeypatch, {
        "DEPLOY_COMMAND": "echo ok", "DEPLOY_COMMAND_SOURCE": "repo-secret",
    })
    assert spec.pipeline.deploy == ["echo ok"]


def test_repo_spec_file_honored(tmp_path, monkeypatch):
    _write_spec(tmp_path, {"pipeline": {"deploy": ["echo from-repo-spec"]}})
    spec = _load(tmp_path, monkeypatch)
    assert spec.pipeline.deploy == ["echo from-repo-spec"]


# --- The holes: ROLLBACK_COMMAND was unguarded ---------------------------

def test_no_spec_rollback_command_without_marker_refused(tmp_path, monkeypatch):
    """Was the hole: ROLLBACK_COMMAND ran unguarded in the no-spec branch."""
    with pytest.raises(ValueError, match="ROLLBACK_COMMAND refused"):
        _load(tmp_path, monkeypatch, {"ROLLBACK_COMMAND": "echo PWNED"})


def test_no_spec_rollback_command_with_marker_honored(tmp_path, monkeypatch):
    spec = _load(tmp_path, monkeypatch, {
        "ROLLBACK_COMMAND": "echo rb", "ROLLBACK_COMMAND_SOURCE": "repo-secret",
    })
    assert spec.pipeline.rollback == ["echo rb"]


# --- Raw spec, no pipeline: BOTH env commands were unguarded -------------

def test_raw_spec_no_pipeline_both_refused_without_markers(tmp_path, monkeypatch):
    _write_spec(tmp_path, {"version": 1})
    with pytest.raises(ValueError, match="DEPLOY_COMMAND refused"):
        _load(tmp_path, monkeypatch, {"DEPLOY_COMMAND": "echo PWNED"})
    # Clear DEPLOY_COMMAND so the second load reaches the rollback guard.
    monkeypatch.delenv("DEPLOY_COMMAND", raising=False)
    with pytest.raises(ValueError, match="ROLLBACK_COMMAND refused"):
        _load(tmp_path, monkeypatch, {"ROLLBACK_COMMAND": "echo PWNED"})


def test_raw_spec_no_pipeline_both_honored_with_markers(tmp_path, monkeypatch):
    _write_spec(tmp_path, {"version": 1})
    spec = _load(tmp_path, monkeypatch, {
        "DEPLOY_COMMAND": "echo ok", "DEPLOY_COMMAND_SOURCE": "repo-secret",
        "ROLLBACK_COMMAND": "echo rb", "ROLLBACK_COMMAND_SOURCE": "repo-secret",
    })
    assert spec.pipeline.deploy == ["echo ok"]
    assert spec.pipeline.rollback == ["echo rb"]


# --- phase() env fallback: was unguarded ---------------------------------

def test_phase_env_fallback_refused_without_markers(tmp_path, monkeypatch):
    _write_spec(tmp_path, {"pipeline": {"test": ["echo t"]}})
    with pytest.raises(ValueError, match="DEPLOY_COMMAND refused"):
        _load(tmp_path, monkeypatch, {"DEPLOY_COMMAND": "echo PWNED"})
    # Clear DEPLOY_COMMAND so the second load reaches the rollback guard.
    monkeypatch.delenv("DEPLOY_COMMAND", raising=False)
    with pytest.raises(ValueError, match="ROLLBACK_COMMAND refused"):
        _load(tmp_path, monkeypatch, {"ROLLBACK_COMMAND": "echo PWNED"})


def test_phase_env_fallback_honored_with_markers(tmp_path, monkeypatch):
    _write_spec(tmp_path, {"pipeline": {"test": ["echo t"]}})
    spec = _load(tmp_path, monkeypatch, {
        "DEPLOY_COMMAND": "echo ok", "DEPLOY_COMMAND_SOURCE": "repo-secret",
        "ROLLBACK_COMMAND": "echo rb", "ROLLBACK_COMMAND_SOURCE": "repo-secret",
    })
    assert spec.pipeline.deploy == ["echo ok"]
    assert spec.pipeline.rollback == ["echo rb"]


def test_phase_spec_command_beats_env(tmp_path, monkeypatch):
    """A spec-listed phase wins over the env fallback, as before."""
    _write_spec(tmp_path, {"pipeline": {"deploy": ["echo from-spec"]}})
    spec = _load(tmp_path, monkeypatch, {
        "DEPLOY_COMMAND": "echo env", "DEPLOY_COMMAND_SOURCE": "repo-secret",
    })
    assert spec.pipeline.deploy == ["echo from-spec"]


# --- Token gate in main(): CONFIG_ERROR 78 before anything runs ---------

def test_main_missing_token_returns_78_and_runs_nothing(tmp_path, monkeypatch, capsys):
    _write_spec(tmp_path, {"pipeline": {"deploy": ["touch marker.txt"]}})
    marker = tmp_path / "marker.txt"
    assert ai_deploy.main(["--root", str(tmp_path)]) == 78
    assert not marker.exists()
    assert "DEPLOY_APPROVAL token missing" in capsys.readouterr().err


def test_main_missing_secret_returns_78_and_runs_nothing(tmp_path, monkeypatch, capsys):
    _write_spec(tmp_path, {"pipeline": {"deploy": ["touch marker.txt"]}})
    config_sha = ai_deploy.deploy_config_sha(tmp_path, None)
    token = ApprovalVerifier(TEST_SECRET).mint(
        config_sha=config_sha, granted_by="tester"
    )
    monkeypatch.setenv("DEPLOY_APPROVAL_TOKEN", json.dumps(token))
    assert ai_deploy.main(["--root", str(tmp_path)]) == 78
    assert not (tmp_path / "marker.txt").exists()
    assert "DEPLOY_APPROVAL secret missing" in capsys.readouterr().err


def test_main_valid_token_runs_deploy(tmp_path, monkeypatch):
    _write_spec(tmp_path, {"pipeline": {"deploy": ["touch marker.txt"]}})
    _valid_token_env(tmp_path, monkeypatch)
    assert ai_deploy.main(["--root", str(tmp_path)]) == 0
    assert (tmp_path / "marker.txt").exists()


def test_main_token_for_different_config_refused(tmp_path, monkeypatch, capsys):
    _write_spec(tmp_path, {"pipeline": {"deploy": ["touch marker.txt"]}})
    token = ApprovalVerifier(TEST_SECRET).mint(
        config_sha="0" * 64, granted_by="tester"
    )
    monkeypatch.setenv("DEPLOY_APPROVAL_TOKEN", json.dumps(token))
    monkeypatch.setenv("DEPLOY_APPROVAL_SECRET", TEST_SECRET)
    assert ai_deploy.main(["--root", str(tmp_path)]) == 78
    assert not (tmp_path / "marker.txt").exists()
    assert "binding mismatch: config_sha" in capsys.readouterr().err


def test_main_print_spec_stays_token_free(tmp_path, monkeypatch, capsys):
    _write_spec(tmp_path, {"pipeline": {"deploy": ["touch marker.txt"]}})
    assert ai_deploy.main(["--root", str(tmp_path), "--print-spec"]) == 0
    assert "touch marker.txt" in capsys.readouterr().out
    assert not (tmp_path / "marker.txt").exists()
