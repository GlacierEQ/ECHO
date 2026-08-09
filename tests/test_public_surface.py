from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"


def test_public_readme_excludes_sensitive_domain_repository_identifiers() -> None:
    text = README.read_text(encoding="utf-8").casefold()
    forbidden = (
        "hawaii-family-court-legal-automation",
        "family-court",
        "1fdv",
    )
    assert all(marker not in text for marker in forbidden)


def test_public_readme_declares_sanitized_capability_boundary() -> None:
    text = README.read_text(encoding="utf-8").casefold()
    assert "only separately admitted, sanitized capabilities may cross this boundary" in text
    assert "case-specific" in text
    assert (
        "a private/domain system can contribute only a separately sanitized "
        "transferable capability after its own admission gate passes."
    ) in text
