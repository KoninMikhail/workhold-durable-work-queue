"""Contract tests for Phase 10 opt-in GlitchTip documentation (OPS-10 / D-05 / D-10)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

DOC_PATHS = (
    ROOT / "docs" / "05-operations" / "03-observability.md",
    ROOT / "docs" / "05-operations" / "01-security.md",
    ROOT / "docs" / "05-operations" / "02-deployment.md",
    ROOT / "docs" / "03-reference" / "01-commands.md",
    ROOT / "docs" / "_ai" / "tech-stack.md",
)

OPT_IN_H2 = "## Optional error reporting"
OBSERVABILITY_BODY_TOKENS = ("GlitchTip", "Sentry protocol", "sentry_sdk", "SENTRY_DSN")
FORBIDDEN_TOKEN = "SENTRY_ENABLED"
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_sentry_dsn_opt_in_doc_files_exist() -> None:
    for path in DOC_PATHS:
        assert path.is_file(), f"missing {path.relative_to(ROOT)}"


def test_sentry_dsn_opt_in_observability_section() -> None:
    text = _read(DOC_PATHS[0])
    assert OPT_IN_H2 in text
    assert not CYRILLIC_RE.search(OPT_IN_H2)
    for token in OBSERVABILITY_BODY_TOKENS:
        assert token in text, f"missing {token} in observability.md"


def test_sentry_dsn_opt_in_security_and_deployment() -> None:
    security = _read(DOC_PATHS[1])
    deployment = _read(DOC_PATHS[2])
    assert "SENTRY_DSN" in security
    assert "SENTRY_DSN_FILE" in security
    assert "SENTRY_DSN" in deployment


def test_sentry_dsn_opt_in_commands_and_tech_stack() -> None:
    commands = _read(DOC_PATHS[3])
    tech_stack = _read(DOC_PATHS[4])
    assert "sentry-sdk" in commands
    assert "SENTRY_DSN" in commands
    assert "SENTRY_DSN_FILE" in commands
    assert "sentry-sdk" in tech_stack
    assert "SENTRY_DSN" in tech_stack
    assert "observability.md" in tech_stack


def test_sentry_dsn_opt_in_forbidden_tokens_and_no_docs_06_tree() -> None:
    for path in DOC_PATHS:
        text = _read(path)
        assert FORBIDDEN_TOKEN not in text, f"{FORBIDDEN_TOKEN} in {path.name}"
        rel = path.relative_to(ROOT).as_posix()
        assert not rel.startswith("docs/06-"), f"plan 10-05 must not add docs under docs/06-*: {rel}"
