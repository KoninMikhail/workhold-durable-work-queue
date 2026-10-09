"""Static contract locks for SENTRY_DSN / SENTRY_DSN_FILE (Phase 10-04)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = ROOT / ".env.example"
ENTRYPOINT = ROOT / "docker" / "entrypoint.sh"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_env_example_documents_optional_exclusive_sentry_dsn_pair() -> None:
    assert ENV_EXAMPLE.is_file(), ENV_EXAMPLE
    text = _read(ENV_EXAMPLE)
    for token in ("SENTRY_DSN", "SENTRY_DSN_FILE", "GlitchTip"):
        assert token in text, token
    lowered = text.lower()
    assert "exclusive" in lowered or "both" in lowered
    assert "SENTRY_ENABLED" not in text
    assert "SENTRY_DEBUG" not in text
    assert re.search(r"(?m)^SENTRY_DSN=https://", text) is None
    assert "DATABASE_URL=" in text


def test_entrypoint_allowlist_includes_sentry_dsn_when_present() -> None:
    if not ENTRYPOINT.is_file():
        pytest.skip("Phase 8 docker/entrypoint.sh not landed")
    text = _read(ENTRYPOINT)
    raw = ENTRYPOINT.read_bytes()
    assert "file_env" in text
    assert "SENTRY_DSN" in text
    assert "set -x" not in text
    assert b"\r" not in raw
    assert 'exec workhold "$@"' in text
