"""Static contract locks for DEP-05 secret-file entrypoint (Phase 08-01)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = ROOT / "docker" / "entrypoint.sh"
DOCKERFILE = ROOT / "Dockerfile"
ENV_EXAMPLE = ROOT / ".env.example"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_secret_file_artifacts_exist() -> None:
    assert ENTRYPOINT.is_file(), ENTRYPOINT
    assert DOCKERFILE.is_file(), DOCKERFILE
    assert ENV_EXAMPLE.is_file(), ENV_EXAMPLE


def test_dockerfile_runtime_entrypoint_contract() -> None:
    text = _read_text(DOCKERFILE)
    assert 'ENTRYPOINT ["/app/entrypoint.sh"]' in text
    assert 'CMD ["--help"]' in text
    assert 'ENTRYPOINT ["queue"]' not in text


def test_env_example_documents_file_siblings_and_exclusivity() -> None:
    text = _read_text(ENV_EXAMPLE)
    for name in (
        "DATABASE_URL_FILE",
        "QUEUE_API_BEARER_TOKEN_FILE",
        "QUEUE_API_BEARER_TOKEN_PREVIOUS_FILE",
    ):
        assert name in text, name
    lowered = text.lower()
    assert "exclusive" in lowered or "both" in lowered


def test_entrypoint_script_shape_and_leak_safety() -> None:
    text = _read_text(ENTRYPOINT)
    raw = ENTRYPOINT.read_bytes()
    assert text.startswith("#!/bin/sh")
    assert "exec workhold" in text
    assert "file_env" in text
    assert "materialize_secrets" in text
    for name in (
        "DATABASE_URL",
        "QUEUE_API_BEARER_TOKEN",
        "QUEUE_API_BEARER_TOKEN_PREVIOUS",
        "SENTRY_DSN",
    ):
        assert name in text, name
    assert "set -x" not in text
    assert b"\r" not in raw
    # Negative path: old ENTRYPOINT target must not appear in the script body.
    assert 'ENTRYPOINT ["queue"]' not in text
