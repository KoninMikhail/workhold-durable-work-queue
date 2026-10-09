"""Synthetic secret tracking and failure redaction for the public test kit."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Final

from _workhold_client_core.redaction import (
    REDACTED,
    redact_headers,
    redact_text,
    sanitize_for_diagnostics,
)

_PLACEHOLDER: Final[str] = REDACTED
_SYNTHETIC: Final[set[str]] = set()

_BEARER_PREFIX: Final[str] = "testkit-bearer-"
_CLAIM_PREFIX: Final[str] = "testkit-claim-"
_IDEM_PREFIX: Final[str] = "testkit-idem-"


def register_secret(value: str) -> str:
    if value:
        _SYNTHETIC.add(value)
    return value


def clear_registered_secrets() -> None:
    _SYNTHETIC.clear()


def registered_secrets() -> frozenset[str]:
    return frozenset(_SYNTHETIC)


def synthetic_bearer_token(*, suffix: str = "001") -> str:
    return register_secret(f"{_BEARER_PREFIX}{suffix}")


def synthetic_claim_token(*, suffix: str = "001") -> str:
    return register_secret(f"{_CLAIM_PREFIX}{suffix}")


def synthetic_idempotency_key(*, suffix: str = "001") -> str:
    return register_secret(f"{_IDEM_PREFIX}{suffix}")


def redact_failure_text(text: str, *, extra_secrets: Iterable[str] = ()) -> str:
    cleaned = redact_text(text)
    sanitized = sanitize_for_diagnostics({"message": cleaned})
    if isinstance(sanitized, Mapping):
        cleaned_text = str(sanitized.get("message", sanitized))
    else:
        cleaned_text = str(sanitized)

    secrets = sorted(
        {*(s for s in _SYNTHETIC if s), *(s for s in extra_secrets if s)},
        key=len,
        reverse=True,
    )
    for secret in secrets:
        cleaned_text = cleaned_text.replace(secret, _PLACEHOLDER)

    cleaned_text = re.sub(
        rf"{re.escape(_BEARER_PREFIX)}[A-Za-z0-9._\-]+",
        _PLACEHOLDER,
        cleaned_text,
    )
    cleaned_text = re.sub(
        rf"{re.escape(_CLAIM_PREFIX)}[A-Za-z0-9._\-]+",
        _PLACEHOLDER,
        cleaned_text,
    )
    cleaned_text = re.sub(
        rf"{re.escape(_IDEM_PREFIX)}[A-Za-z0-9._\-]+",
        _PLACEHOLDER,
        cleaned_text,
    )
    cleaned_text = re.sub(
        r"(?i)(idempotency[_-]key[\"']?\s*[:=]\s*[\"']?)([^\"'\s,}+]+)",
        rf"\1{_PLACEHOLDER}",
        cleaned_text,
    )
    cleaned_text = re.sub(
        r"(?i)(payload[\"']?\s*[:=]\s*)(\{.*?\}|\".*?\"|'.*?')",
        rf"\1{_PLACEHOLDER}",
        cleaned_text,
        flags=re.DOTALL,
    )
    return cleaned_text


def safe_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    return redact_headers(dict(headers or {}))


def safe_json(value: object) -> object:
    return sanitize_for_diagnostics(value)


class RedactedAssertionError(AssertionError):
    def __init__(self, message: str, *, extra_secrets: Iterable[str] = ()) -> None:
        super().__init__(redact_failure_text(message, extra_secrets=extra_secrets))
