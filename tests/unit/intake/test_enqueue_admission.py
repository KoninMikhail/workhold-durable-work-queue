"""Unit tests for producer enqueue byte/cardinality admission (OPS-04, API-02)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from workhold.intake.admission import (
    DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
    DEFAULT_PAYLOAD_MAX_BYTES,
    DEFAULT_REQUEST_MAX_BYTES,
    EnqueueAdmissionLimits,
    validate_producer_enqueue_admission,
)
from workhold.intake.contracts import IntakeValidationError

_SECRET = {"password": "hunter2", "token": "leak-me"}


def _body(*, payload: Any = None, **extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "payload": payload if payload is not None else {"n": 1},
        "priority": 0,
    }
    body.update(extra)
    return body


def _encode(body: dict[str, Any]) -> bytes:
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


def _admit(
    *,
    body: dict[str, Any] | None = None,
    idempotency_key: str | None = "idem-1",
    body_bytes: bytes | None = None,
    limits: EnqueueAdmissionLimits | None = None,
) -> None:
    request = body if body is not None else _body()
    validate_producer_enqueue_admission(
        idempotency_key=idempotency_key,
        body=request,
        body_bytes=body_bytes if body_bytes is not None else _encode(request),
        limits=limits,
    )


def _string_payload_with_encoded_size(size: int) -> str:
    """JSON string payloads add two quote bytes when encoded."""
    assert size >= 2
    return "p" * (size - 2)


def test_exact_payload_and_request_byte_limits_pass() -> None:
    payload = _string_payload_with_encoded_size(DEFAULT_PAYLOAD_MAX_BYTES)
    payload_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    assert len(payload_bytes) == DEFAULT_PAYLOAD_MAX_BYTES
    _admit(body=_body(payload=payload))

    tiny = _body(payload={"ok": True})
    encoded = _encode(tiny)
    assert len(encoded) < DEFAULT_REQUEST_MAX_BYTES
    limits = EnqueueAdmissionLimits(
        max_payload_bytes=min(DEFAULT_PAYLOAD_MAX_BYTES, len(encoded)),
        max_request_bytes=len(encoded),
        max_idempotency_key_chars=DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
    )
    _admit(body=tiny, body_bytes=encoded, limits=limits)


def test_one_byte_over_payload_is_non_retryable_payload_too_large() -> None:
    payload = _string_payload_with_encoded_size(DEFAULT_PAYLOAD_MAX_BYTES + 1)
    with pytest.raises(IntakeValidationError) as exc_info:
        _admit(body=_body(payload=payload))
    err = exc_info.value
    assert err.code == "payload_too_large"
    assert err.retryable is False
    assert err.retry_after_ms is None
    assert "hunter2" not in err.message
    assert "hunter2" not in repr(err)
    assert _SECRET["token"] not in err.message


def test_one_byte_over_request_body_is_non_retryable_payload_too_large() -> None:
    body = _body(payload={"ok": True})
    encoded = _encode(body)
    limits = EnqueueAdmissionLimits(
        max_payload_bytes=min(DEFAULT_PAYLOAD_MAX_BYTES, len(encoded)),
        max_request_bytes=len(encoded),
        max_idempotency_key_chars=DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
    )
    _admit(body=body, body_bytes=encoded, limits=limits)

    with pytest.raises(IntakeValidationError) as exc_info:
        _admit(body=body, body_bytes=encoded + b"x", limits=limits)
    err = exc_info.value
    assert err.code == "payload_too_large"
    assert err.retryable is False


def test_exact_idempotency_key_length_passes_and_one_over_fails() -> None:
    exact = "k" * DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS
    _admit(idempotency_key=exact)

    with pytest.raises(IntakeValidationError) as exc_info:
        _admit(idempotency_key=exact + "x")
    err = exc_info.value
    assert err.code == "idempotency_key_required"
    assert err.retryable is False


@pytest.mark.parametrize("smuggled", ["spawn", "events", "delivery_events"])
def test_smuggled_fan_out_fields_are_non_retryable_size_errors(smuggled: str) -> None:
    body = _body(**{smuggled: [{"x": 1}]})
    with pytest.raises(IntakeValidationError) as exc_info:
        _admit(body=body)
    err = exc_info.value
    assert err.code == "payload_too_large"
    assert err.retryable is False
    assert "hunter2" not in err.message


def test_unknown_body_fields_are_validation_failed() -> None:
    body = _body(unexpected=True)
    with pytest.raises(IntakeValidationError) as exc_info:
        _admit(body=body)
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False


def test_non_object_body_is_validation_failed() -> None:
    with pytest.raises(IntakeValidationError) as exc_info:
        validate_producer_enqueue_admission(
            idempotency_key="idem-1",
            body=["not", "an", "object"],  # type: ignore[arg-type]
            body_bytes=b'["not","an","object"]',
        )
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False


def test_deployment_hard_ceiling_cannot_be_raised_above_contract() -> None:
    from workhold.security.payload_policy import HARD_PAYLOAD_CEILING_BYTES

    with pytest.raises(ValueError):
        EnqueueAdmissionLimits(
            max_payload_bytes=HARD_PAYLOAD_CEILING_BYTES + 1,
            max_request_bytes=HARD_PAYLOAD_CEILING_BYTES,
            max_idempotency_key_chars=DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
        )
    with pytest.raises(ValueError):
        EnqueueAdmissionLimits(
            max_payload_bytes=DEFAULT_PAYLOAD_MAX_BYTES,
            max_request_bytes=HARD_PAYLOAD_CEILING_BYTES + 1,
            max_idempotency_key_chars=DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
        )


def test_runtime_may_only_tighten_payload_ceiling() -> None:
    tight = EnqueueAdmissionLimits(
        max_payload_bytes=1024,
        max_request_bytes=DEFAULT_REQUEST_MAX_BYTES,
        max_idempotency_key_chars=DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
    )
    payload = _string_payload_with_encoded_size(1024)
    _admit(body=_body(payload=payload), limits=tight)
    with pytest.raises(IntakeValidationError) as exc_info:
        _admit(
            body=_body(payload=_string_payload_with_encoded_size(1025)),
            limits=tight,
        )
    assert exc_info.value.code == "payload_too_large"


def test_admission_does_not_mutate_or_require_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preflight must fail closed before any storage import side effects."""
    import builtins

    real_import = builtins.__import__

    def _block_storage(name: str, *args: Any, **kwargs: Any):  # noqa: ANN001
        if name.startswith("workhold.storage") or name.startswith("sqlalchemy"):
            raise AssertionError(f"storage import during admission: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_storage)
    _admit(body=_body(payload={"safe": True}))
    with pytest.raises(IntakeValidationError):
        _admit(
            body=_body(
                payload=_string_payload_with_encoded_size(DEFAULT_PAYLOAD_MAX_BYTES + 1)
            )
        )
