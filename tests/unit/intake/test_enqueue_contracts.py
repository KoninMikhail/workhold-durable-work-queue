"""Unit tests for producer enqueue intake contracts (WORK-02, WORK-10, API-02, WORK-16).

Phase 12 Wave 0 priority scaffolds are temporarily skipped; Plan 04 removes the markers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from queue_service.intake.contracts import (
    FINGERPRINT_SIZE_BYTES,
    EnqueueCommand,
    IntakeValidationError,
    normalize_enqueue_command,
)
from queue_service.priority import PRIORITY_MAX, PRIORITY_MIN

_SECRET_PAYLOAD = {"password": "hunter2", "nested": {"token": "leak-me"}}
_NOW = datetime(2026, 9, 18, 12, 0, 0, tzinfo=UTC)


def _normalize(**overrides: Any) -> EnqueueCommand:
    base: dict[str, Any] = {
        "producer_id": "producer-a",
        "queue_name": "orders",
        "idempotency_key": "idem-1",
        "payload": {"a": 1, "b": [True, None]},
        "priority": 0,
        "available_at": None,
    }
    base.update(overrides)
    return normalize_enqueue_command(**base)


def test_semantically_identical_requests_share_exact_32_byte_fingerprint() -> None:
    left = _normalize(payload={"b": [True, None], "a": 1})
    right = _normalize(payload={"a": 1, "b": [True, None]})
    assert isinstance(left.fingerprint, bytes)
    assert len(left.fingerprint) == FINGERPRINT_SIZE_BYTES
    assert left.fingerprint == right.fingerprint
    assert left.fingerprint == _normalize().fingerprint


def test_queue_visible_field_changes_alter_fingerprint() -> None:
    base = _normalize()
    changed_payload = _normalize(payload={"a": 2, "b": [True, None]})
    assert changed_payload.fingerprint != base.fingerprint

    with_past_availability = _normalize(
        available_at=_NOW - timedelta(seconds=1),
    )
    assert with_past_availability.fingerprint != base.fingerprint


def test_repeated_normalization_is_byte_for_byte_stable() -> None:
    first = _normalize()
    second = _normalize()
    third = _normalize(payload={"b": [True, None], "a": 1})
    assert first.fingerprint == second.fingerprint == third.fingerprint


@pytest.mark.parametrize(
    "idempotency_key",
    [None, "", " ", "\t", "x" * 257],
)
def test_missing_blank_or_overlong_idempotency_key_is_stable_error(
    idempotency_key: str | None,
) -> None:
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(idempotency_key=idempotency_key)
    err = exc_info.value
    assert err.code == "idempotency_key_required"
    assert err.retryable is False
    assert "hunter2" not in err.message
    assert "hunter2" not in repr(err)
    assert _SECRET_PAYLOAD["password"] not in err.message


def test_zero_priority_normalizes_to_zero() -> None:
    zero = _normalize(priority=0)
    assert zero.priority == 0
    assert zero.fingerprint == _normalize().fingerprint


def test_omitted_immediate_availability_accepted() -> None:
    cmd = _normalize(available_at=None)
    assert cmd.available_at is None
    assert cmd.priority == 0


def test_aware_future_available_at_accepted_for_fingerprinting() -> None:
    future = _NOW + timedelta(hours=1)
    cmd = _normalize(available_at=future, payload=_SECRET_PAYLOAD)
    assert cmd.available_at == future
    assert cmd.fingerprint != _normalize().fingerprint


def test_aware_past_and_current_available_at_accepted_unchanged() -> None:
    past = _NOW - timedelta(seconds=30)
    current = _NOW
    past_cmd = _normalize(available_at=past)
    current_cmd = _normalize(available_at=current)
    assert past_cmd.available_at == past
    assert current_cmd.available_at == current


def test_naive_available_at_rejected_without_payload_leakage() -> None:
    naive = datetime(2026, 9, 18, 12, 0, 0)
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(available_at=naive, payload=_SECRET_PAYLOAD)
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False
    assert err.details.get("field") == "available_at"
    assert "hunter2" not in err.message
    assert "leak-me" not in err.message
    assert "hunter2" not in repr(err)
    assert "leak-me" not in repr(err)
    assert str(_SECRET_PAYLOAD) not in err.message


def test_errors_never_include_payload_or_credentials() -> None:
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(priority="7", payload=_SECRET_PAYLOAD)  # type: ignore[arg-type]
    err = exc_info.value
    blob = f"{err!r}|{err}|{err.message}|{err.code}|{err.details!r}"
    assert "hunter2" not in blob
    assert "leak-me" not in blob
    assert "password" not in blob


def test_command_carries_authenticated_scope_not_payload_identity() -> None:
    cmd = _normalize(producer_id="auth-producer", queue_name="billing.events")
    assert cmd.producer_id == "auth-producer"
    assert cmd.queue_name == "billing.events"
    # Scope changes do not alter the request fingerprint (scope is separate).
    other_scope = _normalize(producer_id="other", queue_name="other.queue")
    assert cmd.fingerprint == other_scope.fingerprint


def test_oversized_payload_rejected_before_fingerprint_without_body_echo() -> None:
    huge = {"blob": "x" * (1_048_576)}
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(payload=huge)
    err = exc_info.value
    assert err.code == "payload_too_large"
    assert err.retryable is False
    assert "x" * 32 not in err.message


# ---------------------------------------------------------------------------
# Phase 12 WORK-16 bounded priority (Plan 04)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("priority", [PRIORITY_MIN, PRIORITY_MAX, 42, -100])
def test_bounded_priority_endpoints_and_non_zero_accepted_exactly(
    priority: int,
) -> None:
    cmd = _normalize(priority=priority)
    assert type(cmd.priority) is int
    assert cmd.priority == priority


@pytest.mark.parametrize(
    "priority",
    [PRIORITY_MIN - 1, PRIORITY_MAX + 1],
)
def test_priority_outside_signed_smallint_range_rejected_without_coercion(
    priority: int,
) -> None:
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(priority=priority, payload=_SECRET_PAYLOAD)
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False
    assert "hunter2" not in err.message
    assert "hunter2" not in repr(err)
    details = err.details or {}
    assert "min" in details or "max" in details or "field" in details


@pytest.mark.parametrize(
    "priority",
    [
        True,
        False,
        "100",
        1.5,
        float(PRIORITY_MAX),
    ],
)
def test_priority_non_integer_inputs_rejected_without_coercion(
    priority: object,
) -> None:
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(priority=priority)  # type: ignore[arg-type]
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False


def test_required_priority_field_rejects_null_without_coercion() -> None:
    with pytest.raises(IntakeValidationError) as exc_info:
        _normalize(priority=None)
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False


def test_same_priority_produces_identical_fingerprint_for_replay_identity() -> None:
    left = _normalize(priority=500, idempotency_key="idem-priority-replay")
    right = _normalize(
        priority=500,
        idempotency_key="idem-priority-replay",
        payload={"b": [True, None], "a": 1},
    )
    assert left.fingerprint == right.fingerprint
    assert left.priority == right.priority == 500


def test_changed_priority_alters_fingerprint_for_replay_conflict() -> None:
    low = _normalize(priority=100, idempotency_key="idem-priority-conflict")
    high = _normalize(priority=200, idempotency_key="idem-priority-conflict")
    assert low.fingerprint != high.fingerprint
