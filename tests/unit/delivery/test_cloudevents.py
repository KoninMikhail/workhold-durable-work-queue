"""CloudEvents 1.0 structured-mode contract (DLVR-04 / ADR 021)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from workhold.delivery.cloudevents import (
    CLOUDEVENTS_JSON_MEDIA_TYPE,
    CloudEventInput,
    CloudEventValidationError,
    StoredCloudEvent,
    serialize_structured_event,
    validate_event_input,
)
from workhold.security.payload_policy import HARD_PAYLOAD_CEILING_BYTES

_QUEUE_TIME = datetime(2026, 9, 19, 8, 44, 26, 123456, tzinfo=UTC)
_SECRET_DATA = {"password": "hunter2", "token": "leak-me-not"}
_EVENT_ID = "550e8400-e29b-41d4-a716-446655440000"


def _valid_input(**overrides: Any) -> CloudEventInput:
    base: dict[str, Any] = {
        "source": "https://app.example/orders",
        "type": "com.example.order.created",
        "subject": "order-42",
        "datacontenttype": "application/json",
        "data": {"order_id": 42},
        "extensions": {},
    }
    base.update(overrides)
    return CloudEventInput(**base)


def _assert_error_hides_secrets(exc: CloudEventValidationError) -> None:
    blob = f"{exc!s}\n{exc!r}\n{exc.code}\n{exc.message}\n{exc.details!r}"
    assert "hunter2" not in blob
    assert "leak-me-not" not in blob
    assert _EVENT_ID not in blob or exc.code == "validation_failed"


def test_valid_input_receives_queue_owned_id_time_and_specversion() -> None:
    stored = validate_event_input(
        _valid_input(),
        queue_time=_QUEUE_TIME,
        event_id=_EVENT_ID,
    )
    assert isinstance(stored, StoredCloudEvent)
    assert stored.id == _EVENT_ID
    assert UUID(stored.id)
    assert stored.time == _QUEUE_TIME
    assert stored.specversion == "1.0"
    assert stored.source == "https://app.example/orders"
    assert stored.type == "com.example.order.created"
    assert stored.subject == "order-42"
    assert stored.datacontenttype == "application/json"
    assert stored.data == {"order_id": 42}


def test_minimal_input_allows_omitted_optional_fields() -> None:
    stored = validate_event_input(
        CloudEventInput(source="/cloudevents/spec/pull/123", type="example.type"),
        queue_time=_QUEUE_TIME,
        event_id=_EVENT_ID,
    )
    assert stored.subject is None
    assert stored.datacontenttype is None
    assert stored.data is None
    assert dict(stored.extensions) == {}
    assert stored.specversion == "1.0"


@pytest.mark.parametrize("forbidden", ["id", "time", "specversion"])
def test_caller_supplied_queue_owned_attributes_rejected(forbidden: str) -> None:
    raw: dict[str, Any] = {
        "source": "https://app.example",
        "type": "example.type",
        "data": _SECRET_DATA,
        forbidden: "attacker-value",
    }
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(raw, queue_time=_QUEUE_TIME, event_id=_EVENT_ID)
    err = exc_info.value
    assert err.code == "validation_failed"
    _assert_error_hides_secrets(err)
    assert "attacker-value" not in f"{err!s}{err!r}{err.message}{err.details!r}"


@pytest.mark.parametrize(
    "source",
    ["", " \t", "https://example.com/\x00evil", None],
)
def test_invalid_source_rejected(source: Any) -> None:
    with pytest.raises(CloudEventValidationError) as exc_info:
        if source is None:
            validate_event_input(
                {"type": "example.type", "data": _SECRET_DATA},
                queue_time=_QUEUE_TIME,
                event_id=_EVENT_ID,
            )
        else:
            validate_event_input(
                _valid_input(source=source, data=_SECRET_DATA),
                queue_time=_QUEUE_TIME,
                event_id=_EVENT_ID,
            )
    err = exc_info.value
    assert err.code == "validation_failed"
    _assert_error_hides_secrets(err)


@pytest.mark.parametrize("type_value", ["", " ", None])
def test_invalid_type_rejected(type_value: Any) -> None:
    raw: dict[str, Any] = {
        "source": "https://app.example",
        "data": _SECRET_DATA,
    }
    if type_value is not None:
        raw["type"] = type_value
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(raw, queue_time=_QUEUE_TIME, event_id=_EVENT_ID)
    assert exc_info.value.code == "validation_failed"
    _assert_error_hides_secrets(exc_info.value)


def test_disallowed_extension_rejected_by_default_empty_allowlist() -> None:
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            _valid_input(extensions={"partitionkey": "A", "data": _SECRET_DATA}),
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
        )
    err = exc_info.value
    assert err.code == "validation_failed"
    _assert_error_hides_secrets(err)


def test_allowlisted_extension_accepted() -> None:
    stored = validate_event_input(
        _valid_input(extensions={"partitionkey": "shard-1"}),
        queue_time=_QUEUE_TIME,
        event_id=_EVENT_ID,
        allowed_extensions=frozenset({"partitionkey"}),
    )
    assert stored.extensions["partitionkey"] == "shard-1"


def test_reserved_attribute_name_cannot_be_extension() -> None:
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            _valid_input(extensions={"source": "https://evil.example"}),
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
            allowed_extensions=frozenset({"source"}),
        )
    assert exc_info.value.code == "validation_failed"


def test_invalid_extension_name_rejected() -> None:
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            _valid_input(extensions={"PartitionKey": "x"}),
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
            allowed_extensions=frozenset({"PartitionKey", "partitionkey"}),
        )
    assert exc_info.value.code == "validation_failed"


def test_non_json_extension_value_rejected() -> None:
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            _valid_input(extensions={"partitionkey": {1, 2}}),
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
            allowed_extensions=frozenset({"partitionkey"}),
        )
    assert exc_info.value.code == "validation_failed"


def test_non_json_data_rejected_without_leaking_repr() -> None:
    class NotJson:
        def __repr__(self) -> str:
            return "hunter2-in-repr"

    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            _valid_input(data=NotJson()),
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
        )
    err = exc_info.value
    assert err.code == "validation_failed"
    assert "hunter2" not in f"{err!s}{err!r}{err.message}{err.details!r}"


def test_serialization_is_deterministic_utf8_cloudevents_json() -> None:
    stored = validate_event_input(
        _valid_input(data={"b": 2, "a": 1}, extensions={"zk": 1, "aa": True}),
        queue_time=_QUEUE_TIME,
        event_id=_EVENT_ID,
        allowed_extensions=frozenset({"aa", "zk"}),
    )
    first = serialize_structured_event(stored)
    second = serialize_structured_event(stored)
    assert first.media_type == CLOUDEVENTS_JSON_MEDIA_TYPE
    assert first.media_type == "application/cloudevents+json"
    assert first.body == second.body
    assert isinstance(first.body, bytes)
    payload = json.loads(first.body.decode("utf-8"))
    assert payload["specversion"] == "1.0"
    assert payload["id"] == _EVENT_ID
    assert payload["time"] == "2026-09-19T08:44:26.123456Z"
    assert list(payload.keys()) == sorted(payload.keys())
    assert first.body == json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )


def test_oversized_serialized_event_rejected_before_persistence() -> None:
    huge = {"blob": "x" * (HARD_PAYLOAD_CEILING_BYTES + 64)}
    stored = validate_event_input(
        _valid_input(data=huge),
        queue_time=_QUEUE_TIME,
        event_id=_EVENT_ID,
        max_event_bytes=HARD_PAYLOAD_CEILING_BYTES,
        enforce_size=False,
    )
    with pytest.raises(CloudEventValidationError) as exc_info:
        serialize_structured_event(stored, max_event_bytes=HARD_PAYLOAD_CEILING_BYTES)
    err = exc_info.value
    assert err.code == "payload_too_large"
    assert "xxxxx" not in err.message
    assert "hunter2" not in f"{err!s}{err!r}"


def test_validate_rejects_oversized_event_by_default() -> None:
    huge = {"blob": "y" * (HARD_PAYLOAD_CEILING_BYTES + 64)}
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            _valid_input(data=huge),
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
            max_event_bytes=HARD_PAYLOAD_CEILING_BYTES,
        )
    assert exc_info.value.code == "payload_too_large"


def test_validation_errors_omit_payload_and_queue_owned_values_from_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    logger = logging.getLogger("workhold.delivery.cloudevents")
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            {
                "source": "https://app.example",
                "type": "example.type",
                "id": _EVENT_ID,
                "data": _SECRET_DATA,
            },
            queue_time=_QUEUE_TIME,
            event_id="00000000-0000-4000-8000-000000000099",
            logger=logger,
        )
    err = exc_info.value
    _assert_error_hides_secrets(err)
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "hunter2" not in joined
    assert "leak-me-not" not in joined
    assert _EVENT_ID not in joined
    assert "00000000-0000-4000-8000-000000000099" not in joined


def test_queue_time_must_be_timezone_aware_utc() -> None:
    naive = datetime(2026, 9, 19, 8, 44, 26)
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(_valid_input(), queue_time=naive, event_id=_EVENT_ID)
    assert exc_info.value.code == "validation_failed"


def test_dataschema_and_other_unlisted_standard_attributes_rejected() -> None:
    with pytest.raises(CloudEventValidationError) as exc_info:
        validate_event_input(
            {
                "source": "https://app.example",
                "type": "example.type",
                "dataschema": "https://example.com/schema",
                "data": _SECRET_DATA,
            },
            queue_time=_QUEUE_TIME,
            event_id=_EVENT_ID,
        )
    assert exc_info.value.code == "validation_failed"
    _assert_error_hides_secrets(exc_info.value)
