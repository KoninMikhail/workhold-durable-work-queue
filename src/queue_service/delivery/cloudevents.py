"""Transport-neutral CloudEvents 1.0 structured-content contract (ADR 021)."""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Mapping, Set
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import urlparse

from queue_service.security.payload_policy import HARD_PAYLOAD_CEILING_BYTES

CLOUDEVENTS_JSON_MEDIA_TYPE: Final[str] = "application/cloudevents+json"
SPEC_VERSION: Final[str] = "1.0"

_QUEUE_OWNED: Final[frozenset[str]] = frozenset({"id", "time", "specversion"})
_APPLICATION_OWNED: Final[frozenset[str]] = frozenset(
    {"source", "type", "subject", "datacontenttype", "data", "extensions"}
)
_RESERVED_NAMES: Final[frozenset[str]] = frozenset(
    {
        "id",
        "source",
        "specversion",
        "type",
        "data",
        "data_base64",
        "databas64",
        "dataschema",
        "subject",
        "datacontenttype",
        "time",
    }
)
_EXTENSION_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9]{1,20}$")


class CloudEventValidationError(Exception):
    """CloudEvents validation failure that never embeds payload values."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.details: dict[str, Any] = dict(details or {})
        super().__init__(message)

    def __repr__(self) -> str:
        return (
            "CloudEventValidationError("
            f"code={self.code!r}, message={self.message!r}, "
            f"details={self.details!r})"
        )


@dataclass(frozen=True, slots=True)
class CloudEventInput:
    """Application-owned CloudEvents attributes per ADR 021."""

    source: str
    type: str
    subject: str | None = None
    datacontenttype: str | None = None
    data: Any = None
    extensions: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class StoredCloudEvent:
    """Validated envelope including Queue-owned id, time, and specversion."""

    id: str
    source: str
    type: str
    specversion: str
    time: datetime
    subject: str | None
    datacontenttype: str | None
    data: Any
    extensions: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class StructuredEventSerialization:
    """Deterministic structured-mode serialization result."""

    body: bytes
    media_type: str


def validate_event_input(
    value: CloudEventInput | Mapping[str, Any],
    *,
    queue_time: datetime,
    event_id: str | None = None,
    allowed_extensions: Set[str] = frozenset(),
    max_event_bytes: int = HARD_PAYLOAD_CEILING_BYTES,
    enforce_size: bool = True,
    logger: logging.Logger | None = None,
) -> StoredCloudEvent:
    """Validate application input and assign Queue-owned CloudEvents fields."""
    log = logger or logging.getLogger(__name__)
    try:
        app_input = _coerce_input(value)
        _require_aware_queue_time(queue_time)
        resolved_id = _resolve_event_id(event_id)
        _validate_source(app_input.source)
        _validate_type(app_input.type)
        subject = _optional_non_empty_string(app_input.subject, field="subject")
        datacontenttype = _optional_non_empty_string(
            app_input.datacontenttype,
            field="datacontenttype",
        )
        data = _require_json_value(app_input.data, field="data")
        extensions = _validate_extensions(
            app_input.extensions,
            allowed_extensions=allowed_extensions,
        )
        stored = StoredCloudEvent(
            id=resolved_id,
            source=app_input.source,
            type=app_input.type,
            specversion=SPEC_VERSION,
            time=queue_time,
            subject=subject,
            datacontenttype=datacontenttype,
            data=data,
            extensions=extensions,
        )
        if enforce_size:
            serialize_structured_event(stored, max_event_bytes=max_event_bytes)
        return stored
    except CloudEventValidationError as exc:
        log.debug(
            "cloudevents_validation_failed code=%s field=%s",
            exc.code,
            exc.details.get("field", "n/a"),
        )
        raise


def serialize_structured_event(
    event: StoredCloudEvent,
    *,
    max_event_bytes: int = HARD_PAYLOAD_CEILING_BYTES,
) -> StructuredEventSerialization:
    """Serialize a stored event as deterministic UTF-8 CloudEvents JSON."""
    if event.specversion != SPEC_VERSION:
        raise CloudEventValidationError(
            "validation_failed",
            "specversion must be 1.0",
            details={"field": "specversion"},
        )
    _require_aware_queue_time(event.time)

    payload: dict[str, Any] = {
        "id": event.id,
        "source": event.source,
        "specversion": SPEC_VERSION,
        "type": event.type,
        "time": _format_rfc3339_utc(event.time),
    }
    if event.subject is not None:
        payload["subject"] = event.subject
    if event.datacontenttype is not None:
        payload["datacontenttype"] = event.datacontenttype
    if event.data is not None:
        payload["data"] = event.data
    for name, value in event.extensions.items():
        payload[name] = value

    try:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CloudEventValidationError(
            "validation_failed",
            "event is not JSON-serializable",
            details={"field": "event"},
        ) from exc

    if len(body) > max_event_bytes:
        raise CloudEventValidationError(
            "payload_too_large",
            "serialized CloudEvent exceeds hard byte ceiling",
            details={
                "field": "event",
                "limit_bytes": max_event_bytes,
                "size_bytes": len(body),
            },
        )
    return StructuredEventSerialization(
        body=body,
        media_type=CLOUDEVENTS_JSON_MEDIA_TYPE,
    )


def _coerce_input(value: CloudEventInput | Mapping[str, Any]) -> CloudEventInput:
    if isinstance(value, CloudEventInput):
        return CloudEventInput(
            source=value.source,
            type=value.type,
            subject=value.subject,
            datacontenttype=value.datacontenttype,
            data=value.data,
            extensions=dict(value.extensions),
        )
    if not isinstance(value, Mapping):
        raise CloudEventValidationError(
            "validation_failed",
            "event input must be a mapping or CloudEventInput",
            details={"field": "event"},
        )

    queue_owned = sorted(key for key in value if key in _QUEUE_OWNED)
    if queue_owned:
        raise CloudEventValidationError(
            "validation_failed",
            "Queue-owned CloudEvents attributes must not be supplied by the caller",
            details={"field": queue_owned[0]},
        )

    top_level_extensions: dict[str, Any] = {}
    for key in value:
        if key in _APPLICATION_OWNED:
            continue
        if key in _RESERVED_NAMES:
            raise CloudEventValidationError(
                "validation_failed",
                "attribute is not an application-owned CloudEvents field",
                details={"field": key},
            )
        top_level_extensions[key] = value[key]

    extensions_raw = value.get("extensions", {})
    if "extensions" in value and not isinstance(extensions_raw, Mapping):
        raise CloudEventValidationError(
            "validation_failed",
            "extensions must be a mapping",
            details={"field": "extensions"},
        )
    merged = dict(extensions_raw or {})
    overlap = set(merged).intersection(top_level_extensions)
    if overlap:
        raise CloudEventValidationError(
            "validation_failed",
            "duplicate extension attribute",
            details={"field": sorted(overlap)[0]},
        )
    merged.update(top_level_extensions)

    if "source" not in value:
        raise CloudEventValidationError(
            "validation_failed",
            "source is required",
            details={"field": "source"},
        )
    if "type" not in value:
        raise CloudEventValidationError(
            "validation_failed",
            "type is required",
            details={"field": "type"},
        )

    return CloudEventInput(
        source=value["source"],  # validated later
        type=value["type"],
        subject=value.get("subject"),
        datacontenttype=value.get("datacontenttype"),
        data=value.get("data"),
        extensions=merged,
    )


def _resolve_event_id(event_id: str | None) -> str:
    if event_id is None:
        return str(uuid.uuid4())
    if not isinstance(event_id, str) or not event_id:
        raise CloudEventValidationError(
            "validation_failed",
            "event_id must be a non-empty UUID string",
            details={"field": "id"},
        )
    try:
        return str(uuid.UUID(event_id))
    except ValueError as exc:
        raise CloudEventValidationError(
            "validation_failed",
            "event_id must be a UUID",
            details={"field": "id"},
        ) from exc


def _require_aware_queue_time(queue_time: datetime) -> None:
    if not isinstance(queue_time, datetime) or queue_time.tzinfo is None:
        raise CloudEventValidationError(
            "validation_failed",
            "queue_time must be timezone-aware",
            details={"field": "time"},
        )


def _validate_source(source: Any) -> None:
    if not isinstance(source, str) or not source.strip():
        raise CloudEventValidationError(
            "validation_failed",
            "source must be a non-empty URI-reference",
            details={"field": "source"},
        )
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in source):
        raise CloudEventValidationError(
            "validation_failed",
            "source must be a non-empty URI-reference",
            details={"field": "source"},
        )
    try:
        parsed = urlparse(source)
    except ValueError as exc:
        raise CloudEventValidationError(
            "validation_failed",
            "source must be a non-empty URI-reference",
            details={"field": "source"},
        ) from exc
    if not any(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    ):
        raise CloudEventValidationError(
            "validation_failed",
            "source must be a non-empty URI-reference",
            details={"field": "source"},
        )


def _validate_type(type_value: Any) -> None:
    if not isinstance(type_value, str) or not type_value.strip():
        raise CloudEventValidationError(
            "validation_failed",
            "type must be a non-empty string",
            details={"field": "type"},
        )


def _optional_non_empty_string(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise CloudEventValidationError(
            "validation_failed",
            f"{field} must be a non-empty string when provided",
            details={"field": field},
        )
    return value


def _validate_extensions(
    extensions: Mapping[str, Any],
    *,
    allowed_extensions: Set[str],
) -> Mapping[str, Any]:
    if not isinstance(extensions, Mapping):
        raise CloudEventValidationError(
            "validation_failed",
            "extensions must be a mapping",
            details={"field": "extensions"},
        )
    normalized: dict[str, Any] = {}
    for name, raw_value in extensions.items():
        if not isinstance(name, str) or not _EXTENSION_NAME_RE.fullmatch(name):
            raise CloudEventValidationError(
                "validation_failed",
                "extension name must match CloudEvents lowercase token rules",
                details={"field": "extensions"},
            )
        if name in _RESERVED_NAMES:
            raise CloudEventValidationError(
                "validation_failed",
                "extension name conflicts with a reserved CloudEvents attribute",
                details={"field": "extensions"},
            )
        if name not in allowed_extensions:
            raise CloudEventValidationError(
                "validation_failed",
                "extension is not allowlisted",
                details={"field": "extensions"},
            )
        normalized[name] = _require_json_value(raw_value, field="extensions")
    return MappingProxyType(normalized)


def _require_json_value(value: Any, *, field: str) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):  # noqa: PLR0124
            raise CloudEventValidationError(
                "validation_failed",
                "value must be JSON-serializable",
                details={"field": field},
            )
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [_require_json_value(item, field=field) for item in value]
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CloudEventValidationError(
                    "validation_failed",
                    "JSON object keys must be strings",
                    details={"field": field},
                )
            out[key] = _require_json_value(item, field=field)
        return out
    raise CloudEventValidationError(
        "validation_failed",
        "value must be JSON-serializable",
        details={"field": field},
    )


def _format_rfc3339_utc(value: datetime) -> str:
    utc_value = value.astimezone(UTC)
    if utc_value.microsecond:
        return utc_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return utc_value.strftime("%Y-%m-%dT%H:%M:%SZ")
