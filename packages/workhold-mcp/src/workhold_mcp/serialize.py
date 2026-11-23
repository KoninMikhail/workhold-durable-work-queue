"""Project observer client values to MCP tool JSON."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from workhold_admin.models import format_datetime


def to_tool_payload(value: Any) -> Any:
    """Return a JSON-ready object without redacting business payload."""

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = dataclasses.fields(value)
        if len(fields) == 1 and fields[0].name == "value":
            raw = getattr(value, "value")
            if isinstance(raw, str):
                return raw
        projected: dict[str, Any] = {}
        for field in fields:
            if field.name == "extra":
                continue
            item = getattr(value, field.name)
            if field.name == "payload":
                projected["payload"] = _copy_payload(item)
                continue
            projected[field.name] = to_tool_payload(item)
        extra = getattr(value, "extra", None)
        if isinstance(extra, Mapping):
            for key, item in extra.items():
                if key not in projected:
                    projected[str(key)] = to_tool_payload(item)
        return projected
    if isinstance(value, datetime):
        return format_datetime(value)
    if isinstance(value, tuple):
        return [to_tool_payload(item) for item in value]
    if isinstance(value, list):
        return [to_tool_payload(item) for item in value]
    return value


def canonical_json(payload: Any) -> str:
    """Return deterministic compact JSON text for a tool payload."""

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _copy_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return dict(payload)
    if isinstance(payload, list):
        return list(payload)
    return payload
