"""Wave 0 Nyquist predecessor: named-queue catalog JSON Schema companion (D-20).

Owned by 13-06 once ``docs/04-architecture/schemas/named-queue-catalog.schema.json``
lands. Stdlib-only; no ``jsonschema`` package.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = (
    ROOT / "docs" / "04-architecture" / "schemas" / "named-queue-catalog.schema.json"
)
_NAME_PATTERN = r"^[a-z0-9][a-z0-9._-]*$"


def _load_schema() -> dict[str, Any]:
    assert SCHEMA_PATH.is_file(), f"missing schema companion: {SCHEMA_PATH}"
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_schema_declares_draft_2020_12() -> None:
    schema = _load_schema()
    assert schema.get("$schema") == "https://json-schema.org/draft/2020-12/schema"


def test_envelope_additional_properties_false() -> None:
    schema = _load_schema()
    assert schema.get("additionalProperties") is False


def test_queue_entry_additional_properties_false() -> None:
    schema = _load_schema()
    queues = (schema.get("properties") or {}).get("queues") or {}
    items = queues.get("items") or {}
    assert items.get("additionalProperties") is False


def test_initial_policy_additional_properties_false() -> None:
    schema = _load_schema()
    queues = (schema.get("properties") or {}).get("queues") or {}
    items = queues.get("items") or {}
    props = items.get("properties") or {}
    policy = props.get("initial_policy") or {}
    assert policy.get("additionalProperties") is False


def test_schema_version_const_is_one() -> None:
    schema = _load_schema()
    props = schema.get("properties") or {}
    version = props.get("schema_version") or {}
    assert version.get("const") == 1


def test_queue_name_pattern_and_length() -> None:
    schema = _load_schema()
    queues = (schema.get("properties") or {}).get("queues") or {}
    items = queues.get("items") or {}
    name = (items.get("properties") or {}).get("name") or {}
    assert name.get("pattern") == _NAME_PATTERN
    assert name.get("minLength") == 1
    assert name.get("maxLength") == 128
