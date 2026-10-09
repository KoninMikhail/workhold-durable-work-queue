"""Contract tests for the app-local outbox bridge protocol (BRDG-02 / Plan 06-01)."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

import pytest

ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "docs" / "04-architecture" / "application-outbox-bridge.md"
SCHEMA_PATH = (
    ROOT / "docs" / "04-architecture" / "schemas" / "application-outbox-intent.schema.json"
)
ADR004_PATH = ROOT / "docs" / "04-architecture" / "adr" / "004-app-local-outbox-bridge.md"

REQUIRED_INTENT_FIELDS = (
    "schema_version",
    "source_namespace",
    "source_row_id",
    "target_queue",
    "enqueue_request",
    "created_at",
)
OPTIONAL_INTENT_FIELDS = ("traceparent", "tracestate", "extensions")

PROHIBITED_CLAIM_PATTERNS = (
    re.compile(r"\bexactly[- ]once\b", re.IGNORECASE),
    re.compile(r"\b2\s*PC\b", re.IGNORECASE),
    re.compile(r"\btwo[- ]phase\s+commit\b", re.IGNORECASE),
    re.compile(r"\bdistributed\s+transaction\b", re.IGNORECASE),
    re.compile(
        r"\bQueue\b.{0,80}\b(owns|own|queries|query)\b.{0,40}\bapp(?:lication)?[- ]?"
        r"(?:owned\s+)?(?:table|schema|outbox)\b",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"\b(mandatory|required|must)\b.{0,60}\b(application|app)\s+(database|DB)\b",
        re.IGNORECASE | re.DOTALL,
    ),
)

# Negation windows that make an otherwise-matching phrase normative denial rather
# than a prohibited claim (e.g. "does not claim exactly-once").
NEGATION_PREFIX = re.compile(
    r"(?:\bnot\b|\bno\b|\bnever\b|\bwithout\b|\bforbid(?:s|den)?\b|"
    r"\bprohibit(?:s|ed)?\b|\bmust\s+not\b|\bcannot\b|\bdoes\s+not\b|"
    r"\bdo\s+not\b|\bdon'?t\b|\bexplicit(?:ly)?\s+non[- ]guarantee)",
    re.IGNORECASE,
)


def _load_protocol() -> str:
    assert PROTOCOL_PATH.is_file(), f"missing protocol: {PROTOCOL_PATH}"
    return PROTOCOL_PATH.read_text(encoding="utf-8")


def _load_schema() -> dict[str, Any]:
    assert SCHEMA_PATH.is_file(), f"missing schema: {SCHEMA_PATH}"
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _matches_type(value: Any, expected: str) -> bool:
    actual = _type_name(value)
    if expected == "number":
        return actual in {"number", "integer"}
    return actual == expected


class _SchemaFindings(list[str]):
    pass


def _validate(schema: Mapping[str, Any], value: Any, path: str, findings: _SchemaFindings) -> None:
    """Minimal Draft-ish JSON Schema validator (stdlib only; no jsonschema dep)."""
    if "const" in schema and value != schema["const"]:
        findings.append(f"{path}: expected const {schema['const']!r}, got {value!r}")
        return

    if "enum" in schema and isinstance(schema["enum"], list) and value not in schema["enum"]:
        findings.append(f"{path}: value {value!r} not in enum")

    raw_type = schema.get("type")
    if isinstance(raw_type, list):
        types = [str(t) for t in raw_type]
    elif isinstance(raw_type, str):
        types = [raw_type]
    else:
        types = []
    if types and not any(_matches_type(value, t) for t in types):
        findings.append(f"{path}: expected {'|'.join(types)}, got {_type_name(value)}")
        return

    if isinstance(value, str):
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            findings.append(f"{path}: shorter than minLength {schema['minLength']}")
        if "maxLength" in schema and len(value) > int(schema["maxLength"]):
            findings.append(f"{path}: longer than maxLength {schema['maxLength']}")
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.fullmatch(pattern, value) is None:
            findings.append(f"{path}: failed pattern {pattern}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            findings.append(f"{path}: {value} below minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            findings.append(f"{path}: {value} above maximum {schema['maximum']}")

    if isinstance(value, dict):
        props = schema.get("properties") or {}
        required = schema.get("required") or []
        if isinstance(required, list):
            for key in required:
                if key not in value:
                    findings.append(f"{path}: missing required property '{key}'")
        additional = schema.get("additionalProperties", True)
        for key, child in value.items():
            if key in props and isinstance(props[key], Mapping):
                _validate(props[key], child, f"{path}/{key}", findings)
                continue
            if additional is False:
                findings.append(f"{path}: unknown property '{key}'")
            elif isinstance(additional, Mapping):
                _validate(additional, child, f"{path}/{key}", findings)

    if isinstance(value, list):
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for idx, item in enumerate(value):
                _validate(item_schema, item, f"{path}/{idx}", findings)

    if "allOf" in schema:
        for option in schema["allOf"]:
            if isinstance(option, Mapping):
                _validate(option, value, path, findings)


def _assert_valid(schema: Mapping[str, Any], value: Any) -> None:
    findings = _SchemaFindings()
    _validate(schema, value, "$", findings)
    assert not findings, "unexpected schema findings:\n" + "\n".join(findings)


def _assert_invalid(schema: Mapping[str, Any], value: Any, *, must_mention: str | None = None) -> None:
    findings = _SchemaFindings()
    _validate(schema, value, "$", findings)
    assert findings, f"expected validation failure for {value!r}"
    if must_mention is not None:
        joined = "\n".join(findings)
        assert must_mention in joined, f"expected {must_mention!r} in findings:\n{joined}"


def _valid_intent(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_version": 1,
        "source_namespace": "orders.checkout",
        "source_row_id": "outbox-row-42",
        "target_queue": "fulfillment",
        "enqueue_request": {
            "payload": {"order_id": "o-1"},
            "priority": 0,
        },
        "created_at": "2026-09-19T12:00:00Z",
    }
    base.update(overrides)
    return base


def bridge_idempotency_key(source_namespace: str, source_row_id: str) -> str:
    """Reference implementation of the Plan 01 / protocol mapping algorithm."""
    ns = source_namespace.encode("utf-8")
    row = source_row_id.encode("utf-8")
    canonical = len(ns).to_bytes(4, "big") + ns + len(row).to_bytes(4, "big") + row
    digest = hashlib.sha256(canonical).digest()
    token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=").lower()
    return f"bridge:v1:{token}"


def _claim_is_prohibited(text: str, match: re.Match[str]) -> bool:
    """Return True when the match is a prohibited affirmative claim."""
    start = max(0, match.start() - 120)
    window = text[start : match.start()]
    return NEGATION_PREFIX.search(window) is None


# ---------------------------------------------------------------------------
# Presence / wiring
# ---------------------------------------------------------------------------


def test_protocol_and_schema_files_exist() -> None:
    assert PROTOCOL_PATH.is_file()
    assert SCHEMA_PATH.is_file()
    assert ADR004_PATH.is_file()


def test_protocol_references_adr_004_and_schema() -> None:
    text = _load_protocol()
    assert re.search(r"ADR\s*004|004-app-local-outbox-bridge", text)
    assert "application-outbox-intent.schema.json" in text


# ---------------------------------------------------------------------------
# Schema fixtures
# ---------------------------------------------------------------------------


def test_valid_v1_intent_accepts() -> None:
    schema = _load_schema()
    _assert_valid(schema, _valid_intent())
    _assert_valid(
        schema,
        _valid_intent(
            traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            tracestate="rojo=00f067aa0ba902b7",
            extensions={"app.region": "eu-central"},
        ),
    )


@pytest.mark.parametrize("field", REQUIRED_INTENT_FIELDS)
def test_missing_required_field_rejected(field: str) -> None:
    schema = _load_schema()
    intent = _valid_intent()
    del intent[field]
    _assert_invalid(schema, intent, must_mention=field)


def test_unknown_major_schema_version_rejected() -> None:
    schema = _load_schema()
    _assert_invalid(schema, _valid_intent(schema_version=2), must_mention="schema_version")
    _assert_invalid(schema, _valid_intent(schema_version=0), must_mention="schema_version")


def test_additive_extension_fields_permitted() -> None:
    schema = _load_schema()
    _assert_valid(
        schema,
        _valid_intent(extensions={"bridge.hint": "batch-a", "numeric": 1}),
    )


@pytest.mark.parametrize(
    "bad_id",
    [
        "",
        "ab\x00c",
        "row\nwith\nnewline",
        123,
        None,
    ],
)
def test_ambiguous_or_mutation_prone_identifiers_rejected(bad_id: Any) -> None:
    schema = _load_schema()
    intent = _valid_intent()
    intent["source_row_id"] = bad_id
    _assert_invalid(schema, intent, must_mention="source_row_id")


def test_whitespace_significant_opaque_identity_accepted() -> None:
    """Leading/trailing whitespace is significant opaque identity, not trimmed."""
    schema = _load_schema()
    _assert_valid(schema, _valid_intent(source_namespace=" ns ", source_row_id=" row "))


def test_schema_and_protocol_agree_on_required_optional_fields() -> None:
    schema = _load_schema()
    text = _load_protocol()
    required = set(schema.get("required") or [])
    assert required == set(REQUIRED_INTENT_FIELDS)
    props = set((schema.get("properties") or {}).keys())
    assert set(REQUIRED_INTENT_FIELDS).issubset(props)
    assert set(OPTIONAL_INTENT_FIELDS).issubset(props)
    for name in REQUIRED_INTENT_FIELDS + OPTIONAL_INTENT_FIELDS:
        assert name in text, f"protocol must document field {name}"


# ---------------------------------------------------------------------------
# Deterministic idempotency mapping
# ---------------------------------------------------------------------------


def test_deterministic_mapping_stable_and_length_prefixed() -> None:
    a = bridge_idempotency_key("ab", "c")
    b = bridge_idempotency_key("ab", "c")
    c = bridge_idempotency_key("a", "bc")
    assert a == b
    assert a != c
    assert a.startswith("bridge:v1:")
    assert re.fullmatch(r"bridge:v1:[a-z0-9_-]+", a)
    # SHA-256 base64url without padding is 43 chars; prefix is 10 → 53 total.
    assert len(a) == 53
    assert len(a) <= 256


def test_protocol_documents_bridge_v1_mapping_algorithm() -> None:
    text = _load_protocol()
    assert "bridge:v1:" in text
    assert re.search(r"SHA-256|sha256", text)
    assert re.search(r"base64url", text, re.IGNORECASE)
    assert re.search(r"length[- ]prefixed", text, re.IGNORECASE)
    assert "source_namespace" in text and "source_row_id" in text
    assert re.search(r"immutable", text, re.IGNORECASE)
    assert re.search(r"idempotency_conflict|fingerprint", text, re.IGNORECASE)


# ---------------------------------------------------------------------------
# Lifecycle / operational semantics presence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "needle",
    [
        r"\bpending\b",
        r"\bleased\b|\bprocessing\b",
        r"\bdelivered\b",
        r"retryable",
        r"terminal",
        r"\breplay\b",
        r"\blag\b",
        r"oldest pending",
        r"created_at",
        r"\bhealth\b|\breadiness\b",
        r"compatib",
        r"rolling[- ]upgrade|mixed.*replica",
        r"retention|tombstone",
        r"metric labels|labels/default logs|forbidden",
    ],
)
def test_protocol_covers_lifecycle_lag_health_compatibility(needle: str) -> None:
    text = _load_protocol()
    assert re.search(needle, text, re.IGNORECASE), f"missing semantics for {needle!r}"


def test_protocol_preserves_optional_db_less_direct_enqueue() -> None:
    text = _load_protocol()
    assert re.search(r"DB-less|without\s+(an\s+)?application\s+(database|DB)", text, re.IGNORECASE)
    assert re.search(r"enqueue\s+directly|direct\s+enqueue", text, re.IGNORECASE)
    assert re.search(r"\boptional\b", text, re.IGNORECASE)


# ---------------------------------------------------------------------------
# Prohibited claims
# ---------------------------------------------------------------------------


def test_protocol_rejects_prohibited_guarantee_claims() -> None:
    text = _load_protocol()
    offenders: list[str] = []
    for pattern in PROHIBITED_CLAIM_PATTERNS:
        for match in pattern.finditer(text):
            if _claim_is_prohibited(text, match):
                snippet = text[max(0, match.start() - 40) : match.end() + 40].replace("\n", " ")
                offenders.append(f"{pattern.pattern!r} → …{snippet}…")
    assert not offenders, "prohibited affirmative claims found:\n" + "\n".join(offenders)


def test_schema_version_const_is_major_one() -> None:
    schema = _load_schema()
    props = schema.get("properties") or {}
    version = props.get("schema_version") or {}
    assert version.get("const") == 1 or version.get("enum") == [1]
    assert "schema_version" in (schema.get("required") or [])
