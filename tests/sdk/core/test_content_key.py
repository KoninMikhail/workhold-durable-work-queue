"""Deterministic task-payload envelope: parse, seal, detect, open."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core import QueueClientError
from _workhold_client_core.codecs import encode_payload, measure_json_bytes
from _workhold_client_core.content_key import (
    ContentDecryptError,
    is_content_envelope,
    open_task_payload,
    parse_content_key,
    seal_task_payload,
    task_with_opened_payload,
)
from _workhold_client_core.models import Task

KEY_BYTES = bytes(range(32))
KEY_B64 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
OTHER_KEY_BYTES = bytes(range(32, 64))
OTHER_KEY_B64 = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="

KEY_ERROR = "content_key must be 32 bytes or standard base64 that decodes to 32 bytes"
SENTINEL = "plaintext-sentinel"


def _task(payload: Any) -> Task:
    return Task.parse(
        {
            "task_id": "task-1",
            "queue_name": "orders",
            "producer_id": "producer-1",
            "state": "ready",
            "priority": 0,
            "available_at": "2026-01-01T00:00:00Z",
            "retry_policy_version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "spawned_task_ids": [],
            "delivery_event_ids": [],
            "payload": payload,
        }
    )


def test_fixture_keys_match_documented_base64() -> None:
    assert base64.b64encode(KEY_BYTES).decode("ascii") == KEY_B64
    assert base64.b64encode(OTHER_KEY_BYTES).decode("ascii") == OTHER_KEY_B64


def test_parse_none_is_none_and_ignores_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKHOLD_CONTENT_KEY", KEY_B64)
    assert parse_content_key(None) is None


def test_parse_accepts_32_bytes_and_standard_base64() -> None:
    assert parse_content_key(KEY_BYTES) is KEY_BYTES
    assert parse_content_key(KEY_B64) == KEY_BYTES


@pytest.mark.parametrize(
    "bad",
    [
        "",
        bytes(31),
        bytes(33),
        bytearray(KEY_BYTES),
        memoryview(KEY_BYTES),
        KEY_B64.rstrip("="),
        base64.urlsafe_b64encode(bytes([251, 255] * 16)).decode("ascii"),
        base64.b64encode(bytes(31)).decode("ascii"),
        "not-base64-secret-text!",
        123,
    ],
)
def test_parse_rejects_invalid_key_without_echoing_it(bad: Any) -> None:
    with pytest.raises(ValueError) as excinfo:
        parse_content_key(bad)
    assert str(excinfo.value) == KEY_ERROR
    if isinstance(bad, str) and bad:
        assert bad not in str(excinfo.value)


def test_same_plaintext_and_key_seal_equal() -> None:
    value = {"b": 1, "a": 2, "nested": {"y": [1, 2], "x": None}}
    reordered = {"nested": {"x": None, "y": [1, 2]}, "a": 2, "b": 1}
    first = seal_task_payload(value, KEY_BYTES)
    second = seal_task_payload(value, KEY_BYTES)
    assert first == second
    assert first == seal_task_payload(reordered, KEY_BYTES)
    assert seal_task_payload({"b": 1, "a": 2}, KEY_BYTES) == seal_task_payload(
        {"a": 2, "b": 1}, KEY_BYTES
    )


def test_seal_is_key_and_plaintext_dependent() -> None:
    value = {"a": 1}
    assert seal_task_payload(value, KEY_BYTES) != seal_task_payload(
        value, OTHER_KEY_BYTES
    )
    assert seal_task_payload({"a": 1}, KEY_BYTES) != seal_task_payload(
        {"a": 2}, KEY_BYTES
    )


def test_seal_envelope_shape() -> None:
    envelope = seal_task_payload({"a": 1}, KEY_BYTES)
    assert set(envelope) == {"$workhold_enc", "n", "c"}
    assert envelope["$workhold_enc"] == 1
    assert type(envelope["$workhold_enc"]) is int
    assert len(base64.b64decode(envelope["n"], validate=True)) == 12
    # 16-byte GCM tag is appended to the ciphertext.
    plaintext = json.dumps(
        {"a": 1}, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    assert len(base64.b64decode(envelope["c"], validate=True)) == len(plaintext) + 16


def test_seal_matches_specified_nonce_and_aad() -> None:
    import hashlib
    import hmac

    value = {"k": "v"}
    plaintext = b'{"k":"v"}'
    envelope = seal_task_payload(value, KEY_BYTES)
    expected_nonce = hmac.new(KEY_BYTES, plaintext, hashlib.sha256).digest()[:12]
    assert base64.b64decode(envelope["n"]) == expected_nonce


def test_str_and_bytes_key_forms_seal_equal() -> None:
    value = {"a": [1, 2, 3]}
    assert seal_task_payload(value, parse_content_key(KEY_B64)) == seal_task_payload(
        value, KEY_BYTES
    )


@pytest.mark.parametrize(
    "value",
    [
        42,
        "text",
        [1, "two", None],
        None,
        True,
        {"unicode": "Привет, мир \u2603 \U0001f600", "n": 1.5},
    ],
)
def test_round_trip_json_values(value: Any) -> None:
    envelope = seal_task_payload(value, KEY_BYTES)
    assert is_content_envelope(envelope)
    assert open_task_payload(envelope, KEY_BYTES) == value


def test_seal_rejects_non_json_value_without_repr() -> None:
    class Secret:
        def __repr__(self) -> str:
            return "Secret-repr-should-not-leak"

    with pytest.raises(ValueError) as excinfo:
        seal_task_payload({"x": Secret()}, KEY_BYTES)
    assert str(excinfo.value) == "payload must be a JSON value"
    assert "Secret-repr-should-not-leak" not in str(excinfo.value)

    with pytest.raises(ValueError) as nan_info:
        seal_task_payload(float("nan"), KEY_BYTES)
    assert str(nan_info.value) == "payload must be a JSON value"


def test_is_content_envelope_exact_shape() -> None:
    good = {"$workhold_enc": 1, "n": "AA==", "c": "AA=="}
    assert is_content_envelope(good) is True
    assert is_content_envelope({**good, "extra": 1}) is False
    assert is_content_envelope({"$workhold_enc": 1, "n": "AA=="}) is False
    assert is_content_envelope({"n": "AA==", "c": "AA=="}) is False
    assert is_content_envelope({**good, "$workhold_enc": 2}) is False
    assert is_content_envelope({**good, "$workhold_enc": True}) is False
    assert is_content_envelope({**good, "n": 1}) is False
    assert is_content_envelope({**good, "c": None}) is False
    assert is_content_envelope(None) is False
    assert is_content_envelope([good]) is False
    assert is_content_envelope("envelope") is False


def test_wrong_key_fails_closed_without_leaking() -> None:
    secret = {"secret": SENTINEL}
    envelope = seal_task_payload(secret, KEY_BYTES)
    with pytest.raises(ContentDecryptError) as excinfo:
        open_task_payload(envelope, OTHER_KEY_BYTES)
    err = excinfo.value
    assert str(err) == "content_decrypt_failed"
    assert repr(err) == "ContentDecryptError(reason='content_decrypt_failed')"
    assert err.reason == "content_decrypt_failed"
    assert err.raw is envelope
    assert err.__cause__ is None
    rendered = f"{err!s}{err!r}"
    assert SENTINEL not in rendered
    assert KEY_B64 not in rendered
    assert OTHER_KEY_B64 not in rendered
    assert isinstance(err, QueueClientError)
    assert not isinstance(err, ValueError)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda e: {**e, "n": "not base64!!"},
        lambda e: {**e, "n": base64.b64encode(bytes(8)).decode("ascii")},
        lambda e: {**e, "c": "%%%"},
        lambda e: {**e, "c": base64.b64encode(b"short").decode("ascii")},
        lambda e: {
            **e,
            "c": base64.b64encode(
                bytes([base64.b64decode(e["c"])[0] ^ 1]) + base64.b64decode(e["c"])[1:]
            ).decode("ascii"),
        },
    ],
)
def test_malformed_or_tampered_envelope_fails_closed(mutate: Any) -> None:
    envelope = seal_task_payload({"secret": SENTINEL}, KEY_BYTES)
    broken = mutate(envelope)
    with pytest.raises(ContentDecryptError) as excinfo:
        open_task_payload(broken, KEY_BYTES)
    assert str(excinfo.value) == "content_decrypt_failed"
    assert excinfo.value.__cause__ is None
    assert excinfo.value.raw is broken


def test_open_passes_through_non_envelope_with_key() -> None:
    for value in (
        {"a": 1},
        {"$workhold_enc": 1, "n": "AA==", "c": "AA==", "extra": 1},
        [1, 2],
        "text",
        None,
        7,
    ):
        assert open_task_payload(value, KEY_BYTES) is value


def test_open_without_key_returns_envelope_unchanged() -> None:
    envelope = seal_task_payload({"a": 1}, KEY_BYTES)
    assert open_task_payload(envelope, None) is envelope


def test_task_with_opened_payload_unchanged_returns_same_task() -> None:
    task = _task({"plain": True})
    assert task_with_opened_payload(task, KEY_BYTES) is task
    envelope = seal_task_payload({"a": 1}, KEY_BYTES)
    keyless = _task(envelope)
    assert task_with_opened_payload(keyless, None) is keyless


def test_task_with_opened_payload_replaces_envelope_payload() -> None:
    value = {"a": 1, "text": "Привет"}
    envelope = seal_task_payload(value, KEY_BYTES)
    task = _task(envelope)
    opened = task_with_opened_payload(task, KEY_BYTES)
    assert opened is not task
    assert opened.payload == value
    assert opened.task_id == task.task_id
    assert task.payload is envelope


class _FixedEncoder:
    codec_name = "fixed-v1"

    def encode(self, value: Any) -> Any:
        return {"encoded": True, "n": 1}


def test_encode_payload_without_key_keeps_identity() -> None:
    payload = {"order_id": 7}
    assert encode_payload(payload) is payload
    assert encode_payload(payload, content_key=None) is payload


def test_encode_payload_seals_after_encoder() -> None:
    original = {"order_id": 7}
    wire = encode_payload(original, _FixedEncoder(), content_key=KEY_BYTES)
    assert is_content_envelope(wire)
    assert original == {"order_id": 7}
    assert open_task_payload(wire, KEY_BYTES) == {"encoded": True, "n": 1}


def test_encode_payload_measures_envelope_not_plaintext() -> None:
    plaintext = {"order_id": 7}
    plain_size = measure_json_bytes(plaintext)
    assert plain_size < measure_json_bytes(seal_task_payload(plaintext, KEY_BYTES))
    assert encode_payload(plaintext, max_bytes=plain_size) is plaintext
    with pytest.raises(ValueError, match="payload exceeds"):
        encode_payload(plaintext, max_bytes=plain_size, content_key=KEY_BYTES)
