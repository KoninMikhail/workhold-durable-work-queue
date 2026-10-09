"""Wave 0 Nyquist predecessor: fail-closed catalog parse (CTRL-10 / D-07 / D-08).

Owned by 13-02 once ``workhold.domain.catalog`` lands. Do not import
``admin_queues``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from workhold.domain.catalog import parse_catalog_bytes
from workhold.domain.queue_control import (
    BackoffStrategy,
    DomainValidationError,
    RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
)

_CEILING = RETRY_DELAY_SECONDS_ABSOLUTE_MAX
_CATALOG_MAX_BYTES = 1_048_576

_VALID_ORDERS_OBJ: dict[str, Any] = {
    "schema_version": 1,
    "queues": [
        {
            "name": "orders",
            "initial_policy": {
                "enabled": True,
                "max_attempts": 5,
                "backoff_strategy": "fixed",
                "retry_delay_seconds": 30,
            },
        }
    ],
}


def _assert_validation_failed(raw: bytes) -> None:
    with pytest.raises(DomainValidationError) as exc:
        parse_catalog_bytes(raw, ceiling=_CEILING)
    assert exc.value.code == "validation_failed"


def test_empty_bytes_fail_closed() -> None:
    _assert_validation_failed(b"")


def test_whitespace_only_bytes_fail_closed() -> None:
    _assert_validation_failed(b"   \n\t  ")


def test_root_json_array_fail_closed() -> None:
    _assert_validation_failed(b'[{"name":"orders"}]')


def test_schema_version_2_fail_closed() -> None:
    body = dict(_VALID_ORDERS_OBJ)
    body["schema_version"] = 2
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_schema_version_bool_true_fail_closed() -> None:
    # JSON true must not pass as schema_version 1 (identity, not truthiness).
    body = dict(_VALID_ORDERS_OBJ)
    body["schema_version"] = True
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_extra_envelope_key_namespace_fail_closed() -> None:
    body = dict(_VALID_ORDERS_OBJ)
    body["namespace"] = "tenant-a"
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_extra_entry_key_fail_closed() -> None:
    body = {
        "schema_version": 1,
        "queues": [
            {
                "name": "orders",
                "extra": True,
                "initial_policy": {
                    "enabled": True,
                    "max_attempts": 5,
                    "backoff_strategy": "fixed",
                    "retry_delay_seconds": 30,
                },
            }
        ],
    }
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_extra_policy_key_jitter_fail_closed() -> None:
    body = {
        "schema_version": 1,
        "queues": [
            {
                "name": "orders",
                "initial_policy": {
                    "enabled": True,
                    "max_attempts": 5,
                    "backoff_strategy": "fixed",
                    "retry_delay_seconds": 30,
                    "jitter": 0.1,
                },
            }
        ],
    }
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_duplicate_name_orders_fail_closed() -> None:
    entry = {
        "name": "orders",
        "initial_policy": {
            "enabled": True,
            "max_attempts": 5,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 30,
        },
    }
    body = {"schema_version": 1, "queues": [entry, dict(entry)]}
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_invalid_queue_name_fail_closed() -> None:
    body = {
        "schema_version": 1,
        "queues": [
            {
                "name": "Bad Name!",
                "initial_policy": {
                    "enabled": True,
                    "max_attempts": 5,
                    "backoff_strategy": "fixed",
                    "retry_delay_seconds": 30,
                },
            }
        ],
    }
    _assert_validation_failed(json.dumps(body).encode("utf-8"))


def test_oversized_raw_fail_closed() -> None:
    raw = b"{" + (b"x" * (_CATALOG_MAX_BYTES + 1))
    _assert_validation_failed(raw)


def test_valid_orders_envelope_returns_catalog_entry() -> None:
    entries = parse_catalog_bytes(
        json.dumps(_VALID_ORDERS_OBJ).encode("utf-8"),
        ceiling=_CEILING,
    )
    assert isinstance(entries, tuple)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.name == "orders"
    policy = entry.initial_policy
    assert policy.enabled is True
    assert policy.max_attempts == 5
    assert policy.backoff_strategy is BackoffStrategy.FIXED
    assert policy.retry_delay_seconds == 30


def test_empty_queues_array_returns_empty_tuple() -> None:
    raw = json.dumps({"schema_version": 1, "queues": []}).encode("utf-8")
    entries = parse_catalog_bytes(raw, ceiling=_CEILING)
    assert entries == ()
