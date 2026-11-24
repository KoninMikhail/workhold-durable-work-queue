"""Admin MCP tools delegate to AdminClient (recording transport)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from workhold_admin.admin import AdminClient
from workhold_client_testing import ScriptedSyncTransport, ok, page_body
from workhold_mcp.errors import project_error
from workhold_mcp.registry import REGISTRY_BY_NAME, load_manifest

_ADMIN_TOKEN = "admin-secret-token"
_QUEUE_NAME = "orders"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_FILTER_CANARY = "WHMCP-FILTER-CANARY"

_BULK_EXECUTE_NAMES = frozenset(
    {
        "admin_execute_bulk_replay",
        "admin_execute_bulk_cancel",
    }
)


def _queue_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "queue_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "name": _QUEUE_NAME,
        "state": "active",
        "config_version": 1,
        "active_policy": {
            "version": 1,
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 5,
            "created_at": "2026-09-19T00:00:00Z",
        },
        "created_at": "2026-09-19T00:00:00Z",
        "updated_at": "2026-09-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _mutation_result(**queue_overrides: Any) -> dict[str, Any]:
    return {
        "queue": _queue_body(**queue_overrides),
        "replayed": False,
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }


def _preview_result(operation: str = "bulk_cancel", **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": operation,
        "queue": _QUEUE_NAME,
        "candidate_count": 0,
        "truncated": False,
        "sample_task_ids": [],
        "confirmation_token": "preview-token-abc",
        "confirmation_expires_at": "2026-09-19T01:00:00Z",
        "max_batch": 25,
    }
    body.update(overrides)
    return body


def _admin(
    public_steps: list[Any],
    admin_steps: list[Any],
) -> tuple[AdminClient, ScriptedSyncTransport, ScriptedSyncTransport]:
    public = ScriptedSyncTransport(public_steps)
    admin_transport = ScriptedSyncTransport(admin_steps)
    client = AdminClient(
        public,
        bearer_token=_ADMIN_TOKEN,
        admin_transport=admin_transport,
    )
    return client, public, admin_transport


def test_non_bulk_admin_names_are_callable() -> None:
    names = [
        row.name
        for row in load_manifest()
        if row.role == "admin" and row.name not in _BULK_EXECUTE_NAMES
    ]
    assert names
    for name in names:
        assert name in REGISTRY_BY_NAME, name
        assert callable(REGISTRY_BY_NAME[name].call)
    for bulk_name in _BULK_EXECUTE_NAMES:
        assert callable(REGISTRY_BY_NAME[bulk_name].call)


def test_activate_forwards_expected_version() -> None:
    admin_transport = ScriptedSyncTransport([ok(_mutation_result(config_version=2))])
    public = ScriptedSyncTransport([])
    client = AdminClient(
        public,
        bearer_token=_ADMIN_TOKEN,
        admin_transport=admin_transport,
    )
    idem = "idem-activate-1"
    REGISTRY_BY_NAME["admin_activate_queue_policy"].call(
        client,
        _QUEUE_NAME,
        2,
        expected_config_version=1,
        idempotency_key=idem,
    )
    assert len(admin_transport.calls) == 1
    call = admin_transport.calls[0]
    headers = call.headers(include_secrets=True)
    assert headers["Authorization"] == f"Bearer {_ADMIN_TOKEN}"
    assert headers["Idempotency-Key"] == idem
    body = call.json_body(include_secrets=True)
    assert body == {"expected_config_version": 1}


def test_omitted_expected_version_does_not_call_http() -> None:
    admin_transport = ScriptedSyncTransport([ok(_mutation_result())])
    public = ScriptedSyncTransport([])
    client = AdminClient(
        public,
        bearer_token=_ADMIN_TOKEN,
        admin_transport=admin_transport,
    )
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME["admin_activate_queue_policy"].call(
            client,
            _QUEUE_NAME,
            2,
            expected_config_version=None,
            idempotency_key="idem-1",
        )
    assert admin_transport.calls == []


def test_replay_requires_reason() -> None:
    replay_body = {
        "task_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "source_task_id": _TASK_ID,
        "queue": _QUEUE_NAME,
        "policy_version": 1,
        "replayed": False,
        "warning": "Replay is at-least-once.",
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }
    admin_transport = ScriptedSyncTransport([ok(replay_body)])
    public = ScriptedSyncTransport([])
    client = AdminClient(
        public,
        bearer_token=_ADMIN_TOKEN,
        admin_transport=admin_transport,
    )
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME["admin_replay_dead_letter"].call(
            client,
            _QUEUE_NAME,
            _TASK_ID,
            idempotency_key="replay-key-1",
            reason=None,
        )
    assert admin_transport.calls == []

    admin_transport = ScriptedSyncTransport([ok(replay_body)])
    client = AdminClient(
        ScriptedSyncTransport([]),
        bearer_token=_ADMIN_TOKEN,
        admin_transport=admin_transport,
    )
    REGISTRY_BY_NAME["admin_replay_dead_letter"].call(
        client,
        _QUEUE_NAME,
        _TASK_ID,
        idempotency_key="replay-key-2",
        reason="operator recovery",
    )
    assert len(admin_transport.calls) == 1
    assert admin_transport.calls[0].json_body() == {"reason": "operator recovery"}


def test_forbidden_filter_omits_value() -> None:
    client, _, admin_transport = _admin([], [])
    with pytest.raises(ValueError) as exc_info:
        REGISTRY_BY_NAME["admin_preview_bulk_cancel"].call(
            client,
            _QUEUE_NAME,
            filters={"payload": _FILTER_CANARY},
        )
    assert admin_transport.calls == []
    projected = project_error(exc_info.value)
    blob = json.dumps(projected)
    assert _FILTER_CANARY not in blob
    assert projected["kind"] == "invalid_argument"


def test_list_is_one_page() -> None:
    client, _, admin_transport = _admin([], [ok(page_body([_queue_body()]))])
    REGISTRY_BY_NAME["admin_list_queues"].call(client)
    admin_transport.assert_request_count(1)
    assert "cursor=" not in (admin_transport.calls[0].path or "")


def test_preview_does_not_execute() -> None:
    client, _, admin_transport = _admin([], [ok(_preview_result("bulk_cancel"))])
    REGISTRY_BY_NAME["admin_preview_bulk_cancel"].call(client, _QUEUE_NAME)
    admin_transport.assert_request_count(1)
    path = admin_transport.calls[0].path
    assert path is not None
    assert "preview-cancel" in path
    assert "execute" not in path