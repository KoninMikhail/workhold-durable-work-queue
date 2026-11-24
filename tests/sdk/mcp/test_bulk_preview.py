"""Bulk execute MCP tools bind to the whole preview object (recording transport)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from workhold_admin.admin import AdminClient
from workhold_client_testing import ScriptedSyncTransport, ok
from workhold_mcp.errors import project_error
from workhold_mcp.registry import REGISTRY_BY_NAME
from workhold_mcp.server import call_tool

_ADMIN_TOKEN = "admin-secret-token"
_QUEUE_NAME = "orders"
_OTHER_QUEUE = "billing"
_PREVIEW_TOKEN = "preview-token-abc"
_PREVIEW_CANARY = "WHMCP-PREVIEW-CANARY"
_IDEMPOTENCY_KEY = "bulk-replay-key-1"
_REASON = "operator recovery"
_FILTERS = {"failure_code": "handler_error"}

_REPLAY = "admin_execute_bulk_replay"
_CANCEL = "admin_execute_bulk_cancel"


def _preview_dict(
    operation: str = "bulk_replay",
    *,
    queue: str = _QUEUE_NAME,
    token: str = _PREVIEW_TOKEN,
    **overrides: Any,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": operation,
        "queue": queue,
        "candidate_count": 2,
        "truncated": False,
        "sample_task_ids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
        "confirmation_token": token,
        "confirmation_expires_at": "2026-09-19T01:00:00Z",
        "max_batch": 25,
    }
    body.update(overrides)
    return body


def _execute_result(operation: str = "bulk_replay") -> dict[str, Any]:
    return {
        "operation": operation,
        "queue": _QUEUE_NAME,
        "candidate_count": 2,
        "start_index": 0,
        "processed": 0,
        "succeeded": 0,
        "skipped": 0,
        "failed": 0,
        "partial": False,
        "outcomes": [],
    }


def _admin(
    admin_steps: list[Any],
) -> tuple[AdminClient, ScriptedSyncTransport]:
    admin_transport = ScriptedSyncTransport(admin_steps)
    client = AdminClient(
        ScriptedSyncTransport([]),
        bearer_token=_ADMIN_TOKEN,
        admin_transport=admin_transport,
    )
    return client, admin_transport


def _replay_arguments(**overrides: Any) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "queue_name": _QUEUE_NAME,
        "preview": _preview_dict("bulk_replay"),
        "idempotency_key": _IDEMPOTENCY_KEY,
        "reason": _REASON,
        "filters": dict(_FILTERS),
    }
    arguments.update(overrides)
    return arguments


def _cancel_arguments(**overrides: Any) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "queue_name": _QUEUE_NAME,
        "preview": _preview_dict("bulk_cancel"),
        "reason": _REASON,
        "filters": dict(_FILTERS),
    }
    arguments.update(overrides)
    return arguments


def test_execute_replay_parses_preview() -> None:
    client, admin_transport = _admin([ok(_execute_result("bulk_replay"))])
    REGISTRY_BY_NAME[_REPLAY].call(client, **_replay_arguments())
    admin_transport.assert_request_count(1)
    call = admin_transport.calls[0]
    assert call.path is not None
    assert "execute-replay" in call.path
    headers = call.headers(include_secrets=True)
    assert headers["Idempotency-Key"] == _IDEMPOTENCY_KEY
    body = call.json_body(include_secrets=True)
    assert isinstance(body, dict)
    assert body["confirmation_token"] == _PREVIEW_TOKEN
    assert body["reason"] == _REASON
    assert body["filters"] == _FILTERS


def test_execute_forwards_start_index_and_batch_limit() -> None:
    client, admin_transport = _admin(
        [
            ok(_execute_result("bulk_replay")),
            ok(_execute_result("bulk_replay")),
            ok(_execute_result("bulk_cancel")),
            ok(_execute_result("bulk_cancel")),
        ]
    )
    REGISTRY_BY_NAME[_REPLAY].call(
        client, **_replay_arguments(start_index=7, batch_limit=3)
    )
    REGISTRY_BY_NAME[_REPLAY].call(client, **_replay_arguments())
    REGISTRY_BY_NAME[_CANCEL].call(
        client, **_cancel_arguments(start_index=9, batch_limit=4)
    )
    REGISTRY_BY_NAME[_CANCEL].call(client, **_cancel_arguments())
    admin_transport.assert_request_count(4)
    bodies = [
        call.json_body(include_secrets=True) for call in admin_transport.calls
    ]
    assert bodies[0]["start_index"] == 7
    assert bodies[0]["batch_limit"] == 3
    assert bodies[1]["start_index"] == 0
    assert "batch_limit" not in bodies[1]
    assert bodies[2]["start_index"] == 9
    assert bodies[2]["batch_limit"] == 4
    assert bodies[3]["start_index"] == 0
    assert "batch_limit" not in bodies[3]
    for call in admin_transport.calls[2:]:
        assert "Idempotency-Key" not in call.headers(include_secrets=True)


def test_execute_cancel_has_no_idempotency_header() -> None:
    cancel_schema = REGISTRY_BY_NAME[_CANCEL].input_schema
    assert "idempotency_key" not in cancel_schema["properties"]
    assert "idempotency_key" not in cancel_schema["required"]
    client, admin_transport = _admin([ok(_execute_result("bulk_cancel"))])
    REGISTRY_BY_NAME[_CANCEL].call(client, **_cancel_arguments())
    admin_transport.assert_request_count(1)
    call = admin_transport.calls[0]
    assert call.path is not None
    assert "execute-cancel" in call.path
    assert "Idempotency-Key" not in call.headers(include_secrets=True)


def test_execute_schema_requires_preview_object_not_a_token() -> None:
    for name in (_REPLAY, _CANCEL):
        schema = REGISTRY_BY_NAME[name].input_schema
        assert "confirmation_token" not in schema["properties"]
        assert "preview" in schema["required"]
        assert schema["properties"]["preview"]["type"] == "object"


def test_bare_confirmation_token_makes_no_http_request() -> None:
    client, admin_transport = _admin(
        [ok(_execute_result("bulk_replay")), ok(_execute_result("bulk_cancel"))]
    )
    replay = REGISTRY_BY_NAME[_REPLAY]
    cancel = REGISTRY_BY_NAME[_CANCEL]
    replay_arguments = {
        "queue_name": _QUEUE_NAME,
        "confirmation_token": _PREVIEW_TOKEN,
        "idempotency_key": _IDEMPOTENCY_KEY,
        "reason": _REASON,
        "filters": dict(_FILTERS),
    }
    cancel_arguments = {
        "queue_name": _QUEUE_NAME,
        "confirmation_token": _PREVIEW_TOKEN,
        "reason": _REASON,
        "filters": dict(_FILTERS),
    }
    assert call_tool(client, replay.name, replay_arguments).is_error is True
    assert call_tool(client, cancel.name, cancel_arguments).is_error is True
    with pytest.raises((TypeError, ValueError)):
        replay.call(client, **replay_arguments)
    with pytest.raises((TypeError, ValueError)):
        cancel.call(client, **cancel_arguments)
    assert admin_transport.calls == []


def test_queue_mismatch_makes_no_http_request() -> None:
    client, admin_transport = _admin([ok(_execute_result("bulk_replay"))])
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME[_REPLAY].call(
            client,
            **_replay_arguments(
                preview=_preview_dict("bulk_replay", queue=_OTHER_QUEUE)
            ),
        )
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME[_CANCEL].call(
            client,
            **_cancel_arguments(
                preview=_preview_dict("bulk_cancel", queue=_OTHER_QUEUE)
            ),
        )
    assert admin_transport.calls == []


def test_operation_mismatch_makes_no_http_request() -> None:
    client, admin_transport = _admin([ok(_execute_result("bulk_cancel"))])
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME[_CANCEL].call(
            client,
            **_cancel_arguments(preview=_preview_dict("bulk_replay")),
        )
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME[_REPLAY].call(
            client,
            **_replay_arguments(preview=_preview_dict("bulk_cancel")),
        )
    assert admin_transport.calls == []


def test_execute_does_not_preview_or_reuse_a_prior_preview() -> None:
    client, admin_transport = _admin(
        [ok(_preview_dict("bulk_replay")), ok(_execute_result("bulk_replay"))]
    )
    REGISTRY_BY_NAME["admin_preview_bulk_replay"].call(
        client, queue_name=_QUEUE_NAME
    )
    admin_transport.assert_request_count(1)
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME[_REPLAY].call(
            client,
            queue_name=_QUEUE_NAME,
            idempotency_key=_IDEMPOTENCY_KEY,
            reason=_REASON,
            filters=dict(_FILTERS),
        )
    admin_transport.assert_request_count(1)
    paths = [call.path or "" for call in admin_transport.calls]
    assert all("execute" not in path for path in paths)


def test_preview_result_includes_confirmation_token() -> None:
    client, admin_transport = _admin([ok(_preview_dict("bulk_cancel"))])
    result = call_tool(client, "admin_preview_bulk_cancel", {"queue_name": _QUEUE_NAME})
    admin_transport.assert_request_count(1)
    assert result.is_error is False
    assert result.structured_content["confirmation_token"] == _PREVIEW_TOKEN
    assert result.structured_content["operation"] == "bulk_cancel"
    assert _PREVIEW_TOKEN in json.dumps(result.structured_content)

    bad_preview = _preview_dict(
        "bulk_replay", token=_PREVIEW_CANARY, candidate_count=999
    )
    failed = call_tool(
        client,
        _REPLAY,
        _replay_arguments(preview=bad_preview),
    )
    assert failed.is_error is True
    assert failed.structured_content["kind"] == "invalid_argument"
    blob = json.dumps(failed.structured_content) + "".join(
        item.text for item in failed.content if hasattr(item, "text")
    )
    assert _PREVIEW_CANARY not in blob
    with pytest.raises(ValueError) as exc_info:
        REGISTRY_BY_NAME[_REPLAY].call(
            client, **_replay_arguments(preview=bad_preview)
        )
    assert _PREVIEW_CANARY not in json.dumps(project_error(exc_info.value))
    admin_transport.assert_request_count(1)
