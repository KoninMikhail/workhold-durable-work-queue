"""Tool policy only narrows Workhold authorization (MCP-12, MCP-13).

A selected tool is not a grant. The role token and the downstream queue scope
still decide the call. Every row runs on a scripted transport: no socket, no
Workhold server, no authorizer.
"""

from __future__ import annotations

from typing import Any

import pytest

from _workhold_client_core.errors import ProtocolError, raise_for_protocol_status
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin.admin import AdminClient
from workhold_admin.observer import ObserverClient
from workhold_client_testing import (
    ScriptedSyncTransport,
    enqueue_response_body,
    ok,
    protocol_error,
    task_body,
)
from workhold_mcp.config import ConfigError, load_settings
from workhold_mcp.policy import resolve_tool_policy
from workhold_mcp.registry import load_manifest, preset_names
from workhold_mcp.server import build_server, call_tool
from workhold_producer.client import ProducerClient

_OBSERVER = "matrix-observer-token"
_PRODUCER = "matrix-producer-token"
_ADMIN = "matrix-admin-token"
_PUBLIC = "http://127.0.0.1:9"
_ADMIN_URL = "http://127.0.0.1:10"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_REQUEST_ID = "33333333-3333-4333-8333-333333333333"
_CANARY = "matrix-denial-message-canary"
_ALL_TOKENS = {
    "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
    "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN_URL,
    "WORKHOLD_MCP_OBSERVER_TOKEN": _OBSERVER,
    "WORKHOLD_MCP_PRODUCER_TOKEN": _PRODUCER,
    "WORKHOLD_MCP_ADMIN_TOKEN": _ADMIN,
}
_PRODUCER_POLICY = frozenset(preset_names("producer"))
_EXCLUDED_NAMES = (
    "claimTasks",
    "forceLeaseExpiry",
    "consumer_claim",
    "worker_claim",
    "break_glass_force_lease_expiry",
)
_ERROR_KEYS = {
    "kind",
    "code",
    "retryable",
    "status_code",
    "request_id",
    "retry_after_ms",
}


def _denied(*, retryable: bool = False) -> ProtocolError:
    """The error the real transport raises for a scripted HTTP 403 body.

    ``ScriptedSyncTransport`` returns an error-status response as data, while
    ``HttpJsonTransport`` raises on it. Parsing the 403 body through
    ``raise_for_protocol_status`` yields the exception the client would see.
    """

    response = protocol_error(
        403,
        code="permission_denied",
        message=f"denied {_CANARY}",
        retryable=retryable,
        request_id=_REQUEST_ID,
        details={"echo": _CANARY},
    )
    try:
        raise_for_protocol_status(response.status_code, response.body)
    except ProtocolError as exc:
        return exc
    raise AssertionError("403 body did not raise")


def _patch_transports(
    monkeypatch: pytest.MonkeyPatch,
    steps: list[Any],
) -> list[ScriptedSyncTransport]:
    """Give every built client its own scripted transport, in build order."""

    built: list[ScriptedSyncTransport] = []

    def _from_config(cls: Any, config: Any) -> ScriptedSyncTransport:
        transport = ScriptedSyncTransport(list(steps))
        built.append(transport)
        return transport

    monkeypatch.setattr(HttpJsonTransport, "from_config", classmethod(_from_config))
    return built


def _build(
    monkeypatch: pytest.MonkeyPatch,
    names: frozenset[str],
    steps: list[Any],
    env: dict[str, str] | None = None,
) -> tuple[Any, list[ScriptedSyncTransport]]:
    built = _patch_transports(monkeypatch, steps)
    source = dict(_ALL_TOKENS if env is None else env)
    return build_server(load_settings(source, names=names), names), built


def _handler(server: Any, name: str) -> Any:
    for tool in server._tool_manager.list_tools():
        if tool.name == name:
            return tool.fn
    raise AssertionError(name)


def _names(server: Any) -> list[str]:
    return [tool.name for tool in server._tool_manager.list_tools()]


def _all_calls(transports: list[ScriptedSyncTransport]) -> list[Any]:
    return [call for transport in transports for call in transport.calls]


def _bearer(call: Any) -> str:
    return call.headers(include_secrets=True)["Authorization"]


def _enqueue(handler: Any, queue_name: str, key: str) -> Any:
    return handler(queue_name=queue_name, idempotency_key=key, payload={"n": 1})


def test_selected_tool_sends_role_token_and_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    server, built = _build(
        monkeypatch,
        _PRODUCER_POLICY,
        [ok({"task": task_body(), "replayed": False})],
    )
    result = _enqueue(_handler(server, "producer_enqueue"), "orders", "idem-matrix-1")
    assert result.is_error is False
    calls = _all_calls(built)
    assert len(calls) == 1
    assert _bearer(calls[0]) == f"Bearer {_PRODUCER}"
    assert "/orders" in calls[0].path
    for other in (_OBSERVER, _ADMIN):
        assert other not in _bearer(calls[0])


def test_each_role_sends_only_its_own_token(monkeypatch: pytest.MonkeyPatch) -> None:
    names = frozenset({"producer_enqueue", "observer_get_task", "admin_list_queues"})
    server, built = _build(monkeypatch, names, [_denied(), _denied(), _denied()])
    assert sorted(_names(server)) == sorted(names)
    seen: list[Any] = []
    expected = {
        "producer_enqueue": _PRODUCER,
        "observer_get_task": _OBSERVER,
        "admin_list_queues": _ADMIN,
    }
    for name, token in expected.items():
        handler = _handler(server, name)
        if name == "producer_enqueue":
            handler_result = _enqueue(handler, "orders", "idem-matrix-2")
        elif name == "observer_get_task":
            handler_result = handler(task_id=_TASK_ID)
        else:
            handler_result = handler()
        assert handler_result.is_error is True
        assert handler_result.structured_content["code"] == "permission_denied"
        fresh = [call for call in _all_calls(built) if all(call is not old for old in seen)]
        assert len(fresh) == 1
        assert _bearer(fresh[0]) == f"Bearer {token}"
        seen.append(fresh[0])


def test_unselected_tool_makes_no_http_request(monkeypatch: pytest.MonkeyPatch) -> None:
    server, built = _build(monkeypatch, _PRODUCER_POLICY, [ok({})])
    assert "admin_list_queues" not in _names(server)
    assert "observer_get_task" not in _names(server)
    with pytest.raises(AssertionError):
        _handler(server, "admin_list_queues")

    script = ScriptedSyncTransport([ok({})])
    admin = AdminClient(script, bearer_token=_ADMIN)
    observer = ObserverClient(script, bearer_token=_OBSERVER, admin_transport=script)
    for client, name in (
        (admin, "admin_list_queues"),
        (observer, "observer_get_task"),
        (observer, "observer_get_queue"),
    ):
        result = call_tool(client, name, {}, selected=_PRODUCER_POLICY)
        assert result.is_error is True
        assert result.structured_content["kind"] == "invalid_argument"
    assert script.calls == []
    assert _all_calls(built) == []


@pytest.mark.parametrize("name", _EXCLUDED_NAMES)
def test_excluded_names_make_no_http_request(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    known = frozenset(row.name for row in load_manifest())
    assert name not in known
    with pytest.raises(ConfigError):
        resolve_tool_policy(("--allow-tool", name), _ALL_TOKENS, known)

    server, built = _build(monkeypatch, _PRODUCER_POLICY, [ok({})])
    assert name not in _names(server)

    script = ScriptedSyncTransport([ok({})])
    producer = ProducerClient(script, bearer_token=_PRODUCER)
    result = call_tool(producer, name, {"queue_name": "orders"}, selected=_PRODUCER_POLICY)
    assert result.is_error is True
    assert script.calls == []
    assert _all_calls(built) == []


def test_selected_tool_projects_permission_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for retryable in (False, True):
        server, built = _build(
            monkeypatch,
            _PRODUCER_POLICY,
            [_denied(retryable=retryable)],
        )
        result = _enqueue(_handler(server, "producer_enqueue"), "orders", "idem-matrix-3")
        assert result.is_error is True
        projected = result.structured_content
        assert set(projected) == _ERROR_KEYS
        assert projected["kind"] == "protocol"
        assert projected["code"] == "permission_denied"
        assert projected["retryable"] is retryable
        assert projected["status_code"] == 403
        assert projected["request_id"] == _REQUEST_ID
        assert "message" not in projected
        assert _CANARY not in result.content[0].text
        calls = _all_calls(built)
        assert len(calls) == 1
        assert _bearer(calls[0]) == f"Bearer {_PRODUCER}"


def test_both_queue_names_are_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    server, built = _build(
        monkeypatch,
        _PRODUCER_POLICY,
        [ok({"task": task_body(), "replayed": False}), _denied()],
    )
    handler = _handler(server, "producer_enqueue")
    allowed = _enqueue(handler, "orders", "idem-matrix-4")
    denied = _enqueue(handler, "billing", "idem-matrix-5")
    assert allowed.is_error is False
    assert denied.is_error is True
    assert denied.structured_content["code"] == "permission_denied"
    calls = _all_calls(built)
    assert len(calls) == 2
    assert "/orders" in calls[0].path
    assert "/billing" in calls[1].path
    for call in calls:
        assert _bearer(call) == f"Bearer {_PRODUCER}"


def test_adapter_keeps_no_queue_allowlist() -> None:
    script = ScriptedSyncTransport([ok(enqueue_response_body()) for _ in range(3)])
    producer = ProducerClient(script, bearer_token=_PRODUCER)
    for index, queue in enumerate(("orders", "billing", "never-seen-queue")):
        result = call_tool(
            producer,
            "producer_enqueue",
            {"queue_name": queue, "idempotency_key": f"idem-{index}", "payload": {"n": 1}},
            selected=_PRODUCER_POLICY,
        )
        assert result.is_error is False
    assert [queue in call.path for queue, call in zip(
        ("orders", "billing", "never-seen-queue"), script.calls, strict=True
    )] == [True, True, True]
