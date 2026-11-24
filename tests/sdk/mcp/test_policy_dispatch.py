"""Selected MCP tools are the process catalog and the only dispatch targets."""

from __future__ import annotations

import base64
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from _workhold_client_core.content_key import seal_task_payload
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin.admin import AdminClient
from workhold_admin.observer import ObserverClient
from workhold_client_testing import ScriptedSyncTransport, ok, task_body
from workhold_mcp.config import load_settings
from workhold_mcp.policy import resolve_tool_policy
from workhold_mcp.registry import load_manifest, preset_names
from workhold_mcp.server import build_server, call_tool
from workhold_producer.client import ProducerClient

_OBSERVER = "WHMCP-TOKEN-CANARY"
_PRODUCER = "WHMCP-PRODUCER-CANARY"
_ADMIN = "WHMCP-ADMIN-CANARY"
_KEY = "WHMCP-KEY-CANARY"
_PUBLIC = "http://127.0.0.1:9"
_ADMIN_URL = "http://127.0.0.1:10"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_QUEUE = "orders"
_PLAINTEXT = {"order_id": 7, "sku": "plaintext-sentinel"}
_KEY_BYTES = bytes(range(32))
_KEY_B64 = base64.b64encode(_KEY_BYTES).decode("ascii")
_REPO_ROOT = Path(__file__).resolve().parents[3]
_MCP_SRC = _REPO_ROOT / "packages" / "workhold-mcp" / "src"
_PROCESS_ENV_KEYS = frozenset(
    {
        "systemroot",
        "systemdrive",
        "windir",
        "path",
        "pathext",
        "comspec",
        "temp",
        "tmp",
        "userprofile",
        "homedrive",
        "homepath",
        "localappdata",
        "appdata",
        "programfiles",
        "programdata",
        "programfiles(x86)",
    }
)
_CANARIES = (_OBSERVER, _PRODUCER, _ADMIN, _KEY)


def _known_names() -> frozenset[str]:
    return frozenset(row.name for row in load_manifest())


def _observer_env(**extra: str) -> dict[str, str]:
    env = {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN_URL,
        "WORKHOLD_MCP_OBSERVER_TOKEN": _OBSERVER,
    }
    env.update(extra)
    return env


def _names(server: Any) -> list[str]:
    return [tool.name for tool in server._tool_manager.list_tools()]


def _handler(server: Any, name: str) -> Any:
    for tool in server._tool_manager.list_tools():
        if tool.name == name:
            return tool.fn
    raise AssertionError(name)


def _serve(monkeypatch: pytest.MonkeyPatch, argv: tuple[str, ...], env: dict[str, str]) -> Any:
    """Run ``cli.main`` until stdio, returning the server it built."""

    for key in list(os.environ):
        if key.startswith("WORKHOLD_MCP_") or key == "WORKHOLD_CONTENT_KEY":
            monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["workhold-mcp", *argv])
    captured: dict[str, Any] = {}

    def _run(self: Any, transport: str = "stdio") -> None:
        captured["server"] = self
        captured["transport"] = transport

    monkeypatch.setattr("workhold_mcp.server.MCPServer.run", _run)
    from workhold_mcp.cli import main

    with pytest.raises(SystemExit) as caught:
        main()
    assert caught.value.code == 0
    assert captured["transport"] == "stdio"
    return captured["server"]


def _patch_scripted(monkeypatch: pytest.MonkeyPatch, scripted: ScriptedSyncTransport) -> None:
    monkeypatch.setattr(
        HttpJsonTransport,
        "from_config",
        classmethod(lambda cls, config: scripted),
    )


def _process_env() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key.lower() in _PROCESS_ENV_KEYS
    }


def _run(env: dict[str, str], args: tuple[str, ...] = ()) -> subprocess.CompletedProcess[bytes]:
    merged = _process_env()
    merged.update(env)
    return subprocess.run(
        [sys.executable, "-m", "workhold_mcp", *args],
        cwd=_REPO_ROOT,
        env=merged,
        capture_output=True,
        check=False,
        timeout=20,
    )


def _assert_exit_2(completed: subprocess.CompletedProcess[bytes], setting: str) -> None:
    assert completed.returncode == 2
    assert completed.stdout == b""
    lines = completed.stderr.splitlines()
    assert len(lines) == 1
    assert lines[0] == f"workhold-mcp: invalid configuration: {setting}".encode()
    combined = completed.stdout + completed.stderr
    for canary in _CANARIES:
        assert canary.encode("utf-8") not in combined
    assert b"Traceback" not in completed.stderr
    assert b"ConfigError" not in completed.stderr
    assert b"ValueError" not in completed.stderr


def test_default_list_is_stable(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = list(preset_names("observer"))
    assert expected == sorted(expected)
    assert len(expected) == 9
    server = _serve(monkeypatch, (), _observer_env())
    first = _names(server)
    second = _names(server)
    assert first == second == expected
    resolved = resolve_tool_policy((), _observer_env(), _known_names())
    assert resolved == frozenset(expected)


def test_producer_preset_uses_producer_token(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = list(preset_names("producer"))
    assert expected == sorted(expected)
    assert len(expected) == 5
    scripted = ScriptedSyncTransport([ok({"task": task_body(), "replayed": False})])
    _patch_scripted(monkeypatch, scripted)
    env = {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_PRODUCER_TOKEN": _PRODUCER,
        "WORKHOLD_MCP_OBSERVER_TOKEN": _OBSERVER,
        "WORKHOLD_MCP_ADMIN_TOKEN": _ADMIN,
        "WORKHOLD_MCP_PRESET": "producer",
    }
    server = _serve(monkeypatch, (), env)
    assert _names(server) == expected
    handler = _handler(server, "producer_enqueue")
    handler(queue_name=_QUEUE, idempotency_key="idem-producer", payload={"n": 1})
    assert len(scripted.calls) == 1
    headers = scripted.calls[0].headers(include_secrets=True)
    assert headers["Authorization"] == f"Bearer {_PRODUCER}"
    assert _OBSERVER not in headers["Authorization"]
    assert _ADMIN not in headers["Authorization"]


def test_unselected_name_makes_no_http_request(monkeypatch: pytest.MonkeyPatch) -> None:
    scripted = ScriptedSyncTransport([ok(task_body())])
    _patch_scripted(monkeypatch, scripted)
    env = {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_PRODUCER_TOKEN": _PRODUCER,
        "WORKHOLD_MCP_PRESET": "producer",
    }
    server = _serve(monkeypatch, (), env)
    assert "observer_get_task" not in _names(server)
    observer = ObserverClient(scripted, bearer_token=_OBSERVER, admin_transport=scripted)
    result = call_tool(
        observer,
        "observer_get_task",
        {"task_id": _TASK_ID},
        selected=frozenset(preset_names("producer")),
    )
    assert scripted.calls == []
    assert result.is_error is True


def test_missing_selected_token_exits_2() -> None:
    producer_only = _run(
        {
            "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
            "WORKHOLD_MCP_OBSERVER_TOKEN": _OBSERVER,
            "WORKHOLD_MCP_PRESET": "producer",
        }
    )
    _assert_exit_2(producer_only, "WORKHOLD_MCP_PRODUCER_TOKEN")

    observer_only = _run(
        {
            "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
            "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN_URL,
            "WORKHOLD_MCP_ADMIN_TOKEN": _ADMIN,
            "WORKHOLD_MCP_PRESET": "observer",
        }
    )
    _assert_exit_2(observer_only, "WORKHOLD_MCP_OBSERVER_TOKEN")


def test_content_key_without_producer_tool_exits_2() -> None:
    keyed = _run(_observer_env(WORKHOLD_MCP_CONTENT_KEY=_KEY))
    _assert_exit_2(keyed, "WORKHOLD_MCP_CONTENT_KEY")

    empty = _run(
        {
            "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
            "WORKHOLD_MCP_PRODUCER_TOKEN": _PRODUCER,
            "WORKHOLD_MCP_PRESET": "producer",
            "WORKHOLD_MCP_CONTENT_KEY": "",
            "WORKHOLD_CONTENT_KEY": _KEY,
        }
    )
    _assert_exit_2(empty, "WORKHOLD_MCP_CONTENT_KEY")


def test_registered_enqueue_uses_mcp_content_key(monkeypatch: pytest.MonkeyPatch) -> None:
    constructed: dict[str, list[dict[str, Any]]] = {
        "producer": [],
        "observer": [],
        "admin": [],
    }
    import workhold_mcp.server as server_module

    def spy_producer(transport: Any, *, bearer_token: str, content_key: str | None = None) -> Any:
        constructed["producer"].append(
            {"bearer_token": bearer_token, "content_key": content_key}
        )
        return ProducerClient(transport, bearer_token=bearer_token, content_key=content_key)

    def spy_observer(
        transport: Any,
        *,
        bearer_token: str,
        admin_transport: Any,
        **kwargs: Any,
    ) -> Any:
        constructed["observer"].append(kwargs)
        assert "content_key" not in kwargs
        return ObserverClient(
            transport,
            bearer_token=bearer_token,
            admin_transport=admin_transport,
        )

    def spy_admin(*_args: Any, **kwargs: Any) -> Any:
        constructed["admin"].append(kwargs)
        raise AssertionError("AdminClient must not be constructed")

    monkeypatch.setattr(server_module, "ProducerClient", spy_producer, raising=False)
    monkeypatch.setattr(server_module, "ObserverClient", spy_observer, raising=False)
    monkeypatch.setattr(server_module, "AdminClient", spy_admin, raising=False)
    sealed = seal_task_payload(_PLAINTEXT, _KEY_BYTES)
    scripted = ScriptedSyncTransport(
        [ok({"task": task_body(payload=sealed), "replayed": False})]
    )
    _patch_scripted(monkeypatch, scripted)
    env = _observer_env(
        WORKHOLD_MCP_PRODUCER_TOKEN=_PRODUCER,
        WORKHOLD_MCP_CONTENT_KEY=_KEY_B64,
        WORKHOLD_MCP_ALLOWED_TOOLS="observer_get_task,producer_enqueue",
        WORKHOLD_CONTENT_KEY=_KEY,
    )
    server = _serve(monkeypatch, (), env)
    assert constructed["admin"] == []
    assert constructed["producer"] == [
        {"bearer_token": _PRODUCER, "content_key": _KEY_B64}
    ]
    assert constructed["observer"] == [{}]
    handler = _handler(server, "producer_enqueue")
    result = handler(
        queue_name=_QUEUE,
        idempotency_key="idem-envelope",
        payload=_PLAINTEXT,
    )
    wire = scripted.calls[0].json_body(include_secrets=True)
    assert isinstance(wire, dict)
    payload = wire["payload"]
    assert isinstance(payload, dict)
    assert payload.get("$workhold_enc") == 1
    assert "n" in payload and "c" in payload
    assert result.structured_content["task"]["payload"] == _PLAINTEXT
    assert _KEY_B64 not in result.content[0].text
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in _MCP_SRC.rglob("*.py")
    )
    assert "WORKHOLD_CONTENT_KEY" not in source


def test_custom_allowlist_lists_exactly_two_names(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = sorted(("observer_get_task", "producer_enqueue"))
    base = _observer_env(WORKHOLD_MCP_PRODUCER_TOKEN=_PRODUCER)
    scripted = ScriptedSyncTransport([ok(task_body())])
    _patch_scripted(monkeypatch, scripted)
    via_argv = _serve(
        monkeypatch,
        ("--allow-tool", "observer_get_task", "--allow-tool", "producer_enqueue"),
        base,
    )
    assert _names(via_argv) == expected
    argv_policy = resolve_tool_policy(
        ("--allow-tool", "observer_get_task", "--allow-tool", "producer_enqueue"),
        base,
        _known_names(),
    )
    assert argv_policy == frozenset(expected)

    env = dict(base)
    env["WORKHOLD_MCP_ALLOWED_TOOLS"] = "observer_get_task,producer_enqueue"
    via_env = _serve(monkeypatch, (), env)
    assert _names(via_env) == expected
    env_policy = resolve_tool_policy((), env, _known_names())
    assert env_policy == frozenset(expected)

    observer = ObserverClient(scripted, bearer_token=_OBSERVER, admin_transport=scripted)
    result = call_tool(
        observer,
        "observer_get_queue",
        {"queue_name": _QUEUE},
        selected=env_policy,
    )
    assert scripted.calls == []
    assert result.is_error is True
    direct = build_server(load_settings(env, names=env_policy), env_policy)
    assert _names(direct) == expected


def test_admin_origin_is_public_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    import workhold_mcp.server as server_module

    def spy(
        transport: Any,
        *,
        bearer_token: str,
        admin_transport: Any = None,
    ) -> AdminClient:
        captured["transport"] = transport
        captured["admin_transport"] = admin_transport
        captured["bearer_token"] = bearer_token
        return AdminClient(
            transport,
            bearer_token=bearer_token,
            admin_transport=admin_transport,
        )

    monkeypatch.setattr(server_module, "AdminClient", spy, raising=False)
    server = _serve(
        monkeypatch,
        ("--allow-tool", "admin_list_queues"),
        {
            "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN_URL,
            "WORKHOLD_MCP_ADMIN_TOKEN": _ADMIN,
        },
    )
    assert _names(server) == [] or "admin_list_queues" in _names(server)
    plane = captured["admin_transport"] or captured["transport"]
    assert plane.config.public_base_url.rstrip("/") == _ADMIN_URL
    assert captured["bearer_token"] == _ADMIN
    source = (server_module.__file__ or "").replace("\\", "/")
    text = Path(source).read_text(encoding="utf-8") if source else ""
    if not text:
        text = (_MCP_SRC / "workhold_mcp" / "server.py").read_text(encoding="utf-8")
    assert "AdminClient" in text
    assert "BreakGlassClient" not in text
    assert "workhold_consumer" not in text
