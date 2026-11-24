"""Loopback smoke of the published ``workhold-mcp`` Streamable HTTP command.

The flags come from the fenced command in the guide, so the guide and the
process cannot drift apart. Every socket binds ``127.0.0.1``. The key used to
sign the bearer is a throwaway test-only fixture served from a loopback JWKS
listener. It is not an identity provider and it is not part of the product.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from tests.sdk.mcp.jwks_fixture import base_claims, jwks_document, make_rsa_key, mint

_REPO_ROOT = Path(__file__).resolve().parents[3]
_GUIDE = _REPO_ROOT / "docs" / "02-guides" / "07-local-mcp-observer.md"
_LOOPBACK = "127.0.0.1"
_OBSERVER_CANARY = "WHMCP-SMOKE-OBSERVER-CANARY"
_START_TIMEOUT_SECONDS = 40.0
_READ_TIMEOUT_SECONDS = 15.0
_PUBLISHED_FLAGS = frozenset(
    {
        "--transport",
        "--resource-url",
        "--issuer-url",
        "--jwks-url",
        "--allowed-host",
        "--no-browser-origins",
        "--bind-host",
        "--bind-port",
    }
)
_TOOL_NAMES = (
    "observer_get_capabilities",
    "observer_get_task",
    "observer_list_task_attempts",
    "observer_get_queue",
    "observer_get_stats",
    "observer_get_maintenance_status",
    "observer_list_inspection_tasks",
    "observer_list_inspection_attempts",
    "observer_list_dead_letters",
)


def _published_arguments() -> list[str]:
    """Return the arguments of the guide's first remote ``workhold-mcp`` command."""

    text = _GUIDE.read_text(encoding="utf-8")
    for block in re.findall(r"```bash\n(.*?)```", text, flags=re.DOTALL):
        if "--transport streamable-http" not in block:
            continue
        joined = re.sub(r"\\\r?\n", " ", block)
        tokens = shlex.split(joined)
        commands = [index for index, token in enumerate(tokens) if token == "workhold-mcp"]
        assert commands, "the published remote command must start with workhold-mcp"
        return tokens[commands[0] + 1 :]
    pytest.fail("the guide publishes no `workhold-mcp --transport streamable-http` command")


def _localize(arguments: list[str], *, port: int, issuer: str, jwks: str) -> list[str]:
    """Point every published value at the loopback listeners, keeping the flag set."""

    replacements = {
        "--resource-url": f"http://{_LOOPBACK}:{port}/mcp",
        "--issuer-url": issuer,
        "--jwks-url": jwks,
        "--allowed-host": f"{_LOOPBACK}:{port}",
        "--bind-host": _LOOPBACK,
        "--bind-port": str(port),
    }
    out: list[str] = []
    seen: set[str] = set()
    index = 0
    while index < len(arguments):
        token = arguments[index]
        assert token.startswith("--"), f"unexpected positional argument: {token}"
        assert token in _PUBLISHED_FLAGS, f"unpublished flag in the guide command: {token}"
        seen.add(token)
        out.append(token)
        if token in replacements:
            out.append(replacements[token])
            index += 2
        elif token == "--transport":
            out.append(arguments[index + 1])
            index += 2
        else:
            index += 1
    assert {"--transport", "--resource-url", "--issuer-url", "--jwks-url", "--allowed-host"} <= seen
    assert "--no-browser-origins" in seen
    return out


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((_LOOPBACK, 0))
        return int(probe.getsockname()[1])


class _QuietHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class _JwksHandler(_QuietHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] == "/jwks":
            self._send(200, self.server.document)  # type: ignore[attr-defined]
            return
        self._send(404, {})


class _WorkholdHandler(_QuietHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.server.recorded.append(  # type: ignore[attr-defined]
            {"path": self.path, "authorization": self.headers.get("Authorization")}
        )
        if self.path.split("?", 1)[0] == "/v1/capabilities":
            self._send(200, _capabilities_body())
            return
        self._send(
            404,
            {
                "code": "task_not_found",
                "message": "missing route",
                "retryable": False,
                "request_id": "00000000-0000-4000-8000-000000000099",
                "details": {},
            },
        )


def _capabilities_body() -> dict[str, Any]:
    return {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": False,
        "max_claim_tasks": 1,
        "max_wait_seconds": 0,
        "payload_runtime_max_bytes": 262144,
        "payload_hard_max_bytes": 1048576,
        "enqueue_dedup_ttl_seconds": 7776000,
        "enqueue_dedup_ttl_min_seconds": 2592000,
        "enqueue_dedup_ttl_max_seconds": 31536000,
        "terminal_replay_ttl_seconds": 604800,
        "terminal_replay_ttl_min_seconds": 86400,
        "terminal_replay_ttl_max_seconds": 2592000,
        "admin_replay_ttl_seconds": 2592000,
        "admin_replay_ttl_min_seconds": 604800,
        "admin_replay_ttl_max_seconds": 7776000,
    }


def _serve(handler: type[BaseHTTPRequestHandler]) -> tuple[HTTPServer, threading.Thread]:
    server = HTTPServer((_LOOPBACK, 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _wait_until_listening(process: subprocess.Popen[bytes], port: int) -> None:
    deadline = time.monotonic() + _START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"workhold-mcp exited early with code {process.returncode}")
        try:
            with socket.create_connection((_LOOPBACK, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    pytest.fail("workhold-mcp did not listen on the loopback port in time")


@pytest.fixture
def loopback() -> Iterator[dict[str, Any]]:
    key = make_rsa_key("smoke-rsa")
    jwks_server, jwks_thread = _serve(_JwksHandler)
    jwks_server.document = jwks_document(key)  # type: ignore[attr-defined]
    workhold_server, workhold_thread = _serve(_WorkholdHandler)
    workhold_server.recorded = []  # type: ignore[attr-defined]
    issuer = f"http://{_LOOPBACK}:{jwks_server.server_address[1]}"
    try:
        yield {
            "key": key,
            "issuer": issuer,
            "jwks": f"{issuer}/jwks",
            "workhold_url": f"http://{_LOOPBACK}:{workhold_server.server_address[1]}",
            "recorded": workhold_server.recorded,  # type: ignore[attr-defined]
        }
    finally:
        for server, thread in ((jwks_server, jwks_thread), (workhold_server, workhold_thread)):
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


def _child_environment(workhold_url: str) -> dict[str, str]:
    env = {name: value for name, value in os.environ.items() if not name.startswith("WORKHOLD_MCP_")}
    env.update(
        {
            "WORKHOLD_MCP_PUBLIC_BASE_URL": workhold_url,
            "WORKHOLD_MCP_ADMIN_BASE_URL": workhold_url,
            "WORKHOLD_MCP_OBSERVER_TOKEN": _OBSERVER_CANARY,
        }
    )
    return env


async def test_loopback_smoke(loopback: dict[str, Any]) -> None:
    port = _free_port()
    arguments = _localize(
        _published_arguments(),
        port=port,
        issuer=loopback["issuer"],
        jwks=loopback["jwks"],
    )
    resource = f"http://{_LOOPBACK}:{port}/mcp"
    token = mint(
        loopback["key"],
        base_claims(
            issuer=loopback["issuer"],
            resource=resource,
            scope="workhold-mcp:read",
        ),
    )
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
        process = subprocess.Popen(
            [sys.executable, "-m", "workhold_mcp", *arguments],
            cwd=str(_REPO_ROOT),
            env=_child_environment(loopback["workhold_url"]),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=errlog,
        )
        try:
            _wait_until_listening(process, port)
            async with httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {token}"},
                timeout=_READ_TIMEOUT_SECONDS,
            ) as http:
                async with streamable_http_client(
                    resource,
                    http_client=http,
                    terminate_on_close=False,
                ) as (read, write):
                    async with ClientSession(
                        read,
                        write,
                        read_timeout_seconds=_READ_TIMEOUT_SECONDS,
                    ) as session:
                        await session.initialize()
                        listed = await session.list_tools()
                        result = await session.call_tool("observer_get_capabilities", arguments={})
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        errlog.seek(0)
        captured = errlog.read()

    assert [tool.name for tool in listed.tools] == sorted(_TOOL_NAMES)
    assert result.is_error is False
    assert result.structured_content is not None
    assert result.structured_content["protocol_major"] == 1
    recorded = loopback["recorded"]
    assert recorded
    assert recorded[0]["authorization"] == f"Bearer {_OBSERVER_CANARY}"
    assert token not in recorded[0]["authorization"]
    assert token not in captured
    assert _OBSERVER_CANARY not in captured
    assert process.poll() is not None
