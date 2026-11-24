"""Validate MCP startup settings, then serve on the selected transport."""

from __future__ import annotations

import logging
import os
import sys
from typing import NoReturn

from workhold_mcp.config import ConfigError, load_settings, resolve_http_config
from workhold_mcp.policy import resolve_tool_policy
from workhold_mcp.registry import load_manifest
from workhold_mcp.server import build_server


def main() -> None:
    """Validate policy and settings before serving on stdio or HTTP.

    Invalid configuration raises ``SystemExit(2)`` before the server runs.
    A clean shutdown raises ``SystemExit(0)``.
    """

    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    try:
        http = resolve_http_config(sys.argv[1:], os.environ)
        known_names = frozenset(row.name for row in load_manifest())
        policy = resolve_tool_policy(http.policy_argv, os.environ, known_names)
        settings = load_settings(names=policy)
        server = build_server(settings, policy)
    except ConfigError as exc:
        _reject(exc.setting_name)
    if http.transport == "streamable-http":
        from workhold_mcp.http_app import serve_streamable_http

        serve_streamable_http(server, http)
    else:
        server.run(transport="stdio")
    raise SystemExit(0)


def _reject(setting_name: str | None) -> NoReturn:
    if setting_name:
        message = f"workhold-mcp: invalid configuration: {setting_name}"
    else:
        message = "workhold-mcp: invalid configuration"
    print(message, file=sys.stderr)
    raise SystemExit(2)
