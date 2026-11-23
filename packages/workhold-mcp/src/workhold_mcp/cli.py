"""Validate the tool policy and MCP settings, then serve on stdio."""

from __future__ import annotations

import logging
import os
import sys
from typing import NoReturn

from workhold_mcp.config import ConfigError, load_settings
from workhold_mcp.policy import resolve_tool_policy
from workhold_mcp.registry import load_manifest
from workhold_mcp.server import build_server


def main() -> None:
    """Configure stderr logging, validate policy and settings, and block on stdio.

    Invalid configuration raises ``SystemExit(2)`` before the server runs.
    A clean shutdown raises ``SystemExit(0)``.
    """

    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    try:
        known_names = frozenset(row.name for row in load_manifest())
        policy = resolve_tool_policy(sys.argv[1:], os.environ, known_names)
        settings = load_settings(names=policy)
        server = build_server(settings, policy)
    except ConfigError as exc:
        _reject(exc.setting_name)
    server.run(transport="stdio")
    raise SystemExit(0)


def _reject(setting_name: str | None) -> NoReturn:
    if setting_name:
        message = f"workhold-mcp: invalid configuration: {setting_name}"
    else:
        message = "workhold-mcp: invalid configuration"
    print(message, file=sys.stderr)
    raise SystemExit(2)
