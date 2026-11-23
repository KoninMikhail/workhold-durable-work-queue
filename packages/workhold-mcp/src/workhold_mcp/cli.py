"""Validate MCP settings, then serve observer tools on stdio."""

from __future__ import annotations

import logging
import sys
from typing import NoReturn

from workhold_mcp.config import ConfigError, build_observer, load_settings
from workhold_mcp.server import build_server


def main() -> None:
    """Configure stderr logging, validate settings, and block on stdio.

    Invalid configuration raises ``SystemExit(2)`` before the server runs.
    A clean shutdown raises ``SystemExit(0)``.
    """

    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    if len(sys.argv) != 1:
        _reject(None)
    try:
        settings = load_settings()
    except ConfigError as exc:
        _reject(exc.setting_name)
    observer = build_observer(settings)
    server = build_server(observer)
    server.run(transport="stdio")
    raise SystemExit(0)


def _reject(setting_name: str | None) -> NoReturn:
    if setting_name:
        message = f"workhold-mcp: invalid configuration: {setting_name}"
    else:
        message = "workhold-mcp: invalid configuration"
    print(message, file=sys.stderr)
    raise SystemExit(2)
