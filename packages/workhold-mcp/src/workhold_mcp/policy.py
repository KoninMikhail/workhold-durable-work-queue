"""Fail-closed startup selection of manifest tool names.

``resolve_tool_policy`` accepts ``--preset`` or repeatable ``--allow-tool``,
or the matching ``WORKHOLD_MCP_*`` variables, and never both channels.
Omitted policy is the observer preset. The returned names are manifest tool
names. ``ConfigError`` carries a setting or flag name and never a supplied value.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from typing import NoReturn

from workhold_mcp.config import ConfigError
from workhold_mcp.registry import preset_names

_PRESET_ENV = "WORKHOLD_MCP_PRESET"
_ALLOWED_ENV = "WORKHOLD_MCP_ALLOWED_TOOLS"
_PRESET_FLAG = "--preset"
_ALLOW_FLAG = "--allow-tool"


class _PolicyParser(argparse.ArgumentParser):
    """Parse policy flags without writing argparse text to either stream."""

    def error(self, message: str) -> NoReturn:
        setting = None
        if _ALLOW_FLAG in message:
            setting = _ALLOW_FLAG
        elif _PRESET_FLAG in message:
            setting = _PRESET_FLAG
        raise ConfigError(setting)

    def exit(self, status: int = 0, message: str | None = None) -> NoReturn:
        raise ConfigError(None)


def resolve_tool_policy(
    argv: Sequence[str],
    environ: Mapping[str, str],
    known_names: frozenset[str],
) -> frozenset[str]:
    """Return the selected manifest tool names.

    ``known_names`` is the manifest ``name`` set. Preset membership comes from
    ``preset_names``. Invalid input raises ``ConfigError``.
    """

    cli_preset, cli_allow = _parse_argv(argv)
    cli_present = cli_preset is not None or cli_allow is not None
    preset_set = _PRESET_ENV in environ
    allowed_set = _ALLOWED_ENV in environ

    cli_names: frozenset[str] | None = None
    if cli_allow is not None:
        cli_names = _allow_names(cli_allow, _ALLOW_FLAG, known_names)
    env_names: frozenset[str] | None = None
    if allowed_set:
        env_names = _allow_names((environ[_ALLOWED_ENV],), _ALLOWED_ENV, known_names)

    if cli_preset is not None and cli_allow is not None:
        raise ConfigError(_PRESET_FLAG)
    if cli_present and preset_set:
        raise ConfigError(_PRESET_ENV)
    if cli_present and allowed_set:
        raise ConfigError(_ALLOWED_ENV)
    if preset_set and allowed_set:
        raise ConfigError(_PRESET_ENV)

    if cli_names is not None:
        return cli_names
    if cli_preset is not None:
        return _from_preset(cli_preset, _PRESET_FLAG)
    if env_names is not None:
        return env_names
    if preset_set:
        return _from_preset(environ[_PRESET_ENV], _PRESET_ENV)
    return frozenset(preset_names("observer"))


def _parse_argv(argv: Sequence[str]) -> tuple[str | None, tuple[str, ...] | None]:
    parser = _PolicyParser(prog="workhold-mcp", allow_abbrev=False, add_help=False)
    parser.add_argument(_PRESET_FLAG, default=None)
    parser.add_argument(_ALLOW_FLAG, dest="allow_tool", action="append")
    try:
        parsed, unknown = parser.parse_known_args(list(argv))
    except SystemExit:
        raise ConfigError(None) from None
    if unknown:
        token = unknown[0]
        if token.startswith("-"):
            flag, _sep, _value = token.partition("=")
            raise ConfigError(flag)
        raise ConfigError(None)
    if parsed.allow_tool is None:
        return parsed.preset, None
    return parsed.preset, tuple(parsed.allow_tool)


def _allow_names(
    values: Sequence[str],
    setting: str,
    known_names: frozenset[str],
) -> frozenset[str]:
    seen: set[str] = set()
    for value in values:
        for part in value.split(","):
            piece = part.strip()
            if piece == "" or piece in seen or piece not in known_names:
                raise ConfigError(setting)
            seen.add(piece)
    return frozenset(seen)


def _from_preset(preset_id: str, setting: str) -> frozenset[str]:
    try:
        names = preset_names(preset_id)
    except ValueError:
        raise ConfigError(setting) from None
    return frozenset(names)
