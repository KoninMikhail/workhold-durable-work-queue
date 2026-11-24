"""Map a tool's manifest role to the one OAuth scope that guards its group.

The map is keyed by the manifest ``role`` only. A preset id or an allowlist
entry never retags a tool. Scopes are an MCP-side filter on top of the startup
policy: they cannot add a tool the policy left out, and they never become a
Workhold credential.
"""

from __future__ import annotations

from collections.abc import Collection

from workhold_mcp.registry import load_manifest

__all__ = [
    "OPERATE_SCOPE",
    "PRODUCE_SCOPE",
    "READ_SCOPE",
    "required_scope",
    "scopes_allow",
    "tool_scope",
]

READ_SCOPE = "workhold-mcp:read"
PRODUCE_SCOPE = "workhold-mcp:produce"
OPERATE_SCOPE = "workhold-mcp:operate"

_SCOPE_BY_ROLE = {
    "observer": READ_SCOPE,
    "producer": PRODUCE_SCOPE,
    "admin": OPERATE_SCOPE,
}
_ROLE_BY_TOOL = {row.name: row.role for row in load_manifest()}


def required_scope(role: str) -> str:
    """Return the scope for one manifest role. An unknown role fails closed."""

    scope = _SCOPE_BY_ROLE.get(role)
    if scope is None:
        raise ValueError(f"unknown tool role: {role}")
    return scope


def tool_scope(tool_name: str) -> str | None:
    """Return the scope for one manifest tool name, or ``None`` if it is unknown."""

    role = _ROLE_BY_TOOL.get(tool_name)
    if role is None:
        return None
    return required_scope(role)


def scopes_allow(tool_name: str, granted: Collection[str]) -> bool:
    """True when ``granted`` holds the scope that guards ``tool_name``."""

    needed = tool_scope(tool_name)
    return needed is not None and needed in granted
