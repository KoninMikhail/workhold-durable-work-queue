"""Minimal stdlib YAML mapping loader for qualification profiles.

Supports the constrained subset used by ``reference-environment.yaml``:
nested mappings and plain/quoted scalars (bool/null/int/float/string).
Lists are not required by the profile schema. No PyYAML dependency.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


class ProfileLoadError(ValueError):
    """Invalid or unsupported profile YAML."""


def load_profile(path: Path | str) -> dict[str, Any]:
    """Load a qualification reference-environment profile mapping."""
    text = Path(path).read_text(encoding="utf-8")
    data = loads_mapping(text)
    if not isinstance(data, dict):
        raise ProfileLoadError("profile root must be a mapping")
    return data


def loads_mapping(text: str) -> dict[str, Any]:
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        stripped = raw.lstrip(" ")
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw) - len(stripped)
        if "\t" in raw[: len(raw) - len(stripped)]:
            raise ProfileLoadError("tabs are not allowed in profile YAML")
        lines.append((indent, stripped))
    if not lines:
        raise ProfileLoadError("empty profile")
    value, next_idx = _parse_map(lines, 0, lines[0][0])
    if next_idx != len(lines):
        raise ProfileLoadError("trailing content after root mapping")
    return value


def _parse_map(
    lines: list[tuple[int, str]], idx: int, indent: int
) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {}
    while idx < len(lines):
        line_indent, content = lines[idx]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise ProfileLoadError(f"unexpected indent at: {content!r}")
        if content.startswith("- "):
            raise ProfileLoadError(
                f"lists are unsupported in reference profiles: {content!r}"
            )
        if ":" not in content:
            raise ProfileLoadError(f"expected key: value at: {content!r}")
        key, _, rest = content.partition(":")
        key = key.strip()
        rest = rest.strip()
        idx += 1
        if rest == "":
            if idx < len(lines) and lines[idx][0] > indent:
                child, idx = _parse_map(lines, idx, lines[idx][0])
                result[key] = child
            else:
                result[key] = {}
        else:
            result[key] = _parse_scalar(rest)
    return result, idx


def _parse_scalar(raw: str) -> Any:
    if raw in ("null", "~", "Null", "NULL"):
        return None
    if raw in ("true", "True", "TRUE"):
        return True
    if raw in ("false", "False", "FALSE"):
        return False
    if (raw.startswith('"') and raw.endswith('"')) or (
        raw.startswith("'") and raw.endswith("'")
    ):
        return raw[1:-1]
    try:
        if any(c in raw for c in ".eE"):
            return float(raw)
        return int(raw)
    except ValueError:
        return raw
