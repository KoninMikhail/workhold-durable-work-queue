"""Docs contract for the optional local MCP observer adapter."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADR_DIR = ROOT / "docs" / "04-architecture" / "adr"
GUIDE = ROOT / "docs" / "02-guides" / "07-local-mcp-observer.md"
PROTOCOL = ROOT / "docs" / "04-architecture" / "09-client-protocol.md"

_UNTOUCHED = (
    "031-separate-server-and-client-release-trains.md",
    "013-separate-security-planes.md",
    "015-single-image-multi-role.md",
    "032-client-side-content-key.md",
)


def _adapter_adr() -> Path:
    matches = sorted(ADR_DIR.glob("*-optional-local-mcp-adapter.md"))
    assert len(matches) == 1
    return matches[0]


def test_local_mcp_adr_and_guide() -> None:
    adr = _adapter_adr()
    text = adr.read_text(encoding="utf-8")
    assert "Optional local MCP adapter" in text.splitlines()[0]
    prefix = int(adr.name[:3])
    others = [
        int(path.name[:3])
        for path in ADR_DIR.glob("[0-9][0-9][0-9]-*.md")
        if path != adr
    ]
    assert others
    assert prefix != 32
    assert "workhold-mcp" in text
    assert "stdio" in text

    guide = GUIDE.read_text(encoding="utf-8")
    for name in (
        "WORKHOLD_MCP_PUBLIC_BASE_URL",
        "WORKHOLD_MCP_ADMIN_BASE_URL",
        "WORKHOLD_MCP_OBSERVER_TOKEN",
        "observer_list_dead_letters",
    ):
        assert name in guide
    assert "--token" not in guide

    protocol = PROTOCOL.read_text(encoding="utf-8")
    assert adr.name in protocol

    for name in _UNTOUCHED:
        body = (ADR_DIR / name).read_text(encoding="utf-8")
        assert "Superseded by" not in body
