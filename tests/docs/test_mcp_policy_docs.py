"""Docs contract for MCP presets, the exposure ceiling, and the admin warning."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADR_DIR = ROOT / "docs" / "04-architecture" / "adr"
GUIDE = ROOT / "docs" / "02-guides" / "07-local-mcp-observer.md"
ADMIN = ROOT / "docs" / "05-operations" / "07-admin-tools.md"
PROTOCOL = ROOT / "docs" / "04-architecture" / "09-client-protocol.md"
README = ADR_DIR / "README.md"

_PRESERVED = (
    "013-separate-security-planes.md",
    "015-single-image-multi-role.md",
    "031-separate-server-and-client-release-trains.md",
    "032-client-side-content-key.md",
)


def _one(pattern: str) -> Path:
    matches = sorted(ADR_DIR.glob(pattern))
    assert len(matches) == 1, matches
    return matches[0]


def test_guide_documents_presets_and_authority() -> None:
    text = GUIDE.read_text(encoding="utf-8")
    for preset in ("observer", "producer", "operator", "routine"):
        assert preset in text
    assert "Omitted policy selects `observer`" in text
    assert "Role presets are not cumulative" in text
    assert "`routine` is the union" in text
    for name in (
        "--preset",
        "--allow-tool",
        "WORKHOLD_MCP_PRESET",
        "WORKHOLD_MCP_ALLOWED_TOOLS",
        "WORKHOLD_MCP_OBSERVER_TOKEN",
        "WORKHOLD_MCP_PRODUCER_TOKEN",
        "WORKHOLD_MCP_ADMIN_TOKEN",
    ):
        assert name in text
    assert "CLI and env together fail startup" in text
    assert "are not interchangeable" in text
    assert "content key is not a role credential" in text
    assert "ProducerClient" in text
    assert "constructor secret" in text
    assert "observer reads of encrypted payloads stay sealed" in text
    assert "observer reads of keyed payloads stay sealed" in text
    assert "execute needs the preview object" in text
    assert "selected tools" in text
    assert "role token" in text
    assert "Workhold queue scope" in text
    assert "no claim, heartbeat, complete, fail, or break-glass tool" in text


def test_guide_has_no_secret_flags() -> None:
    text = GUIDE.read_text(encoding="utf-8")
    assert "--token" not in text
    assert "--content-key" not in text


def test_adr_is_exposure_ceiling() -> None:
    adr = _one("*-mcp-tool-exposure-ceiling.md")
    text = adr.read_text(encoding="utf-8")
    assert "exposure ceiling" in text
    assert "MCP tool exposure ceiling" in text.splitlines()[0]
    prefix = int(adr.name[:3])
    others = [
        int(path.name[:3])
        for path in ADR_DIR.glob("[0-9][0-9][0-9]-*.md")
        if path != adr
    ]
    assert others
    assert prefix not in others
    adapter_prefix = int(_one("*-optional-local-mcp-adapter.md").name[:3])
    assert prefix == adapter_prefix + 1
    assert prefix != 32
    readme = README.read_text(encoding="utf-8")
    assert adr.name in readme
    for name in _PRESERVED:
        body = (ADR_DIR / name).read_text(encoding="utf-8")
        assert "Superseded by" not in body
    adapter = _one("*-optional-local-mcp-adapter.md").read_text(encoding="utf-8")
    assert "Superseded by" not in adapter


def test_admin_docs_warn_routine_mcp() -> None:
    text = ADMIN.read_text(encoding="utf-8")
    assert "routine MCP" in text
    assert "not a break-glass" in text
    assert "preview and execute stay bound" in text


def test_protocol_mentions_three_clients() -> None:
    text = PROTOCOL.read_text(encoding="utf-8")
    assert "Optional MCP policy narrows `ObserverClient`, `ProducerClient`, and `AdminClient`" in text
    assert "HTTP remains normative" in text
