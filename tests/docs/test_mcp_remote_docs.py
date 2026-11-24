"""Docs contract for the remote MCP resource server."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADR_DIR = ROOT / "docs" / "04-architecture" / "adr"
GUIDE = ROOT / "docs" / "02-guides" / "07-local-mcp-observer.md"
GUIDES_INDEX = ROOT / "docs" / "02-guides" / "README.md"
DOCS_INDEX = ROOT / "docs" / "README.md"
SECURITY = ROOT / "docs" / "05-operations" / "01-security.md"
DEPLOYMENT = ROOT / "docs" / "05-operations" / "02-deployment.md"
PROTOCOL = ROOT / "docs" / "04-architecture" / "09-client-protocol.md"
ADR_README = ADR_DIR / "README.md"

_ADR_SLUG = "remote-mcp-resource-server"
_ADR_TITLE = "Remote MCP resource server"

_PHRASES = (
    "workhold-mcp",
    "/mcp",
    "workhold-mcp:read",
    "workhold-mcp:produce",
    "workhold-mcp:operate",
    "Host allowlist",
    "Origin allowlist",
    "no-browser",
    "startup tool policy",
    "MCP caller scope",
    "Workhold role and queue scope",
    "MCP does not add Workhold authority",
    "MCP does not add exactly-once",
    "https://mcp.example.invalid/mcp",
    "https://idp.example.invalid",
    "test-only",
    "is not one of `api`, `migrate`, `maintain`, `relay`, or `apply`",
)

_PLANNING_PATTERNS = (
    r"\.planning",
    r"ROADMAP\.md",
    r"PLAN\.md",
    r"SUMMARY\.md",
    r"VERIFICATION\.md",
    r"STATE\.md",
    r"CONTEXT\.md",
    r"\bMCP-\d{2}\b",
    r"\bWH-\d{2}\b",
)

_PRESERVED = (
    "013-separate-security-planes.md",
    "015-single-image-multi-role.md",
    "031-separate-server-and-client-release-trains.md",
    "032-client-side-content-key.md",
    "033-optional-local-mcp-adapter.md",
    "034-mcp-tool-exposure-ceiling.md",
)


def _remote_adr() -> Path:
    matches = sorted(ADR_DIR.glob(f"*-{_ADR_SLUG}.md"))
    assert len(matches) == 1, matches
    return matches[0]


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_remote_adr_is_next_number() -> None:
    adr = _remote_adr()
    text = _text(adr)
    assert _ADR_TITLE in text.splitlines()[0]
    prefix = int(adr.name[:3])
    others = [
        int(path.name[:3])
        for path in ADR_DIR.glob("[0-9][0-9][0-9]-*.md")
        if path != adr
    ]
    assert others
    assert prefix == max(others) + 1
    assert prefix != 32
    assert adr.name in _text(ADR_README)
    for name in _PRESERVED:
        assert "Superseded by" not in _text(ADR_DIR / name), name
    assert "Superseded by" not in text


def test_remote_phrases() -> None:
    adr = _remote_adr()
    pages = {
        "guide": GUIDE,
        "adr": adr,
        "security": SECURITY,
        "deployment": DEPLOYMENT,
    }
    for label, path in pages.items():
        body = _text(path)
        for phrase in _PHRASES:
            assert phrase in body, f"{label} is missing: {phrase}"
    for label, path in (
        ("guide", GUIDE),
        ("security", SECURITY),
        ("deployment", DEPLOYMENT),
        ("protocol", PROTOCOL),
    ):
        assert adr.name in _text(path), f"{label} does not link {adr.name}"


def test_public_text_has_no_planning_vocabulary() -> None:
    adr = _remote_adr()
    for path in (GUIDE, adr, SECURITY, DEPLOYMENT, PROTOCOL):
        body = _text(path)
        for pattern in _PLANNING_PATTERNS:
            assert re.search(pattern, body) is None, f"{path.name}: {pattern}"
    for path in (GUIDE, adr):
        assert "Phase 5" not in _text(path), path.name


def test_adr_is_short_and_rejects_the_listed_options() -> None:
    adr = _remote_adr()
    text = _text(adr)
    assert len(text.splitlines()) <= 60
    assert "Streamable HTTP" in text
    for heading in (
        "**Status:**",
        "**Date:**",
        "**Scope:**",
        "## Context",
        "## Decision",
        "## Alternatives considered",
        "## Consequences",
        "## References",
    ):
        assert heading in text, heading
    for option in (
        "SSE",
        "embedded IdP",
        "runtime image",
        "caller token",
        "rebinding protection",
    ):
        assert option in text, option
    assert "013-separate-security-planes.md" in text
    assert "015-single-image-multi-role.md" in text


def test_guide_documents_remote_setup() -> None:
    text = _text(GUIDE)
    for name in (
        "stdio",
        "--transport streamable-http",
        "--resource-url",
        "--issuer-url",
        "--jwks-url",
        "--allowed-host",
        "--allowed-origin",
        "--no-browser-origins",
        "--forwarded-allow-ip",
        "WORKHOLD_MCP_RESOURCE_URL",
        "WORKHOLD_MCP_ISSUER_URL",
        "WORKHOLD_MCP_JWKS_URL",
        "WORKHOLD_MCP_ALLOWED_HOSTS",
        "WORKHOLD_MCP_ALLOWED_ORIGINS",
        "WORKHOLD_MCP_NO_BROWSER_ORIGINS",
        "WORKHOLD_MCP_FORWARDED_ALLOW_IPS",
        "service principal",
    ):
        assert name in text, name
    assert "--disable-dns-rebinding-protection" not in text
    assert "--token" not in text
    assert "--content-key" not in text
    assert "BEGIN PRIVATE KEY" not in text


def test_security_and_deployment_boundaries() -> None:
    security = _text(SECURITY)
    assert "OAuth is not a service principal" in security
    deployment = _text(DEPLOYMENT)
    assert "separate process" in deployment
    protocol = _text(PROTOCOL)
    assert "optional adapter over the HTTP contract" in protocol


def test_guide_index_is_not_local_only() -> None:
    for path in (GUIDES_INDEX, DOCS_INDEX):
        text = _text(path)
        assert "07-local-mcp-observer.md" in text
        assert "local-only" not in text.lower()
    row = next(
        line for line in _text(GUIDES_INDEX).splitlines() if "07-local-mcp-observer.md" in line
    )
    assert "remote" in row.lower()
    assert "local stdio adapter" not in row
    assert not list((ROOT / "docs" / "02-guides").glob("*mcp*remote*"))
