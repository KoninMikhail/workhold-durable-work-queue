"""Wave 0 contract tests for English developer documentation landing path."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

READING_PATH = Path("docs/00-onboarding/01-reading-path.md")
DOCS_README = Path("docs/README.md")
ROOT_README = Path("README.md")
CONCEPTS_README = Path("docs/01-concepts/README.md")
AI_README = Path("docs/_ai/README.md")

READING_PATH_H2 = (
    "## Start",
    "## How it works",
    "## First integration",
    "## FAQ",
    "## Reference",
)

ALLOWED_NUMBERED_SECTIONS = frozenset(
    {
        "00-onboarding",
        "01-concepts",
        "02-guides",
        "03-reference",
        "04-architecture",
        "05-operations",
        "06-faq",
        "07-troubleshooting",
        "08-examples",
    }
)

_ATX_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
_FENCE_RE = re.compile(r"^```(\w*)\s*\n(.*?)^```\s*$", re.MULTILINE | re.DOTALL)
_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def atx_headings(text: str, levels: tuple[int, ...] = (1, 2)) -> list[str]:
    wanted = set(levels)
    out: list[str] = []
    for match in _ATX_RE.finditer(text):
        level = len(match.group(1))
        if level in wanted:
            out.append(f"{'#' * level} {match.group(2).strip()}")
    return out


def has_cyrillic(s: str) -> bool:
    return _CYRILLIC_RE.search(s) is not None


def fences(text: str) -> list[tuple[str, str]]:
    return [(m.group(1), m.group(2)) for m in _FENCE_RE.finditer(text)]


def test_reading_path_exists() -> None:
    assert (ROOT / READING_PATH).is_file()


def test_reading_path_step_headings_in_order() -> None:
    text = read(READING_PATH.as_posix())
    h2 = [h for h in atx_headings(text, levels=(2,)) if h in READING_PATH_H2]
    assert h2 == list(READING_PATH_H2)


def test_docs_readme_opens_with_reading_path_headings() -> None:
    text = read(DOCS_README.as_posix())
    table_marker = "| Section | Contents |"
    table_at = text.index(table_marker)
    preface = text[:table_at]
    h2 = [h for h in atx_headings(preface, levels=(2,)) if h in READING_PATH_H2]
    assert h2 == list(READING_PATH_H2)


def test_reading_path_and_docs_readme_link_next_pages() -> None:
    for rel in (READING_PATH.as_posix(), DOCS_README.as_posix()):
        text = read(rel)
        assert "how-it-works.md" in text
        assert "06-faq" in text
        assert "integrate-application.md" in text or "02-guides" in text
        assert "03-reference" in text or "04-architecture" in text


def test_root_readme_where_next_leads_with_reading_path() -> None:
    text = read(ROOT_README.as_posix())
    marker = "## Where next"
    assert marker in text
    after = text.split(marker, 1)[1]
    rows = [
        line
        for line in after.splitlines()
        if line.startswith("|") and "---" not in line and "Topic" not in line and "Where" not in line
    ]
    assert rows, "expected a markdown table under ## Where next"
    assert "reading-path.md" in rows[0]


def test_indexes_link_reading_path() -> None:
    for rel in (CONCEPTS_README.as_posix(), AI_README.as_posix()):
        text = read(rel)
        assert "reading-path.md" in text
        assert "how-it-works.md" in text
        assert "06-faq" in text
    for rel in (DOCS_README.as_posix(), AI_README.as_posix()):
        text = read(rel)
        assert "02-guides" in text
        assert "06-faq" in text
        assert "07-troubleshooting" in text
        assert "08-examples" in text


def test_numbered_docs_sections_are_allowlisted() -> None:
    docs = ROOT / "docs"
    numbered = [
        p.name
        for p in docs.iterdir()
        if p.is_dir() and re.fullmatch(r"\d{2}-[a-z0-9-]+", p.name)
    ]
    assert numbered, "expected at least one numbered docs/NN-* directory"
    unexpected = sorted(set(numbered) - ALLOWED_NUMBERED_SECTIONS)
    assert not unexpected, f"unexpected numbered docs sections: {unexpected}"


def test_no_bilingual_en_ru_files() -> None:
    assert not list(ROOT.glob("docs/**/*.en.md"))
    assert not list(ROOT.glob("docs/**/*.ru.md"))


# --- FAQ section (Phase 09 Plan 03 / DOC-04) ---

FAQ_DIR = Path("docs/06-faq")
FAQ_README = FAQ_DIR / "README.md"
FAQ_LOCKED_SLUGS = (
    "05-exactly-once.md",
    "02-shared-platform-bus.md",
    "04-application-db.md",
    "06-business-result.md",
    "07-workflow-dag.md",
    "09-spawn-vs-events.md",
    "10-pause-vs-drain.md",
    "13-worker-dies.md",
    "11-missing-named-queue.md",
    "12-claim-id-vs-claim-token.md",
    "01-why-not-rabbitmq-kafka.md",
)
FAQ_REQUIRED_TOKENS = (
    "exactly-once",
    "at-least-once",
    "spawn[]",
    "events[]",
    "claim_id",
    "claim_token",
    "RabbitMQ",
    "Kafka",
    "why-queue.md",
)
_H1_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)


def _faq_question_texts() -> str:
    parts: list[str] = []
    for slug in FAQ_LOCKED_SLUGS:
        parts.append(read((FAQ_DIR / slug).as_posix()))
    return "\n".join(parts)


def test_faq_section_exists() -> None:
    assert (ROOT / FAQ_README).is_file()
    text = read(FAQ_README.as_posix())
    for slug in FAQ_LOCKED_SLUGS:
        assert slug in text, f"FAQ README must link {slug}"
    assert "reading-path.md" in text


def test_faq_one_file_per_locked_slug() -> None:
    for slug in FAQ_LOCKED_SLUGS:
        path = ROOT / FAQ_DIR / slug
        assert path.is_file(), f"missing FAQ file: {slug}"


def test_faq_required_tokens() -> None:
    body = _faq_question_texts()
    for token in FAQ_REQUIRED_TOKENS:
        assert token in body, f"FAQ bodies must contain {token!r}"
    assert ("pause" in body) or ("paused" in body)
    assert ("drain" in body) or ("draining" in body)


def test_faq_no_monolithic_faq_md() -> None:
    assert not (ROOT / "docs/01-concepts/faq.md").exists()


def test_faq_one_h1_per_file() -> None:
    for slug in FAQ_LOCKED_SLUGS:
        text = read((FAQ_DIR / slug).as_posix())
        h1s = _H1_RE.findall(text)
        assert len(h1s) == 1, f"{slug} must have exactly one ATX H1, found {len(h1s)}"
        assert not has_cyrillic(h1s[0]), f"{slug} H1 must not contain Cyrillic"
        assert "POST /v1/tasks" not in text
        assert "claim_token=" not in text


# --- Guides section (Phase 09 Plan 04 / DOC-03) ---

GUIDES_DIR = Path("docs/02-guides")
GUIDE_FILES = (
    "README.md",
    "02-producer-enqueue.md",
    "03-worker-claim-complete.md",
    "04-admin-queues.md",
    "01-integrate-application.md",
)
GUIDE_FORBIDDEN_SUBSTRINGS = (
    "POST /v1/tasks",
    "/claims/{claim_id}/heartbeat",
    "/claims/{claim_id}/complete",
    "claim_token=",
)


def test_guide_files_exist() -> None:
    for name in GUIDE_FILES:
        path = ROOT / GUIDES_DIR / name
        assert path.is_file(), f"missing guide file: {name}"
    readme = read((GUIDES_DIR / "README.md").as_posix())
    assert "reading-path.md" in readme


def test_guide_paths_are_safe() -> None:
    roots = (ROOT / GUIDES_DIR, ROOT / EXAMPLES_DIR)
    offenders: list[str] = []
    for guides_root in roots:
        if not guides_root.is_dir():
            continue
        for path in sorted(guides_root.rglob("*")):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8")
            rel = path.relative_to(ROOT).as_posix()
            for needle in GUIDE_FORBIDDEN_SUBSTRINGS:
                if needle in text:
                    offenders.append(f"{rel}: {needle!r}")
    assert not offenders, "forbidden guide/example path strings:\n" + "\n".join(offenders)


def test_worker_guide_has_claim_token_header() -> None:
    text = read((GUIDES_DIR / "03-worker-claim-complete.md").as_posix())
    assert "X-Queue-Claim-Token" in text
    assert "POST /v1/claims/{claim_id}:heartbeat" in text
    assert "tasks: []" in text
    assert "204" not in text or "not HTTP 204" in text


# --- why-queue (Phase 09 Plan 10 / DOC-06) ---

WHY_QUEUE = Path("docs/01-concepts/02-why-queue.md")
WHY_QUEUE_H2 = (
    "## Why Workhold for developers",
    "## What Workhold is not",
    "## Comparison with RabbitMQ and Kafka",
    "## When they complement each other",
    "## When Workhold is not needed",
)
WHY_QUEUE_TOKENS = (
    "Work Queue",
    "RabbitMQ",
    "Kafka",
    "at-least-once",
    "spawn[]",
    "events[]",
    "Delivery Outbox",
    "```mermaid",
)


def test_why_queue_exists() -> None:
    assert (ROOT / WHY_QUEUE).is_file()
    for rel in (
        READING_PATH.as_posix(),
        CONCEPTS_README.as_posix(),
        AI_README.as_posix(),
    ):
        assert "why-queue.md" in read(rel)


def test_why_queue_required_headings() -> None:
    text = read(WHY_QUEUE.as_posix())
    h2 = [h for h in atx_headings(text, levels=(2,)) if h in WHY_QUEUE_H2]
    assert h2 == list(WHY_QUEUE_H2)
    for heading in WHY_QUEUE_H2:
        assert not has_cyrillic(heading)


def test_why_queue_required_tokens() -> None:
    text = read(WHY_QUEUE.as_posix())
    for token in WHY_QUEUE_TOKENS:
        assert token in text, f"why-queue.md must contain {token!r}"
    assert "product-boundary.md" in text
    assert "018-http-first-delivery-relay.md" in text
    assert "POST /v1/tasks" not in text
    assert "claim_token=" not in text
    mermaid_bodies = [body for lang, body in fences(text) if lang == "mermaid"]
    assert mermaid_bodies, "expected at least one mermaid fence"
    first = mermaid_bodies[0].lstrip()
    assert first.startswith("flowchart"), "first mermaid fence must be flowchart"


# --- Troubleshooting (Phase 09 Plan 11 / DOC-07) ---

TROUBLE_DIR = Path("docs/07-troubleshooting")
TROUBLE_README = TROUBLE_DIR / "README.md"
TROUBLE_LOCKED_SLUGS = (
    "01-enqueue-response-lost.md",
    "02-idempotency-key-conflict.md",
    "03-lease-lost.md",
    "04-side-effect-then-lease-lost.md",
    "05-complete-response-lost.md",
    "06-paused-empty-claim.md",
    "07-draining-enqueue-rejected.md",
    "08-dead-letter.md",
    "09-cancel-vs-complete.md",
    "10-relay-duplicate-publish.md",
)
TROUBLE_REQUIRED_TOKENS = (
    "at-least-once",
    "claim_token",
    "lease",
    "spawn[]",
    "events[]",
)


def _trouble_bodies() -> str:
    return "\n".join(read((TROUBLE_DIR / slug).as_posix()) for slug in TROUBLE_LOCKED_SLUGS)


def test_troubleshooting_section_exists() -> None:
    assert (ROOT / TROUBLE_README).is_file()
    text = read(TROUBLE_README.as_posix())
    assert "reading-path.md" in text
    for slug in TROUBLE_LOCKED_SLUGS:
        assert slug in text


def test_troubleshooting_one_file_per_locked_slug() -> None:
    for slug in TROUBLE_LOCKED_SLUGS:
        assert (ROOT / TROUBLE_DIR / slug).is_file()


def test_troubleshooting_required_tokens() -> None:
    body = _trouble_bodies()
    for token in TROUBLE_REQUIRED_TOKENS:
        assert token in body, f"troubleshooting must contain {token!r}"
    assert ("dead letter" in body) or ("dead-letter" in body) or ("dead letter" in body.lower())
    assert "guarantees.md" in body
    assert "POST /v1/tasks" not in body
    assert "claim_token=" not in body


def test_troubleshooting_one_h1_per_file() -> None:
    for slug in TROUBLE_LOCKED_SLUGS:
        text = read((TROUBLE_DIR / slug).as_posix())
        h1s = _H1_RE.findall(text)
        assert len(h1s) == 1, f"{slug} must have exactly one ATX H1"
        assert not has_cyrillic(h1s[0])


# --- Examples (Phase 09 Plan 12 / DOC-08) ---

EXAMPLES_DIR = Path("docs/08-examples")
EXAMPLES_README = EXAMPLES_DIR / "README.md"
EXAMPLES_LOCKED_SLUGS = (
    "01-dbless-app.md",
    "02-business-db-bridge.md",
    "03-complete-and-spawn.md",
    "04-complete-and-events.md",
    "05-retry-then-dead-letter.md",
    "06-cooperative-cancel.md",
)
EXAMPLES_REQUIRED_TOKENS = (
    "Work Queue",
    "spawn[]",
    "events[]",
    "Delivery Outbox",
    "at-least-once",
    "outbox",
)


def _examples_bodies() -> str:
    return "\n".join(
        read((EXAMPLES_DIR / slug).as_posix()) for slug in EXAMPLES_LOCKED_SLUGS
    )


def test_examples_section_exists() -> None:
    assert (ROOT / EXAMPLES_README).is_file()
    text = read(EXAMPLES_README.as_posix())
    assert "reading-path.md" in text
    for slug in EXAMPLES_LOCKED_SLUGS:
        assert slug in text


def test_examples_one_file_per_locked_slug() -> None:
    for slug in EXAMPLES_LOCKED_SLUGS:
        assert (ROOT / EXAMPLES_DIR / slug).is_file()


def test_examples_required_tokens() -> None:
    body = _examples_bodies()
    for token in EXAMPLES_REQUIRED_TOKENS:
        assert token in body, f"examples must contain {token!r}"


def test_examples_cite_guides() -> None:
    body = _examples_bodies()
    assert "integrate-application.md" in body
    assert "worker-claim-complete.md" in body


def test_examples_one_h1_per_file() -> None:
    for slug in EXAMPLES_LOCKED_SLUGS:
        text = read((EXAMPLES_DIR / slug).as_posix())
        h1s = _H1_RE.findall(text)
        assert len(h1s) == 1
        assert not has_cyrillic(h1s[0])
        assert "POST /v1/tasks" not in text
        assert "claim_token=" not in text


# --- Phase gate lint (Phase 09 Plan 09 / DOC-01 DOC-02 DOC-05) ---

ADR_H2_ALLOWLIST = frozenset(
    {
        "## Context",
        "## Decision",
        "## Alternatives considered",
        "## Consequences",
        "## References",
    }
)
REQUIRED_MERMAID_PAGES = (
    "docs/01-concepts/03-how-it-works.md",
    "docs/01-concepts/02-why-queue.md",
    "docs/04-architecture/03-data-flow.md",
    "docs/04-architecture/01-state-machine.md",
    "docs/01-concepts/10-transactional-outbox.md",
    "docs/01-concepts/11-inbox.md",
)
NARRATIVE_AND_GUIDE_FILES = (
    "docs/01-concepts/03-how-it-works.md",
    "docs/01-concepts/02-why-queue.md",
    "docs/02-guides/README.md",
    "docs/02-guides/02-producer-enqueue.md",
    "docs/02-guides/03-worker-claim-complete.md",
    "docs/02-guides/04-admin-queues.md",
    "docs/02-guides/01-integrate-application.md",
)


def test_required_narrative_and_guide_files_exist() -> None:
    for rel in NARRATIVE_AND_GUIDE_FILES:
        assert (ROOT / rel).is_file(), f"missing {rel}"


def test_how_it_works_lifecycle_tokens() -> None:
    text = read("docs/01-concepts/03-how-it-works.md")
    for token in (
        "enqueue",
        "claim",
        "heartbeat",
        "spawn[]",
        "events[]",
        "```mermaid",
    ):
        assert token in text


def test_required_mermaid_pages() -> None:
    for rel in REQUIRED_MERMAID_PAGES:
        text = read(rel)
        assert "```mermaid" in text, f"{rel} must contain mermaid fence"


def test_no_plantuml() -> None:
    offenders: list[str] = []
    for path in list((ROOT / "docs").rglob("*.md")) + [ROOT / "README.md"]:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        if "```plantuml" in text or "```puml" in text:
            offenders.append(path.relative_to(ROOT).as_posix())
    assert not offenders, f"plantuml fences forbidden: {offenders}"


def test_data_flow_and_state_machine_have_no_text_fences() -> None:
    for rel in (
        "docs/04-architecture/03-data-flow.md",
        "docs/04-architecture/01-state-machine.md",
    ):
        text = read(rel)
        assert "```text" not in text, f"{rel} must not contain ```text"


def test_architecture_operations_heading_language() -> None:
    roots = (
        ROOT / "docs" / "04-architecture",
        ROOT / "docs" / "05-operations",
    )
    offenders: list[str] = []
    for root in roots:
        for path in root.rglob("*.md"):
            text = path.read_text(encoding="utf-8")
            for heading in atx_headings(text, levels=(1, 2)):
                if heading in ADR_H2_ALLOWLIST:
                    continue
                if has_cyrillic(heading):
                    offenders.append(
                        f"{path.relative_to(ROOT).as_posix()}: {heading}"
                    )
    assert not offenders, "Cyrillic architecture/operations headings:\n" + "\n".join(
        offenders
    )


def test_agents_links_developer_path() -> None:
    text = read("AGENTS.md")
    for token in (
        "reading-path.md",
        "how-it-works.md",
        "why-queue.md",
        "06-faq",
        "07-troubleshooting",
        "08-examples",
        "02-guides",
    ):
        assert token in text
    assert not (ROOT / "docs/00-onboarding/README.md").exists()
    assert not (ROOT / "docs/03-reference/README.md").exists()


def test_superseded_reference_banners() -> None:
    for rel in (
        "docs/03-reference/03-formats.md",
        "docs/03-reference/04-storage.md",
        "docs/03-reference/05-http-api.md",
    ):
        text = read(rel)
        head = "\n".join(text.splitlines()[:12])
        assert "Superseded" in head
        assert "not a contract" in head.lower()


def test_deployment_connection_budget_text_fence() -> None:
    text = read("docs/05-operations/02-deployment.md")
    assert "```text" in text
    assert "usable = max_connections" in text
