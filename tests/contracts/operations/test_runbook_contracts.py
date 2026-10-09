"""Documentation contracts for Phase 4 kernel alerts and recovery runbooks.

Remapped from plan path ``tests/contract/operations/`` to the existing
``tests/contracts/`` layout used by OpenAPI/storage contract suites.

Covers DEP-04 (PITR/correctness registries/duplicate-aware recovery) and
OPS-09 (alerts + runbooks for readiness, saturation, leases, DLQ, partitions,
restore).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
OPS = ROOT / "docs" / "05-operations"
RUNBOOKS = OPS / "runbooks.md"
DEPLOYMENT = OPS / "deployment.md"
OBSERVABILITY = OPS / "observability.md"
ADMIN_TOOLS = OPS / "admin-tools.md"
SECURITY = OPS / "security.md"
STORAGE = ROOT / "docs" / "04-architecture" / "storage-topology.md"
OPENAPI = ROOT / "openapi" / "queue.openapi.json"

REQUIRED_RUNBOOK_HEADINGS = (
    "API not ready / schema mismatch",
    "Connection pressure / slow claim",
    "Lease / reclaim storm",
    "Poison tasks / DLQ growth",
    "Partition premake / retention failure",
    "Disk / WAL / autovacuum pressure",
    "PITR restore and duplicate-aware recovery",
    "Credential rotation",
)

REQUIRED_SECTION_LABELS = (
    "### Detection signals",
    "### Severity",
    "### Prerequisites",
    "### Safe diagnosis",
    "### Supported private operations",
    "### Stop conditions",
    "### Rollback / containment",
    "### Post-recovery verification",
)

# Supported private ops / endpoints referenced by runbooks (Plans 01–09).
SUPPORTED_OPERATIONS = (
    "/readyz",
    "/healthz",
    "/admin/v1/stats",
    "/admin/v1/maintenance",
    "/admin/v1/maintenance:run",
    "/admin/v1/dead-letters",
    "/admin/v1/audit",
    "/admin/v1/tasks",
    "/admin/v1/attempts",
    ":set-state",
    ":replay",
    "bulk:preview-replay",
    "bulk:execute-replay",
    "bulk:preview-cancel",
    "bulk:execute-cancel",
    ":force-lease-expiry",
    ":reconcile-counters",
    ":raise-replay-limit",
    ":force-drop",
    "registry:repair",
    "/v1/claims",
    "/v1/queues/{queue_name}/tasks",
)

PITR_REQUIRED_PHRASES = (
    "stop workers",
    "block intake",
    "one consistent",
    "correctness registries",
    "enqueue_dedup",
    "complete_replay",
    "terminal",
    "attempt",
    "admin_audit_log",
    "schema",
    "horizon",
    "non-authoritative counters",
    "at-least-once",
    "duplicate",
    "retention is not backup",
    "idempoten",
)

PROHIBITED_PATTERNS = (
    re.compile(r"\bexactly[- ]once\b", re.IGNORECASE),
    re.compile(r"\bexact[- ]once\b", re.IGNORECASE),
    re.compile(r"\bSELECT\b.+\bFROM\b", re.IGNORECASE | re.DOTALL),
    re.compile(
        r"\b(INSERT|UPDATE|DELETE|TRUNCATE|ALTER)\b\s+\w+",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bDROP\s+(TABLE|INDEX|DATABASE|SCHEMA|PARTITION)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bpsql\b", re.IGNORECASE),
    re.compile(r"\bdirect SQL\b", re.IGNORECASE),
    re.compile(r"\barbitrary SQL\b", re.IGNORECASE),
)

# Phrases that explicitly forbid unsafe practice are allowed even if they
# mention SQL / exactly-once as negatives.
ALLOWLIST_NEGATIVE_CONTEXT = (
    re.compile(
        r"(do not|never|not|forbidden|prohibited|without).{0,80}"
        r"(exactly[- ]once|exact[- ]once|sql|psql)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(exactly[- ]once|exact[- ]once|sql|psql).{0,80}"
        r"(do not|never|not|forbidden|prohibited|required)",
        re.IGNORECASE | re.DOTALL,
    ),
)


def _read(path: Path) -> str:
    assert path.is_file(), f"missing documentation file: {path}"
    return path.read_text(encoding="utf-8")


def _heading_slug(title: str) -> str:
    """GitHub-like heading anchors: punctuation removed; each space becomes '-'."""
    slug = title.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug, flags=re.UNICODE)
    slug = slug.strip().replace(" ", "-")
    return slug.strip("-")


def _strip_allowlisted_negatives(text: str) -> str:
    cleaned = text
    for pattern in ALLOWLIST_NEGATIVE_CONTEXT:
        cleaned = pattern.sub(" ", cleaned)
    return cleaned


@pytest.fixture(scope="module")
def runbooks_text() -> str:
    return _read(RUNBOOKS)


@pytest.fixture(scope="module")
def deployment_text() -> str:
    return _read(DEPLOYMENT)


@pytest.fixture(scope="module")
def observability_text() -> str:
    return _read(OBSERVABILITY)


def test_required_runbook_files_exist() -> None:
    for path in (RUNBOOKS, DEPLOYMENT, OBSERVABILITY, ADMIN_TOOLS, SECURITY, STORAGE):
        assert path.is_file(), path


def test_required_runbook_headings_present(runbooks_text: str) -> None:
    for title in REQUIRED_RUNBOOK_HEADINGS:
        assert f"## {title}" in runbooks_text, f"missing runbook heading: {title}"


def test_each_runbook_has_required_sections(runbooks_text: str) -> None:
    parts = re.split(r"\n## ", runbooks_text)
    bodies: dict[str, str] = {}
    for part in parts[1:]:
        heading, _, body = part.partition("\n")
        bodies[heading.strip()] = body

    for title in REQUIRED_RUNBOOK_HEADINGS:
        assert title in bodies, title
        body = bodies[title]
        for label in REQUIRED_SECTION_LABELS:
            assert label in body, f"{title} missing {label}"


def test_alert_runbook_map_links_kernel_alerts(runbooks_text: str) -> None:
    assert "## Alert → runbook map" in runbooks_text
    for anchor_title in REQUIRED_RUNBOOK_HEADINGS:
        slug = _heading_slug(anchor_title)
        assert f"(#{slug})" in runbooks_text, f"map missing link to #{slug}"


def test_internal_markdown_links_resolve(runbooks_text: str) -> None:
    link_re = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
    for _label, target in link_re.findall(runbooks_text):
        if target.startswith("http://") or target.startswith("https://"):
            continue
        if target.startswith("#"):
            slug = target[1:]
            headings = re.findall(r"^## (.+)$", runbooks_text, flags=re.MULTILINE)
            slugs = {_heading_slug(h) for h in headings}
            sub = re.findall(r"^### (.+)$", runbooks_text, flags=re.MULTILINE)
            slugs |= {_heading_slug(h) for h in sub}
            assert slug in slugs, f"broken in-doc anchor: {target}"
            continue
        path_part, _, frag = target.partition("#")
        resolved = (RUNBOOKS.parent / path_part).resolve()
        assert resolved.is_file(), f"broken relative link: {target}"
        if frag:
            target_text = resolved.read_text(encoding="utf-8")
            headings = re.findall(r"^#{1,3} (.+)$", target_text, flags=re.MULTILINE)
            slugs = {_heading_slug(h) for h in headings}
            assert frag in slugs, f"broken fragment {target} (#{frag})"


def test_deployment_and_observability_link_runbooks(
    deployment_text: str, observability_text: str
) -> None:
    assert "runbooks.md" in deployment_text
    assert "enqueue_dedup" in deployment_text or "correctness registries" in deployment_text
    assert "at-least-once" in deployment_text
    assert "runbooks.md" in observability_text


def test_phase5_delivery_outbox_not_implemented_as_kernel_runbook(
    runbooks_text: str, deployment_text: str
) -> None:
    assert "Delivery Outbox lag" in deployment_text
    assert "Phase 5" in runbooks_text or "Phase 5" in deployment_text
    assert "## Delivery Outbox" not in runbooks_text


def test_supported_operations_are_named(runbooks_text: str) -> None:
    missing = [op for op in SUPPORTED_OPERATIONS if op not in runbooks_text]
    assert not missing, f"runbooks missing supported ops: {missing}"


def test_supported_admin_paths_exist_in_openapi(runbooks_text: str) -> None:
    catalog = _read(OPENAPI)
    required_in_openapi = (
        "/admin/v1/stats",
        "/admin/v1/maintenance",
        "/admin/v1/maintenance:run",
        "/admin/v1/dead-letters",
        "/admin/v1/audit",
        "/admin/v1/queues/{queue_name}:set-state",
        "/admin/v1/queues/{queue_name}/dead-letters/{task_id}:replay",
        "/admin/v1/queues/{queue_name}/bulk:preview-replay",
        "/admin/v1/queues/{queue_name}/bulk:execute-replay",
        "/admin/v1/queues/{queue_name}/bulk:preview-cancel",
        "/admin/v1/queues/{queue_name}/bulk:execute-cancel",
        "/admin/v1/queues/{queue_name}/tasks/{task_id}:force-lease-expiry",
        "/admin/v1/queues/{queue_name}:reconcile-counters",
        "/admin/v1/queues/{queue_name}:raise-replay-limit",
        "/admin/v1/partitions/{partition_name}:force-drop",
        "/admin/v1/queues/{queue_name}/registry:repair",
    )
    for path in required_in_openapi:
        assert path in catalog, f"OpenAPI missing {path}"

    for marker in (
        "maintenance:run",
        "set-state",
        "dead-letters",
        "preview-replay",
        "execute-replay",
        "preview-cancel",
        "execute-cancel",
        "force-lease-expiry",
        "reconcile-counters",
        "raise-replay-limit",
        "force-drop",
        "registry:repair",
        "/admin/v1/stats",
        "/admin/v1/maintenance",
        "/admin/v1/audit",
        "/admin/v1/tasks",
        "/admin/v1/attempts",
    ):
        assert marker in runbooks_text, f"runbooks omit OpenAPI op {marker}"


def test_pitr_documents_full_restore_and_duplicate_awareness(
    runbooks_text: str, deployment_text: str
) -> None:
    combined = runbooks_text + "\n" + deployment_text
    lower = combined.lower()
    for phrase in PITR_REQUIRED_PHRASES:
        assert phrase.lower() in lower, f"PITR contract missing phrase: {phrase}"


def test_prohibited_unsafe_language_absent(
    runbooks_text: str, deployment_text: str
) -> None:
    """Gate affirmative exactly-once / operator SQL instructions."""
    for path, text in ((RUNBOOKS, runbooks_text), (DEPLOYMENT, deployment_text)):
        scrubbed = _strip_allowlisted_negatives(text)
        match = None
        for pattern in PROHIBITED_PATTERNS:
            match = pattern.search(scrubbed)
            if match is not None:
                break
        assert match is None, (
            f"{path.name} contains prohibited language near: {match.group(0)!r}"
        )


def test_ops_readme_lists_runbooks() -> None:
    readme = _read(OPS / "README.md")
    assert "runbooks.md" in readme


def test_no_credential_or_payload_examples(runbooks_text: str) -> None:
    # Threat T-04-10-I: no tokens/payloads in runbooks.
    assert "Bearer " not in runbooks_text
    assert not re.search(r"(api[_-]?key|password|secret)\s*=\s*\S+", runbooks_text, re.I)
    assert '"payload"' not in runbooks_text
