"""Doc contract tests for bounded static priority (WORK-16 / Phase 12 Plan 12)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]

PRODUCER = ROOT / "docs" / "02-guides" / "02-producer-enqueue.md"
WORKER = ROOT / "docs" / "02-guides" / "03-worker-claim-complete.md"
GUARANTEES = ROOT / "docs" / "01-concepts" / "09-guarantees.md"
USE_CASES = ROOT / "docs" / "01-concepts" / "08-use-cases.md"
STATE_MACHINE = ROOT / "docs" / "04-architecture" / "01-state-machine.md"
CONCURRENCY = ROOT / "docs" / "04-architecture" / "02-concurrency.md"
STORAGE_TOPO = ROOT / "docs" / "04-architecture" / "04-storage-topology.md"
BRIDGE = ROOT / "docs" / "04-architecture" / "10-application-outbox-bridge.md"
ARCH_MB = ROOT / "docs" / "_ai" / "architecture.md"
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"
STORAGE_CONTRACT = ROOT / "docs" / "03-reference" / "02-storage-contract.md"

PRIORITY_MIN = -32_768
PRIORITY_MAX = 32_767

DOC_PATHS = (
    PRODUCER,
    WORKER,
    GUARANTEES,
    USE_CASES,
    STATE_MACHINE,
    CONCURRENCY,
    STORAGE_TOPO,
    BRIDGE,
    ARCH_MB,
)

STALE_PATTERNS = (
    re.compile(r"MVP\s+принимает\s+только\s+`priority=0`", re.IGNORECASE),
    re.compile(r"для\s+MVP\s+—\s+`0`;\s+ненулевой\s+priority", re.IGNORECASE),
    re.compile(r"priority\s+enablement\s+\(Phase\s+12\)", re.IGNORECASE),
    re.compile(r"Ненулевой\s+`priority`\s+по-прежнему\s+отклоняется", re.IGNORECASE),
    re.compile(r"включение\s+ненулевого\s+`priority`\s+\(Phase\s+12\)", re.IGNORECASE),
    re.compile(r"CHECK,\s+принимающим\s+только\s+`0`", re.IGNORECASE),
    re.compile(r"отклоняет\s+ненулевой\s+priority", re.IGNORECASE),
)

EXCLUSION_PATTERNS = (
    re.compile(r"\bfairness\b", re.IGNORECASE),
    re.compile(r"\baging\b", re.IGNORECASE),
    re.compile(r"weighted\s+queue", re.IGNORECASE),
    re.compile(r"lease\s+preempt", re.IGNORECASE),
    re.compile(r"priority\s+band", re.IGNORECASE),
)

NEGATION_PREFIX = re.compile(
    r"(?:\bnot\b|\bno\b|\bnever\b|\bwithout\b|\bnon-goals?\b|\bвне\s+scope\b|"
    r"\bне\s+|\bбез\s+|\bExplicit\s+non-goals\b)",
    re.IGNORECASE,
)


def _read(path: Path) -> str:
    assert path.is_file(), f"missing {path}"
    return path.read_text(encoding="utf-8")


def _combined_docs() -> str:
    return "\n\n".join(_read(p) for p in DOC_PATHS)


def _load_openapi() -> dict[str, Any]:
    return json.loads(_read(OPENAPI_PATH))


def _openapi_priority_schemas(spec: dict[str, Any]) -> list[dict[str, Any]]:
    schemas = spec["components"]["schemas"]
    names = ("EnqueueTaskRequest", "SpawnRequest", "Task")
    out: list[dict[str, Any]] = []
    for name in names:
        out.append(schemas[name]["properties"]["priority"])
    return out


# ---------------------------------------------------------------------------
# Range / default / strict validation
# ---------------------------------------------------------------------------


def test_priority_range_and_default_documented() -> None:
    text = _combined_docs()
    assert "-32768" in text and "32767" in text
    assert re.search(r"default\s+`0`|default\s+0", text, re.IGNORECASE)
    assert re.search(r"strict\s+integer|строго\s+цел", text, re.IGNORECASE)


def test_openapi_priority_bounds_match_docs() -> None:
    spec = _load_openapi()
    schemas = spec["components"]["schemas"]
    enqueue = schemas["EnqueueTaskRequest"]["properties"]["priority"]
    assert enqueue["minimum"] == PRIORITY_MIN
    assert enqueue["maximum"] == PRIORITY_MAX
    assert enqueue.get("default") == 0
    desc = enqueue.get("description", "")
    assert "Higher numeric value" in desc
    assert "due candidates" in desc
    for name in ("SpawnRequest", "Task"):
        schema = schemas[name]["properties"]["priority"]
        assert schema["minimum"] == PRIORITY_MIN
        assert schema["maximum"] == PRIORITY_MAX


# ---------------------------------------------------------------------------
# Polarity and due-before-priority
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [PRODUCER, WORKER, GUARANTEES, CONCURRENCY, ARCH_MB],
)
def test_higher_priority_wins_among_due(path: Path) -> None:
    text = _read(path)
    assert re.search(
        r"выше\s+число|Higher numeric|`priority`\s+DESC|priority\s+DESC",
        text,
        re.IGNORECASE,
    )
    assert re.search(r"due|eligible|claimable", text, re.IGNORECASE)


def test_due_eligibility_precedes_priority_ordering() -> None:
    text = _read(CONCURRENCY) + _read(GUARANTEES) + _read(WORKER)
    assert re.search(r"Due gate|due eligibility|Due eligibility|due-first|раньше.*priority", text, re.IGNORECASE)
    assert "priority DESC" in text or "`priority` DESC" in text


def test_claim_tuple_order_documented() -> None:
    text = _read(CONCURRENCY) + _read(WORKER)
    assert re.search(
        r"priority\s+DESC.*available_at.*id|priority\s+DESC,\s*then\s+`available_at`\s+ASC,\s+then\s+`id`\s+ASC",
        text,
        re.IGNORECASE | re.DOTALL,
    )


def test_concurrency_claim_tuple_order_alone() -> None:
    text = _read(CONCURRENCY)
    assert re.search(
        r"`priority`\s+DESC.*`available_at`\s+ASC.*`id`\s+ASC",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    assert not re.search(
        r"scheduling policy \(`available_at`,\s*`priority\s+DESC`",
        text,
    )


def test_concurrency_index_alone() -> None:
    text = _read(CONCURRENCY)
    assert re.search(
        r"\(queue_id,\s*state_code,\s*priority\s+DESC,\s*available_at,\s*id\)",
        text,
    )
    assert not re.search(
        r"\(queue_id,\s*state_code,\s*available_at,\s*priority\s+DESC",
        text,
    )


def test_concurrency_due_gate_alone() -> None:
    text = _read(CONCURRENCY)
    assert re.search(
        r"due gate|Due eligibility|всегда раньше.*static priority",
        text,
        re.IGNORECASE,
    )


def test_concurrency_non_preemption_and_starvation_alone() -> None:
    text = _read(CONCURRENCY)
    assert re.search(r"неистёкш|unexpired\s+lease", text, re.IGNORECASE)
    assert re.search(r"будущ.*`available_at`|future\s+`available_at`", text, re.IGNORECASE)
    assert re.search(r"не\s+вытесн|Non-preemption", text, re.IGNORECASE)
    assert re.search(r"starve", text, re.IGNORECASE)
    assert re.search(r"истёкш|reclaim", text, re.IGNORECASE)


# ---------------------------------------------------------------------------
# Non-preemption, starvation, replay
# ---------------------------------------------------------------------------


def test_unexpired_lease_and_future_non_preemption() -> None:
    text = _read(WORKER) + _read(CONCURRENCY) + _read(GUARANTEES)
    assert re.search(r"неистёкш|unexpired\s+lease", text, re.IGNORECASE)
    assert re.search(r"future\s+`available_at`|будущ", text, re.IGNORECASE)
    assert re.search(r"не\s+вытесн|not\s+preempt|Non-preemption", text, re.IGNORECASE)


def test_starvation_and_expired_reclaim_documented() -> None:
    text = _read(CONCURRENCY) + _read(GUARANTEES)
    assert re.search(r"starve", text, re.IGNORECASE)
    assert re.search(r"expired\s+lease|истёкш", text, re.IGNORECASE)


def test_replay_preserves_source_priority() -> None:
    text = _read(GUARANTEES) + _read(USE_CASES) + _read(CONCURRENCY)
    assert re.search(r"Replay.*priority|сохраняет\s+source\s+`priority`", text, re.IGNORECASE | re.DOTALL)


# ---------------------------------------------------------------------------
# Storage / migration / downgrade
# ---------------------------------------------------------------------------


def test_storage_priority_first_index_and_migration() -> None:
    text = _read(STORAGE_TOPO) + _read(STORAGE_CONTRACT)
    assert "tasks_active_claim_idx" in text
    assert re.search(
        r"\(queue_id,\s*state_code,\s*priority\s+DESC,\s*available_at,\s*id\)",
        text,
        re.IGNORECASE,
    )
    assert "1201_bounded_priority_claim_ordering" in text
    assert re.search(r"fail-closed|fail closed", text, re.IGNORECASE)
    assert re.search(r"priority\s+<>\s+0", text)
    for match in re.finditer(r"reset\s+данн|destructive\s+cleanup|truncate", text, re.IGNORECASE):
        start = max(0, match.start() - 80)
        window = text[start : match.start()]
        assert re.search(r"\*\*не\*\*|не\s+предлаг|must not|do not", window, re.IGNORECASE), (
            f"doc must not recommend data reset near: {match.group(0)!r}"
        )


# ---------------------------------------------------------------------------
# Bridge capability rollout
# ---------------------------------------------------------------------------


def test_bridge_capability_rollout_documented() -> None:
    text = _read(BRIDGE)
    assert "Capability rollout" in text or "capability-rollout-priority" in text
    assert re.search(r"priority\s*=\s*\*\*false\*\*|capability.*false", text, re.IGNORECASE)
    assert re.search(r"priority=true", text, re.IGNORECASE)
    assert re.search(r"major\s+\*\*1\*\*|schema major\s+1", text, re.IGNORECASE)
    assert re.search(r"`priority:\s+0`|priority:\s+0", text)


# ---------------------------------------------------------------------------
# Producer idempotency / worker claim wording
# ---------------------------------------------------------------------------


def test_producer_idempotency_priority_conflict() -> None:
    text = _read(PRODUCER)
    assert re.search(r"idempotency_conflict|Idempotency-Key", text)
    assert "priority" in text


def test_worker_claim_section_title() -> None:
    text = _read(WORKER)
    assert "Claim order among due tasks" in text
    assert "priority" in text and "available_at" in text


# ---------------------------------------------------------------------------
# Stale zero-only statements
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", DOC_PATHS)
def test_no_stale_zero_only_priority_statements(path: Path) -> None:
    text = _read(path)
    offenders: list[str] = []
    for pattern in STALE_PATTERNS:
        for match in pattern.finditer(text):
            offenders.append(f"{path.name}: {pattern.pattern} -> {match.group(0)!r}")
    assert not offenders, "\n".join(offenders)


# ---------------------------------------------------------------------------
# Out-of-scope mechanisms not presented as shipped features
# ---------------------------------------------------------------------------


def test_exclusions_only_in_negated_context_when_present() -> None:
    """If fairness/aging/etc. appear, they must be in exclusion/non-goal wording."""
    text = _combined_docs()
    offenders: list[str] = []
    for pattern in EXCLUSION_PATTERNS:
        for match in pattern.finditer(text):
            start = max(0, match.start() - 100)
            window = text[start : match.start()]
            if NEGATION_PREFIX.search(window) is None:
                snippet = text[max(0, match.start() - 30) : match.end() + 30].replace("\n", " ")
                offenders.append(f"{pattern.pattern!r} without negation: …{snippet}…")
    assert not offenders, "\n".join(offenders)
