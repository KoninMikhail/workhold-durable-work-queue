"""Offline shape checks for the initial physical-contract Alembic revision."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REVISION_PATH = (
    ROOT / "alembic" / "versions" / "0001_physical_contract_foundations.py"
)

REVISION_ID = "0001_physical_contract_foundations"

UNPARTITIONED = {
    "queues",
    "queue_policy_versions",
    "tasks_active",
    "task_payloads_active",
    "delivery_events_active",
    "enqueue_dedup",
    "claim_registry",
    "complete_replay",
    "admin_replay",
    "completion_effects",
    "queue_counters",
    "partition_maintenance_status",
}

DAILY_RANGE_PARENTS = {
    "admin_audit_log": "audit_at",
    "task_attempts": "claimed_at",
    "tasks_terminal": "terminal_at",
    "delivery_events_terminal": "terminal_at",
}

BASELINE_INDEXES = {
    "tasks_active_claim_idx",
    "tasks_active_spawn_lineage_uidx",
    "enqueue_dedup_expires_at_idx",
    "complete_replay_expires_at_idx",
    "admin_replay_expires_at_idx",
    "admin_audit_log_queue_audit_idx",
    "task_attempts_task_claimed_idx",
    "tasks_terminal_task_terminal_idx",
    "tasks_terminal_spawn_lineage_idx",
    "delivery_events_terminal_event_idx",
}

HORIZON_DAYS_AHEAD = 30


def _load_revision_source() -> str:
    assert REVISION_PATH.is_file(), f"missing revision: {REVISION_PATH}"
    return REVISION_PATH.read_text(encoding="utf-8")


def _upgrade_body(source: str) -> str:
    match = re.search(
        r"def upgrade\(\)[^:]*:\n(.*)\ndef downgrade\(\)",
        source,
        flags=re.DOTALL,
    )
    assert match, "upgrade() body not found"
    return match.group(1)


def _downgrade_body(source: str) -> str:
    match = re.search(r"def downgrade\(\)[^:]*:\n(.*)\Z", source, flags=re.DOTALL)
    assert match, "downgrade() body not found"
    return match.group(1)


@pytest.fixture(scope="module")
def source() -> str:
    return _load_revision_source()


def test_revision_identity_and_down_revision(source: str) -> None:
    assert re.search(
        rf'^revision(?:\s*:\s*str)?\s*=\s*["\']{REVISION_ID}["\']',
        source,
        flags=re.MULTILINE,
    )
    assert re.search(
        r"^down_revision(?:\s*:\s*[^=]+)?\s*=\s*None\b",
        source,
        flags=re.MULTILINE,
    )


def test_covers_every_accepted_relation(source: str) -> None:
    for table in sorted(UNPARTITIONED | set(DAILY_RANGE_PARENTS)):
        assert re.search(
            rf"\bCREATE\s+TABLE\s+{table}\b",
            source,
            flags=re.IGNORECASE,
        ), f"missing CREATE TABLE for {table}"


def test_covers_every_baseline_index(source: str) -> None:
    for index_name in sorted(BASELINE_INDEXES):
        assert re.search(
            rf"\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+{index_name}\b",
            source,
            flags=re.IGNORECASE,
        ), f"missing index {index_name}"


def test_daily_utc_range_parents(source: str) -> None:
    for parent, key in DAILY_RANGE_PARENTS.items():
        pattern = (
            rf"CREATE\s+TABLE\s+{parent}\b[\s\S]*?"
            rf"PARTITION\s+BY\s+RANGE\s*\(\s*{key}\s*\)"
        )
        assert re.search(pattern, source, flags=re.IGNORECASE), (
            f"{parent} missing PARTITION BY RANGE ({key})"
        )


def test_partition_horizon_uses_database_utc_date(source: str) -> None:
    # CURRENT_TIMESTAMP AT TIME ZONE 'UTC' yields the UTC calendar day even when
    # session TimeZone ≠ UTC; CURRENT_DATE AT TIME ZONE 'UTC' does not.
    assert "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date" in source
    assert "CURRENT_DATE AT TIME ZONE 'UTC'" not in source
    assert re.search(
        rf"generate_series\s*\(\s*0\s*,\s*{HORIZON_DAYS_AHEAD}\s*\)",
        source,
        flags=re.IGNORECASE,
    ) or re.search(
        rf"\bFOR\s+\w+\s+IN\s+0\s*\.\.\s*{HORIZON_DAYS_AHEAD}\b",
        source,
        flags=re.IGNORECASE,
    )
    # No Python/client wall-clock partition dating.
    assert "datetime.now" not in source
    assert "date.today" not in source
    assert "timezone.utc" not in source


def test_partition_child_naming_and_half_open_bounds(source: str) -> None:
    for parent in DAILY_RANGE_PARENTS:
        assert f"{parent}_" in source
    assert "YYYYMMDD" in source
    assert re.search(r"to_char\s*\(", source, flags=re.IGNORECASE)
    # Half-open [day, day+1) must appear in FOR VALUES FROM ... TO ...
    assert re.search(
        r"FOR\s+VALUES\s+FROM\s*\([^)]+\)\s+TO\s*\([^)]+\)",
        source,
        flags=re.IGNORECASE,
    )
    assert "interval '1 day'" in source.lower()


def test_no_default_partition_or_extensions(source: str) -> None:
    assert not re.search(
        r"PARTITION\s+OF\s+\w+\s+DEFAULT\b",
        source,
        flags=re.IGNORECASE,
    )
    assert not re.search(r"\bCREATE\s+EXTENSION\b", source, flags=re.IGNORECASE)
    assert "pg_partman" not in source.lower()
    assert "timescaledb" not in source.lower()


def test_no_per_queue_list_partitions(source: str) -> None:
    assert not re.search(r"PARTITION\s+BY\s+LIST\b", source, flags=re.IGNORECASE)


def test_deferrable_active_policy_fk(source: str) -> None:
    assert "queues_active_policy_version_id_fkey" in source
    assert re.search(
        r"DEFERRABLE\s+INITIALLY\s+DEFERRED",
        source,
        flags=re.IGNORECASE,
    )


def test_payload_ceiling_and_compact_identity(source: str) -> None:
    assert "1048576" in source
    assert re.search(
        r"GENERATED\s+ALWAYS\s+AS\s+IDENTITY",
        source,
        flags=re.IGNORECASE,
    )
    assert "statement_timestamp()" in source


def test_upgrade_ordering_markers(source: str) -> None:
    upgrade = _upgrade_body(source)
    positions = {
        name: upgrade.lower().find(f"create table {name}")
        for name in (
            "queues",
            "queue_policy_versions",
            "tasks_active",
            "task_payloads_active",
            "enqueue_dedup",
            "admin_audit_log",
            "task_attempts",
            "tasks_terminal",
            "delivery_events_terminal",
        )
    }
    assert all(pos >= 0 for pos in positions.values())
    assert positions["queues"] < positions["queue_policy_versions"]
    assert positions["queue_policy_versions"] < positions["tasks_active"]
    assert positions["tasks_active"] < positions["task_payloads_active"]
    assert positions["task_payloads_active"] < positions["enqueue_dedup"]
    assert positions["enqueue_dedup"] < positions["admin_audit_log"]
    assert positions["admin_audit_log"] < positions["task_attempts"]
    assert positions["task_attempts"] < positions["tasks_terminal"]
    assert positions["tasks_terminal"] < positions["delivery_events_terminal"]
    # Premake children after parents (horizon SQL executed at end of upgrade).
    assert "_HORIZON_SQL" in upgrade or "PARTITION OF" in source.upper()
    horizon_call = upgrade.find("_HORIZON_SQL")
    if horizon_call >= 0:
        assert horizon_call > positions["delivery_events_terminal"]
    else:
        child_marker = source.upper().find("PARTITION OF")
        assert child_marker > source.lower().find("create table delivery_events_terminal")


def test_downgrade_drops_children_before_parents(source: str) -> None:
    downgrade = _downgrade_body(source)
    lower_source = source.lower()
    assert "_DROP_CHILDREN_SQL" in downgrade
    child_phase = downgrade.find("_DROP_CHILDREN_SQL")
    assert "generate_series" in lower_source
    assert any(f"{parent}_" in source for parent in DAILY_RANGE_PARENTS)
    parent_drop_in_downgrade = min(
        m.start()
        for m in re.finditer(
            r"drop\s+table\s+if\s+exists\s+"
            r"(?:admin_audit_log|task_attempts|tasks_terminal|delivery_events_terminal)\b",
            downgrade.lower(),
        )
    )
    assert child_phase < parent_drop_in_downgrade
    # Parent drop order reverses create order among history parents.
    positions = {
        name: downgrade.lower().find(f"drop table if exists {name}")
        for name in DAILY_RANGE_PARENTS
    }
    assert all(pos >= 0 for pos in positions.values())
    assert positions["delivery_events_terminal"] < positions["tasks_terminal"]
    assert positions["tasks_terminal"] < positions["task_attempts"]
    assert positions["task_attempts"] < positions["admin_audit_log"]



def test_no_runtime_queue_behavior(source: str) -> None:
    forbidden = (
        "claim_task",
        "enqueue_task",
        "heartbeat",
        "delivery_relay",
        "pg_partman",
    )
    lower = source.lower()
    for token in forbidden:
        assert token not in lower
