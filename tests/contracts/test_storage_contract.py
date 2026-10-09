"""Metadata conformance tests for the authoritative PostgreSQL storage contract.

Phase 12 Plan 05 bounded priority catalog tests; Plan 07
removes the markers.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Integer, SmallInteger, Text
from sqlalchemy.dialects.postgresql import ARRAY, BYTEA, JSONB, UUID

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "docs" / "03-reference" / "02-storage-contract.md"

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

EXACT_TABLE_NAMES = {
    "admin_replay",
    "partition_maintenance_status",
    "completion_effects",
}

PAYLOAD_DB_CEILING = 1_048_576
PAYLOAD_RUNTIME_DEFAULT = 262_144

TTL = {
    "enqueue_dedup": {
        "min_days": 30,
        "max_days": 365,
        "default_days": 90,
        "min_seconds": 2_592_000,
        "max_seconds": 31_536_000,
        "default_seconds": 7_776_000,
    },
    "complete_replay": {
        "min_days": 1,
        "max_days": 30,
        "default_days": 7,
        "min_seconds": 86_400,
        "max_seconds": 2_592_000,
        "default_seconds": 604_800,
    },
    "admin_replay": {
        "min_days": 7,
        "max_days": 90,
        "default_days": 30,
        "min_seconds": 604_800,
        "max_seconds": 7_776_000,
        "default_seconds": 2_592_000,
    },
}

CODE_MAPS = {
    "queue_state": {1: "active", 2: "paused", 3: "draining"},
    "active_task_state": {1: "delayed", 2: "ready", 3: "leased"},
    "replay_result_state": {3: "retry_scheduled", 10: "succeeded", 11: "dead_lettered", 12: "cancelled"},
    "terminal_task_state": {10: "succeeded", 11: "dead_lettered", 12: "cancelled"},
    "attempt_outcome": {
        1: "active",
        2: "succeeded",
        3: "retry_scheduled",
        4: "dead_lettered",
        5: "expired",
        6: "cancelled",
    },
    "delivery_state": {1: "pending", 2: "publishing", 10: "published", 11: "dead_lettered"},
    "backoff_strategy": {1: "fixed"},
    "terminal_operation": {1: "complete", 2: "fail", 3: "ack_cancel"},
    "admin_operation": {
        1: "create_queue",
        2: "create_policy",
        3: "activate_policy",
        4: "set_state",
        5: "run_maintenance",
    },
    "completion_effect": {1: "spawn", 2: "event"},
}

FORBIDDEN_COLUMN_TOKENS = (
    "business_result",
    "parser_v1",
    "parse_result",
)


def _load_models():
    from queue_service.db import Base
    import queue_service.storage.models  # noqa: F401

    return Base.metadata


def _pg_type_name(column) -> str:
    coltype = column.type
    if isinstance(coltype, UUID):
        return "uuid"
    if isinstance(coltype, BYTEA):
        return "bytea"
    if isinstance(coltype, JSONB):
        return "jsonb"
    if isinstance(coltype, ARRAY):
        item = coltype.item_type
        if isinstance(item, UUID):
            return "uuid[]"
        return f"{item!s}[]"
    if isinstance(coltype, DateTime) and coltype.timezone:
        return "timestamptz"
    if isinstance(coltype, Date):
        return "date"
    if isinstance(coltype, BigInteger):
        return "bigint"
    if isinstance(coltype, SmallInteger):
        return "smallint"
    if isinstance(coltype, Integer):
        return "integer"
    if isinstance(coltype, Boolean):
        return "boolean"
    if isinstance(coltype, Text):
        return "text"
    return coltype.__class__.__name__.lower()


def _constraint_sql(constraint) -> str:
    return " ".join(str(constraint.sqltext).lower().split())


def _index_columns(index) -> list[str]:
    cols: list[str] = []
    for col in index.expressions:
        element = getattr(col, "element", None)
        if element is not None and hasattr(element, "name"):
            name = element.name
            is_desc = "desc" in type(col).__name__.lower()
            cols.append(f"{name} DESC" if is_desc else name)
            continue
        name = getattr(col, "name", None) or str(col)
        cols.append(name)
    return cols


@pytest.fixture(scope="module")
def metadata():
    return _load_models()


@pytest.fixture(scope="module")
def contract_text():
    assert CONTRACT_PATH.is_file(), f"missing storage contract: {CONTRACT_PATH}"
    return CONTRACT_PATH.read_text(encoding="utf-8")


def test_exact_relation_set(metadata):
    assert set(metadata.tables) == UNPARTITIONED | set(DAILY_RANGE_PARENTS)


def test_exact_authoritative_names_without_aliases(metadata, contract_text):
    for name in EXACT_TABLE_NAMES:
        assert name in metadata.tables
        assert name in contract_text
    for alias in ("admin_idempotency", "maintenance_status", "spawn_registry", "effects_registry"):
        assert alias not in metadata.tables
        # Exact alias tokens only — avoid substring hits inside authoritative names.
        assert not re.search(rf"(?<![a-z_]){alias}(?![a-z_])", contract_text)


def test_hot_cold_split_and_partition_parents(metadata):
    for table_name in UNPARTITIONED:
        table = metadata.tables[table_name]
        assert table.dialect_options.get("postgresql", {}).get("partition_by") in (None, "")
    for table_name, key in DAILY_RANGE_PARENTS.items():
        table = metadata.tables[table_name]
        partition_by = table.dialect_options["postgresql"]["partition_by"]
        assert partition_by.upper() == f"RANGE ({key.upper()})" or partition_by == f"RANGE ({key})"
        pk_cols = [c.name for c in table.primary_key.columns]
        assert key in pk_cols
        assert "id" in pk_cols


def test_compact_types_and_identity(metadata):
    queues = metadata.tables["queues"]
    assert _pg_type_name(queues.c.id) == "bigint"
    assert queues.c.id.identity is not None
    assert queues.c.id.identity.always is True
    assert _pg_type_name(queues.c.queue_id) == "uuid"
    assert queues.c.queue_id.server_default is None
    assert _pg_type_name(queues.c.state_code) == "smallint"
    assert _pg_type_name(queues.c.created_at) == "timestamptz"

    # STOR-06 / ADR 007: identity PKs are GENERATED ALWAYS AS IDENTITY
    identity_columns = [
        column
        for table in metadata.tables.values()
        for column in table.columns
        if column.identity is not None
    ]
    assert identity_columns, "expected catalog identity primary keys"
    for column in identity_columns:
        assert column.identity.always is True, f"{column.table.name}.{column.name}"

    payloads = metadata.tables["task_payloads_active"]
    assert _pg_type_name(payloads.c.payload) == "jsonb"
    assert _pg_type_name(payloads.c.payload_bytes) == "integer"
    assert payloads.c.task_id.identity is None

    counters = metadata.tables["queue_counters"]
    assert counters.c.queue_id.identity is None

    dedup = metadata.tables["enqueue_dedup"]
    assert _pg_type_name(dedup.c.key_hash) == "bytea"
    assert _pg_type_name(dedup.c.request_fingerprint) == "bytea"

    replay = metadata.tables["complete_replay"]
    assert _pg_type_name(replay.c.spawned_task_ids) == "uuid[]"
    assert _pg_type_name(replay.c.event_ids) == "uuid[]"

    maint = metadata.tables["partition_maintenance_status"]
    assert _pg_type_name(maint.c.premade_through) == "date"
    assert _pg_type_name(maint.c.retained_from) == "date"
    assert maint.c.singleton_id.identity is None


def test_payload_ceiling_runtime_vs_db(metadata, contract_text):
    for table_name, column_name in (
        ("task_payloads_active", "payload_bytes"),
        ("tasks_terminal", "payload_bytes"),
        ("delivery_events_active", "envelope_bytes"),
        ("delivery_events_terminal", "envelope_bytes"),
    ):
        checks = [
            _constraint_sql(c)
            for c in metadata.tables[table_name].constraints
            if c.__class__.__name__ == "CheckConstraint"
        ]
        joined = " | ".join(checks)
        assert "1048576" in joined or "1_048_576" in joined or "1048576" in joined.replace("_", "")
        assert str(PAYLOAD_DB_CEILING) in joined.replace("_", "")
        assert str(PAYLOAD_RUNTIME_DEFAULT) not in joined.replace("_", "")

    assert str(PAYLOAD_DB_CEILING) in contract_text
    assert str(PAYLOAD_RUNTIME_DEFAULT) in contract_text
    assert "1 MiB" in contract_text or "1048576" in contract_text
    assert "262144" in contract_text


def test_no_gin_or_payload_indexes(metadata):
    for table in metadata.tables.values():
        for index in table.indexes:
            using = (index.dialect_options.get("postgresql", {}) or {}).get("using")
            # SQLAlchemy may store absent USING as False rather than None.
            assert using in (None, False, "btree"), f"unexpected index using={using} on {index.name}"
            cols = [getattr(c, "name", str(c)) for c in index.columns]
            assert "payload" not in cols
            assert "envelope" not in cols


def test_claim_nullability_and_spawn_pairing(metadata):
    tasks = metadata.tables["tasks_active"]
    checks = [_constraint_sql(c) for c in tasks.constraints if c.__class__.__name__ == "CheckConstraint"]
    blob = " | ".join(checks)
    assert "current_claim_id" in blob
    assert "claimed_at" in blob
    assert "lease_expires_at" in blob
    assert "worker_id" in blob
    assert "source_task_id" in blob
    assert "spawn_ordinal" in blob

    events = metadata.tables["delivery_events_active"]
    event_checks = [
        _constraint_sql(c) for c in events.constraints if c.__class__.__name__ == "CheckConstraint"
    ]
    event_blob = " | ".join(event_checks)
    assert "current_claim_id" in event_blob
    assert "claimed_at" in event_blob
    assert "lease_expires_at" in event_blob


def test_ttl_bounds_in_constraints_and_contract(metadata, contract_text):
    for table_name, spec in TTL.items():
        checks = [
            _constraint_sql(c)
            for c in metadata.tables[table_name].constraints
            if c.__class__.__name__ == "CheckConstraint"
        ]
        blob = " | ".join(checks)
        assert "expires_at" in blob
        assert "created_at" in blob
        # interval day bounds appear in CHECK expressions
        if table_name == "enqueue_dedup":
            assert "30 days" in blob or "interval '30" in blob
            assert "365 days" in blob or "interval '365" in blob
        elif table_name == "complete_replay":
            assert "1 day" in blob or "interval '1" in blob
            assert "30 days" in blob or "interval '30" in blob
        else:
            assert "7 days" in blob or "interval '7" in blob
            assert "90 days" in blob or "interval '90" in blob

        for key, value in spec.items():
            assert str(value) in contract_text, f"{table_name}.{key}={value} missing from contract"


def test_replay_result_shape(metadata):
    replay = metadata.tables["complete_replay"]
    checks = [_constraint_sql(c) for c in replay.constraints if c.__class__.__name__ == "CheckConstraint"]
    blob = " | ".join(checks)
    assert "result_state_code" in blob
    assert "available_at" in blob
    assert "terminal_at" in blob
    # state 3 vs 10..12 pairing
    assert "3" in blob
    assert "10" in blob and "11" in blob and "12" in blob


def test_admin_replay_and_maintenance_status(metadata):
    admin = metadata.tables["admin_replay"]
    assert {c.name for c in admin.columns} >= {
        "admin_principal_id",
        "operation_code",
        "key_hash",
        "request_fingerprint",
        "http_status",
        "response_body",
        "created_at",
        "expires_at",
    }
    uniques = [
        tuple(col.name for col in c.columns)
        for c in admin.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("admin_principal_id", "operation_code", "key_hash") in uniques

    maint = metadata.tables["partition_maintenance_status"]
    assert list(maint.primary_key.columns)[0].name == "singleton_id"
    checks = [_constraint_sql(c) for c in maint.constraints if c.__class__.__name__ == "CheckConstraint"]
    assert any("singleton_id" in c and "1" in c for c in checks)


def test_global_spawn_ordinal_uniqueness(metadata):
    effects = metadata.tables["completion_effects"]
    uniques = [
        tuple(col.name for col in c.columns)
        for c in effects.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("source_claim_id", "effect_kind_code", "ordinal") in uniques
    assert ("resource_id",) in uniques or any(u == ("resource_id",) for u in uniques)

    tasks = metadata.tables["tasks_active"]
    partial = [
        idx
        for idx in tasks.indexes
        if idx.unique and set(idx.columns.keys()) >= {"source_task_id", "spawn_ordinal"}
    ]
    assert partial, "missing partial unique spawn lineage index on tasks_active"
    where = str(partial[0].dialect_options.get("postgresql", {}).get("where", "")).lower()
    assert "source_task_id" in where and "not null" in where


def _index_column_names(index) -> set[str]:
    names: set[str] = set(index.columns.keys())
    for expr in index.expressions:
        element = getattr(expr, "element", None)
        if element is not None and hasattr(element, "name"):
            names.add(element.name)
        elif isinstance(element, str):
            names.add(element)
        elif hasattr(expr, "name"):
            names.add(expr.name)
        else:
            text_form = str(expr)
            # e.g. "priority DESC"
            names.add(text_form.split()[0])
    return names


def test_baseline_indexes(metadata):
    tasks = metadata.tables["tasks_active"]
    claim_idx = next(
        (idx for idx in tasks.indexes if idx.name == "tasks_active_claim_idx"),
        None,
    )
    assert claim_idx is not None, "missing tasks_active_claim_idx on tasks_active"
    cols = _index_columns(claim_idx)
    assert cols == [
        "queue_id",
        "state_code",
        "priority DESC",
        "available_at",
        "id",
    ]

    attempts = metadata.tables["task_attempts"]
    assert any(
        _index_column_names(idx) >= {"task_id", "claimed_at", "id"} for idx in attempts.indexes
    )

    terminal = metadata.tables["tasks_terminal"]
    assert any(
        _index_column_names(idx) >= {"task_id", "terminal_at"} for idx in terminal.indexes
    )
    assert any(
        _index_column_names(idx) >= {"source_task_id", "spawn_ordinal", "terminal_at"}
        for idx in terminal.indexes
    )

    events_term = metadata.tables["delivery_events_terminal"]
    assert any(
        _index_column_names(idx) >= {"event_id", "terminal_at"} for idx in events_term.indexes
    )

    audit = metadata.tables["admin_audit_log"]
    assert any(
        _index_column_names(idx) >= {"queue_id", "audit_at", "id"} for idx in audit.indexes
    )

    for table_name in ("enqueue_dedup", "complete_replay", "admin_replay"):
        table = metadata.tables[table_name]
        assert any("expires_at" in _index_column_names(idx) for idx in table.indexes)


def test_no_history_blocking_fk_or_cascade_into_history(metadata):
    for table_name in DAILY_RANGE_PARENTS:
        table = metadata.tables[table_name]
        fks = list(table.foreign_keys)
        assert fks == [], f"{table_name} must not have FKs that block detach, found {fks}"

    for table in metadata.tables.values():
        for fk in table.foreign_keys:
            target = fk.column.table.name
            if target in DAILY_RANGE_PARENTS:
                pytest.fail(f"FK from {table.name} to history parent {target} is forbidden")


def test_active_policy_fk_deferrable(metadata):
    queues = metadata.tables["queues"]
    fks = [fk for fk in queues.foreign_keys if fk.parent.name == "active_policy_version_id"]
    assert len(fks) == 1
    fk = fks[0]
    assert fk.column.table.name == "queue_policy_versions"
    assert fk.deferrable is True
    assert str(fk.initially).upper() == "DEFERRED"


def test_code_maps_published(contract_text):
    for map_name, mapping in CODE_MAPS.items():
        for code, label in mapping.items():
            assert re.search(rf"\b{code}\b.*\b{re.escape(label)}\b|\b{re.escape(label)}\b.*\b{code}\b", contract_text), (
                f"missing code map entry {map_name}: {code}={label}"
            )


def test_forbidden_surfaces_absent(metadata, contract_text):
    column_names = {c.name for t in metadata.tables.values() for c in t.columns}
    for token in FORBIDDEN_COLUMN_TOKENS:
        assert token not in column_names
    assert "result" not in column_names
    assert "business_result" not in contract_text.lower()
    assert "parser_v1" not in contract_text.lower()
    # Out-of-scope section must explicitly reject transport/broker surfaces.
    lowered = contract_text.lower()
    assert "out of scope" in lowered
    assert "broker" in contract_text.lower() or "transport" in contract_text.lower()


_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767
_PRIORITY_RANGE_EXPR = f"priority between {_PRIORITY_MIN} and {_PRIORITY_MAX}"


def _named_check_sql(metadata, *, table_name: str, constraint_name: str) -> str:
    table = metadata.tables[table_name]
    for constraint in table.constraints:
        if (
            constraint.__class__.__name__ == "CheckConstraint"
            and constraint.name == constraint_name
        ):
            return _constraint_sql(constraint)
    pytest.fail(f"missing named check {constraint_name} on {table_name}")


def test_tasks_active_and_terminal_priority_checks_allow_full_smallint_range(
    metadata,
) -> None:
    active_sql = _named_check_sql(
        metadata,
        table_name="tasks_active",
        constraint_name="tasks_active_priority_check",
    )
    terminal_sql = _named_check_sql(
        metadata,
        table_name="tasks_terminal",
        constraint_name="tasks_terminal_priority_check",
    )
    for sql in (active_sql, terminal_sql):
        assert _PRIORITY_RANGE_EXPR in sql
        assert "priority = 0" not in sql


def test_claim_index_orders_priority_before_available_at(metadata) -> None:
    tasks = metadata.tables["tasks_active"]
    claim_idx = next(
        (idx for idx in tasks.indexes if idx.name == "tasks_active_claim_idx"),
        None,
    )
    assert claim_idx is not None, "missing tasks_active_claim_idx on tasks_active"
    cols = _index_columns(claim_idx)
    assert cols == [
        "queue_id",
        "state_code",
        "priority DESC",
        "available_at",
        "id",
    ]


def test_queues_and_policy_uniques(metadata):
    queues = metadata.tables["queues"]
    uniques = [
        tuple(col.name for col in c.columns)
        for c in queues.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("queue_id",) in uniques
    assert ("name",) in uniques

    policies = metadata.tables["queue_policy_versions"]
    policy_uniques = [
        tuple(col.name for col in c.columns)
        for c in policies.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("queue_id", "version") in policy_uniques


def test_claim_registry_and_complete_replay_uniques(metadata):
    claims = metadata.tables["claim_registry"]
    claim_uniques = [
        tuple(col.name for col in c.columns)
        for c in claims.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("claim_id",) in claim_uniques
    assert ("claim_token",) in claim_uniques

    replay = metadata.tables["complete_replay"]
    replay_uniques = [
        tuple(col.name for col in c.columns)
        for c in replay.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("claim_id", "operation_code") in replay_uniques


def test_contract_documents_same_queue_policy_invariant(contract_text):
    assert "same-queue" in contract_text.lower() or "same queue" in contract_text.lower()
    assert "transaction" in contract_text.lower()
