"""Live pg_catalog conformance for the physical storage contract (PostgreSQL 18.6)."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
STORAGE_CONTRACT = ROOT / "docs" / "03-reference" / "02-storage-contract.md"

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

# Head layout after 039_apply_qualified_storage_layout (QUAL-03 recommendation).
QUALIFIED_REVISION = "039_apply_qualified_storage_layout"
CLAIM_IDX_COLUMNS = (
    "queue_id",
    "state_code",
    "priority",
    "available_at",
    "id",
)

# Phase 12 bounded priority catalog targets (Wave 0 scaffolds → Plan 05 / Plan 07).
PRIORITY_MIN = -32768
PRIORITY_MAX = 32767
PHASE12_PRIORITY_CHECK_BOUNDS = (
    f"priority >= {PRIORITY_MIN} AND priority <= {PRIORITY_MAX}"
)
PHASE12_CLAIM_IDX_COLUMNS = (
    "queue_id",
    "state_code",
    "priority",
    "available_at",
    "id",
)
PHASE12_CLAIM_IDX_SPEC = (
    ("queue_id", "ASC"),
    ("state_code", "ASC"),
    ("priority", "DESC"),
    ("available_at", "ASC"),
    ("id", "ASC"),
)
BOUNDED_PRIORITY_REVISION = "1201_bounded_priority_claim_ordering"
_SKIP_PHASE12_CATALOG = pytest.mark.skip(
    reason="Wave 0 scaffold; implemented by 12-05",
)
_SKIP_PHASE12_EXPLAIN = pytest.mark.skip(
    reason="Wave 0 scaffold; implemented by 12-07",
)

BASELINE_INDEXES = {
    "tasks_active_claim_idx",
    "tasks_active_spawn_lineage_uidx",
    "complete_replay_expires_at_idx",
    "admin_replay_expires_at_idx",
    "admin_audit_log_queue_audit_idx",
    "task_attempts_task_claimed_idx",
    "tasks_terminal_task_terminal_idx",
    "tasks_terminal_spawn_lineage_idx",
    "delivery_events_terminal_event_idx",
}

DB_PAYLOAD_CEILING = 1_048_576
RUNTIME_DEFAULT_PAYLOAD_CEILING = 262_144


def test_missing_test_database_url_fails_not_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    from tests.integration.conftest import require_test_database_url

    with pytest.raises(pytest.fail.Exception, match="TEST_DATABASE_URL"):
        require_test_database_url()


def test_runtime_default_payload_ceiling_documented() -> None:
    text = STORAGE_CONTRACT.read_text(encoding="utf-8")
    assert str(RUNTIME_DEFAULT_PAYLOAD_CEILING) in text
    assert str(DB_PAYLOAD_CEILING) in text
    assert "262144" in text
    assert "1048576" in text


def test_server_is_postgresql_18_6(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute("SHOW server_version_num")
        version_num = int(cur.fetchone()[0])
        cur.execute("SHOW server_version")
        version = cur.fetchone()[0]
    major, minor = divmod(version_num, 10_000)
    assert (major, minor) == (18, 6), (
        f"expected PostgreSQL 18.6.x (server_version_num=180006), "
        f"got server_version_num={version_num}"
    )
    assert str(version).startswith("18.6"), (
        f"expected server_version to start with 18.6, got {version!r}"
    )
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        assert cur.fetchone()[0] == schema


def test_catalog_relations_and_partition_parents(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relkind IN ('r', 'p')
            ORDER BY c.relname
            """,
            (schema,),
        )
        rows = {name: kind for name, kind in cur.fetchall()}

    for table in UNPARTITIONED:
        assert table in rows, f"missing unpartitioned table {table}"
        assert rows[table] == "r", f"{table} must be ordinary table"

    for parent in DAILY_RANGE_PARENTS:
        assert parent in rows, f"missing partition parent {parent}"
        assert rows[parent] == "p", f"{parent} must be partitioned table"


def test_catalog_compact_types_and_identity(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attidentity
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = 'tasks_active'
              AND a.attnum > 0
              AND NOT a.attisdropped
            """,
            (schema,),
        )
        cols = {name: (typ, identity) for name, typ, identity in cur.fetchall()}

    assert cols["id"][0].startswith("bigint")
    assert cols["id"][1] == "a"  # GENERATED ALWAYS AS IDENTITY
    assert cols["task_id"][0] == "uuid"
    assert cols["priority"][0] == "smallint"
    assert "timestamp with time zone" in cols["available_at"][0]
    assert "payload" not in cols  # payload separation

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = 'task_payloads_active'
              AND a.attname = 'payload'
            """,
            (schema,),
        )
        assert cur.fetchone()[0] == "jsonb"

        cur.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = 'enqueue_dedup'
              AND a.attname = 'key_hash'
            """,
            (schema,),
        )
        assert cur.fetchone()[0] == "bytea"


def test_named_constraints_and_baseline_indexes(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT conname
            FROM pg_constraint con
            JOIN pg_namespace n ON n.oid = con.connamespace
            WHERE n.nspname = %s
            """,
            (schema,),
        )
        constraints = {row[0] for row in cur.fetchall()}

    for required in (
        "tasks_active_priority_check",
        "task_payloads_active_payload_bytes_check",
        "enqueue_dedup_key_hash_check",
        "enqueue_dedup_producer_id_queue_id_key_hash_key",
        "queues_active_policy_version_id_fkey",
    ):
        assert required in constraints, required

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind IN ('i', 'I')
            """,
            (schema,),
        )
        indexes = {row[0] for row in cur.fetchall()}

    for idx in BASELINE_INDEXES:
        assert idx in indexes, f"missing baseline index {idx}"

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = %s
            """,
            (schema,),
        )
        defs = [row[0].lower() for row in cur.fetchall()]
    assert not any("using gin" in d for d in defs)


def test_tasks_active_claim_idx_exact_catalog_definition(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND indexname = 'tasks_active_claim_idx'
            """,
            (schema,),
        )
        row = cur.fetchone()
    assert row is not None, "missing tasks_active_claim_idx in live catalog"
    _assert_priority_first_index_definition(row[0])


def test_qualified_claim_index_present_at_migration_head(migrated_schema) -> None:
    """Phase 11 must not reorder or drop the qualified claim index at Alembic head."""
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        head_revision = cur.fetchone()[0]
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND indexname = 'tasks_active_claim_idx'
            """,
            (schema,),
        )
        row = cur.fetchone()
    assert head_revision
    assert row is not None, "tasks_active_claim_idx missing at migration head"


def _fetch_named_check_definition(
    conn: Any, schema: str, constraint_name: str
) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class rel ON rel.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = %s AND c.conname = %s
            """,
            (schema, constraint_name),
        )
        row = cur.fetchone()
    assert row is not None, f"missing constraint {constraint_name}"
    return row[0]


def _parse_index_column_spec(defn: str) -> list[tuple[str, str]]:
    match = re.search(r"\(([^)]+)\)\s*$", defn.strip())
    assert match, f"could not parse index columns from {defn!r}"
    parsed: list[tuple[str, str]] = []
    for raw in match.group(1).split(","):
        parts = raw.strip().lower().split()
        assert parts, f"empty index column entry in {defn!r}"
        column = parts[0]
        direction = parts[1] if len(parts) > 1 else "asc"
        assert direction in {"asc", "desc"}, (
            f"unsupported sort direction in index column {raw!r} from {defn!r}"
        )
        parsed.append((column, direction.upper()))
    return parsed


def _assert_priority_first_index_definition(defn: str) -> None:
    parsed = _parse_index_column_spec(defn)
    expected = list(PHASE12_CLAIM_IDX_SPEC)
    assert parsed == expected, (
        "expected exact tasks_active_claim_idx column order and directions "
        f"{expected!r}, got {parsed!r} from:\n{defn}"
    )


def _walk_explain_nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans") or []:
        yield from _walk_explain_nodes(child)


def _assert_claim_explain_plan(plan: dict[str, Any]) -> None:
    nodes = list(_walk_explain_nodes(plan))
    for node in nodes:
        if (
            node.get("Relation Name") == "tasks_active"
            and node.get("Node Type") == "Seq Scan"
        ):
            pytest.fail(
                "Seq Scan on tasks_active forbidden:\n"
                f"{json.dumps(plan, indent=2)}"
            )
    uses_idx = any(
        node.get("Index Name") == "tasks_active_claim_idx"
        and node.get("Node Type") in ("Index Scan", "Bitmap Index Scan")
        for node in nodes
    )
    assert uses_idx, (
        "expected tasks_active_claim_idx in EXPLAIN plan:\n"
        f"{json.dumps(plan, indent=2)}"
    )


def _seed_queue_with_policy(cur, *, name: str) -> tuple[int, int]:
    cur.execute(
        """
        INSERT INTO queues (queue_id, name)
        VALUES (gen_random_uuid(), %s)
        RETURNING id
        """,
        (name,),
    )
    queue_pk = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO queue_policy_versions (
            queue_id, version, enabled, max_attempts,
            backoff_strategy_code, retry_delay_seconds
        ) VALUES (%s, 1, true, 3, 1, 0)
        RETURNING id
        """,
        (queue_pk,),
    )
    policy_id = cur.fetchone()[0]
    cur.execute(
        "UPDATE queues SET active_policy_version_id = %s WHERE id = %s",
        (policy_id, queue_pk),
    )
    return queue_pk, policy_id


def _bulk_insert_tasks(
    cur,
    *,
    queue_pk: int,
    policy_id: int,
    state_code: int,
    count: int,
    available_offset: str,
) -> None:
    cur.execute(
        f"""
        INSERT INTO tasks_active (
            task_id, queue_id, producer_id, state_code, available_at,
            retry_policy_version_id
        )
        SELECT
            gen_random_uuid(), %s, 'producer-explain', %s,
            transaction_timestamp() {available_offset}, %s
        FROM generate_series(1, %s)
        """,
        (queue_pk, state_code, policy_id, count),
    )


def test_claim_selection_explain_uses_tasks_active_claim_idx(
    migrated_schema,
) -> None:
    """Broadened due predicate must use tasks_active_claim_idx, not seq scan."""
    conn, _schema = migrated_schema
    target_name = f"explain-target-{uuid.uuid4().hex[:8]}"
    noise_name = f"explain-noise-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        target_pk, target_policy = _seed_queue_with_policy(cur, name=target_name)
        noise_pk, noise_policy = _seed_queue_with_policy(cur, name=noise_name)

        _bulk_insert_tasks(
            cur,
            queue_pk=target_pk,
            policy_id=target_policy,
            state_code=2,
            count=200,
            available_offset="- interval '1 second'",
        )
        _bulk_insert_tasks(
            cur,
            queue_pk=target_pk,
            policy_id=target_policy,
            state_code=1,
            count=50,
            available_offset="- interval '1 second'",
        )
        _bulk_insert_tasks(
            cur,
            queue_pk=target_pk,
            policy_id=target_policy,
            state_code=1,
            count=500,
            available_offset="+ interval '1 hour'",
        )
        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, available_at,
                retry_policy_version_id, generation, current_claim_id,
                claimed_at, lease_expires_at, worker_id
            )
            SELECT
                gen_random_uuid(), %s, 'producer-explain', 3,
                transaction_timestamp(), %s, 1, gen_random_uuid(),
                transaction_timestamp(),
                transaction_timestamp() + interval '30 seconds',
                'worker-leased'
            FROM generate_series(1, 50)
            """,
            (target_pk, target_policy),
        )

        _bulk_insert_tasks(
            cur,
            queue_pk=noise_pk,
            policy_id=noise_policy,
            state_code=2,
            count=150,
            available_offset="- interval '1 second'",
        )
        _bulk_insert_tasks(
            cur,
            queue_pk=noise_pk,
            policy_id=noise_policy,
            state_code=1,
            count=150,
            available_offset="+ interval '1 hour'",
        )

        # Inflate tasks_active so the planner prefers the compound index over seq scan
        # while preserving the exact target/noise cardinalities above.
        for filler_index in range(18):
            filler_pk, filler_policy = _seed_queue_with_policy(
                cur,
                name=f"explain-filler-{filler_index}-{uuid.uuid4().hex[:6]}",
            )
            _bulk_insert_tasks(
                cur,
                queue_pk=filler_pk,
                policy_id=filler_policy,
                state_code=1,
                count=5000,
                available_offset="+ interval '1 hour'",
            )

        cur.execute("ANALYZE tasks_active")

        cur.execute(
            """
            EXPLAIN (FORMAT JSON)
            SELECT id
            FROM tasks_active
            WHERE queue_id = %s
              AND (
                (state_code IN (1, 2) AND available_at <= transaction_timestamp())
                OR (
                    state_code = 3
                    AND lease_expires_at <= transaction_timestamp()
                )
              )
            ORDER BY available_at, priority DESC, id
            LIMIT 1
            """,
            (target_pk,),
        )
        explain_rows = cur.fetchone()[0]
    conn.commit()

    plan = explain_rows[0]["Plan"]
    _assert_claim_explain_plan(plan)


def test_positive_inserts_active_payload_attempt_terminal_dedup(
    migrated_schema,
) -> None:
    conn, schema = migrated_schema
    qname = f"integ-queue-a-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queues (queue_id, name)
            VALUES (gen_random_uuid(), %s)
            RETURNING id
            """,
            (qname,),
        )
        queue_pk = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO queue_policy_versions (
                queue_id, version, enabled, max_attempts,
                backoff_strategy_code, retry_delay_seconds
            ) VALUES (%s, 1, true, 3, 1, 0)
            RETURNING id
            """,
            (queue_pk,),
        )
        policy_id = cur.fetchone()[0]
        cur.execute(
            "UPDATE queues SET active_policy_version_id = %s WHERE id = %s",
            (policy_id, queue_pk),
        )
        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, available_at,
                retry_policy_version_id
            ) VALUES (
                gen_random_uuid(), %s, 'producer-a', 2, statement_timestamp(), %s
            )
            RETURNING id, task_id
            """,
            (queue_pk, policy_id),
        )
        task_pk, task_uuid = cur.fetchone()
        cur.execute(
            """
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            VALUES (%s, '{"k":"v"}'::jsonb, 9)
            """,
            (task_pk,),
        )
        cur.execute(
            """
            INSERT INTO task_attempts (
                task_id, claim_id, generation, claimed_at, worker_id,
                lease_expires_at, outcome_code
            ) VALUES (
                %s, gen_random_uuid(), 1, statement_timestamp(), 'worker-a',
                statement_timestamp() + interval '30 seconds', 1
            )
            """,
            (task_uuid,),
        )
        cur.execute(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at
            ) VALUES (
                %s, %s, 'producer-a', 10, 0,
                statement_timestamp(), 1, '{"k":"v"}'::jsonb, 9,
                statement_timestamp(), statement_timestamp()
            )
            """,
            (task_uuid, queue_pk),
        )
        fp = bytes(range(32))
        cur.execute(
            """
            INSERT INTO enqueue_dedup (
                producer_id, queue_id, key_hash, request_fingerprint, task_id,
                expires_at
            ) VALUES (
                'producer-a', %s, %s, %s, %s,
                statement_timestamp() + interval '90 days'
            )
            """,
            (queue_pk, fp, fp, task_uuid),
        )
    conn.commit()


def test_rejects_invalid_priority_and_oversized_payload(migrated_schema) -> None:
    conn, schema = migrated_schema
    qname = f"integ-queue-neg-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queues (queue_id, name)
            VALUES (gen_random_uuid(), %s)
            RETURNING id
            """,
            (qname,),
        )
        queue_pk = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO queue_policy_versions (
                queue_id, version, enabled, max_attempts,
                backoff_strategy_code, retry_delay_seconds
            ) VALUES (%s, 1, true, 3, 1, 0)
            RETURNING id
            """,
            (queue_pk,),
        )
        policy_id = cur.fetchone()[0]
    conn.commit()

    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO tasks_active (
                    task_id, queue_id, producer_id, state_code, priority,
                    available_at, retry_policy_version_id
                ) VALUES (
                    gen_random_uuid(), %s, 'p', 2, %s,
                    statement_timestamp(), %s
                )
                """,
                (queue_pk, PRIORITY_MAX + 1, policy_id),
            )
    conn.rollback()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, available_at,
                retry_policy_version_id
            ) VALUES (
                gen_random_uuid(), %s, 'p', 2, statement_timestamp(), %s
            )
            RETURNING id
            """,
            (queue_pk, policy_id),
        )
        task_pk = cur.fetchone()[0]
    conn.commit()

    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
                VALUES (%s, '{}'::jsonb, %s)
                """,
                (task_pk, DB_PAYLOAD_CEILING + 1),
            )
    conn.rollback()


def test_rejects_wrong_fingerprint_and_duplicate_dedup_key(migrated_schema) -> None:
    conn, schema = migrated_schema
    qname = f"integ-queue-dedup-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queues (queue_id, name)
            VALUES (gen_random_uuid(), %s)
            RETURNING id
            """,
            (qname,),
        )
        queue_pk = cur.fetchone()[0]
    conn.commit()

    short_fp = bytes(range(16))
    good_fp = bytes(range(32))
    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO enqueue_dedup (
                    producer_id, queue_id, key_hash, request_fingerprint, task_id,
                    expires_at
                ) VALUES (
                    'producer-a', %s, %s, %s, gen_random_uuid(),
                    statement_timestamp() + interval '90 days'
                )
                """,
                (queue_pk, short_fp, good_fp),
            )
    conn.rollback()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO enqueue_dedup (
                producer_id, queue_id, key_hash, request_fingerprint, task_id,
                expires_at
            ) VALUES (
                'producer-a', %s, %s, %s, gen_random_uuid(),
                statement_timestamp() + interval '90 days'
            )
            """,
            (queue_pk, good_fp, good_fp),
        )
    conn.commit()

    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO enqueue_dedup (
                    producer_id, queue_id, key_hash, request_fingerprint, task_id,
                    expires_at
                ) VALUES (
                    'producer-a', %s, %s, %s, gen_random_uuid(),
                    statement_timestamp() + interval '90 days'
                )
                """,
                (queue_pk, good_fp, good_fp),
            )
    conn.rollback()


def test_tasks_active_priority_check_admits_signed_smallint_range(
    migrated_schema,
) -> None:
    """Both endpoints of native smallint must be named and inspectable."""
    conn, schema = migrated_schema
    defn = _fetch_named_check_definition(
        conn, schema, "tasks_active_priority_check"
    ).lower()
    assert str(PRIORITY_MIN) in defn
    assert str(PRIORITY_MAX) in defn
    assert "priority" in defn


def test_tasks_terminal_priority_check_admits_signed_smallint_range(
    migrated_schema,
) -> None:
    conn, schema = migrated_schema
    defn = _fetch_named_check_definition(
        conn, schema, "tasks_terminal_priority_check"
    ).lower()
    assert str(PRIORITY_MIN) in defn
    assert str(PRIORITY_MAX) in defn
    assert "priority" in defn


def test_tasks_active_claim_idx_priority_first_exact_catalog_definition(
    migrated_schema,
) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND indexname = 'tasks_active_claim_idx'
            """,
            (schema,),
        )
        row = cur.fetchone()
    assert row is not None
    _assert_priority_first_index_definition(row[0])


def test_bounded_priority_clean_downgrade_upgrade_round_trip(
    migrated_schema, test_database_url: str
) -> None:
    """Zero-only database restores old checks/index then re-upgrades."""
    from tests.integration import conftest as integ

    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        head_before = cur.fetchone()[0]
    assert head_before == BOUNDED_PRIORITY_REVISION

    integ.run_alembic(
        "downgrade",
        "0502_delivery_pending_generation",
        schema=schema,
        database_url=test_database_url,
    )
    conn.rollback()
    old_active = _fetch_named_check_definition(
        conn, schema, "tasks_active_priority_check"
    ).lower()
    assert "priority=0" in old_active.replace(" ", "")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND indexname = 'tasks_active_claim_idx'
            """,
            (schema,),
        )
        old_idx = cur.fetchone()[0].lower()
    assert old_idx.index("available_at") < old_idx.index("priority")

    integ.run_alembic(
        "upgrade", "head", schema=schema, database_url=test_database_url
    )
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        assert cur.fetchone()[0] == BOUNDED_PRIORITY_REVISION


def test_downgrade_refused_when_active_has_non_zero_priority(
    migrated_schema, test_database_url: str
) -> None:
    from tests.integration import conftest as integ

    conn, schema = migrated_schema
    conn.rollback()
    qname = f"downgrade-active-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        revision_before = cur.fetchone()[0]
        queue_pk, policy_id = _seed_queue_with_policy(cur, name=qname)
        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id
            ) VALUES (
                gen_random_uuid(), %s, 'producer-downgrade', 2, %s,
                transaction_timestamp(), %s
            )
            """,
            (queue_pk, PRIORITY_MAX, policy_id),
        )
    conn.commit()

    with pytest.raises(Exception, match="(?i)downgrade|non.?zero|priority"):
        integ.run_alembic(
            "downgrade",
            "0502_delivery_pending_generation",
            schema=schema,
            database_url=test_database_url,
        )

    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        assert cur.fetchone()[0] == revision_before
        cur.execute(
            """
            SELECT pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class rel ON rel.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = %s AND c.conname = 'tasks_active_priority_check'
            """,
            (schema,),
        )
        defn = cur.fetchone()[0].lower()
        assert str(PRIORITY_MIN) in defn or "between" in defn
        cur.execute(
            "SELECT COUNT(*) FROM tasks_active WHERE priority <> 0"
        )
        assert int(cur.fetchone()[0]) >= 1
        cur.execute(
            "DELETE FROM tasks_active WHERE producer_id = 'producer-downgrade'"
        )
    conn.commit()


def test_downgrade_refused_when_terminal_partition_has_non_zero_priority(
    migrated_schema, test_database_url: str
) -> None:
    from tests.integration import conftest as integ

    conn, schema = migrated_schema
    conn.rollback()
    qname = f"downgrade-terminal-{uuid.uuid4().hex[:8]}"
    task_uuid = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        revision_before = cur.fetchone()[0]
        queue_pk, _policy_id = _seed_queue_with_policy(cur, name=qname)
        cur.execute(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at
            ) VALUES (
                %s, %s, 'producer-downgrade', 10, %s,
                transaction_timestamp(), 1, '{}'::jsonb, 2,
                transaction_timestamp(), transaction_timestamp()
            )
            """,
            (task_uuid, queue_pk, PRIORITY_MIN),
        )
    conn.commit()

    with pytest.raises(Exception, match="(?i)downgrade|non.?zero|priority"):
        integ.run_alembic(
            "downgrade",
            "0502_delivery_pending_generation",
            schema=schema,
            database_url=test_database_url,
        )

    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_num
            FROM alembic_version
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        assert cur.fetchone()[0] == revision_before
        terminal_defn = _fetch_named_check_definition(
            conn, schema, "tasks_terminal_priority_check"
        ).lower()
        assert str(PRIORITY_MIN) in terminal_defn or "between" in terminal_defn
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND indexname = 'tasks_active_claim_idx'
            """,
            (schema,),
        )
        idx_row = cur.fetchone()
        assert idx_row is not None
        _assert_priority_first_index_definition(idx_row[0])
        cur.execute(
            "SELECT COUNT(*) FROM tasks_terminal WHERE priority <> 0"
        )
        assert int(cur.fetchone()[0]) >= 1
        cur.execute(
            "DELETE FROM tasks_terminal WHERE producer_id = 'producer-downgrade'"
        )
    conn.commit()


def test_priority_claim_selection_explain_uses_tasks_active_claim_idx(
    migrated_schema,
) -> None:
    """Phase 12 order must use tasks_active_claim_idx; Sort/Incremental Sort allowed."""
    conn, _schema = migrated_schema
    target_name = f"explain-priority-{uuid.uuid4().hex[:8]}"
    noise_name = f"explain-priority-noise-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        target_pk, target_policy = _seed_queue_with_policy(cur, name=target_name)
        noise_pk, noise_policy = _seed_queue_with_policy(cur, name=noise_name)

        for state_code, count, offset, priority in (
            (2, 200, "- interval '1 second'", 0),
            (1, 50, "- interval '1 second'", -100),
            (1, 500, "+ interval '1 hour'", PRIORITY_MAX),
        ):
            cur.execute(
                f"""
                INSERT INTO tasks_active (
                    task_id, queue_id, producer_id, state_code, priority,
                    available_at, retry_policy_version_id
                )
                SELECT
                    gen_random_uuid(), %s, 'producer-explain-priority', %s, %s,
                    transaction_timestamp() {offset}, %s
                FROM generate_series(1, %s)
                """,
                (target_pk, state_code, priority, target_policy, count),
            )

        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id, generation,
                current_claim_id, claimed_at, lease_expires_at, worker_id
            )
            SELECT
                gen_random_uuid(), %s, 'producer-explain-priority', 3, %s,
                transaction_timestamp(), %s, 1, gen_random_uuid(),
                transaction_timestamp(),
                transaction_timestamp() + interval '30 seconds',
                'worker-leased'
            FROM generate_series(1, 25)
            UNION ALL
            SELECT
                gen_random_uuid(), %s, 'producer-explain-priority', 3, %s,
                transaction_timestamp(), %s, 1, gen_random_uuid(),
                transaction_timestamp(),
                transaction_timestamp() - interval '1 second',
                'worker-expired'
            FROM generate_series(1, 25)
            """,
            (
                target_pk,
                100,
                target_policy,
                target_pk,
                -50,
                target_policy,
            ),
        )

        _bulk_insert_tasks(
            cur,
            queue_pk=noise_pk,
            policy_id=noise_policy,
            state_code=2,
            count=150,
            available_offset="- interval '1 second'",
        )
        _bulk_insert_tasks(
            cur,
            queue_pk=noise_pk,
            policy_id=noise_policy,
            state_code=1,
            count=150,
            available_offset="+ interval '1 hour'",
        )

        for filler_index in range(18):
            filler_pk, filler_policy = _seed_queue_with_policy(
                cur,
                name=f"explain-priority-filler-{filler_index}-{uuid.uuid4().hex[:6]}",
            )
            _bulk_insert_tasks(
                cur,
                queue_pk=filler_pk,
                policy_id=filler_policy,
                state_code=1,
                count=5000,
                available_offset="+ interval '1 hour'",
            )

        cur.execute("ANALYZE tasks_active")

        cur.execute(
            """
            EXPLAIN (FORMAT JSON)
            SELECT id
            FROM tasks_active
            WHERE queue_id = %s
              AND (
                (state_code IN (1, 2) AND available_at <= transaction_timestamp())
                OR (
                    state_code = 3
                    AND lease_expires_at <= transaction_timestamp()
                )
              )
            ORDER BY priority DESC, available_at ASC, id ASC
            LIMIT 1
            """,
            (target_pk,),
        )
        explain_rows = cur.fetchone()[0]
    conn.commit()

    plan = explain_rows[0]["Plan"]
    _assert_claim_explain_plan(plan)


def test_downgrade_removes_all_queue_relations(
    migrated_schema, test_database_url: str
) -> None:
    """Downgrade leaves no Queue relation in the isolated schema."""
    from tests.integration import conftest as integ

    conn, schema = migrated_schema
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM tasks_active WHERE priority <> 0")
        cur.execute("DELETE FROM tasks_terminal WHERE priority <> 0")
    conn.commit()
    integ.run_alembic(
        "downgrade", "base", schema=schema, database_url=test_database_url
    )
    # Refresh connection after DDL outside this session.
    conn.rollback()
    integ.assert_schema_has_no_queue_relations(conn, schema)
    integ.run_alembic(
        "upgrade", "head", schema=schema, database_url=test_database_url
    )
    conn.rollback()
    # Re-apply search_path after external Alembic DDL on other connections.
    prev = conn.autocommit
    conn.autocommit = True
    conn.execute(f'SET search_path TO "{schema}"')
    conn.autocommit = prev
