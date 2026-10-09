"""QUAL-03: schema/config must match the checksum-valid Phase 3.9 recommendation."""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.errors import UniqueViolation

from queue_service.intake.admission import (
    DEFAULT_PAYLOAD_MAX_BYTES,
    DEFAULT_REQUEST_MAX_BYTES,
    HARD_PAYLOAD_CEILING_BYTES,
)
from queue_service import settings as deployment_settings
from tests.integration.conftest import (
    _open_schema_connection,
    _validate_schema_name,
    require_test_database_url,
    run_alembic,
    to_psycopg_conninfo,
)

ROOT = Path(__file__).resolve().parents[2]
CANDIDATES_DIR = ROOT / "benchmarks" / "results" / "phase-3.9-candidates"
RECOMMENDATION_PATH = CANDIDATES_DIR / "recommendation.json"
MANIFEST_PATH = CANDIDATES_DIR / "index-manifest.json"
SHA256SUMS_PATH = CANDIDATES_DIR / "SHA256SUMS"

PRIOR_REVISION = "0001_physical_contract_foundations"
QUALIFIED_REVISION = "039_apply_qualified_storage_layout"

CORRECTNESS_INDEXES = frozenset({"tasks_active_spawn_lineage_uidx"})
OMITTED_BY_RECOMMENDATION = frozenset({"enqueue_dedup_expires_at_idx"})


def _load_recommendation() -> dict:
    return json.loads(RECOMMENDATION_PATH.read_text(encoding="utf-8"))


def _verify_sha256sums() -> None:
    for line in SHA256SUMS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        digest, _, rel = line.partition(" ")
        rel = rel.lstrip(" *")
        path = CANDIDATES_DIR / rel
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        assert actual == digest, f"checksum mismatch for {rel}"


def _index_names(conn: psycopg.Connection, schema: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relkind IN ('i', 'I')
            """,
            (schema,),
        )
        return {row[0] for row in cur.fetchall()}


def _measured_signature(present: set[str], selected: set[str]) -> str:
    measured = (present & selected) | (present & OMITTED_BY_RECOMMENDATION)
    measured -= CORRECTNESS_INDEXES
    return ",".join(sorted(measured))


def _hash_modulus(conn: psycopg.Connection, schema: str, table: str) -> int:
    """Unpartitioned ordinary tables count as modulus 1."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relkind, pt.partstrat
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (schema, table),
        )
        row = cur.fetchone()
        assert row is not None, f"missing table {table}"
        relkind, partstrat = row
        if relkind == "r":
            return 1
        if relkind == "p" and partstrat == "h":
            cur.execute(
                """
                SELECT count(*)
                FROM pg_inherits i
                JOIN pg_class parent ON parent.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = parent.relnamespace
                WHERE n.nspname = %s AND parent.relname = %s
                """,
                (schema, table),
            )
            return int(cur.fetchone()[0])
        raise AssertionError(
            f"{table}: expected ordinary or HASH-partitioned table, got {relkind!r}"
        )


def _insert_representative_rows(conn: psycopg.Connection) -> dict[str, object]:
    marker = uuid.uuid4().hex[:12]
    producer = f"producer-{marker}"
    key_hash = hashlib.sha256(f"key-{marker}".encode()).digest()
    fingerprint = hashlib.sha256(f"fp-{marker}".encode()).digest()
    claim_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queues (queue_id, name)
            VALUES (gen_random_uuid(), %s)
            RETURNING id
            """,
            (f"qual-q-{marker}",),
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
        task_uuid = uuid.uuid4()
        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, available_at,
                retry_policy_version_id
            ) VALUES (%s, %s, %s, 2, statement_timestamp(), %s)
            RETURNING id
            """,
            (task_uuid, queue_pk, producer, policy_id),
        )
        task_pk = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            VALUES (%s, %s::jsonb, 13)
            """,
            (task_pk, '{"qual":true}'),
        )
        cur.execute(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at
            ) VALUES (
                %s, %s, %s, 10, 0,
                statement_timestamp(), 1, %s::jsonb, 13,
                statement_timestamp(), statement_timestamp()
            )
            RETURNING id
            """,
            (task_uuid, queue_pk, producer, '{"qual":true}'),
        )
        terminal_pk = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO enqueue_dedup (
                producer_id, queue_id, key_hash, request_fingerprint, task_id,
                expires_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                statement_timestamp() + interval '90 days'
            )
            RETURNING id
            """,
            (producer, queue_pk, key_hash, fingerprint, task_uuid),
        )
        dedup_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO complete_replay (
                claim_id, operation_code, request_fingerprint, task_id,
                result_state_code, terminal_at, created_at, expires_at
            ) VALUES (
                %s, 1, %s, %s, 10, statement_timestamp(),
                statement_timestamp(),
                statement_timestamp() + interval '7 days'
            )
            RETURNING id
            """,
            (claim_id, fingerprint, task_uuid),
        )
        replay_id = cur.fetchone()[0]
    conn.commit()
    return {
        "queue_pk": queue_pk,
        "task_pk": task_pk,
        "terminal_pk": terminal_pk,
        "dedup_id": dedup_id,
        "replay_id": replay_id,
        "producer": producer,
        "key_hash": key_hash,
        "fingerprint": fingerprint,
        "claim_id": claim_id,
    }


def _assert_rows_present(conn: psycopg.Connection, keys: dict[str, object]) -> None:
    with conn.cursor() as cur:
        for table, key in (
            ("queues", "queue_pk"),
            ("tasks_active", "task_pk"),
            ("tasks_terminal", "terminal_pk"),
            ("enqueue_dedup", "dedup_id"),
            ("complete_replay", "replay_id"),
        ):
            cur.execute(f"SELECT 1 FROM {table} WHERE id = %s", (keys[key],))
            assert cur.fetchone() is not None, f"missing row in {table}"


def _assert_idempotency_uniqueness(
    conn: psycopg.Connection, keys: dict[str, object]
) -> None:
    with conn.cursor() as cur:
        with pytest.raises(UniqueViolation):
            cur.execute(
                """
                INSERT INTO enqueue_dedup (
                    producer_id, queue_id, key_hash, request_fingerprint, task_id,
                    expires_at
                ) VALUES (
                    %s, %s, %s, %s, gen_random_uuid(),
                    statement_timestamp() + interval '90 days'
                )
                """,
                (
                    keys["producer"],
                    keys["queue_pk"],
                    keys["key_hash"],
                    keys["fingerprint"],
                ),
            )
    conn.rollback()
    with conn.cursor() as cur:
        with pytest.raises(UniqueViolation):
            cur.execute(
                """
                INSERT INTO complete_replay (
                    claim_id, operation_code, request_fingerprint, task_id,
                    result_state_code, terminal_at, created_at, expires_at
                ) VALUES (
                    %s, 1, %s, gen_random_uuid(), 10, statement_timestamp(),
                    statement_timestamp(),
                    statement_timestamp() + interval '7 days'
                )
                """,
                (keys["claim_id"], keys["fingerprint"]),
            )
    conn.rollback()


@pytest.fixture
def prior_head_schema() -> tuple[psycopg.Connection, str, str]:
    database_url = require_test_database_url()
    schema = _validate_schema_name(f"qqual_{uuid.uuid4().hex}")
    admin = psycopg.connect(to_psycopg_conninfo(database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", PRIOR_REVISION, schema=schema, database_url=database_url)
    conn = _open_schema_connection(database_url, schema)
    try:
        yield conn, schema, database_url
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            run_alembic("downgrade", "base", schema=schema, database_url=database_url)
        finally:
            drop = psycopg.connect(to_psycopg_conninfo(database_url))
            drop.autocommit = True
            try:
                drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                drop.close()


def test_recommendation_artifact_is_checksum_valid_pass() -> None:
    _verify_sha256sums()
    rec = _load_recommendation()
    assert rec["verdict"] == "PASS"
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest["mode"] == "synthetic"


def test_runtime_defaults_match_qualified_payload_ceiling() -> None:
    rec = _load_recommendation()
    ceiling = int(rec["payload"]["selected_ceiling_bytes"])
    assert ceiling == 1_048_576
    assert ceiling <= 1_048_576
    assert hasattr(deployment_settings, "QUALIFIED_PAYLOAD_CEILING_BYTES")
    assert hasattr(deployment_settings, "QUALIFIED_REQUEST_MAX_BYTES")
    assert deployment_settings.QUALIFIED_PAYLOAD_CEILING_BYTES == ceiling
    assert deployment_settings.QUALIFIED_REQUEST_MAX_BYTES == ceiling
    assert DEFAULT_REQUEST_MAX_BYTES == ceiling
    assert HARD_PAYLOAD_CEILING_BYTES == ceiling
    assert DEFAULT_PAYLOAD_MAX_BYTES <= ceiling


def test_qualified_layout_matches_recommendation_and_round_trips(
    prior_head_schema: tuple[psycopg.Connection, str, str],
) -> None:
    _verify_sha256sums()
    rec = _load_recommendation()
    assert rec["verdict"] == "PASS"
    selected = set(rec["indexes"]["selected_signature"].split(","))
    expected_sig = rec["indexes"]["selected_signature"]
    expected_enq = int(rec["hash"]["enqueue_dedup"]["selected_count"])
    expected_cmp = int(rec["hash"]["complete_replay"]["selected_count"])

    conn, schema, database_url = prior_head_schema
    present = _index_names(conn, schema)
    assert OMITTED_BY_RECOMMENDATION <= present
    assert selected <= present
    assert CORRECTNESS_INDEXES <= present

    keys = _insert_representative_rows(conn)
    _assert_idempotency_uniqueness(conn, keys)

    conn.close()
    run_alembic(
        "upgrade", QUALIFIED_REVISION, schema=schema, database_url=database_url
    )
    conn = _open_schema_connection(database_url, schema)

    present = _index_names(conn, schema)
    assert _measured_signature(present, selected) == expected_sig
    assert not (OMITTED_BY_RECOMMENDATION & present)
    assert CORRECTNESS_INDEXES <= present
    assert selected <= present
    assert _hash_modulus(conn, schema, "enqueue_dedup") == expected_enq
    assert _hash_modulus(conn, schema, "complete_replay") == expected_cmp
    _assert_rows_present(conn, keys)
    _assert_idempotency_uniqueness(conn, keys)

    conn.close()
    run_alembic("downgrade", PRIOR_REVISION, schema=schema, database_url=database_url)
    conn = _open_schema_connection(database_url, schema)

    present = _index_names(conn, schema)
    assert OMITTED_BY_RECOMMENDATION <= present
    _assert_rows_present(conn, keys)
    _assert_idempotency_uniqueness(conn, keys)

    conn.close()
    run_alembic("upgrade", "head", schema=schema, database_url=database_url)
    conn = _open_schema_connection(database_url, schema)
    present = _index_names(conn, schema)
    assert _measured_signature(present, selected) == expected_sig
    _assert_rows_present(conn, keys)
    _assert_idempotency_uniqueness(conn, keys)
