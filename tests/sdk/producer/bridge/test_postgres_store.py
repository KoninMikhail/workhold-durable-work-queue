"""Real-PostgreSQL OutboxStore claim/reclaim/fencing and bounded health (06-03 / BRDG-02)."""

from __future__ import annotations

import json
import os
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# DB helpers (test-owned DDL — adapter must never ship migrations)
# ---------------------------------------------------------------------------


def _require_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for bridge postgres store tests "
            "(PostgreSQL). Refusing to skip or xfail."
        )
    return url


def _to_psycopg_conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


@pytest.fixture(scope="module")
def psycopg_mod():
    """psycopg is a *test* dependency of the repo; workhold-producer must not import it."""
    import psycopg

    return psycopg


@pytest.fixture
def app_outbox_schema(psycopg_mod) -> Iterator[tuple[str, str, Any]]:
    """Create an application-owned outbox table in an isolated schema; drop after."""
    database_url = _require_test_database_url()
    schema = f"app_ob_{uuid.uuid4().hex[:16]}"
    table = "enqueue_outbox"
    conninfo = _to_psycopg_conninfo(database_url)

    admin = psycopg_mod.connect(conninfo)
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
        admin.execute(
            f"""
            CREATE TABLE "{schema}"."{table}" (
                source_namespace text NOT NULL,
                source_row_id text NOT NULL,
                schema_version integer NOT NULL,
                target_queue text NOT NULL,
                enqueue_request jsonb NOT NULL,
                created_at timestamptz NOT NULL,
                traceparent text,
                tracestate text,
                extensions jsonb,
                state text NOT NULL,
                ownership_token text,
                generation integer NOT NULL DEFAULT 0,
                lease_expires_at timestamptz,
                available_at timestamptz NOT NULL,
                updated_at timestamptz NOT NULL,
                queue_task_id text,
                last_failure_code text,
                PRIMARY KEY (source_namespace, source_row_id)
            )
            """
        )
        admin.execute(
            f"""
            CREATE INDEX "{table}_claim_idx"
            ON "{schema}"."{table}" (available_at, source_namespace, source_row_id)
            WHERE state IN ('pending', 'retryable_failure')
               OR (state = 'leased' AND lease_expires_at IS NOT NULL)
            """
        )
        admin.execute(
            f"""
            CREATE INDEX "{table}_pending_created_idx"
            ON "{schema}"."{table}" (created_at)
            WHERE state <> 'delivered'
            """
        )
    finally:
        admin.close()

    yield schema, table, conninfo

    drop = psycopg_mod.connect(conninfo)
    drop.autocommit = True
    try:
        drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        drop.close()


def _connection_factory(psycopg_mod, conninfo: str):
    def factory():
        return psycopg_mod.connect(conninfo)

    return factory


def _make_store(psycopg_mod, schema: str, table: str, conninfo: str):
    from workhold_producer.bridge.postgres_store import (
        PostgresOutboxMapping,
        PostgresOutboxStore,
    )

    mapping = PostgresOutboxMapping(schema=schema, table=table)
    return PostgresOutboxStore(
        connection_factory=_connection_factory(psycopg_mod, conninfo),
        mapping=mapping,
    )


def _insert_pending(
    psycopg_mod,
    conninfo: str,
    schema: str,
    table: str,
    *,
    namespace: str,
    row_id: str,
    available_offset_seconds: float = 0.0,
    created_offset_seconds: float = 0.0,
    enqueue_request: dict[str, Any] | None = None,
) -> None:
    body = enqueue_request or {"payload": {"k": row_id}, "priority": 0}
    conn = psycopg_mod.connect(conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO "{schema}"."{table}" (
                    source_namespace, source_row_id, schema_version, target_queue,
                    enqueue_request, created_at, state, ownership_token, generation,
                    lease_expires_at, available_at, updated_at
                ) VALUES (
                    %s, %s, 1, %s,
                    %s::jsonb,
                    now() + make_interval(secs => %s),
                    'pending', NULL, 0,
                    NULL,
                    now() + make_interval(secs => %s),
                    now()
                )
                """,
                (
                    namespace,
                    row_id,
                    "orders",
                    json.dumps(body),
                    created_offset_seconds,
                    available_offset_seconds,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _force_expire_lease(
    psycopg_mod, conninfo: str, schema: str, table: str, namespace: str, row_id: str
) -> None:
    conn = psycopg_mod.connect(conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE "{schema}"."{table}"
                SET lease_expires_at = now() - interval '1 second',
                    updated_at = now()
                WHERE source_namespace = %s AND source_row_id = %s
                """,
                (namespace, row_id),
            )
        conn.commit()
    finally:
        conn.close()


def _count_state(
    psycopg_mod, conninfo: str, schema: str, table: str, state: str
) -> int:
    conn = psycopg_mod.connect(conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT count(*) FROM "{schema}"."{table}" WHERE state = %s',
                (state,),
            )
            row = cur.fetchone()
            assert row is not None
            return int(row[0])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Packaging / ownership constraints
# ---------------------------------------------------------------------------


def test_postgres_store_module_does_not_import_db_driver() -> None:
    import importlib
    import sys

    # Ensure a clean import path observation for the adapter module.
    for name in list(sys.modules):
        if name == "workhold_producer.bridge.postgres_store" or name.startswith(
            "workhold_producer.bridge.postgres_store."
        ):
            del sys.modules[name]

    before = {k for k in sys.modules if k == "psycopg" or k.startswith("psycopg.")}
    importlib.import_module("workhold_producer.bridge.postgres_store")
    after = {k for k in sys.modules if k == "psycopg" or k.startswith("psycopg.")}
    assert after == before, "PostgresOutboxStore must not import a database driver"


def test_producer_base_import_stays_driver_free() -> None:
    import importlib
    import sys
    import tomllib
    from pathlib import Path

    for name in list(sys.modules):
        if name == "psycopg" or name.startswith("psycopg."):
            # Allow other tests to have imported psycopg; re-check via source.
            break
    src = importlib.import_module("workhold_producer.bridge.postgres_store")
    text = open(src.__file__, encoding="utf-8").read()  # noqa: PTH123
    assert "import psycopg" not in text
    assert "from psycopg" not in text
    assert "CREATE TABLE" not in text.upper()
    assert "CREATE INDEX" not in text.upper()
    assert "migration" not in text.lower()

    pyproject = (
        Path(__file__).resolve().parents[4]
        / "packages"
        / "workhold-producer"
        / "pyproject.toml"
    )
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    base_deps = " ".join(data["project"].get("dependencies", []))
    assert "psycopg" not in base_deps
    extras = data["project"]["optional-dependencies"]["bridge-postgres"]
    assert any("psycopg" in dep for dep in extras)


def test_unsafe_mapping_identifiers_rejected(psycopg_mod, app_outbox_schema) -> None:
    from workhold_producer.bridge.postgres_store import (
        PostgresOutboxMapping,
        PostgresOutboxStore,
    )

    schema, table, conninfo = app_outbox_schema
    with pytest.raises(ValueError, match="identifier"):
        PostgresOutboxMapping(schema=schema, table='outbox"; DROP TABLE x; --')
    with pytest.raises(ValueError, match="identifier"):
        PostgresOutboxStore(
            connection_factory=_connection_factory(psycopg_mod, conninfo),
            mapping=PostgresOutboxMapping(schema="bad-schema", table=table),
        )


# ---------------------------------------------------------------------------
# Claim / concurrent ownership
# ---------------------------------------------------------------------------


def test_claim_returns_bounded_batch_with_ownership(
    psycopg_mod, app_outbox_schema
) -> None:
    schema, table, conninfo = app_outbox_schema
    for i in range(5):
        _insert_pending(psycopg_mod, conninfo, schema, table, namespace="ns", row_id=f"r{i}")

    store = _make_store(psycopg_mod, schema, table, conninfo)
    claimed = store.claim(limit=2, lease_seconds=30)
    assert len(claimed) == 2
    tokens = {c.ownership_token for c in claimed}
    assert len(tokens) == 2
    for item in claimed:
        assert item.source_namespace == "ns"
        assert item.target_queue == "orders"
        assert item.schema_version == 1
        assert item.ownership_token
        assert item.lease_expires_at is not None
        assert item.enqueue_request["payload"]["k"] == item.source_row_id
        # Intent body is immutable mapping (not mutated by store).
        assert isinstance(item.enqueue_request, dict) or hasattr(
            item.enqueue_request, "get"
        )

    assert _count_state(psycopg_mod, conninfo, schema, table, "leased") == 2
    assert _count_state(psycopg_mod, conninfo, schema, table, "pending") == 3


def test_concurrent_claims_never_share_current_owner(
    psycopg_mod, app_outbox_schema
) -> None:
    schema, table, conninfo = app_outbox_schema
    _insert_pending(psycopg_mod, conninfo, schema, table, namespace="ns", row_id="only")

    barrier = threading.Barrier(2)
    results: list[list[Any]] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            store = _make_store(psycopg_mod, schema, table, conninfo)
            barrier.wait(timeout=10)
            results.append(list(store.claim(limit=1, lease_seconds=60)))
        except BaseException as exc:  # noqa: BLE001 — surface in parent
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    assert len(results) == 2
    winners = [batch for batch in results if batch]
    empties = [batch for batch in results if not batch]
    assert len(winners) == 1
    assert len(empties) == 1
    assert winners[0][0].source_row_id == "only"
    assert _count_state(psycopg_mod, conninfo, schema, table, "leased") == 1


# ---------------------------------------------------------------------------
# Reclaim + stale fencing
# ---------------------------------------------------------------------------


def test_expired_lease_is_reclaimable_and_stale_acks_rejected(
    psycopg_mod, app_outbox_schema
) -> None:
    schema, table, conninfo = app_outbox_schema
    _insert_pending(psycopg_mod, conninfo, schema, table, namespace="ns", row_id="row-1")

    store_a = _make_store(psycopg_mod, schema, table, conninfo)
    first = store_a.claim(limit=1, lease_seconds=60)
    assert len(first) == 1
    stale = first[0]

    _force_expire_lease(psycopg_mod, conninfo, schema, table, "ns", "row-1")

    store_b = _make_store(psycopg_mod, schema, table, conninfo)
    second = store_b.claim(limit=1, lease_seconds=60)
    assert len(second) == 1
    fresh = second[0]
    assert fresh.source_row_id == "row-1"
    assert fresh.ownership_token != stale.ownership_token

    # Stale owner cannot mark delivered or schedule retry.
    assert (
        store_a.mark_delivered(
            source_namespace="ns",
            source_row_id="row-1",
            ownership_token=stale.ownership_token,
            queue_task_id="task-stale",
        )
        is False
    )
    assert (
        store_a.schedule_retry(
            source_namespace="ns",
            source_row_id="row-1",
            ownership_token=stale.ownership_token,
            available_at_delay_seconds=5.0,
            failure_code="timeout",
        )
        is False
    )
    assert (
        store_a.mark_terminal_operator_action(
            source_namespace="ns",
            source_row_id="row-1",
            ownership_token=stale.ownership_token,
            reason="idempotency_conflict",
        )
        is False
    )

    # Current owner can mark delivered.
    assert (
        store_b.mark_delivered(
            source_namespace="ns",
            source_row_id="row-1",
            ownership_token=fresh.ownership_token,
            queue_task_id="task-ok",
        )
        is True
    )
    assert _count_state(psycopg_mod, conninfo, schema, table, "delivered") == 1


def test_schedule_retry_uses_app_db_and_defers_availability(
    psycopg_mod, app_outbox_schema
) -> None:
    schema, table, conninfo = app_outbox_schema
    _insert_pending(psycopg_mod, conninfo, schema, table, namespace="ns", row_id="retry-me")

    store = _make_store(psycopg_mod, schema, table, conninfo)
    claimed = store.claim(limit=1, lease_seconds=30)
    assert len(claimed) == 1
    token = claimed[0].ownership_token

    assert (
        store.schedule_retry(
            source_namespace="ns",
            source_row_id="retry-me",
            ownership_token=token,
            available_at_delay_seconds=3600.0,
            failure_code="unavailable",
        )
        is True
    )

    # Not immediately reclaimable while available_at is in the future.
    again = store.claim(limit=1, lease_seconds=30)
    assert list(again) == []
    assert _count_state(psycopg_mod, conninfo, schema, table, "retryable_failure") == 1


def test_claim_transactions_commit_before_return(
    psycopg_mod, app_outbox_schema
) -> None:
    """Network enqueue must sit outside app-DB transactions — claim is committed."""
    schema, table, conninfo = app_outbox_schema
    _insert_pending(psycopg_mod, conninfo, schema, table, namespace="ns", row_id="tx")

    store = _make_store(psycopg_mod, schema, table, conninfo)
    claimed = store.claim(limit=1, lease_seconds=30)
    assert len(claimed) == 1

    # Visible to a fresh connection ⇒ claim transaction already committed.
    assert _count_state(psycopg_mod, conninfo, schema, table, "leased") == 1


# ---------------------------------------------------------------------------
# Bounded health / depth snapshots
# ---------------------------------------------------------------------------


def test_health_snapshot_caps_10001_pending_rows_at_1000(
    psycopg_mod, app_outbox_schema
) -> None:
    schema, table, conninfo = app_outbox_schema
    conn = psycopg_mod.connect(conninfo)
    try:
        with conn.cursor() as cur:
            # Bulk insert 10_001 pending rows without per-row round-trips.
            cur.execute(
                f"""
                INSERT INTO "{schema}"."{table}" (
                    source_namespace, source_row_id, schema_version, target_queue,
                    enqueue_request, created_at, state, ownership_token, generation,
                    lease_expires_at, available_at, updated_at
                )
                SELECT
                    'bulk',
                    'r' || g::text,
                    1,
                    'orders',
                    '{{"payload":{{}},"priority":0}}'::jsonb,
                    now() - make_interval(secs => g),
                    'pending',
                    NULL,
                    0,
                    NULL,
                    now(),
                    now()
                FROM generate_series(1, 10001) AS g
                """
            )
        conn.commit()
    finally:
        conn.close()

    store = _make_store(psycopg_mod, schema, table, conninfo)
    depth = store.get_pending_depth(1000)
    assert depth.depth_cap == 1000
    assert depth.count == 1000
    assert depth.capped is True
    assert depth.as_of.tzinfo is not None

    oldest = store.get_oldest_pending_created_at()
    assert oldest.as_of.tzinfo is not None
    assert oldest.created_at is not None
    assert oldest.created_at <= datetime.now(timezone.utc)

    snap = store.get_health_snapshot(1000)
    assert snap.as_of.tzinfo is not None
    assert snap.query_ok is True
    assert snap.connected is True
    assert snap.pending_count == 1000
    assert snap.pending_capped is True
    assert snap.oldest_pending_created_at is not None
    # No payload / identifier leakage on the snapshot type.
    assert not hasattr(snap, "source_row_id")
    assert not hasattr(snap, "payload")


def test_health_snapshot_empty_store(psycopg_mod, app_outbox_schema) -> None:
    schema, table, conninfo = app_outbox_schema
    store = _make_store(psycopg_mod, schema, table, conninfo)

    depth = store.get_pending_depth(1000)
    assert depth.count == 0
    assert depth.capped is False

    oldest = store.get_oldest_pending_created_at()
    assert oldest.created_at is None

    snap = store.get_health_snapshot(1000)
    assert snap.connected is True
    assert snap.query_ok is True
    assert snap.pending_count == 0
    assert snap.pending_capped is False
    assert snap.oldest_pending_created_at is None


def test_health_snapshot_unavailable_store(psycopg_mod, app_outbox_schema) -> None:
    from workhold_producer.bridge.postgres_store import (
        PostgresOutboxMapping,
        PostgresOutboxStore,
    )

    schema, table, _conninfo = app_outbox_schema

    def boom():
        raise OSError("app db unreachable")

    # Construction may validate schema — use a store that passes startup then fails queries.
    # Build against real DB first for column validation, then swap factory.
    real = _make_store(psycopg_mod, schema, table, _conninfo)
    broken = PostgresOutboxStore(
        connection_factory=boom,
        mapping=PostgresOutboxMapping(schema=schema, table=table),
        validate_on_init=False,
    )
    # Keep real alive so mapping was validated once (no unused var lint).
    assert real is not None

    snap = broken.get_health_snapshot(100)
    assert snap.connected is False
    assert snap.query_ok is False
    assert snap.pending_count == 0
    assert snap.pending_capped is False
    assert snap.oldest_pending_created_at is None
    assert snap.as_of is not None


def test_depth_cap_must_be_positive(psycopg_mod, app_outbox_schema) -> None:
    schema, table, conninfo = app_outbox_schema
    store = _make_store(psycopg_mod, schema, table, conninfo)
    with pytest.raises(ValueError, match="depth_cap"):
        store.get_pending_depth(0)
    with pytest.raises(ValueError, match="depth_cap"):
        store.get_health_snapshot(-1)


@pytest.mark.parametrize("missing_column", ("traceparent", "queue_task_id"))
def test_missing_claim_or_deliver_column_fail_at_startup(
    psycopg_mod, app_outbox_schema, missing_column: str
) -> None:
    from workhold_producer.bridge.postgres_store import (
        PostgresOutboxMapping,
        PostgresOutboxStore,
    )

    schema, table, conninfo = app_outbox_schema
    broken_table = f"broken_{missing_column}"
    conn = psycopg_mod.connect(conninfo)
    try:
        conn.autocommit = True
        cols = [
            "source_namespace text NOT NULL",
            "source_row_id text NOT NULL",
            "schema_version integer NOT NULL",
            "target_queue text NOT NULL",
            "enqueue_request jsonb NOT NULL",
            "created_at timestamptz NOT NULL",
            "traceparent text",
            "tracestate text",
            "extensions jsonb",
            "state text NOT NULL",
            "ownership_token text",
            "generation integer NOT NULL DEFAULT 0",
            "lease_expires_at timestamptz",
            "available_at timestamptz NOT NULL",
            "updated_at timestamptz NOT NULL",
            "queue_task_id text",
            "last_failure_code text",
        ]
        filtered = [c for c in cols if not c.startswith(f"{missing_column} ")]
        conn.execute(
            f"""
            CREATE TABLE "{schema}"."{broken_table}" (
                {", ".join(filtered)},
                PRIMARY KEY (source_namespace, source_row_id)
            )
            """
        )
    finally:
        conn.close()

    with pytest.raises(ValueError, match=missing_column):
        PostgresOutboxStore(
            connection_factory=_connection_factory(psycopg_mod, conninfo),
            mapping=PostgresOutboxMapping(schema=schema, table=broken_table),
        )


def test_missing_required_columns_fail_at_startup(
    psycopg_mod, app_outbox_schema
) -> None:
    from workhold_producer.bridge.postgres_store import (
        PostgresOutboxMapping,
        PostgresOutboxStore,
    )

    schema, _table, conninfo = app_outbox_schema
    conn = psycopg_mod.connect(conninfo)
    try:
        conn.autocommit = True
        conn.execute(
            f"""
            CREATE TABLE "{schema}"."broken_outbox" (
                source_namespace text NOT NULL,
                source_row_id text NOT NULL,
                PRIMARY KEY (source_namespace, source_row_id)
            )
            """
        )
    finally:
        conn.close()

    with pytest.raises((ValueError, RuntimeError), match="column|required|missing"):
        PostgresOutboxStore(
            connection_factory=_connection_factory(psycopg_mod, conninfo),
            mapping=PostgresOutboxMapping(schema=schema, table="broken_outbox"),
        )


def test_protocol_exports_expected_surface() -> None:
    from workhold_producer.bridge import store as store_mod

    assert hasattr(store_mod, "OutboxStore")
    assert hasattr(store_mod, "OutboxIntent")
    assert hasattr(store_mod, "BoundedPendingDepth")
    assert hasattr(store_mod, "AppStoreHealthSnapshot")
    assert hasattr(store_mod, "OldestPendingSnapshot")
