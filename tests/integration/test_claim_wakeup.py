"""Phase 20.1 Plan 02: PostgreSQL LISTEN/NOTIFY claim-wake substrate."""

from __future__ import annotations

import time
import uuid

import psycopg
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import QueuePool

from workhold.infrastructure.postgres.claim_wakeup import (
    CLAIM_WAKE_CHANNEL,
    ClaimWakeListener,
    ListenerHealth,
    QueueGenerationCoordinator,
    is_valid_queue_name_payload,
)
from tests.fixtures.claim_long_poll import (
    WAVE0_SCENARIO_IDS,
    GenerationBarrier,
    ListenerConnectionProbe,
    assert_no_forbidden_diagnostics,
)
from tests.integration.conftest import run_alembic, to_psycopg_conninfo

pytestmark = pytest.mark.usefixtures("migrated_schema")


@pytest.fixture
def wake_barrier() -> GenerationBarrier:
    return GenerationBarrier()


@pytest.fixture
def listener_probe() -> ListenerConnectionProbe:
    return ListenerConnectionProbe()


@pytest.fixture
def dsn(test_database_url: str, migrated_schema: tuple[psycopg.Connection, str]) -> str:
    _conn, schema = migrated_schema
    base = to_psycopg_conninfo(test_database_url)
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}options=-csearch_path%3D{schema}"


def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _seed_queue_with_policy(conn: psycopg.Connection, *, name: str) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queues (queue_id, name)
            VALUES (gen_random_uuid(), %s)
            RETURNING id
            """,
            (name,),
        )
        queue_pk = int(cur.fetchone()[0])
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
        policy_id = int(cur.fetchone()[0])
        cur.execute(
            "UPDATE queues SET active_policy_version_id = %s WHERE id = %s",
            (policy_id, queue_pk),
        )
    return queue_pk, policy_id


def _insert_task(
    conn: psycopg.Connection,
    *,
    queue_pk: int,
    policy_id: int,
    state_code: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, available_at,
                retry_policy_version_id
            ) VALUES (
                gen_random_uuid(), %s, 'wake-producer', %s,
                transaction_timestamp(), %s
            )
            RETURNING id
            """,
            (queue_pk, state_code, policy_id),
        )
        return int(cur.fetchone()[0])


def test_wake_fixture_repr_contains_no_secrets(listener_probe: ListenerConnectionProbe) -> None:
    assert_no_forbidden_diagnostics(repr(listener_probe))


def test_listener_repr_contains_no_dsn(dsn: str) -> None:
    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator)
    rendered = repr(listener)
    assert "postgresql://" not in rendered
    assert "queue:queue@" not in rendered
    assert_no_forbidden_diagnostics(rendered)


def test_commit_wakes_matching_queue_generation(
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    conn, _schema = migrated_schema
    queue_name = f"wake.commit.{uuid.uuid4().hex[:8]}"
    queue_pk, policy_id = _seed_queue_with_policy(conn, name=queue_name)
    conn.commit()

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        observed = coordinator.snapshot(queue_name)
        _insert_task(conn, queue_pk=queue_pk, policy_id=policy_id, state_code=2)
        conn.commit()
        assert coordinator.wait(queue_name, observed, timeout=3.0)
        assert coordinator.snapshot(queue_name) > observed
    finally:
        listener.stop()


def test_rollback_does_not_wake(
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    conn, _schema = migrated_schema
    queue_name = f"wake.rollback.{uuid.uuid4().hex[:8]}"
    queue_pk, policy_id = _seed_queue_with_policy(conn, name=queue_name)
    conn.commit()

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        observed = coordinator.snapshot(queue_name)
        _insert_task(conn, queue_pk=queue_pk, policy_id=policy_id, state_code=2)
        conn.rollback()
        assert not coordinator.wait(queue_name, observed, timeout=1.0)
        assert coordinator.snapshot(queue_name) == observed
    finally:
        listener.stop()


def test_unrelated_queue_does_not_advance_target_generation(
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    conn, _schema = migrated_schema
    target = f"wake.target.{uuid.uuid4().hex[:8]}"
    other = f"wake.other.{uuid.uuid4().hex[:8]}"
    _seed_queue_with_policy(conn, name=target)
    other_pk, other_policy = _seed_queue_with_policy(conn, name=other)
    conn.commit()

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        target_observed = coordinator.snapshot(target)
        other_observed = coordinator.snapshot(other)
        _insert_task(conn, queue_pk=other_pk, policy_id=other_policy, state_code=2)
        conn.commit()
        assert coordinator.wait(other, other_observed, timeout=3.0)
        assert coordinator.snapshot(target) == target_observed
        assert coordinator.snapshot(other) > other_observed
    finally:
        listener.stop()


def test_duplicate_notifies_coalesce_under_single_transaction(
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    conn, _schema = migrated_schema
    queue_name = f"wake.coalesce.{uuid.uuid4().hex[:8]}"
    queue_pk, policy_id = _seed_queue_with_policy(conn, name=queue_name)
    conn.commit()

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        observed = coordinator.snapshot(queue_name)
        _insert_task(conn, queue_pk=queue_pk, policy_id=policy_id, state_code=2)
        _insert_task(conn, queue_pk=queue_pk, policy_id=policy_id, state_code=1)
        conn.commit()
        assert coordinator.wait(queue_name, observed, timeout=3.0)
        # Identical channel+payload notifies in one transaction are folded by PostgreSQL.
        assert coordinator.snapshot(queue_name) == observed + 1
    finally:
        listener.stop()


def test_heartbeat_update_does_not_notify(
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    conn, _schema = migrated_schema
    queue_name = f"wake.heartbeat.{uuid.uuid4().hex[:8]}"
    queue_pk, policy_id = _seed_queue_with_policy(conn, name=queue_name)
    task_id = _insert_task(conn, queue_pk=queue_pk, policy_id=policy_id, state_code=3)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks_active
            SET generation = 1,
                current_claim_id = gen_random_uuid(),
                claimed_at = transaction_timestamp(),
                lease_expires_at = transaction_timestamp() + interval '30 seconds',
                worker_id = 'worker-1'
            WHERE id = %s
            """,
            (task_id,),
        )
    conn.commit()

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        observed = coordinator.snapshot(queue_name)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks_active
                SET lease_expires_at = transaction_timestamp() + interval '60 seconds'
                WHERE id = %s
                """,
                (task_id,),
            )
        conn.commit()
        assert not coordinator.wait(queue_name, observed, timeout=1.0)
    finally:
        listener.stop()


def test_queue_state_change_wakes(
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    conn, _schema = migrated_schema
    queue_name = f"wake.qstate.{uuid.uuid4().hex[:8]}"
    queue_pk, _policy_id = _seed_queue_with_policy(conn, name=queue_name)
    conn.commit()

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        observed = coordinator.snapshot(queue_name)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE queues
                SET state_code = 2, config_version = config_version + 1
                WHERE id = %s
                """,
                (queue_pk,),
            )
        conn.commit()
        assert coordinator.wait(queue_name, observed, timeout=3.0)
    finally:
        listener.stop()


def test_listener_reconnect_bumps_global_and_signals_degraded(dsn: str) -> None:
    coordinator = QueueGenerationCoordinator()
    connections: list[psycopg.Connection] = []
    fail_next = {"value": False}

    def connect() -> psycopg.Connection:
        if fail_next["value"]:
            fail_next["value"] = False
            raise psycopg.OperationalError("simulated listener outage")
        conn = psycopg.connect(dsn, autocommit=True)
        connections.append(conn)
        return conn

    listener = ClaimWakeListener(
        dsn,
        coordinator,
        connect=connect,
        initial_backoff_seconds=0.05,
        max_backoff_seconds=0.2,
        notify_poll_seconds=0.05,
    )
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        before = coordinator.snapshot("any.queue.name")
        assert len(connections) >= 1
        fail_next["value"] = True
        connections[0].close()
        assert _wait_until(
            lambda: listener.health is ListenerHealth.DEGRADED,
            timeout=2.0,
        )
        assert _wait_until(
            lambda: listener.health is ListenerHealth.CONNECTED,
            timeout=3.0,
        )
        assert coordinator.snapshot("any.queue.name") > before
    finally:
        listener.stop()


def test_listener_stop_closes_connection(dsn: str) -> None:
    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        conn = listener.connection
        assert conn is not None
        assert not conn.closed
    finally:
        listener.stop(join_timeout_seconds=2.0)
    assert listener.connection is None
    assert listener.health is ListenerHealth.DEGRADED


def test_listener_uses_direct_connection_not_sqlalchemy_pool(
    test_database_url: str,
    migrated_schema: tuple[psycopg.Connection, str],
    dsn: str,
) -> None:
    _conn, schema = migrated_schema
    engine = create_engine(
        test_database_url,
        poolclass=QueuePool,
        pool_size=2,
        max_overflow=0,
        pool_pre_ping=True,
    )

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    checkouts: list[int] = []

    @event.listens_for(engine, "checkout")
    def _on_checkout(_dbapi_conn, _conn_record, _conn_proxy) -> None:  # noqa: ANN001
        checkouts.append(1)

    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(dsn, coordinator, notify_poll_seconds=0.05)
    listener.start()
    try:
        assert _wait_until(lambda: listener.health is ListenerHealth.CONNECTED)
        listener_conn = listener.connection
        assert listener_conn is not None
        listener_pid = listener_conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        with engine.connect() as pooled:
            pooled_pid = pooled.execute(text("SELECT pg_backend_pid()")).scalar_one()
        assert listener_pid != pooled_pid
        # Listener startup must not checkout the SQLAlchemy pool.
        assert checkouts == [1]
    finally:
        listener.stop()
        engine.dispose()


def test_injection_shaped_payload_is_rejected() -> None:
    assert not is_valid_queue_name_payload("'; DROP TABLE queues;--")
    assert not is_valid_queue_name_payload("';' OR '1'='1")
    assert not is_valid_queue_name_payload("../../etc/passwd")
    assert is_valid_queue_name_payload("orders.intake")


def test_migration_downgrade_removes_trigger_objects(
    migrated_schema: tuple[psycopg.Connection, str],
    test_database_url: str,
) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND NOT t.tgisinternal
              AND t.tgname IN (
                    'tasks_active_claim_wakeup_trg',
                    'queues_claim_wakeup_trg'
              )
            """,
            (schema,),
        )
        assert int(cur.fetchone()[0]) == 2

    run_alembic(
        "downgrade",
        "044_break_glass_elevations",
        schema=schema,
        database_url=test_database_url,
    )
    conn.close()
    fresh = psycopg.connect(to_psycopg_conninfo(test_database_url))
    fresh.autocommit = True
    fresh.execute(f'SET search_path TO "{schema}"')
    try:
        with fresh.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM pg_trigger t
                JOIN pg_class c ON c.oid = t.tgrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND NOT t.tgisinternal
                  AND t.tgname IN (
                        'tasks_active_claim_wakeup_trg',
                        'queues_claim_wakeup_trg'
                  )
                """,
                (schema,),
            )
            assert int(cur.fetchone()[0]) == 0
            cur.execute(
                """
                SELECT COUNT(*) FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = %s
                  AND p.proname IN (
                        'claim_wakeup_notify_task',
                        'claim_wakeup_notify_queue_state'
                  )
                """,
                (schema,),
            )
            assert int(cur.fetchone()[0]) == 0
    finally:
        fresh.close()
        run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)


@pytest.mark.parametrize("scenario_id", WAVE0_SCENARIO_IDS)
@pytest.mark.skip(reason="Wave 0 matrix rows land across Plans 02-06; Plan 03 owns claim loop")
def test_claim_wakeup_matrix_registry(scenario_id: str) -> None:
    raise AssertionError(f"unimplemented matrix row: {scenario_id}")


def test_channel_constant_is_static() -> None:
    assert CLAIM_WAKE_CHANNEL == "queue_claim_wakeup"
