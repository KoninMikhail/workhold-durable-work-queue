"""QUAL-04 / DLVR-01: relay publish-before-ack duplicate publication chaos.

Documents at-least-once publication and consumer-side deduplication by stable
CloudEvents id. This suite proves the unavoidable duplicate window and recovery,
not a single-delivery guarantee.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from sqlalchemy import create_engine, text

from queue_service.delivery.models import STATE_PENDING, STATE_PUBLISHED, STATE_PUBLISHING
from tests.fixtures.http_delivery_sink import HttpDeliverySink
from tests.integration.conftest import require_test_database_url, run_alembic, to_psycopg_conninfo

REPO_ROOT = Path(__file__).resolve().parents[2]
REPEAT_COUNT = 3
LEASE_SECONDS = 1


@pytest.fixture
def relay_chaos_schema() -> Iterator[tuple[str, str]]:
    database_url = require_test_database_url()
    schema = f"qrd_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()
    run_alembic("upgrade", "head", schema=schema, database_url=database_url)
    try:
        yield database_url, schema
    finally:
        drop = psycopg.connect(to_psycopg_conninfo(database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


@pytest.fixture
def http_sink() -> Iterator[HttpDeliverySink]:
    sink = HttpDeliverySink(inbox_mode=True)
    sink.start()
    try:
        yield sink
    finally:
        sink.stop()


def _insert_pending(database_url: str, schema: str) -> tuple[uuid.UUID, bytes]:
    engine = create_engine(database_url)
    eid = uuid.uuid4()
    envelope = {
        "id": str(eid),
        "source": "urn:test:relay-duplicate-chaos",
        "specversion": "1.0",
        "type": "com.example.relay.duplicate.v1",
        "time": "2026-09-19T12:00:00Z",
        "data": {"marker": "at-least-once"},
    }
    envelope_json = json.dumps(envelope, separators=(",", ":"), sort_keys=True)
    body = envelope_json.encode("utf-8")
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{schema}"'))
        now = conn.execute(text("SELECT transaction_timestamp()")).scalar_one()
        conn.execute(
            text(
                """
                INSERT INTO delivery_events_active (
                    event_id, source_task_id, ordinal, state_code,
                    envelope, envelope_bytes, available_at, generation,
                    current_claim_id, claimed_at, lease_expires_at,
                    relay_principal_id, delivery_attempt, last_failure_code,
                    created_at, updated_at
                ) VALUES (
                    :eid, :sid, 0, :pending,
                    CAST(:envelope AS jsonb), :ebytes,
                    :now, 0,
                    NULL, NULL, NULL,
                    NULL, 0, NULL,
                    :now, :now
                )
                """
            ),
            {
                "eid": eid,
                "sid": uuid.uuid4(),
                "pending": STATE_PENDING,
                "envelope": envelope_json,
                "ebytes": len(body),
                "now": now,
            },
        )
    engine.dispose()
    return eid, body


def _event_counts(
    database_url: str, schema: str, event_id: uuid.UUID
) -> dict[str, Any]:
    engine = create_engine(database_url)
    with engine.connect() as conn:
        conn.execute(text(f'SET search_path TO "{schema}"'))
        active = conn.execute(
            text(
                """
                SELECT state_code, generation, delivery_attempt,
                       lease_expires_at IS NOT NULL AS has_lease
                FROM delivery_events_active WHERE event_id = :eid
                """
            ),
            {"eid": event_id},
        ).mappings().all()
        terminal = conn.execute(
            text(
                """
                SELECT state_code, delivery_attempt, failure_code
                FROM delivery_events_terminal WHERE event_id = :eid
                """
            ),
            {"eid": event_id},
        ).mappings().all()
        intent_rows = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT event_id FROM delivery_events_active WHERE event_id = :eid
                    UNION ALL
                    SELECT event_id FROM delivery_events_terminal WHERE event_id = :eid
                ) t
                """
            ),
            {"eid": event_id},
        ).scalar_one()
    engine.dispose()
    return {
        "active": [dict(r) for r in active],
        "terminal": [dict(r) for r in terminal],
        "intent_rows": int(intent_rows),
    }


def _relay_env(
    database_url: str,
    schema: str,
    webhook_url: str,
    *,
    principal: str,
    fault: str | None = None,
    ready_path: str | None = None,
    max_cycles: int = 3,
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": database_url,
            "QUEUE_SCHEMA": schema,
            "QUEUE_ENVIRONMENT": "development",
            "QUEUE_DELIVERY_WEBHOOK_URL": webhook_url,
            "QUEUE_DELIVERY_ALLOWED_HOSTS": "127.0.0.1",
            "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8,::1/128",
            "QUEUE_DELIVERY_BEARER_TOKEN": "chaos-relay-bearer-do-not-log",
            "QUEUE_DELIVERY_CONNECT_TIMEOUT_SECONDS": "1",
            "QUEUE_DELIVERY_READ_TIMEOUT_SECONDS": "2",
            "QUEUE_DELIVERY_TOTAL_TIMEOUT_SECONDS": "3",
            "QUEUE_DELIVERY_MAX_RESPONSE_BYTES": "4096",
            "QUEUE_DELIVERY_CIRCUIT_FAILURE_THRESHOLD": "10",
            "QUEUE_DELIVERY_CIRCUIT_OPEN_SECONDS": "1",
            "QUEUE_DELIVERY_CIRCUIT_SUCCESS_THRESHOLD": "1",
            "QUEUE_DELIVERY_HALF_OPEN_MAX_PROBES": "1",
            "QUEUE_DELIVERY_RETRY_AFTER_CAP_SECONDS": "30",
            "QUEUE_RELAY_PRINCIPAL_ID": principal,
            "QUEUE_RELAY_LEASE_SECONDS": str(LEASE_SECONDS),
            "QUEUE_RELAY_MAX_ATTEMPTS": "5",
            "QUEUE_RELAY_BACKOFF_BASE_SECONDS": "0.01",
            "QUEUE_RELAY_BACKOFF_MAX_SECONDS": "1",
            "QUEUE_RELAY_RETRY_AFTER_CAP_SECONDS": "30",
            "QUEUE_RELAY_JITTER_RATIO": "0",
            "QUEUE_RELAY_DEFAULT_PROBE_SECONDS": "0.05",
            "QUEUE_RELAY_IDLE_SLEEP_SECONDS": "0.05",
            "QUEUE_RELAY_GRACE_SECONDS": "1",
            "QUEUE_RELAY_MAX_CYCLES": str(max_cycles),
        }
    )
    env.pop("QUEUE_TEST_RELAY_FAULT", None)
    env.pop("QUEUE_TEST_RELAY_FAULT_READY_PATH", None)
    if fault:
        env["QUEUE_TEST_RELAY_FAULT"] = fault
        if ready_path:
            env["QUEUE_TEST_RELAY_FAULT_READY_PATH"] = ready_path
    return env


def _start_relay(env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        ["uv", "run", "queue", "relay"],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _kill_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        if sys.platform == "win32":
            proc.kill()
        else:
            os.kill(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _wait_ready(path: Path, *, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.is_file() and path.stat().st_size > 0:
            return
        time.sleep(0.05)
    raise TimeoutError(f"fault ready marker missing: {path}")


def _wait_lease_reclaimable(
    database_url: str, schema: str, event_id: uuid.UUID, *, timeout: float = 10.0
) -> None:
    """Wait until Queue-store lease expiry makes the publishing row reclaimable."""
    deadline = time.monotonic() + timeout
    engine = create_engine(database_url)
    try:
        while time.monotonic() < deadline:
            with engine.connect() as conn:
                conn.execute(text(f'SET search_path TO "{schema}"'))
                row = conn.execute(
                    text(
                        """
                        SELECT state_code, lease_expires_at,
                               lease_expires_at <= transaction_timestamp() AS expired
                        FROM delivery_events_active WHERE event_id = :eid
                        """
                    ),
                    {"eid": event_id},
                ).mappings().first()
            if row is None:
                return
            if int(row["state_code"]) == STATE_PUBLISHING and bool(row["expired"]):
                return
            if int(row["state_code"]) == STATE_PENDING:
                return
            time.sleep(0.05)
        raise TimeoutError("lease did not expire for reclaim within timeout")
    finally:
        engine.dispose()


def _assert_no_single_delivery_guarantee_claim(source: str) -> None:
    """Refuse suite wording that promises a single-delivery publication guarantee."""
    lowered = source.lower()
    banned = ("exactly" + "-once", "exactly" + " once")
    for phrase in banned:
        assert phrase not in lowered, f"forbidden guarantee wording: {phrase!r}"


def _run_duplicate_after_publish_window(
    database_url: str, schema: str, sink: HttpDeliverySink
) -> None:
    """Relay A publishes (2xx) then dies before ack; relay B republishes after lease."""
    eid, expected_body = _insert_pending(database_url, schema)
    with tempfile.TemporaryDirectory(prefix="relay-chaos-") as tmp:
        ready = Path(tmp) / "after-publish.ready"
        env_a = _relay_env(
            database_url,
            schema,
            sink.base_url,
            principal="relay-chaos-a",
            fault="crash_after_publish",
            ready_path=str(ready),
            max_cycles=5,
        )
        proc_a = _start_relay(env_a)
        try:
            _wait_ready(ready, timeout=25.0)
            sink.wait_attempts(1, timeout=5.0)
            counts_mid = _event_counts(database_url, schema, eid)
            assert counts_mid["intent_rows"] == 1
            assert len(counts_mid["active"]) == 1
            assert int(counts_mid["active"][0]["state_code"]) == STATE_PUBLISHING
            assert counts_mid["terminal"] == []
            _kill_process(proc_a)
        finally:
            _kill_process(proc_a)

        _wait_lease_reclaimable(database_url, schema, eid, timeout=LEASE_SECONDS + 5)

        env_b = _relay_env(
            database_url,
            schema,
            sink.base_url,
            principal="relay-chaos-b",
            fault=None,
            max_cycles=5,
        )
        proc_b = _start_relay(env_b)
        try:
            sink.wait_attempts(2, timeout=25.0)
            deadline = time.monotonic() + 25.0
            final: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                final = _event_counts(database_url, schema, eid)
                if (
                    final["intent_rows"] == 1
                    and not final["active"]
                    and len(final["terminal"]) == 1
                    and int(final["terminal"][0]["state_code"]) == STATE_PUBLISHED
                ):
                    break
                time.sleep(0.05)
            assert final is not None
            assert final["intent_rows"] == 1, final
            assert final["active"] == [], final
            assert len(final["terminal"]) == 1, final
            assert int(final["terminal"][0]["state_code"]) == STATE_PUBLISHED
            # Reclaim + successful ack: second delivery attempt is observable.
            assert int(final["terminal"][0]["delivery_attempt"]) >= 2
        finally:
            _kill_process(proc_b)
            if proc_b.poll() is None:
                pytest.fail("relay B still running after cleanup")

    assert sink.attempt_count == 2
    assert sink.bodies_byte_equivalent()
    assert all(a.event_id == str(eid) for a in sink.attempts)
    assert all(
        a.content_type == "application/cloudevents+json" for a in sink.attempts
    )
    # Inbox dedup: two network attempts, one logical consumer effect.
    assert sink.logical_effects == 1
    # Digests match the stored envelope serialization contract.
    expected_digest = __import__("hashlib").sha256(expected_body).hexdigest()
    # Transport re-dumps with sort_keys; stored insert already sorted — equal.
    assert sink.attempts[0].body_sha256 == sink.attempts[1].body_sha256
    assert sink.attempts[0].body_sha256 == expected_digest


def _run_death_before_http_send(
    database_url: str, schema: str, sink: HttpDeliverySink
) -> None:
    """Control: death before HTTP send yields exactly one sink delivery after recovery."""
    eid, _body = _insert_pending(database_url, schema)
    with tempfile.TemporaryDirectory(prefix="relay-chaos-pre-") as tmp:
        ready = Path(tmp) / "before-publish.ready"
        env_a = _relay_env(
            database_url,
            schema,
            sink.base_url,
            principal="relay-chaos-pre-a",
            fault="crash_before_publish",
            ready_path=str(ready),
            max_cycles=5,
        )
        proc_a = _start_relay(env_a)
        try:
            _wait_ready(ready, timeout=25.0)
            assert sink.attempt_count == 0
            counts_mid = _event_counts(database_url, schema, eid)
            assert counts_mid["intent_rows"] == 1
            assert len(counts_mid["active"]) == 1
            assert int(counts_mid["active"][0]["state_code"]) == STATE_PUBLISHING
            _kill_process(proc_a)
        finally:
            _kill_process(proc_a)

        _wait_lease_reclaimable(database_url, schema, eid, timeout=LEASE_SECONDS + 5)

        env_b = _relay_env(
            database_url,
            schema,
            sink.base_url,
            principal="relay-chaos-pre-b",
            fault=None,
            max_cycles=5,
        )
        proc_b = _start_relay(env_b)
        try:
            sink.wait_attempts(1, timeout=25.0)
            deadline = time.monotonic() + 25.0
            final: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                final = _event_counts(database_url, schema, eid)
                if (
                    not final["active"]
                    and len(final["terminal"]) == 1
                    and int(final["terminal"][0]["state_code"]) == STATE_PUBLISHED
                ):
                    break
                time.sleep(0.05)
            assert final is not None
            assert final["intent_rows"] == 1
            assert final["active"] == []
            assert int(final["terminal"][0]["state_code"]) == STATE_PUBLISHED
        finally:
            _kill_process(proc_b)

    assert sink.attempt_count == 1
    assert sink.attempts[0].event_id == str(eid)
    assert sink.logical_effects == 1


@pytest.mark.parametrize("iteration", range(REPEAT_COUNT))
def test_duplicate_publication_after_crash_before_ack(
    relay_chaos_schema: tuple[str, str],
    http_sink: HttpDeliverySink,
    iteration: int,
) -> None:
    """At-least-once: successful HTTP then death before ack ⇒ republish after lease."""
    _ = iteration
    database_url, schema = relay_chaos_schema
    _assert_no_single_delivery_guarantee_claim(
        Path(__file__).read_text(encoding="utf-8")
    )
    _run_duplicate_after_publish_window(database_url, schema, http_sink)


@pytest.mark.parametrize("iteration", range(REPEAT_COUNT))
def test_death_before_http_send_recovers_with_one_delivery(
    relay_chaos_schema: tuple[str, str],
    http_sink: HttpDeliverySink,
    iteration: int,
) -> None:
    """Control window: kill before send; recovery publishes once."""
    _ = iteration
    database_url, schema = relay_chaos_schema
    _run_death_before_http_send(database_url, schema, http_sink)


def test_suite_documents_at_least_once_not_single_delivery_guarantee() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    assert "at-least-once" in source.lower() or "at least once" in source.lower()
    _assert_no_single_delivery_guarantee_claim(source)
    sink_src = Path(
        REPO_ROOT / "tests/fixtures/http_delivery_sink.py"
    ).read_text(encoding="utf-8")
    assert "deduplicat" in sink_src.lower()
    _assert_no_single_delivery_guarantee_claim(sink_src)
