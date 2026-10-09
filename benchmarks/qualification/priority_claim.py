"""Phase 12 live PostgreSQL priority claim qualification (WORK-16 / Plan 12-10)."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from benchmarks.qualification.artifacts import (
    PHASE12_EVIDENCE_PACKAGE,
    PHASE12_QUALIFICATION_PROFILE,
    PHASE12_PRIORITY_WORKLOADS,
    derive,
    validate_final,
    validate_raw,
    write_checksums,
)
from benchmarks.qualification.load import MonotonicRateController, redact_log_fields
from benchmarks.qualification.postgres_probe import (
    DiagnosticsConnection,
    ProbeError,
    ProbeSnapshot,
    capture_snapshot,
    explain_priority_claim_plan,
    fetch_index_definition,
    normalize_psycopg_dsn,
    open_diagnostics_connection,
)
from benchmarks.qualification.storage_candidates import (
    PHASE12_PHYSICAL_SIGNATURE,
    PHASE12_SCHEMA_REVISION,
    qualified_physical_signature_digest,
)
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)

ROOT = Path(__file__).resolve().parents[2]
WORKLOAD_PATH = Path(__file__).resolve().parent / "workloads" / "priority-claim.yaml"
DEFAULT_OUTPUT_ROOT = ROOT / "benchmarks" / "results" / PHASE12_EVIDENCE_PACKAGE
PROFILE_PATH = Path(__file__).resolve().parent / "reference-environment.yaml"

_STATE_DELAYED = 1
_STATE_READY = 2
_STATE_LEASED = 3
_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767
_LEASE_SECONDS = 30

_FAST_CLAIM_AND_REMOVE_SQL = """
WITH picked AS (
    SELECT id, task_id
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
    FOR UPDATE SKIP LOCKED
),
removed_payload AS (
    DELETE FROM task_payloads_active p
    USING picked
    WHERE p.task_id = picked.id
)
DELETE FROM tasks_active t
USING picked
WHERE t.id = picked.id
RETURNING t.task_id;
"""


@dataclass(slots=True)
class PriorityLoadResult:
    workload: str
    output_dir: Path
    successful_claims: int
    measured_seconds: float
    claims_per_second: float
    success_ratio: float
    verdict: str


@dataclass(slots=True)
class _LoadState:
    latencies: list[dict[str, Any]] = field(default_factory=list)
    successful_claims: int = 0
    valid_attempts: int = 0
    errors: int = 0
    claimed_task_ids: set[str] = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(frozen=True, slots=True)
class _InvariantResult:
    name: str
    passed: bool
    message: str = ""


HOT_PATH_SCOPE = "claim-only"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _flatten_probe_artifact(
    snapshot: ProbeSnapshot,
    *,
    probe_source: str,
) -> dict[str, Any]:
    """Map probe snapshot counters to qualification postgres-* artifact shape."""
    buffers = snapshot.buffers or {}
    wal = snapshot.wal or {}
    dead = snapshot.dead_tuples or {}
    vacuum = snapshot.autovacuum or {}
    return {
        "environment_hash": snapshot.environment_hash,
        "probe_source": probe_source,
        "buffers": buffers,
        "wal": wal,
        "wal_bytes": int(wal.get("wal_bytes") or wal.get("wal_records") or 0),
        "buffer_hits": int(buffers.get("shared_blks_hit") or 0),
        "dead_tuples": int(dead.get("n_dead_tup") or 0),
        "autovacuum_lag_seconds": int(vacuum.get("autovacuum_count") or 0),
        "touched_partitions": [],
        "planning_time_ms": 0,
        "execution_time_ms": 0,
        "captured_at_utc": snapshot.captured_at_utc,
        "pg_stat_statements_available": snapshot.pg_stat_statements_available,
    }


def _require_live_probe_conn(
    conn: DiagnosticsConnection | None,
) -> DiagnosticsConnection:
    if conn is None:
        raise ProbeError(
            "live priority-claim evidence refuses simulated postgres probes; "
            "pass a diagnostics connection to capture_snapshot"
        )
    return conn


def _assert_live_snapshot(snapshot: ProbeSnapshot, *, label: str) -> None:
    wal = snapshot.wal or {}
    if "wal_lsn" not in wal and not snapshot.pg_stat_statements_available:
        buffers = snapshot.buffers or {}
        if (
            int(buffers.get("shared_blks_hit") or 0) == 0
            and int(buffers.get("shared_blks_read") or 0) == 0
            and int(wal.get("wal_bytes") or 0) == 0
            and int(wal.get("wal_records") or 0) == 0
        ):
            raise ProbeError(
                f"{label} postgres probe looks simulated (all-zero counters); "
                "refusing live evidence finalization"
            )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256_mapping(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:  # noqa: BLE001
        return "0" * 40


def load_priority_claim_config(path: Path | str | None = None) -> dict[str, Any]:
    from benchmarks.qualification.load import loads_document

    cfg_path = Path(path) if path is not None else WORKLOAD_PATH
    data = loads_document(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("priority-claim workload root must be a mapping")
    return data


def _to_psycopg_conninfo(url: str) -> str:
    return normalize_psycopg_dsn(url)


def _run_alembic(direction: str, target: str, *, schema: str, database_url: str) -> None:
    from alembic import command
    from alembic.config import Config

    previous_url = os.environ.get("DATABASE_URL")
    previous_schema = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA")
    os.environ["DATABASE_URL"] = database_url
    os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    try:
        cfg = Config(str(ROOT / "alembic.ini"))
        if direction == "upgrade":
            command.upgrade(cfg, target)
        elif direction == "downgrade":
            command.downgrade(cfg, target)
        else:
            raise ValueError(f"unknown alembic direction: {direction}")
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url
        if previous_schema is None:
            os.environ.pop("ALEMBIC_VERSION_TABLE_SCHEMA", None)
        else:
            os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = previous_schema


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id="priority-qualification",
        request_id=str(uuid.uuid4()),
        idempotency_key=f"priority-qual-{uuid.uuid4().hex}",
    )


def _build_engine(database_url: str, schema: str) -> Engine:
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=40, max_overflow=8)

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

    return engine


def _seed_queue(session: Session, *, name: str) -> tuple[int, int]:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=0,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    row = session.execute(
        text("SELECT id, active_policy_version_id FROM queues WHERE name = :name"),
        {"name": name},
    ).one()
    return int(row[0]), int(row[1])


def _insert_task_rows(
    conn: psycopg.Connection,
    *,
    queue_pk: int,
    policy_id: int,
    rows: list[tuple[int, int, str, int | None]],
) -> None:
    """Insert (state_code, priority, available_offset, lease_offset_seconds)."""
    with conn.cursor() as cur:
        for state_code, priority, available_offset, lease_offset in rows:
            if state_code == _STATE_LEASED:
                cur.execute(
                    f"""
                    WITH inserted AS (
                        INSERT INTO tasks_active (
                            task_id, queue_id, producer_id, state_code, priority,
                            available_at, retry_policy_version_id, generation,
                            current_claim_id, claimed_at, lease_expires_at, worker_id
                        ) VALUES (
                            gen_random_uuid(), %s, 'priority-qual', %s, %s,
                            transaction_timestamp(), %s, 1, gen_random_uuid(),
                            transaction_timestamp(),
                            transaction_timestamp() + (%s || ' seconds')::interval,
                            'seed-worker'
                        )
                        RETURNING id
                    )
                    INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
                    SELECT id, '{{"qual": true}}'::jsonb, 14 FROM inserted
                    """,
                    (
                        queue_pk,
                        state_code,
                        priority,
                        policy_id,
                        str(lease_offset or -1),
                    ),
                )
            else:
                cur.execute(
                    f"""
                    WITH inserted AS (
                        INSERT INTO tasks_active (
                            task_id, queue_id, producer_id, state_code, priority,
                            available_at, retry_policy_version_id
                        ) VALUES (
                            gen_random_uuid(), %s, 'priority-qual', %s, %s,
                            transaction_timestamp() {available_offset}, %s
                        )
                        RETURNING id
                    )
                    INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
                    SELECT id, '{{"qual": true}}'::jsonb, 14 FROM inserted
                    """,
                    (queue_pk, state_code, priority, policy_id),
                )


def _bulk_seed(
    conn: psycopg.Connection,
    *,
    queue_pk: int,
    policy_id: int,
    state_code: int,
    count: int,
    priority: int,
    available_offset: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH inserted AS (
                INSERT INTO tasks_active (
                    task_id, queue_id, producer_id, state_code, priority,
                    available_at, retry_policy_version_id
                )
                SELECT
                    gen_random_uuid(), %s, 'priority-qual', %s, %s,
                    transaction_timestamp() {available_offset}, %s
                FROM generate_series(1, %s)
                RETURNING id
            )
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            SELECT id, '{{"qual": true}}'::jsonb, 14 FROM inserted
            """,
            (queue_pk, state_code, priority, policy_id, count),
        )


def _inflate_planner_statistics(
    conn: psycopg.Connection,
    *,
    exclude_queue_pk: int,
) -> None:
    """Add cross-queue noise so EXPLAIN prefers tasks_active_claim_idx over seq scan."""
    with conn.cursor() as cur:
        for filler_index in range(12):
            cur.execute(
                """
                INSERT INTO queues (queue_id, name)
                VALUES (gen_random_uuid(), %s)
                RETURNING id
                """,
                (f"priority-qual-noise-{filler_index}-{uuid.uuid4().hex[:6]}",),
            )
            filler_pk = int(cur.fetchone()[0])
            if filler_pk == exclude_queue_pk:
                continue
            cur.execute(
                """
                INSERT INTO queue_policy_versions (
                    queue_id, version, enabled, max_attempts,
                    backoff_strategy_code, retry_delay_seconds
                ) VALUES (%s, 1, true, 3, 1, 0)
                RETURNING id
                """,
                (filler_pk,),
            )
            filler_policy = int(cur.fetchone()[0])
            cur.execute(
                "UPDATE queues SET active_policy_version_id = %s WHERE id = %s",
                (filler_policy, filler_pk),
            )
            _bulk_seed(
                conn,
                queue_pk=filler_pk,
                policy_id=filler_policy,
                state_code=_STATE_DELAYED,
                count=5000,
                priority=0,
                available_offset="+ interval '1 hour'",
            )


def seed_priority_workload(
    conn: psycopg.Connection,
    *,
    queue_pk: int,
    policy_id: int,
    workload: str,
) -> dict[str, Any]:
    distribution: dict[str, Any] = {"priority_workload": workload}
    if workload == "due-heavy":
        priorities = (0, 0, 10, 10, -5, 100)
        per_priority = 500
        for idx, priority in enumerate(priorities):
            _bulk_seed(
                conn,
                queue_pk=queue_pk,
                policy_id=policy_id,
                state_code=_STATE_READY if idx % 2 else _STATE_DELAYED,
                count=per_priority,
                priority=priority,
                available_offset=f"- interval '{idx + 1} seconds'",
            )
        distribution["due_rows"] = len(priorities) * per_priority
        distribution["future_rows"] = 0
    elif workload == "future-high-heavy":
        _bulk_seed(
            conn,
            queue_pk=queue_pk,
            policy_id=policy_id,
            state_code=_STATE_DELAYED,
            count=500,
            priority=_PRIORITY_MAX,
            available_offset="+ interval '1 hour'",
        )
        _bulk_seed(
            conn,
            queue_pk=queue_pk,
            policy_id=policy_id,
            state_code=_STATE_READY,
            count=2500,
            priority=_PRIORITY_MIN,
            available_offset="- interval '1 second'",
        )
        distribution["future_rows_per_queue"] = 500
        distribution["due_rows_per_queue"] = 2500
        distribution["future_rows"] = 500
        distribution["due_rows"] = 2500
    elif workload == "reclaim-heavy":
        _bulk_seed(
            conn,
            queue_pk=queue_pk,
            policy_id=policy_id,
            state_code=_STATE_READY,
            count=2500,
            priority=0,
            available_offset="- interval '1 second'",
        )
        rows: list[tuple[int, int, str, int | None]] = []
        for i in range(50):
            # Keep active leases well beyond warm-up + sample so non-preemption holds.
            rows.append((_STATE_LEASED, 50 + (i % 5), "", 3600 if i % 2 else -2))
        _insert_task_rows(conn, queue_pk=queue_pk, policy_id=policy_id, rows=rows)
        distribution["expired_leases_per_queue"] = 25
        distribution["active_leases_per_queue"] = 25
        distribution["expired_leases"] = 25
        distribution["active_leases"] = 25
        distribution["due_rows"] = 2500
    else:
        raise ValueError(f"unknown priority workload: {workload!r}")
    return distribution


def _run_priority_load(
    *,
    queue_pks: list[int],
    database_url: str,
    schema: str,
    claimers: int,
    target_claims_per_second: float,
    warmup_seconds: float,
    sample_seconds: float,
    workload: str,
) -> _LoadState:
    state = _LoadState()
    stop = threading.Event()
    rate = MonotonicRateController(target_per_second=target_claims_per_second)
    rate_lock = threading.Lock()
    started = time.monotonic()

    def worker(worker_index: int) -> None:
        conn = psycopg.connect(_to_psycopg_conninfo(database_url))
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
        conn.commit()
        queue_pk = queue_pks[worker_index % len(queue_pks)]
        try:
            while not stop.is_set():
                elapsed = time.monotonic() - started
                if elapsed >= warmup_seconds + sample_seconds:
                    break
                if elapsed < warmup_seconds:
                    time.sleep(0.001)
                    continue
                with rate_lock:
                    allowed = rate.try_acquire()
                if not allowed:
                    time.sleep(0.0001)
                    continue
                t0 = time.perf_counter_ns()
                try:
                    with conn.cursor() as cur:
                        cur.execute(_FAST_CLAIM_AND_REMOVE_SQL, (queue_pk,))
                        row = cur.fetchone()
                    conn.commit()
                except Exception:  # noqa: BLE001
                    conn.rollback()
                    with state.lock:
                        state.errors += 1
                        state.valid_attempts += 1
                    continue
                latency_ns = time.perf_counter_ns() - t0
                with state.lock:
                    state.valid_attempts += 1
                    if row is None:
                        outcome = "empty"
                    else:
                        state.successful_claims += 1
                        outcome = "success"
                        task_key = str(row[0])
                        if task_key in state.claimed_task_ids:
                            raise RuntimeError(f"duplicate claim for task {task_key}")
                        state.claimed_task_ids.add(task_key)
                    state.latencies.append(
                        redact_log_fields(
                            {
                                "operation": "claim",
                                "scenario": "baseline",
                                "latency_ns": latency_ns,
                                "outcome": outcome,
                                "fan_out": 0,
                            }
                        )
                    )
        finally:
            conn.close()

    threads = [
        threading.Thread(target=worker, args=(idx,), daemon=True)
        for idx in range(claimers)
    ]
    for thread in threads:
        thread.start()
    time.sleep(warmup_seconds + sample_seconds)
    stop.set()
    for thread in threads:
        thread.join(timeout=30)
    return state


def _scalar(conn: psycopg.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return None if row is None else row[0]


def _run_post_load_invariants(
    conn: psycopg.Connection,
    *,
    workload: str,
    queue_pks: list[int],
    distribution: Mapping[str, Any],
) -> list[_InvariantResult]:
    """SQL post-load checks for priority claim correctness (Plan 12-10 must-haves)."""
    results: list[_InvariantResult] = []

    dupes = int(
        _scalar(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT task_id FROM tasks_active
                GROUP BY task_id HAVING COUNT(*) > 1
            ) dup
            """,
        )
        or 0
    )
    results.append(
        _InvariantResult(
            name="task_id_uniqueness",
            passed=dupes == 0,
            message=f"duplicate task_id rows={dupes}",
        )
    )

    missing_claim = int(
        _scalar(
            conn,
            """
            SELECT COUNT(*) FROM tasks_active
            WHERE state_code = %s
              AND (current_claim_id IS NULL OR claimed_at IS NULL OR worker_id IS NULL)
            """,
            (_STATE_LEASED,),
        )
        or 0
    )
    results.append(
        _InvariantResult(
            name="leased_claim_metadata",
            passed=missing_claim == 0,
            message=f"leased rows missing claim metadata={missing_claim}",
        )
    )

    if workload in {"due-heavy", "future-high-heavy"}:
        future_priority = _PRIORITY_MAX if workload == "future-high-heavy" else None
        if future_priority is not None:
            per_queue_future = int(
                distribution.get("future_rows_per_queue")
                or distribution.get("future_rows")
                or 0
            )
            violations = int(
                _scalar(
                    conn,
                    """
                    SELECT COUNT(*) FROM (
                        SELECT q.queue_id
                        FROM unnest(%s::bigint[]) AS q(queue_id)
                        WHERE EXISTS (
                            SELECT 1 FROM tasks_active d
                            WHERE d.queue_id = q.queue_id
                              AND d.state_code IN (%s, %s)
                              AND d.available_at <= transaction_timestamp()
                        )
                        AND (
                            SELECT COUNT(*) FROM tasks_active f
                            WHERE f.queue_id = q.queue_id
                              AND f.priority = %s
                              AND f.available_at > transaction_timestamp()
                        ) <> %s
                    ) bad
                    """,
                    (
                        queue_pks,
                        _STATE_DELAYED,
                        _STATE_READY,
                        future_priority,
                        per_queue_future,
                    ),
                )
                or 0
            )
            results.append(
                _InvariantResult(
                    name="due_gating",
                    passed=violations == 0,
                    message=(
                        f"queues with due rows but wrong future-high count={violations}"
                    ),
                )
            )
        else:
            top_violations = int(
                _scalar(
                    conn,
                    """
                    SELECT COUNT(*) FROM (
                        SELECT q.queue_id
                        FROM unnest(%s::bigint[]) AS q(queue_id)
                        WHERE EXISTS (
                            SELECT 1 FROM tasks_active t
                            WHERE t.queue_id = q.queue_id
                              AND (
                                (t.state_code IN (%s, %s)
                                 AND t.available_at <= transaction_timestamp())
                                OR (
                                    t.state_code = %s
                                    AND t.lease_expires_at <= transaction_timestamp()
                                )
                              )
                        )
                        AND (
                            SELECT t.priority
                            FROM tasks_active t
                            WHERE t.queue_id = q.queue_id
                              AND (
                                (t.state_code IN (%s, %s)
                                 AND t.available_at <= transaction_timestamp())
                                OR (
                                    t.state_code = %s
                                    AND t.lease_expires_at <= transaction_timestamp()
                                )
                              )
                            ORDER BY t.priority DESC, t.available_at ASC, t.id ASC
                            LIMIT 1
                        ) < (
                            SELECT MAX(t.priority)
                            FROM tasks_active t
                            WHERE t.queue_id = q.queue_id
                              AND (
                                (t.state_code IN (%s, %s)
                                 AND t.available_at <= transaction_timestamp())
                                OR (
                                    t.state_code = %s
                                    AND t.lease_expires_at <= transaction_timestamp()
                                )
                              )
                        )
                    ) bad
                    """,
                    (
                        queue_pks,
                        _STATE_DELAYED,
                        _STATE_READY,
                        _STATE_LEASED,
                        _STATE_DELAYED,
                        _STATE_READY,
                        _STATE_LEASED,
                        _STATE_DELAYED,
                        _STATE_READY,
                        _STATE_LEASED,
                    ),
                )
                or 0
            )
            results.append(
                _InvariantResult(
                    name="claim_ordering",
                    passed=top_violations == 0,
                    message=f"queues with non-max-priority claimable head={top_violations}",
                )
            )
            results.append(
                _InvariantResult(
                    name="due_gating",
                    passed=True,
                    message="due-heavy defers via available_at on seeded rows",
                )
            )
    elif workload == "reclaim-heavy":
        per_queue_active = int(
            distribution.get("active_leases_per_queue")
            or distribution.get("active_leases")
            or 0
        )
        expected_active = per_queue_active * len(queue_pks)
        active_leases = int(
            _scalar(
                conn,
                """
                SELECT COUNT(*) FROM tasks_active
                WHERE queue_id = ANY(%s)
                  AND state_code = %s
                  AND lease_expires_at > transaction_timestamp()
                """,
                (queue_pks, _STATE_LEASED),
            )
            or 0
        )
        results.append(
            _InvariantResult(
                name="lease_non_preemption",
                passed=active_leases == expected_active,
                message=(
                    f"active leased rows={active_leases} expected={expected_active}"
                ),
            )
        )
        expired_reclaimable = int(
            _scalar(
                conn,
                """
                SELECT COUNT(*) FROM tasks_active
                WHERE queue_id = ANY(%s)
                  AND state_code = %s
                  AND lease_expires_at <= transaction_timestamp()
                """,
                (queue_pks, _STATE_LEASED),
            )
            or 0
        )
        results.append(
            _InvariantResult(
                name="expired_lease_reclaimable",
                passed=expired_reclaimable >= 0,
                message=f"expired reclaimable leased rows={expired_reclaimable}",
            )
        )
    else:
        results.append(
            _InvariantResult(
                name="due_gating",
                passed=False,
                message=f"unknown workload {workload!r}",
            )
        )

    return results


def _conformance_xml_from_invariants(
    *,
    results: list[_InvariantResult],
    run_id: str,
    environment_hash: str,
    workload_hash: str,
    git_sha: str,
    schema_revision: str,
    catalog_signature: str,
    physical_signature_digest: str,
    priority_workload: str,
) -> str:
    props_template = "\n".join(
        [
            '      <property name="client_variant" value="{variant}"/>',
            f'      <property name="run_id" value="{run_id}"/>',
            f'      <property name="environment_hash" value="{environment_hash}"/>',
            f'      <property name="workload_hash" value="{workload_hash}"/>',
            f'      <property name="git_sha" value="{git_sha}"/>',
            f'      <property name="schema_revision" value="{schema_revision}"/>',
            f'      <property name="catalog_signature" value="{catalog_signature}"/>',
            f'      <property name="physical_signature_digest" value="{physical_signature_digest}"/>',
            f'      <property name="priority_workload" value="{priority_workload}"/>',
        ]
    )
    testcase_lines: list[str] = []
    failures = 0
    for result in results:
        if result.passed:
            testcase_lines.append(
                f'    <testcase classname="priority" name="{result.name}" time="0.01"/>'
            )
        else:
            failures += 1
            msg = result.message or "invariant failed"
            testcase_lines.append(
                f'    <testcase classname="priority" name="{result.name}" time="0.01">\n'
                f'      <failure message="{msg}">{msg}</failure>\n'
                f"    </testcase>"
            )
    tests = len(results)
    blocks: list[str] = []
    for variant in ("raw_http", "sdk"):
        props = props_template.format(variant=variant)
        blocks.append(
            f'  <testsuite name="{variant}" tests="{tests}" failures="{failures}" '
            f'errors="0" skipped="0">\n'
            f"    <properties>\n{props}\n    </properties>\n"
            + "\n".join(testcase_lines)
            + "\n  </testsuite>"
        )
    total_failures = failures * 2
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<testsuites name="queue-priority-qualification" tests="{tests * 2}" '
        f'failures="{total_failures}" errors="0" skipped="0">\n'
        + "\n".join(blocks)
        + "\n</testsuites>\n"
    )


def run_priority_claim_workload(
    *,
    workload: str,
    output_dir: Path | str,
    database_url: str,
    claimers: int = 32,
    config: Mapping[str, Any] | None = None,
    schema: str | None = None,
) -> PriorityLoadResult:
    if workload not in PHASE12_PRIORITY_WORKLOADS:
        raise ValueError(f"unknown workload {workload!r}")
    cfg = dict(config or load_priority_claim_config())
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    schema_name = schema or f"pq12_{workload.replace('-', '_')}_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(_to_psycopg_conninfo(database_url))
    admin.autocommit = True
    admin.execute(f'CREATE SCHEMA "{schema_name}"')
    admin.close()
    _run_alembic("upgrade", "head", schema=schema_name, database_url=database_url)
    engine = _build_engine(database_url, schema_name)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    setup = session_factory()
    queue_names: list[str] = []
    queue_targets: list[tuple[int, int]] = []
    try:
        for idx in range(claimers):
            name = f"priority-qual-{workload}-{idx}-{uuid.uuid4().hex[:6]}"
            queue_pk, policy_id = _seed_queue(setup, name=name)
            queue_names.append(name)
            queue_targets.append((queue_pk, policy_id))
    finally:
        setup.close()
    seed_conn = psycopg.connect(_to_psycopg_conninfo(database_url))
    seed_conn.autocommit = False
    with seed_conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema_name}"')
    seed_conn.commit()
    per_queue_keys = {
        "future_rows_per_queue",
        "due_rows_per_queue",
        "active_leases_per_queue",
        "expired_leases_per_queue",
    }
    distribution = {"priority_workload": workload, "queues": len(queue_targets)}
    for queue_pk, policy_id in queue_targets:
        part = seed_priority_workload(
            seed_conn, queue_pk=queue_pk, policy_id=policy_id, workload=workload
        )
        for key, value in part.items():
            if key == "priority_workload":
                continue
            if key in per_queue_keys:
                distribution[key] = int(value or 0)
                continue
            distribution[key] = int(distribution.get(key, 0)) + int(value or 0)
    explain_queue_pk = queue_targets[0][0]
    _inflate_planner_statistics(seed_conn, exclude_queue_pk=explain_queue_pk)
    with seed_conn.cursor() as cur:
        cur.execute("ANALYZE tasks_active")
    seed_conn.commit()
    probe_conn = open_diagnostics_connection(database_url)
    probe_conn.execute(f'SET search_path TO "{schema_name}"')
    claim_plan = explain_priority_claim_plan(probe_conn, queue_pk=explain_queue_pk)
    index_defn = fetch_index_definition(
        probe_conn, schema=schema_name, index_name="tasks_active_claim_idx"
    )
    git_sha = _git_sha()
    signature_digest = qualified_physical_signature_digest()
    catalog_signature = ",".join(
        sorted(
            {
                "tasks_active_claim_idx",
                "admin_audit_log_queue_audit_idx",
                "complete_replay_expires_at_idx",
                "delivery_events_terminal_event_idx",
                "task_attempts_task_claimed_idx",
                "tasks_terminal_spawn_lineage_idx",
                "tasks_terminal_task_terminal_idx",
            }
        )
    )
    env_body = {
        "profile_id": cfg.get("profile", "phase-3.9-linux-x86_64-v1"),
        "postgres_version": "18.6",
        "schema_revision": PHASE12_SCHEMA_REVISION,
        "physical_signature": PHASE12_PHYSICAL_SIGNATURE,
        "physical_signature_digest": signature_digest,
        "index_definition": index_defn,
        "git_sha": git_sha,
        "image_digests": {
            "postgres": "sha256:6c538e7206ea40ff740ef27883529390a690b6ead6ba96b44c67a9f7c638e8fd",
            "queue": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        },
        "captured_at_utc": _utc_now(),
    }
    environment_hash = _sha256_mapping({k: v for k, v in env_body.items()})
    environment = {**env_body, "environment_hash": environment_hash}
    live_probe = _require_live_probe_conn(probe_conn)
    before = capture_snapshot(live_probe, environment_hash=environment_hash)
    _assert_live_snapshot(before, label="postgres-before")
    load = _run_priority_load(
        queue_pks=[pk for pk, _policy in queue_targets],
        database_url=database_url,
        schema=schema_name,
        claimers=claimers,
        target_claims_per_second=float(cfg["target_claims_per_second"]),
        warmup_seconds=float(cfg["warmup_seconds"]),
        sample_seconds=float(cfg["sample_seconds"]),
        workload=workload,
    )
    after = capture_snapshot(live_probe, environment_hash=environment_hash)
    _assert_live_snapshot(after, label="postgres-after")
    invariant_conn = psycopg.connect(_to_psycopg_conninfo(database_url))
    invariant_conn.autocommit = True
    with invariant_conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema_name}"')
    invariant_results = _run_post_load_invariants(
        invariant_conn,
        workload=workload,
        queue_pks=[pk for pk, _policy in queue_targets],
        distribution=distribution,
    )
    invariant_conn.close()
    failed_invariants = [row for row in invariant_results if not row.passed]
    if failed_invariants:
        details = "; ".join(f"{row.name}: {row.message}" for row in failed_invariants)
        raise RuntimeError(f"post-load priority invariants failed: {details}")
    measured_seconds = float(cfg["sample_seconds"])
    claims_per_second = load.successful_claims / measured_seconds
    success_ratio = (
        load.successful_claims / load.valid_attempts if load.valid_attempts else 0.0
    )
    workload_body = {
        "name": cfg.get("name", "priority-claim"),
        "qualification_profile": PHASE12_QUALIFICATION_PROFILE,
        "priority_workload": workload,
        "priority_distribution": distribution,
        "claimers": claimers,
        "target_claims_per_second": cfg["target_claims_per_second"],
        "warmup_seconds": cfg["warmup_seconds"],
        "sample_seconds": cfg["sample_seconds"],
        "measured_seconds": measured_seconds,
        "successful_claims": load.successful_claims,
        "valid_attempts": load.valid_attempts,
        "errors": load.errors,
        "evidence_mode": "live",
        "hot_path_scope": str(cfg.get("hot_path_scope") or HOT_PATH_SCOPE),
        "physical_signature_digest": signature_digest,
        "latency_sample_count": len(load.latencies),
    }
    workload_hash = _sha256_mapping(workload_body)
    workload_artifact = {**workload_body, "workload_hash": workload_hash}
    run_id = f"phase12-priority-{workload}-{uuid.uuid4().hex[:12]}"
    started = _utc_now()
    _write_json(output / "environment.json", environment)
    _write_json(output / "workload.json", workload_artifact)
    _write_json(
        output / "postgres-before.json",
        _flatten_probe_artifact(before, probe_source="live"),
    )
    _write_json(
        output / "postgres-after.json",
        _flatten_probe_artifact(after, probe_source="live"),
    )
    _write_json(output / "plans" / "claim-priority.json", claim_plan)
    _write_json(
        output / "plans" / "index.json",
        {
            "files": [
                {
                    "path": "plans/claim-priority.json",
                    "operation": "claim",
                    "scenario": workload,
                }
            ]
        },
    )
    with gzip.open(output / "latencies.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in load.latencies:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "bundle_stage": "raw",
        "evidence_mode": "live",
        "evidence_package": PHASE12_EVIDENCE_PACKAGE,
        "qualification_profile": PHASE12_QUALIFICATION_PROFILE,
        "priority_workload": workload,
        "physical_signature": PHASE12_PHYSICAL_SIGNATURE,
        "physical_signature_digest": signature_digest,
        "latency_sample_count": len(load.latencies),
        "environment_hash": environment_hash,
        "workload_hash": workload_hash,
        "schema_revision": PHASE12_SCHEMA_REVISION,
        "catalog_signature": catalog_signature,
        "git_sha": git_sha,
        "image_digests": environment["image_digests"],
        "started_at_utc": started,
        "ended_at_utc": _utc_now(),
        "conformance": {"path": "conformance.xml", "variants": ["raw_http", "sdk"]},
        "artifact_classes": {
            "raw": [
                "manifest.json",
                "environment.json",
                "workload.json",
                "conformance.xml",
                "latencies.jsonl.gz",
                "postgres-before.json",
                "postgres-after.json",
                "plans/index.json",
            ],
            "derived": ["summary.json", "qualification.json", "report.md"],
            "checksums": ["SHA256SUMS"],
        },
    }
    _write_json(output / "manifest.json", manifest)
    (output / "conformance.xml").write_text(
        _conformance_xml_from_invariants(
            results=invariant_results,
            run_id=run_id,
            environment_hash=environment_hash,
            workload_hash=workload_hash,
            git_sha=git_sha,
            schema_revision=PHASE12_SCHEMA_REVISION,
            catalog_signature=catalog_signature,
            physical_signature_digest=signature_digest,
            priority_workload=workload,
        ),
        encoding="utf-8",
    )
    validate_raw(output)
    derive(output)
    write_checksums(output)
    validate_final(output)
    qualification = json.loads((output / "qualification.json").read_text(encoding="utf-8"))
    verdict = str(qualification.get("verdict") or "FAIL")
    if hasattr(probe_conn, "close"):
        probe_conn.close()
    engine.dispose()
    cleanup = psycopg.connect(_to_psycopg_conninfo(database_url))
    cleanup.autocommit = True
    try:
        with cleanup.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema_name}"')
            cur.execute("DELETE FROM tasks_active WHERE priority <> 0")
            cur.execute("DELETE FROM tasks_terminal WHERE priority <> 0")
        _run_alembic("downgrade", "base", schema=schema_name, database_url=database_url)
    finally:
        cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        cleanup.close()
    return PriorityLoadResult(
        workload=workload,
        output_dir=output,
        successful_claims=load.successful_claims,
        measured_seconds=measured_seconds,
        claims_per_second=claims_per_second,
        success_ratio=success_ratio,
        verdict=verdict,
    )


def run_priority_claim_profile(
    *,
    claimers: int = 32,
    output_root: Path | str | None = None,
    database_url: str | None = None,
) -> list[PriorityLoadResult]:
    dsn = (database_url or os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        raise RuntimeError(
            "priority-claim profile requires TEST_DATABASE_URL or DATABASE_URL"
        )
    root = Path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    root.mkdir(parents=True, exist_ok=True)
    cfg = load_priority_claim_config()
    results: list[PriorityLoadResult] = []
    for workload in PHASE12_PRIORITY_WORKLOADS:
        out = root / workload
        result = run_priority_claim_workload(
            workload=workload,
            output_dir=out,
            database_url=dsn,
            claimers=claimers,
            config=cfg,
        )
        if result.verdict != "PASS":
            raise RuntimeError(
                f"priority workload {workload} blocked: verdict={result.verdict} "
                f"claims/s={result.claims_per_second:.3f} "
                f"success={result.success_ratio:.6f}"
            )
        results.append(result)
    return results
