"""PostgreSQL before/after counters and EXPLAIN JSON for hot statements (QUAL-03).

Uses a dedicated read-only diagnostics role when available. Never records
payloads, idempotency keys or claim tokens (T-039-22 / T-039-23).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol

HOT_OPERATIONS: tuple[str, ...] = ("enqueue", "claim", "heartbeat", "complete")

# Allowlisted counter views — no application payload columns.
COUNTER_KEYS: tuple[str, ...] = (
    "buffers",
    "wal",
    "dead_tuples",
    "autovacuum",
    "captured_at_utc",
)

# Representative hot-path SQL shapes (parameter placeholders only — no literals
# that could embed secrets). Used for EXPLAIN when a live connection is present.
# Exact Phase 12 claim selector shape (Plan 07 / claim_repository._select_claimable_task).
# EXPLAIN must use FORMAT JSON only — no ANALYZE on FOR UPDATE SKIP LOCKED paths.
PRIORITY_CLAIM_SELECTOR_SQL = """
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
FOR UPDATE SKIP LOCKED
""".strip()

HOT_STATEMENT_SQL: dict[str, str] = {
    "enqueue": (
        "INSERT INTO task_active (queue_id, idempotency_hash, available_at) "
        "VALUES ($1::uuid, $2::bytea, now()) RETURNING task_id"
    ),
    "claim": PRIORITY_CLAIM_SELECTOR_SQL,
    "heartbeat": (
        "UPDATE task_active SET lease_until = now() + interval '30 seconds' "
        "WHERE task_id = $1::uuid AND lease_generation = $2::bigint"
    ),
    "complete": (
        "UPDATE task_active SET terminal_state = 'succeeded' "
        "WHERE task_id = $1::uuid AND lease_generation = $2::bigint"
    ),
}


class ProbeError(RuntimeError):
    """PostgreSQL probe failed closed."""


def walk_explain_nodes(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Depth-first walk of PostgreSQL EXPLAIN JSON plan nodes."""
    nodes: list[dict[str, Any]] = []

    def _walk(node: Mapping[str, Any]) -> None:
        nodes.append(dict(node))
        for child in node.get("Plans") or []:
            if isinstance(child, Mapping):
                _walk(child)

    _walk(plan)
    return nodes


def validate_claim_explain_plan(plan: Mapping[str, Any]) -> None:
    """Recursively enforce Plan 07 access-path policy on claim EXPLAIN JSON."""
    nodes = walk_explain_nodes(plan)
    for node in nodes:
        if (
            node.get("Relation Name") == "tasks_active"
            and node.get("Node Type") == "Seq Scan"
        ):
            raise ProbeError(
                "Seq Scan on tasks_active forbidden:\n"
                + json.dumps(dict(plan), indent=2, sort_keys=True)
            )
    uses_idx = any(
        node.get("Index Name") == "tasks_active_claim_idx"
        and node.get("Node Type") in ("Index Scan", "Bitmap Index Scan")
        for node in nodes
    )
    if not uses_idx:
        raise ProbeError(
            "expected tasks_active_claim_idx in EXPLAIN plan:\n"
            + json.dumps(dict(plan), indent=2, sort_keys=True)
        )


def explain_priority_claim_plan(
    conn: DiagnosticsConnection,
    *,
    queue_pk: int,
) -> dict[str, Any]:
    """Capture FORMAT JSON EXPLAIN for the exact Phase 12 claim selector."""
    rows = conn.execute(f"EXPLAIN (FORMAT JSON) {PRIORITY_CLAIM_SELECTOR_SQL}", (queue_pk,))
    if not rows:
        raise ProbeError("priority claim EXPLAIN returned no rows")
    payload = rows[0]
    plan_root: dict[str, Any] | None = None
    for key in ("QUERY PLAN", "query_plan", "explain"):
        if key not in payload:
            continue
        value = payload[key]
        if isinstance(value, str):
            parsed = json.loads(value)
            plan_root = parsed[0] if isinstance(parsed, list) else dict(parsed)
            break
        if isinstance(value, list) and value:
            plan_root = dict(value[0])
            break
        if isinstance(value, Mapping):
            plan_root = dict(value)
            break
    if plan_root is None:
        if "Plan" in payload:
            plan_root = dict(payload)
        else:
            raise ProbeError("unrecognized priority claim EXPLAIN payload")
    if "Plan" in plan_root:
        plan = dict(plan_root["Plan"])
        envelope = dict(plan_root)
    else:
        plan = dict(plan_root)
        envelope = {"Plan": plan}
    validate_claim_explain_plan(plan)
    return envelope


def fetch_index_definition(
    conn: DiagnosticsConnection,
    *,
    schema: str,
    index_name: str,
) -> str:
    rows = conn.execute(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND indexname = %s
        """,
        (schema, index_name),
    )
    if not rows:
        raise ProbeError(f"missing index {index_name!r} in schema {schema!r}")
    return str(rows[0]["indexdef"])


class DiagnosticsConnection(Protocol):
    """Minimal read-only connection protocol for probes."""

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[Mapping[str, Any]]:
        ...


@dataclass(slots=True)
class ProbeSnapshot:
    buffers: dict[str, Any]
    wal: dict[str, Any]
    dead_tuples: dict[str, Any]
    autovacuum: dict[str, Any]
    captured_at_utc: str
    pg_stat_statements_available: bool = False
    environment_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "buffers": self.buffers,
            "wal": self.wal,
            "dead_tuples": self.dead_tuples,
            "autovacuum": self.autovacuum,
            "captured_at_utc": self.captured_at_utc,
            "pg_stat_statements_available": self.pg_stat_statements_available,
        }
        if self.environment_hash is not None:
            payload["environment_hash"] = self.environment_hash
        return payload


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def simulate_snapshot(*, environment_hash: str | None = None) -> ProbeSnapshot:
    """Deterministic offline snapshot for unit tests / dry-run backends."""
    return ProbeSnapshot(
        buffers={"shared_blks_hit": 0, "shared_blks_read": 0, "blk_read_time": 0.0},
        wal={"wal_bytes": 0, "wal_records": 0},
        dead_tuples={"n_dead_tup": 0, "n_live_tup": 0},
        autovacuum={"last_autovacuum": None, "autovacuum_count": 0},
        captured_at_utc=_utc_now(),
        pg_stat_statements_available=False,
        environment_hash=environment_hash,
    )


def capture_snapshot(
    conn: DiagnosticsConnection | None = None,
    *,
    environment_hash: str | None = None,
    now: Callable[[], str] | None = None,
) -> ProbeSnapshot:
    """Capture allowlisted PostgreSQL counters.

    When ``conn`` is None, returns a simulated snapshot (smoke unit path).
    Prefer core statistics views; ``pg_stat_statements`` is optional.
    """
    stamp = (now or _utc_now)()
    if conn is None:
        snap = simulate_snapshot(environment_hash=environment_hash)
        snap.captured_at_utc = stamp
        return snap

    buffers = _first_row(
        conn,
        """
        SELECT
          COALESCE(sum(blks_hit), 0) AS shared_blks_hit,
          COALESCE(sum(blks_read), 0) AS shared_blks_read,
          COALESCE(sum(blk_read_time), 0) AS blk_read_time
        FROM pg_stat_database
        """,
    )
    wal = _first_row(
        conn,
        """
        SELECT
          COALESCE(pg_current_wal_lsn()::text, '0/0') AS wal_lsn
        """,
    )
    dead = _first_row(
        conn,
        """
        SELECT
          COALESCE(sum(n_dead_tup), 0) AS n_dead_tup,
          COALESCE(sum(n_live_tup), 0) AS n_live_tup
        FROM pg_stat_user_tables
        """,
    )
    vacuum = _first_row(
        conn,
        """
        SELECT
          max(last_autovacuum) AS last_autovacuum,
          COALESCE(sum(autovacuum_count), 0) AS autovacuum_count
        FROM pg_stat_user_tables
        """,
    )
    pgss = False
    try:
        rows = conn.execute(
            "SELECT 1 AS ok FROM pg_extension WHERE extname = 'pg_stat_statements'"
        )
        pgss = bool(rows)
    except Exception:
        pgss = False

    return ProbeSnapshot(
        buffers=dict(buffers),
        wal=dict(wal),
        dead_tuples=dict(dead),
        autovacuum={
            "last_autovacuum": (
                None
                if vacuum.get("last_autovacuum") is None
                else str(vacuum.get("last_autovacuum"))
            ),
            "autovacuum_count": vacuum.get("autovacuum_count", 0),
        },
        captured_at_utc=stamp,
        pg_stat_statements_available=pgss,
        environment_hash=environment_hash,
    )


def explain_hot_statement(
    operation: str,
    conn: DiagnosticsConnection | None = None,
) -> dict[str, Any]:
    """Return EXPLAIN JSON for a hot operation.

    Live path uses ``EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)``.
    Simulated path returns a minimal plan document suitable for indexing.
    """
    if operation not in HOT_OPERATIONS:
        raise ProbeError(f"unknown hot operation: {operation!r}")
    sql = HOT_STATEMENT_SQL[operation]
    if conn is None:
        return {
            "Plan": {
                "Node Type": "Simulated",
                "Operation": operation,
                "StatementShape": operation,
            },
            "operation": operation,
        }
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) {sql}"
    # Parameters are never bound with real payloads/tokens — EXPLAIN of the
    # statement shape only; callers must not interpolate secrets into SQL.
    rows = conn.execute(explain_sql)
    if not rows:
        raise ProbeError(f"EXPLAIN returned no rows for {operation}")
    payload = rows[0]
    # Drivers may return the JSON plan under different keys.
    for key in ("QUERY PLAN", "query_plan", "explain"):
        if key in payload:
            value = payload[key]
            if isinstance(value, str):
                return json.loads(value)[0] if value.startswith("[") else json.loads(value)
            if isinstance(value, list) and value:
                return dict(value[0])
            if isinstance(value, Mapping):
                return dict(value)
    if "Plan" in payload:
        return dict(payload)
    raise ProbeError(f"unrecognized EXPLAIN payload for {operation}")


def write_explain_files(
    plans_dir: Any,
    *,
    conn: DiagnosticsConnection | None = None,
    cell_id: str = "default",
) -> list[dict[str, str]]:
    """Write one EXPLAIN JSON per hot operation; return index entries.

    ``plans_dir`` is the bundle ``plans/`` directory.
    """
    from pathlib import Path

    root = Path(plans_dir)
    root.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, str]] = []
    for operation in HOT_OPERATIONS:
        path = root / cell_id / f"{operation}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        plan = explain_hot_statement(operation, conn)
        path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        entries.append(
            {"path": f"plans/{cell_id}/{operation}.json", "operation": operation}
        )
    return entries


def _json_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        return value
    # psycopg may return Decimal for aggregate counters.
    try:
        if value == int(value):
            return int(value)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def _first_row(conn: DiagnosticsConnection, sql: str) -> dict[str, Any]:
    rows = conn.execute(sql)
    if not rows:
        return {}
    return {key: _json_scalar(val) for key, val in dict(rows[0]).items()}


def normalize_psycopg_dsn(dsn: str) -> str:
    """Accept SQLAlchemy ``postgresql+psycopg://`` URLs and bare postgres DSNs."""
    text = dsn.strip()
    if text.startswith("postgresql+psycopg://"):
        return "postgresql://" + text[len("postgresql+psycopg://") :]
    return text


@dataclass(slots=True)
class PsycopgDiagnosticsConnection:
    """Thin ``DiagnosticsConnection`` adapter over a psycopg connection."""

    _conn: Any

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[Mapping[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()


def open_diagnostics_connection(dsn: str) -> PsycopgDiagnosticsConnection:
    """Open a real diagnostics connection; fail closed on connect errors."""
    import psycopg

    normalized = normalize_psycopg_dsn(dsn)
    try:
        conn = psycopg.connect(normalized, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001 — fail closed for live probes
        raise ProbeError(f"diagnostics connection failed: {exc}") from exc
    return PsycopgDiagnosticsConnection(_conn=conn)


def resolve_probe_connection(
    backend: str,
    *,
    injected: DiagnosticsConnection | None = None,
    dsn: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> DiagnosticsConnection | None:
    """Return a live diagnostics connection or ``None`` for simulated backends.

    Live path fails closed when neither an injected connection nor a DSN
    (``QUEUE_DIAGNOSTICS_DSN`` / ``DATABASE_URL``) is available.
    """
    if backend == "simulated":
        return None
    if backend != "live":
        raise ProbeError(f"unknown probe backend: {backend!r}")
    if injected is not None:
        return injected
    env = environ if environ is not None else os.environ
    resolved = (
        (dsn or env.get("QUEUE_DIAGNOSTICS_DSN") or env.get("DATABASE_URL") or "")
        .strip()
    )
    if not resolved:
        raise ProbeError(
            "live backend requires QUEUE_DIAGNOSTICS_DSN or DATABASE_URL "
            "for PostgreSQL probes"
        )
    return open_diagnostics_connection(resolved)
