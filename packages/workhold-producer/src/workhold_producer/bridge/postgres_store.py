"""Reference PostgreSQL adapter for the app-owned outbox store (BRDG-02).

Accepts an application-supplied PEP 249/DB-API connection factory. Does not
import a database driver, issue DDL, ship app-table revisions, or require an
application database for base ``workhold-producer`` users.

Install the optional ``bridge-postgres`` extra when the application wants the
tested ``psycopg`` driver available for its injected connection factory:
``pip install workhold-producer[bridge-postgres]``.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from typing import Any

from workhold_producer.bridge.store import (
    AppStoreHealthSnapshot,
    BoundedPendingDepth,
    OldestPendingSnapshot,
    OutboxIntent,
)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_STATE_PENDING = "pending"
_STATE_LEASED = "leased"
_STATE_DELIVERED = "delivered"
_STATE_RETRYABLE = "retryable_failure"
_STATE_TERMINAL = "terminal_operator_action"

_REQUIRED_COLUMNS = frozenset(
    {
        "source_namespace",
        "source_row_id",
        "schema_version",
        "target_queue",
        "enqueue_request",
        "created_at",
        "traceparent",
        "tracestate",
        "extensions",
        "state",
        "ownership_token",
        "generation",
        "lease_expires_at",
        "available_at",
        "updated_at",
        "queue_task_id",
        "last_failure_code",
    }
)

ConnectionFactory = Callable[[], Any]


def _validate_identifier(name: str, *, label: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.fullmatch(name):
        raise ValueError(f"unsafe SQL identifier for {label}: {name!r}")
    return name


def _q(name: str) -> str:
    """Quote a validated identifier for PostgreSQL."""
    return f'"{name}"'


@dataclass(frozen=True, slots=True)
class PostgresOutboxMapping:
    """Application-supplied schema/table/column names for the reference adapter.

    Defaults match a conventional app-owned outbox layout. Applications control
    the physical names; this mapping only validates and quotes them safely.
    """

    table: str
    schema: str | None = None
    source_namespace: str = "source_namespace"
    source_row_id: str = "source_row_id"
    schema_version: str = "schema_version"
    target_queue: str = "target_queue"
    enqueue_request: str = "enqueue_request"
    created_at: str = "created_at"
    traceparent: str = "traceparent"
    tracestate: str = "tracestate"
    extensions: str = "extensions"
    state: str = "state"
    ownership_token: str = "ownership_token"
    generation: str = "generation"
    lease_expires_at: str = "lease_expires_at"
    available_at: str = "available_at"
    updated_at: str = "updated_at"
    queue_task_id: str = "queue_task_id"
    last_failure_code: str = "last_failure_code"

    def __post_init__(self) -> None:
        _validate_identifier(self.table, label="table")
        if self.schema is not None:
            _validate_identifier(self.schema, label="schema")
        for label, value in (
            ("source_namespace", self.source_namespace),
            ("source_row_id", self.source_row_id),
            ("schema_version", self.schema_version),
            ("target_queue", self.target_queue),
            ("enqueue_request", self.enqueue_request),
            ("created_at", self.created_at),
            ("traceparent", self.traceparent),
            ("tracestate", self.tracestate),
            ("extensions", self.extensions),
            ("state", self.state),
            ("ownership_token", self.ownership_token),
            ("generation", self.generation),
            ("lease_expires_at", self.lease_expires_at),
            ("available_at", self.available_at),
            ("updated_at", self.updated_at),
            ("queue_task_id", self.queue_task_id),
            ("last_failure_code", self.last_failure_code),
        ):
            _validate_identifier(value, label=label)


class _Tx:
    """Short-lived connection + transaction context (commit on success)."""

    def __init__(self, factory: ConnectionFactory) -> None:
        self._factory = factory
        self.conn: Any | None = None

    def __enter__(self) -> Any:
        self.conn = self._factory()
        return self.conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self.conn is not None
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            self.conn.close()


class PostgresOutboxStore:
    """Reference ``OutboxStore`` over an injected PEP 249 connection factory."""

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        mapping: PostgresOutboxMapping,
        validate_on_init: bool = True,
        max_claim_limit: int = 100,
    ) -> None:
        if max_claim_limit < 1:
            raise ValueError("max_claim_limit must be >= 1")
        self._factory = connection_factory
        self._m = mapping
        self._max_claim_limit = max_claim_limit
        self._rel = self._relation_sql()
        if validate_on_init:
            self._validate_capabilities()

    def _relation_sql(self) -> str:
        m = self._m
        if m.schema is None:
            return _q(m.table)
        return f"{_q(m.schema)}.{_q(m.table)}"

    def _validate_capabilities(self) -> None:
        m = self._m
        schema_name = m.schema if m.schema is not None else "public"
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """
        with _Tx(self._factory) as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (schema_name, m.table))
                rows = cur.fetchall()
            finally:
                cur.close()
        present = {str(r[0]) for r in rows}
        # Map logical required names to physical column names.
        physical_required = {
            m.source_namespace,
            m.source_row_id,
            m.schema_version,
            m.target_queue,
            m.enqueue_request,
            m.created_at,
            m.traceparent,
            m.tracestate,
            m.extensions,
            m.state,
            m.ownership_token,
            m.generation,
            m.lease_expires_at,
            m.available_at,
            m.updated_at,
            m.queue_task_id,
            m.last_failure_code,
        }
        missing = sorted(physical_required - present)
        if missing:
            raise ValueError(
                "outbox table missing required columns: " + ", ".join(missing)
            )
        # Keep the logical set documented for reviewers / future validators.
        _ = _REQUIRED_COLUMNS

    def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
        if not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive int")
        if limit > self._max_claim_limit:
            raise ValueError(
                f"limit {limit} exceeds max_claim_limit {self._max_claim_limit}"
            )
        if not isinstance(lease_seconds, int) or lease_seconds < 1:
            raise ValueError("lease_seconds must be a positive int")

        m = self._m
        rel = self._rel
        # Select claimable rows under app-DB time, SKIP LOCKED for replicas.
        select_sql = f"""
            SELECT
                {_q(m.source_namespace)},
                {_q(m.source_row_id)},
                {_q(m.schema_version)},
                {_q(m.target_queue)},
                {_q(m.enqueue_request)},
                {_q(m.created_at)},
                {_q(m.traceparent)},
                {_q(m.tracestate)},
                {_q(m.extensions)},
                {_q(m.generation)}
            FROM {rel}
            WHERE (
                (
                    {_q(m.state)} IN (%s, %s)
                    AND {_q(m.available_at)} <= now()
                )
                OR (
                    {_q(m.state)} = %s
                    AND {_q(m.lease_expires_at)} IS NOT NULL
                    AND {_q(m.lease_expires_at)} <= now()
                )
            )
            ORDER BY {_q(m.available_at)}, {_q(m.source_namespace)}, {_q(m.source_row_id)}
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """
        update_sql = f"""
            UPDATE {rel}
            SET
                {_q(m.state)} = %s,
                {_q(m.ownership_token)} = %s,
                {_q(m.generation)} = %s,
                {_q(m.lease_expires_at)} = now() + make_interval(secs => %s),
                {_q(m.updated_at)} = now()
            WHERE {_q(m.source_namespace)} = %s
              AND {_q(m.source_row_id)} = %s
            RETURNING {_q(m.lease_expires_at)}
            """

        claimed: list[OutboxIntent] = []
        with _Tx(self._factory) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    select_sql,
                    (
                        _STATE_PENDING,
                        _STATE_RETRYABLE,
                        _STATE_LEASED,
                        limit,
                    ),
                )
                rows = cur.fetchall()
                for row in rows:
                    (
                        source_namespace,
                        source_row_id,
                        schema_version,
                        target_queue,
                        enqueue_request,
                        created_at,
                        traceparent,
                        tracestate,
                        extensions,
                        generation,
                    ) = row
                    token = str(uuid.uuid4())
                    new_generation = int(generation) + 1
                    cur.execute(
                        update_sql,
                        (
                            _STATE_LEASED,
                            token,
                            new_generation,
                            float(lease_seconds),
                            source_namespace,
                            source_row_id,
                        ),
                    )
                    lease_row = cur.fetchone()
                    if lease_row is None:
                        continue
                    lease_expires_at = lease_row[0]
                    body = _as_mapping(enqueue_request)
                    ext = None if extensions is None else _as_mapping(extensions)
                    claimed.append(
                        OutboxIntent(
                            source_namespace=str(source_namespace),
                            source_row_id=str(source_row_id),
                            schema_version=int(schema_version),
                            target_queue=str(target_queue),
                            enqueue_request=body,
                            created_at=_ensure_aware(created_at),
                            ownership_token=token,
                            generation=new_generation,
                            lease_expires_at=_ensure_aware(lease_expires_at),
                            traceparent=None if traceparent is None else str(traceparent),
                            tracestate=None if tracestate is None else str(tracestate),
                            extensions=ext,
                        )
                    )
            finally:
                cur.close()
        return tuple(claimed)

    def mark_delivered(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        queue_task_id: str | None = None,
    ) -> bool:
        return self._fenced_update(
            source_namespace=source_namespace,
            source_row_id=source_row_id,
            ownership_token=ownership_token,
            set_sql_extra=f"""
                {_q(self._m.state)} = %s,
                {_q(self._m.queue_task_id)} = %s,
                {_q(self._m.ownership_token)} = NULL,
                {_q(self._m.lease_expires_at)} = NULL,
            """,
            extra_params=(_STATE_DELIVERED, queue_task_id),
        )

    def schedule_retry(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        available_at_delay_seconds: float,
        failure_code: str | None = None,
    ) -> bool:
        if available_at_delay_seconds < 0:
            raise ValueError("available_at_delay_seconds must be >= 0")
        return self._fenced_update(
            source_namespace=source_namespace,
            source_row_id=source_row_id,
            ownership_token=ownership_token,
            set_sql_extra=f"""
                {_q(self._m.state)} = %s,
                {_q(self._m.ownership_token)} = NULL,
                {_q(self._m.lease_expires_at)} = NULL,
                {_q(self._m.available_at)} = now() + make_interval(secs => %s),
                {_q(self._m.last_failure_code)} = %s,
            """,
            extra_params=(
                _STATE_RETRYABLE,
                float(available_at_delay_seconds),
                failure_code,
            ),
        )

    def mark_terminal_operator_action(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        reason: str,
    ) -> bool:
        return self._fenced_update(
            source_namespace=source_namespace,
            source_row_id=source_row_id,
            ownership_token=ownership_token,
            set_sql_extra=f"""
                {_q(self._m.state)} = %s,
                {_q(self._m.ownership_token)} = NULL,
                {_q(self._m.lease_expires_at)} = NULL,
                {_q(self._m.last_failure_code)} = %s,
            """,
            extra_params=(_STATE_TERMINAL, reason),
        )

    def _fenced_update(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        set_sql_extra: str,
        extra_params: tuple[Any, ...],
    ) -> bool:
        m = self._m
        sql = f"""
            UPDATE {self._rel}
            SET
                {set_sql_extra}
                {_q(m.updated_at)} = now()
            WHERE {_q(m.source_namespace)} = %s
              AND {_q(m.source_row_id)} = %s
              AND {_q(m.state)} = %s
              AND {_q(m.ownership_token)} = %s
              AND {_q(m.lease_expires_at)} IS NOT NULL
              AND {_q(m.lease_expires_at)} > now()
            """
        params = (*extra_params, source_namespace, source_row_id, _STATE_LEASED, ownership_token)
        with _Tx(self._factory) as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                return cur.rowcount == 1
            finally:
                cur.close()

    def get_pending_depth(self, depth_cap: int) -> BoundedPendingDepth:
        cap = _require_positive_depth_cap(depth_cap)
        m = self._m
        # Scan at most depth_cap + 1 qualifying rows — never unbounded COUNT(*).
        sql = f"""
            SELECT count(*) AS c, now() AS as_of
            FROM (
                SELECT 1
                FROM {self._rel}
                WHERE {_q(m.state)} <> %s
                LIMIT %s
            ) capped
            """
        with _Tx(self._factory) as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (_STATE_DELIVERED, cap + 1))
                row = cur.fetchone()
            finally:
                cur.close()
        assert row is not None
        raw_count = int(row[0])
        as_of = _ensure_aware(row[1])
        capped = raw_count > cap
        return BoundedPendingDepth(
            count=cap if capped else raw_count,
            depth_cap=cap,
            capped=capped,
            as_of=as_of,
        )

    def get_oldest_pending_created_at(self) -> OldestPendingSnapshot:
        m = self._m
        sql = f"""
            SELECT min({_q(m.created_at)}) AS oldest, now() AS as_of
            FROM {self._rel}
            WHERE {_q(m.state)} <> %s
            """
        with _Tx(self._factory) as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (_STATE_DELIVERED,))
                row = cur.fetchone()
            finally:
                cur.close()
        assert row is not None
        oldest, as_of = row[0], row[1]
        return OldestPendingSnapshot(
            created_at=None if oldest is None else _ensure_aware(oldest),
            as_of=_ensure_aware(as_of),
        )

    def get_health_snapshot(self, depth_cap: int) -> AppStoreHealthSnapshot:
        cap = _require_positive_depth_cap(depth_cap)
        try:
            depth = self.get_pending_depth(cap)
            oldest = self.get_oldest_pending_created_at()
        except Exception:
            return AppStoreHealthSnapshot(
                as_of=datetime.now(timezone.utc),
                connected=False,
                query_ok=False,
                pending_count=0,
                pending_capped=False,
                oldest_pending_created_at=None,
            )
        return AppStoreHealthSnapshot(
            as_of=depth.as_of,
            connected=True,
            query_ok=True,
            pending_count=depth.count,
            pending_capped=depth.capped,
            oldest_pending_created_at=oldest.created_at,
        )


def _require_positive_depth_cap(depth_cap: int) -> int:
    if not isinstance(depth_cap, int) or depth_cap < 1:
        raise ValueError("depth_cap must be a positive int")
    return depth_cap


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, Mapping):
            raise TypeError("enqueue_request JSON must be an object")
        return dict(parsed)
    raise TypeError(f"unsupported enqueue_request type: {type(value)!r}")


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
