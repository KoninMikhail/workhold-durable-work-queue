"""Delivery Outbox telemetry, bounded stats, and terminal retention (05-05).

Covers DLVR-02 / OPS-02 / OPS-05: lag & outcome metrics with allowlisted labels,
redacted correlation, bounded delivery stats, 30d published / 90d dead-letter
partition policy, and active-row survival across retention.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from workhold.delivery.models import (
    STATE_DEAD_LETTERED,
    STATE_PENDING,
    STATE_PUBLISHED,
    STATE_PUBLISHING,
)
from workhold.delivery.relay import (
    DeliveryDisposition,
    DeliveryResult,
    DeliveryTransport,
    RelayConfig,
    RelayService,
    TransportReadiness,
)
from workhold.health import DAILY_RANGE_PARENTS
from workhold.infrastructure.postgres.maintenance import run_storage_maintenance
from workhold.observability.metrics import ALLOWED_LABEL_KEYS, KernelMetrics
from workhold.observability.retention import RetentionWindow
from workhold.operations.stats import build_stats_snapshot
from workhold.security.payload_policy import PayloadRetentionPolicy
from workhold.storage.models import DeliveryEventActive

# Under test — RED until modules exist.
from workhold.delivery import telemetry as delivery_telemetry
from workhold.maintenance import delivery_retention

UTC = timezone.utc
RELAY_ID = "relay-obs-01"

FORBIDDEN_METRIC_LABEL_KEYS = frozenset(
    {
        "event_id",
        "task_id",
        "claim_id",
        "claim_token",
        "request_id",
        "relay_principal_id",
        "relay_id",
        "url",
        "payload",
        "destination",
        "endpoint",
    }
)


@pytest.fixture(autouse=True)
def _clean_delivery_tables(session_factory: sessionmaker[Session]) -> Iterator[None]:
    with session_factory() as session:
        with session.begin():
            session.execute(text("DELETE FROM delivery_events_terminal"))
            session.execute(text("DELETE FROM delivery_events_active"))
    yield
    with session_factory() as session:
        with session.begin():
            session.execute(text("DELETE FROM delivery_events_terminal"))
            session.execute(text("DELETE FROM delivery_events_active"))


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for delivery observability tests")
    engine = create_engine(database_url, pool_pre_ping=True)

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

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        engine.dispose()


def _ensure_terminal_day_partition(session: Session, day: date) -> str:
    """Create ``delivery_events_terminal_{YYYYMMDD}`` if missing (literal DDL bounds)."""
    child = f"delivery_events_terminal_{day.strftime('%Y%m%d')}"
    exists = session.execute(
        text("SELECT to_regclass(:name) IS NOT NULL"),
        {"name": child},
    ).scalar_one()
    if exists:
        return child
    bound_from = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    bound_to = bound_from + timedelta(days=1)
    # Partition DDL cannot use bind parameters for the VALUES bounds.
    session.execute(
        text(
            f'CREATE TABLE IF NOT EXISTS "{child}" '
            f"PARTITION OF delivery_events_terminal "
            f"FOR VALUES FROM ('{bound_from.isoformat()}') "
            f"TO ('{bound_to.isoformat()}')"
        )
    )
    return child


def _day_for_age(session: Session, age_days: int) -> date:
    store_today = session.execute(
        text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
    ).scalar_one()
    assert isinstance(store_today, date)
    return store_today - timedelta(days=age_days)


def _default_config(**overrides: Any) -> RelayConfig:
    base = dict(
        lease_seconds=30,
        max_attempts=5,
        backoff_base_seconds=1.0,
        backoff_max_seconds=60.0,
        retry_after_cap_seconds=30.0,
        jitter_ratio=0.0,
        default_probe_seconds=0.01,
    )
    base.update(overrides)
    return RelayConfig(**base)


class _ScriptedTransport:
    def __init__(
        self,
        *,
        readiness: TransportReadiness | None = None,
        results: list[DeliveryResult | Exception] | None = None,
    ) -> None:
        self._readiness = readiness or TransportReadiness(
            accepting=True, reason_code="ok", retry_after_seconds=None
        )
        self._results = list(results or [])
        self.publish_calls = 0

    async def readiness(self) -> TransportReadiness:
        return self._readiness

    async def publish(self, event: Any) -> DeliveryResult:
        self.publish_calls += 1
        if not self._results:
            return DeliveryResult(
                disposition=DeliveryDisposition.ACKNOWLEDGED,
                failure_code=None,
                retry_after_seconds=None,
            )
        item = self._results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _insert_pending(
    session: Session,
    *,
    event_id: UUID | None = None,
    available_at_offset_seconds: float = 0.0,
    delivery_attempt: int = 0,
    generation: int = 0,
    envelope_extra: dict[str, Any] | None = None,
) -> UUID:
    now = session.scalar(select(func.transaction_timestamp()))
    assert now is not None
    eid = event_id or uuid.uuid4()
    envelope: dict[str, Any] = {
        "specversion": "1.0",
        "id": str(eid),
        "source": "urn:test",
        "type": "t.v1",
        "data": {"secret": "should-never-log", "token": "claim-secret"},
    }
    if envelope_extra:
        envelope.update(envelope_extra)
    envelope_json = json.dumps(envelope)
    session.execute(
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
                :now + make_interval(secs => :avail_off), :generation,
                NULL, NULL, NULL,
                NULL, :attempt, NULL,
                :now, :now
            )
            """
        ),
        {
            "eid": eid,
            "sid": uuid.uuid4(),
            "pending": STATE_PENDING,
            "envelope": envelope_json,
            "ebytes": len(envelope_json.encode("utf-8")),
            "now": now,
            "avail_off": available_at_offset_seconds,
            "generation": generation,
            "attempt": delivery_attempt,
        },
    )
    return eid


def _metric_label_keys(metrics: KernelMetrics) -> set[str]:
    keys: set[str] = set()
    for sample in metrics.snapshot():
        keys.update(sample.labels)
    return keys


def _counter(metrics: KernelMetrics, name: str, **labels: str) -> float:
    total = 0.0
    for sample in metrics.snapshot():
        if sample.name != name:
            continue
        if all(sample.labels.get(k) == v for k, v in labels.items()):
            total += sample.value
    return total


def _gauge(metrics: KernelMetrics, name: str, **labels: str) -> float | None:
    for sample in metrics.snapshot():
        if sample.name != name:
            continue
        if all(sample.labels.get(k) == v for k, v in labels.items()):
            return sample.value
    return None


# ---------------------------------------------------------------------------
# Defaults / windows
# ---------------------------------------------------------------------------


def test_delivery_retention_defaults_are_30_published_90_dead_letter() -> None:
    assert delivery_retention.DEFAULT_PUBLISHED_RETENTION_DAYS == 30
    assert delivery_retention.DEFAULT_DEAD_LETTER_RETENTION_DAYS == 90
    days_map = delivery_retention.default_retention_days_by_parent(
        payload_retention_days=90
    )
    assert set(days_map) == set(DAILY_RANGE_PARENTS)
    assert days_map["delivery_events_terminal"] == 90
    assert RetentionWindow.DELIVERY_PUBLISHED.value == "delivery_published"
    assert RetentionWindow.DELIVERY_DEAD_LETTER.value == "delivery_dead_letter"


def test_metric_labels_stay_within_allowlist_and_exclude_ids() -> None:
    metrics = KernelMetrics(process_role="relay")
    tel = delivery_telemetry.DeliveryTelemetry(metrics=metrics)
    tel.record_readiness(result="accepting", reason_code="ok", duration_seconds=0.001)
    tel.record_claim_skip(reason_code="circuit_open", delay_seconds=1.0)
    tel.record_claim(result="success", duration_seconds=0.01, reclaimed=False)
    tel.set_depths(pending=2, publishing=1)
    tel.set_oldest_pending_lag_seconds(12.5)
    tel.record_publish_start()
    tel.record_publish_result(
        result="acknowledged",
        duration_seconds=0.05,
        failure_code=None,
    )
    tel.record_retry(
        failure_code="delivery.retryable",
        delay_source="policy",
        attempt=2,
    )
    tel.record_dead_letter(failure_code="delivery.permanent")
    tel.record_lease_expiry_reclaim()
    tel.record_shutdown(in_flight=0)

    keys = _metric_label_keys(metrics)
    assert keys <= ALLOWED_LABEL_KEYS
    assert not (keys & FORBIDDEN_METRIC_LABEL_KEYS)
    for sample in metrics.snapshot():
        assert "event_id" not in sample.labels
        assert "url" not in sample.labels
        assert sample.labels.keys() <= ALLOWED_LABEL_KEYS


def test_correlation_redacts_payload_token_credentials_and_url_query(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("workhold.delivery.telemetry.test")
    projected = delivery_telemetry.project_delivery_correlation(
        {
            "event_id": str(uuid.uuid4()),
            "source_task_id": str(uuid.uuid4()),
            "generation": 3,
            "operation": "delivery.publish",
            "result": "acknowledged",
            "code": "ok",
            "process_role": "relay",
            "payload": {"secret": "x"},
            "claim_token": "super-secret-token",
            "authorization": "Bearer leak",
            "url": "https://hooks.example/path?token=abc",
            "response_body": '{"ok":true}',
            "credentials": "leak",
        }
    )
    with caplog.at_level(logging.INFO):
        delivery_telemetry.emit_delivery_correlation(
            logger, "delivery.publish", projected
        )

    assert "event_id" in projected
    assert "source_task_id" in projected
    assert "generation" in projected
    assert "payload" not in projected
    assert "claim_token" not in projected
    assert "authorization" not in projected
    assert "url" not in projected
    assert "response_body" not in projected
    assert "credentials" not in projected
    blob = " ".join(r.getMessage() for r in caplog.records)
    assert "super-secret-token" not in blob
    assert "Bearer leak" not in blob
    assert "token=abc" not in blob
    assert "should-never-log" not in blob
    assert '{"ok":true}' not in blob


# ---------------------------------------------------------------------------
# Relay instrumentation points
# ---------------------------------------------------------------------------


def test_relay_instruments_readiness_backpressure_claim_publish_and_outcomes(
    session_factory: sessionmaker[Session],
) -> None:
    metrics = KernelMetrics(process_role="relay")
    tel = delivery_telemetry.DeliveryTelemetry(metrics=metrics)
    cfg = _default_config()

    # 1) Backpressure skip — no DB claim / publish.
    bp = _ScriptedTransport(
        readiness=TransportReadiness(
            accepting=False, reason_code="circuit_open", retry_after_seconds=0.0
        )
    )
    svc_bp = RelayService(
        session_factory=session_factory,
        transport=bp,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel,
        sleep=lambda _s: asyncio.sleep(0),
    )
    out = asyncio.run(svc_bp.process_one())
    assert out.kind == "skipped_backpressure"
    assert bp.publish_calls == 0
    assert _counter(metrics, "queue_operation_total", operation="delivery.claim_skip") >= 1

    # 2) Empty claim.
    empty = _ScriptedTransport()
    svc_empty = RelayService(
        session_factory=session_factory,
        transport=empty,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel,
    )
    out = asyncio.run(svc_empty.process_one())
    assert out.kind == "empty"
    assert empty.publish_calls == 0
    assert _counter(
        metrics, "queue_operation_total", operation="delivery.claim", result="empty"
    ) >= 1

    # 3) Successful claim + ack path with correlation fields.
    with session_factory() as session:
        with session.begin():
            eid = _insert_pending(session, available_at_offset_seconds=-5.0)

    ack = _ScriptedTransport(
        results=[
            DeliveryResult(
                disposition=DeliveryDisposition.ACKNOWLEDGED,
                failure_code=None,
                retry_after_seconds=None,
            )
        ]
    )
    log_records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_records.append(record.getMessage())

    capture = _Capture()
    tel_logger = logging.getLogger("workhold.delivery.telemetry.ack_test")
    tel_logger.handlers.clear()
    tel_logger.addHandler(capture)
    tel_logger.setLevel(logging.INFO)
    tel_logger.propagate = False
    tel_ack = delivery_telemetry.DeliveryTelemetry(metrics=metrics, log=tel_logger)
    svc_ack = RelayService(
        session_factory=session_factory,
        transport=ack,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel_ack,
    )
    out = asyncio.run(svc_ack.process_one())
    assert out.kind == "acknowledged"
    assert _counter(
        metrics,
        "queue_operation_total",
        operation="delivery.publish",
        result="acknowledged",
    ) >= 1
    assert _counter(
        metrics,
        "queue_operation_total",
        operation="delivery.ack",
        result="success",
    ) >= 1
    log_blob = " ".join(log_records)
    assert str(eid) in log_blob
    assert "should-never-log" not in log_blob
    assert "claim-secret" not in log_blob
    tel_logger.removeHandler(capture)

    # Metric cardinality remains bounded.
    keys = _metric_label_keys(metrics)
    assert keys <= ALLOWED_LABEL_KEYS
    assert not (keys & FORBIDDEN_METRIC_LABEL_KEYS)


def test_relay_instruments_retry_delay_source_dead_letter_reclaim_and_exception(
    session_factory: sessionmaker[Session],
) -> None:
    metrics = KernelMetrics(process_role="relay")
    tel = delivery_telemetry.DeliveryTelemetry(metrics=metrics)
    cfg = _default_config(max_attempts=2, backoff_base_seconds=1.0, jitter_ratio=0.0)

    # Retryable with Retry-After later than policy → delay_source=retry_after.
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-1.0)

    retry_transport = _ScriptedTransport(
        results=[
            DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code="http.429",
                retry_after_seconds=25.0,
            )
        ]
    )
    svc = RelayService(
        session_factory=session_factory,
        transport=retry_transport,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel,
        rng=__import__("random").Random(0),
    )
    out = asyncio.run(svc.process_one())
    assert out.kind == "retried"
    assert _counter(metrics, "queue_retry_total") >= 1
    assert (
        _counter(
            metrics,
            "queue_operation_total",
            operation="delivery.retry",
            result="retry_after",
        )
        >= 1
    )

    # Permanent → dead letter.
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-1.0)

    perm = _ScriptedTransport(
        results=[
            DeliveryResult(
                disposition=DeliveryDisposition.PERMANENT,
                failure_code="http.400",
                retry_after_seconds=None,
            )
        ]
    )
    svc2 = RelayService(
        session_factory=session_factory,
        transport=perm,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel,
    )
    out = asyncio.run(svc2.process_one())
    assert out.kind == "dead_lettered"
    assert _counter(metrics, "queue_dead_letter_total") >= 1

    # Publish timeout → uncertain retryable instrumentation.
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-1.0)

    boom = _ScriptedTransport(results=[TimeoutError("timed out")])
    svc3 = RelayService(
        session_factory=session_factory,
        transport=boom,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel,
    )
    out = asyncio.run(svc3.process_one())
    assert out.kind == "retried"
    assert (
        _counter(
            metrics,
            "queue_operation_total",
            operation="delivery.publish",
            result="exception",
        )
        >= 1
    )

    # Lease expiry reclaim: force publishing row with expired lease.
    with session_factory() as session:
        with session.begin():
            eid = _insert_pending(session, available_at_offset_seconds=-1.0)
            now = session.scalar(select(func.transaction_timestamp()))
            assert now is not None
            session.execute(
                text(
                    """
                    UPDATE delivery_events_active
                    SET state_code = :publishing,
                        generation = 1,
                        current_claim_id = :tok,
                        claimed_at = :now - interval '120 seconds',
                        lease_expires_at = :now - interval '60 seconds',
                        relay_principal_id = 'other',
                        delivery_attempt = 1
                    WHERE event_id = :eid
                    """
                ),
                {
                    "publishing": STATE_PUBLISHING,
                    "tok": uuid.uuid4(),
                    "now": now,
                    "eid": eid,
                },
            )

    reclaim_transport = _ScriptedTransport()
    svc4 = RelayService(
        session_factory=session_factory,
        transport=reclaim_transport,
        config=cfg,
        relay_principal_id=RELAY_ID,
        telemetry=tel,
    )
    out = asyncio.run(svc4.process_one())
    assert out.kind == "acknowledged"
    assert _counter(metrics, "queue_lease_expiry_total") >= 1

    tel.record_shutdown(in_flight=0)
    assert (
        _counter(
            metrics,
            "queue_operation_total",
            operation="delivery.shutdown",
            result="clean",
        )
        >= 1
    )


def test_relay_instruments_claim_error_and_fenced_ack_rejection(
    session_factory: sessionmaker[Session],
) -> None:
    """Plan AC: claim error → record_claim(error); fence reject → record_ack(rejected)."""
    from workhold.delivery.repository import DeliveryEventRepository
    from workhold.domain.queue_control import DomainValidationError

    metrics = KernelMetrics(process_role="relay")
    tel = delivery_telemetry.DeliveryTelemetry(metrics=metrics)
    cfg = _default_config()

    # Claim path error: repository claim_next raises → operation result=error.
    class _ClaimBoomRepo(DeliveryEventRepository):
        def claim_next(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("claim_db_failed")

    boom_svc = RelayService(
        session_factory=session_factory,
        transport=_ScriptedTransport(),
        config=cfg,
        relay_principal_id=RELAY_ID,
        repository=_ClaimBoomRepo(),
        telemetry=tel,
    )
    with pytest.raises(RuntimeError, match="claim_db_failed"):
        asyncio.run(boom_svc.process_one())
    assert (
        _counter(
            metrics,
            "queue_operation_total",
            operation="delivery.claim",
            result="error",
        )
        >= 1
    )

    # Fenced ack rejection: acknowledge raises DomainValidationError after publish.
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-1.0)

    class _AckRejectRepo(DeliveryEventRepository):
        def acknowledge(self, *args: Any, **kwargs: Any) -> Any:
            raise DomainValidationError("claim_fence_mismatch", "stale claim token")

    ack_metrics = KernelMetrics(process_role="relay")
    ack_tel = delivery_telemetry.DeliveryTelemetry(metrics=ack_metrics)
    ack_svc = RelayService(
        session_factory=session_factory,
        transport=_ScriptedTransport(
            results=[
                DeliveryResult(
                    disposition=DeliveryDisposition.ACKNOWLEDGED,
                    failure_code=None,
                    retry_after_seconds=None,
                )
            ]
        ),
        config=cfg,
        relay_principal_id=RELAY_ID,
        repository=_AckRejectRepo(),
        telemetry=ack_tel,
    )
    with pytest.raises(DomainValidationError):
        asyncio.run(ack_svc.process_one())
    assert (
        _counter(
            ack_metrics,
            "queue_operation_total",
            operation="delivery.ack",
            result="rejected",
        )
        >= 1
    )


def test_terminal_outcome_counted_once_on_fenced_ack_not_publish_disposition(
    session_factory: sessionmaker[Session],
) -> None:
    """queue_terminal_outcome_total{published} increments once per successful delivery."""
    metrics = KernelMetrics(process_role="relay")
    tel = delivery_telemetry.DeliveryTelemetry(metrics=metrics)
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-1.0)

    svc = RelayService(
        session_factory=session_factory,
        transport=_ScriptedTransport(
            results=[
                DeliveryResult(
                    disposition=DeliveryDisposition.ACKNOWLEDGED,
                    failure_code=None,
                    retry_after_seconds=None,
                )
            ]
        ),
        config=_default_config(),
        relay_principal_id=RELAY_ID,
        telemetry=tel,
    )
    out = asyncio.run(svc.process_one())
    assert out.kind == "acknowledged"
    published = _counter(
        metrics,
        "queue_terminal_outcome_total",
        terminal_outcome="published",
    )
    assert published == 1.0


def test_build_stats_snapshot_reconciles_delivery_projection(
    session_factory: sessionmaker[Session],
) -> None:
    """Production /stats path refreshes depth/lag without a manual reconcile call."""
    metrics = KernelMetrics(process_role="api")
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-20.0)

    with session_factory() as session:
        snapshot = build_stats_snapshot(session, metrics=metrics)

    delivery = snapshot["delivery"]
    assert delivery["availability"] == "available"
    assert delivery["pending_depth"] == 1
    assert delivery["publishing_depth"] == 0
    assert delivery["oldest_pending_lag_seconds"] is not None
    assert delivery["as_of"] is not None
    assert _gauge(metrics, "queue_delivery_depth_pending") == 1.0


def test_published_purge_failure_fails_storage_maintenance_report(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Maintain must not report succeeded after a failed 30d published purge."""
    import workhold.maintenance.delivery_retention as dr
    from workhold.maintenance.delivery_retention import PublishedPurgeResult

    def _failing_purge(connection: Any, **kwargs: Any) -> PublishedPurgeResult:
        del connection, kwargs
        return PublishedPurgeResult(
            deleted=0,
            failures=1,
            failure_code="published_purge_failed",
            last_success_at=None,
            published_policy_days=30,
            store_now=datetime.now(UTC),
        )

    monkeypatch.setattr(dr, "purge_expired_published_events", _failing_purge)

    with session_factory() as session:
        conn = session.connection()
        if conn.in_transaction():
            session.commit()
        report = run_storage_maintenance(
            session.connection(),
            horizon_days=14,
            retention_days_by_parent=dr.default_retention_days_by_parent(
                payload_retention_days=90
            ),
            payload_retention_policy=PayloadRetentionPolicy(retention_days=90),
            registry_purge_batch_size=100,
        )

    assert report.outcome == "failed"
    assert report.error_code == "published_purge_failed"
    assert report.purge_by_registry is not None
    assert "delivery_published" in report.purge_by_registry
    assert report.purge_by_registry["delivery_published"]["failures"] == 1


def test_bounded_stats_include_delivery_depth_lag_as_of_without_history_scan(
    session_factory: sessionmaker[Session],
) -> None:
    metrics = KernelMetrics(process_role="relay")
    tel = delivery_telemetry.DeliveryTelemetry(metrics=metrics)
    with session_factory() as session:
        with session.begin():
            _insert_pending(session, available_at_offset_seconds=-30.0)
            _insert_pending(session, available_at_offset_seconds=-10.0)

    # Reconciliation updates projection counters (not a /stats hot scan).
    with session_factory() as session:
        delivery_telemetry.reconcile_delivery_projection(session, telemetry=tel)

    assert _gauge(metrics, "queue_delivery_depth_pending") == 2.0
    lag = _gauge(metrics, "queue_delivery_oldest_pending_lag_seconds")
    assert lag is not None and lag >= 10.0

    with session_factory() as session:
        # Poison retained history with huge payload JSON — stats must not scan it.
        with session.begin():
            day = _day_for_age(session, 40)
            _ensure_terminal_day_partition(session, day)
            terminal_at = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(
                hours=1
            )
            session.execute(
                text(
                    """
                    INSERT INTO delivery_events_terminal (
                        event_id, source_task_id, ordinal, state_code,
                        envelope, envelope_bytes, terminal_at, created_at,
                        failure_code, failure_detail, delivery_attempt
                    ) VALUES (
                        :eid, :sid, 0, :published,
                        CAST(:envelope AS jsonb), :ebytes,
                        :terminal_at, :terminal_at,
                        NULL, :detail, 1
                    )
                    """
                ),
                {
                    "eid": uuid.uuid4(),
                    "sid": uuid.uuid4(),
                    "published": STATE_PUBLISHED,
                    "envelope": json.dumps(
                        {
                            "specversion": "1.0",
                            "id": "x",
                            "source": "urn:t",
                            "type": "t",
                            "data": {"blob": "x" * 5000},
                        }
                    ),
                    "ebytes": 5200,
                    "detail": "not-scanned",
                    "terminal_at": terminal_at,
                },
            )

        snapshot = build_stats_snapshot(session, metrics=metrics)
        assert "delivery" in snapshot
        delivery = snapshot["delivery"]
        assert delivery["pending_depth"] == 2
        assert delivery["publishing_depth"] == 0
        assert delivery["oldest_pending_lag_seconds"] is not None
        assert delivery["as_of"] is not None
        assert "blob" not in json.dumps(snapshot)
        assert "not-scanned" not in json.dumps(snapshot)


def test_sustained_delivery_lag_alert_predicate() -> None:
    alerts = delivery_telemetry.evaluate_delivery_alerts(
        oldest_pending_lag_seconds=120.0,
        pending_depth=5,
        lag_warn_seconds=60.0,
    )
    assert any(a.kind.value == "delivery_lag_sustained" for a in alerts)
    none = delivery_telemetry.evaluate_delivery_alerts(
        oldest_pending_lag_seconds=10.0,
        pending_depth=5,
        lag_warn_seconds=60.0,
    )
    assert not any(a.kind.value == "delivery_lag_sustained" for a in none)
    empty = delivery_telemetry.evaluate_delivery_alerts(
        oldest_pending_lag_seconds=999.0,
        pending_depth=0,
        lag_warn_seconds=60.0,
    )
    assert not any(a.kind.value == "delivery_lag_sustained" for a in empty)


# ---------------------------------------------------------------------------
# Retention: 30d published / 90d dead-letter; active rows survive
# ---------------------------------------------------------------------------


def test_published_purge_at_30d_keeps_dead_letter_and_active(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        with session.begin():
            active_id = _insert_pending(session)
            pub_old = uuid.uuid4()
            pub_new = uuid.uuid4()
            dl_old = uuid.uuid4()
            for eid, state, age_days in (
                (pub_old, STATE_PUBLISHED, 35),
                (pub_new, STATE_PUBLISHED, 10),
                (dl_old, STATE_DEAD_LETTERED, 35),
            ):
                day = _day_for_age(session, age_days)
                _ensure_terminal_day_partition(session, day)
                terminal_at = datetime.combine(
                    day, datetime.min.time(), tzinfo=UTC
                ) + timedelta(hours=1)
                session.execute(
                    text(
                        """
                        INSERT INTO delivery_events_terminal (
                            event_id, source_task_id, ordinal, state_code,
                            envelope, envelope_bytes, terminal_at, created_at,
                            failure_code, failure_detail, delivery_attempt
                        ) VALUES (
                            :eid, :sid, 0, :state,
                            CAST(:envelope AS jsonb), 20,
                            :terminal_at, :terminal_at,
                            CASE WHEN :state = 11 THEN 'x' ELSE NULL END,
                            NULL, 1
                        )
                        """
                    ),
                    {
                        "eid": eid,
                        "sid": uuid.uuid4(),
                        "state": state,
                        "envelope": json.dumps(
                            {
                                "specversion": "1.0",
                                "id": str(eid),
                                "source": "urn:t",
                                "type": "t",
                            }
                        ),
                        "terminal_at": terminal_at,
                    },
                )

    with session_factory() as session:
        conn = session.connection()
        if conn.in_transaction():
            session.commit()
        report = delivery_retention.purge_expired_published_events(
            session.connection(),
            published_retention_days=30,
        )
        session.commit()

    assert report.deleted >= 1
    assert report.failures == 0
    assert report.last_success_at is not None

    with session_factory() as session:
        remaining = {
            row[0]: row[1]
            for row in session.execute(
                text(
                    "SELECT event_id, state_code FROM delivery_events_terminal"
                )
            )
        }
        assert pub_old not in remaining
        assert remaining[pub_new] == STATE_PUBLISHED
        assert remaining[dl_old] == STATE_DEAD_LETTERED
        active = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == active_id)
        ).scalar_one()
        assert int(active.state_code) == STATE_PENDING


def test_partition_detach_uses_dead_letter_window_and_reports_bounds(
    session_factory: sessionmaker[Session],
) -> None:
    """Fully expired delivery partitions detach at 90d; active work is untouched."""
    with session_factory() as session:
        conn = session.connection()
        store_today = conn.execute(
            text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        ).scalar_one()
        assert isinstance(store_today, date)
        old_day = store_today - timedelta(days=95)
        recent_day = store_today - timedelta(days=10)
        child_old = _ensure_terminal_day_partition(session, old_day)
        child_recent = _ensure_terminal_day_partition(session, recent_day)
        bound_from_old = datetime.combine(old_day, datetime.min.time(), tzinfo=UTC)
        session.commit()

        with session.begin():
            active_id = _insert_pending(session)
            # Dead-letter in old partition (would survive 30d but expire at 90d).
            session.execute(
                text(
                    """
                    INSERT INTO delivery_events_terminal (
                        event_id, source_task_id, ordinal, state_code,
                        envelope, envelope_bytes, terminal_at, created_at,
                        failure_code, failure_detail, delivery_attempt
                    ) VALUES (
                        :eid, :sid, 0, :dl,
                        CAST(:envelope AS jsonb), 20, :terminal_at, :terminal_at,
                        'old', NULL, 1
                    )
                    """
                ),
                {
                    "eid": uuid.uuid4(),
                    "sid": uuid.uuid4(),
                    "dl": STATE_DEAD_LETTERED,
                    "envelope": json.dumps(
                        {
                            "specversion": "1.0",
                            "id": "old",
                            "source": "urn:t",
                            "type": "t",
                        }
                    ),
                    "terminal_at": bound_from_old + timedelta(hours=1),
                },
            )

    with session_factory() as session:
        conn = session.connection()
        if conn.in_transaction():
            session.commit()
        days_map = delivery_retention.default_retention_days_by_parent(
            payload_retention_days=90
        )
        report = run_storage_maintenance(
            session.connection(),
            horizon_days=14,
            retention_days_by_parent=days_map,
            payload_retention_policy=PayloadRetentionPolicy(retention_days=90),
            registry_purge_batch_size=100,
        )
        # Published-row purge + retention report surface.
        purge = delivery_retention.purge_expired_published_events(
            session.connection(),
            published_retention_days=30,
        )
        delivery_report = delivery_retention.build_delivery_retention_report(
            storage_report=report,
            published_purge=purge,
        )

    assert report.outcome == "succeeded"
    assert delivery_report.published_policy_days == 30
    assert delivery_report.dead_letter_policy_days == 90
    assert delivery_report.partitions_dropped >= 0
    assert delivery_report.last_success_at is not None

    with session_factory() as session:
        # Active pending survived.
        active = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == active_id)
        ).scalar_one_or_none()
        assert active is not None

        # Old child should be detached/dropped under 90d policy.
        attached = session.execute(
            text(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_inherits i
                  JOIN pg_class child ON child.oid = i.inhrelid
                  JOIN pg_class parent ON parent.oid = i.inhparent
                  JOIN pg_namespace n ON n.oid = parent.relnamespace
                  WHERE n.nspname = current_schema()
                    AND parent.relname = 'delivery_events_terminal'
                    AND child.relname = :child
                )
                """
            ),
            {"child": child_old},
        ).scalar_one()
        assert attached is False

        # Recent child remains.
        recent_attached = session.execute(
            text(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_inherits i
                  JOIN pg_class child ON child.oid = i.inhrelid
                  JOIN pg_class parent ON parent.oid = i.inhparent
                  JOIN pg_namespace n ON n.oid = parent.relnamespace
                  WHERE n.nspname = current_schema()
                    AND parent.relname = 'delivery_events_terminal'
                    AND child.relname = :child
                )
                """
            ),
            {"child": child_recent},
        ).scalar_one()
        assert recent_attached is True


def test_retention_health_windows_include_delivery_policies() -> None:
    from workhold.infrastructure.postgres.maintenance import StorageMaintenanceReport
    from workhold.observability import retention as retention_obs

    now = datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC)
    report = StorageMaintenanceReport(
        outcome="succeeded",
        premade_through=date(2026, 10, 19),
        retained_from=date(2026, 6, 21),
        last_started_at=now,
        last_succeeded_at=now,
        premake=None,
        retention=None,
        purge=None,
        verification_reason=None,
        error_code=None,
        error_detail=None,
        partitions_created=0,
        partitions_detached=0,
        partitions_dropped=0,
        purge_examined_total=0,
        purge_deleted_total=0,
        purge_by_registry=None,
    )
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=now,
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
        policy_days={
            RetentionWindow.DELIVERY_PUBLISHED: 30,
            RetentionWindow.DELIVERY_DEAD_LETTER: 90,
        },
    )
    by_window = {w.window: w for w in snapshot.windows}
    assert RetentionWindow.DELIVERY_PUBLISHED in by_window
    assert RetentionWindow.DELIVERY_DEAD_LETTER in by_window
    assert by_window[RetentionWindow.DELIVERY_PUBLISHED].policy_days == 30
    assert by_window[RetentionWindow.DELIVERY_DEAD_LETTER].policy_days == 90
    serialized = str(snapshot.as_bounded_dict())
    assert "delivery_events_terminal" not in serialized
    assert "payload" not in serialized
