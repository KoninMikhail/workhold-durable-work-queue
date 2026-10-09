"""Real-PostgreSQL proof of hysteretic adaptive enqueue/pressure controls (OPS-07).

Consumes Plan 04-01 typed ``PressureSnapshot`` values directly (never scraped
metrics text). Proves hysteresis, retry hints, hard-ceiling obedience, idempotent
replay-before-throttle, and uninterrupted claim draining under enqueue throttle.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.admission.adaptive import (
    AdaptivePressureConfig,
    AdaptivePressureController,
    OverloadMode,
)
from workhold.admission.enqueue import (
    HARD_INSTANCE_ENQUEUE_RPS_CEILING,
    HARD_QUEUE_ENQUEUE_RPS_CEILING,
    AdaptiveEnqueueConfig,
    AdaptiveEnqueueGate,
)
from workhold.application.claim_service import ClaimService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.health import ReasonCode, check_readiness
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.contracts import IntakeValidationError
from workhold.intake.service import EnqueueService
from workhold.observability.pressure import (
    Freshness,
    PressureSnapshot,
    build_snapshot,
    unavailable_snapshot,
)
from workhold.storage.models import Queue


@pytest.fixture
def sa_engine(migrated_schema: tuple[Any, str]) -> Iterator[Engine]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for tests/integration")
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection: Any, _connection_record: Any) -> None:
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def session_factory(sa_engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


@pytest.fixture
def sa_session(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _admin_meta(*, actor_id: str = "admin-adaptive") -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(session: Session, *, name: str) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=5,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _body(payload: Any | None = None) -> tuple[dict[str, Any], bytes]:
    body: dict[str, Any] = {"payload": payload if payload is not None else {"n": 1}}
    raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return body, raw


def _warn_snapshot(*, mono: int = 1) -> PressureSnapshot:
    return build_snapshot(
        observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        collected_monotonic_ns=mono,
        freshness=Freshness.FRESH,
        pool_wait_seconds=0.06,
        pool_saturation_ratio=0.75,
        wal_value=0.75,
        disk_value=0.2,
        autovacuum_value=0.2,
    )


def _crit_snapshot(*, mono: int = 1, signal: str = "wal") -> PressureSnapshot:
    wal = 0.95 if signal == "wal" else 0.2
    disk = 0.95 if signal == "disk" else 0.2
    vacuum = 0.95 if signal == "autovacuum" else 0.2
    wait = 0.25 if signal == "pool" else 0.01
    sat = 0.95 if signal == "pool" else 0.2
    return build_snapshot(
        observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        collected_monotonic_ns=mono,
        freshness=Freshness.FRESH,
        pool_wait_seconds=wait,
        pool_saturation_ratio=sat,
        wal_value=wal,
        disk_value=disk,
        autovacuum_value=vacuum,
    )


def _clear_below_warn_snapshot(*, mono: int = 1) -> PressureSnapshot:
    """Strictly under clear thresholds (hysteresis), not merely under enter."""
    return build_snapshot(
        observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        collected_monotonic_ns=mono,
        freshness=Freshness.FRESH,
        pool_wait_seconds=0.01,
        pool_saturation_ratio=0.4,
        wal_value=0.4,
        disk_value=0.4,
        autovacuum_value=0.4,
    )


def test_single_noisy_sample_cannot_enter_or_clear_overload() -> None:
    cfg = AdaptivePressureConfig(
        enter_consecutive_samples=2,
        clear_consecutive_samples=2,
        clear_hold_seconds=1.0,
    )
    ctl = AdaptivePressureController(config=cfg, monotonic_clock=lambda: 0.0)

    ctl.observe(_crit_snapshot(mono=1), now_monotonic=0.0)
    assert ctl.mode is OverloadMode.NORMAL

    ctl.observe(_crit_snapshot(mono=2), now_monotonic=0.1)
    assert ctl.mode is OverloadMode.ENQUEUE_THROTTLE

    ctl.observe(_crit_snapshot(mono=3), now_monotonic=0.2)
    ctl.observe(_crit_snapshot(mono=4), now_monotonic=0.3)
    assert ctl.mode is OverloadMode.READINESS_FAILURE

    ctl.observe(_clear_below_warn_snapshot(mono=5), now_monotonic=0.4)
    assert ctl.mode is OverloadMode.READINESS_FAILURE


def test_hysteresis_requires_lower_clear_threshold_and_hold() -> None:
    clock = {"t": 0.0}

    def mono() -> float:
        return clock["t"]

    cfg = AdaptivePressureConfig(
        enter_consecutive_samples=2,
        clear_consecutive_samples=2,
        clear_hold_seconds=2.0,
    )
    ctl = AdaptivePressureController(config=cfg, monotonic_clock=mono)

    ctl.observe(_warn_snapshot(mono=1), now_monotonic=0.0)
    ctl.observe(_warn_snapshot(mono=2), now_monotonic=0.1)
    assert ctl.mode is OverloadMode.WARNING

    mid = build_snapshot(
        observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        collected_monotonic_ns=3,
        freshness=Freshness.FRESH,
        pool_wait_seconds=0.01,
        pool_saturation_ratio=0.6,
        wal_value=0.6,
        disk_value=0.2,
        autovacuum_value=0.2,
    )
    ctl.observe(mid, now_monotonic=0.2)
    ctl.observe(mid, now_monotonic=0.3)
    assert ctl.mode is OverloadMode.WARNING

    clock["t"] = 1.0
    ctl.observe(_clear_below_warn_snapshot(mono=4), now_monotonic=1.0)
    ctl.observe(_clear_below_warn_snapshot(mono=5), now_monotonic=1.1)
    assert ctl.mode is OverloadMode.WARNING

    clock["t"] = 3.5
    ctl.observe(_clear_below_warn_snapshot(mono=6), now_monotonic=3.5)
    ctl.observe(_clear_below_warn_snapshot(mono=7), now_monotonic=3.6)
    assert ctl.mode is OverloadMode.NORMAL


def test_stale_or_unavailable_observations_degrade_conservatively() -> None:
    cfg = AdaptivePressureConfig(
        enter_consecutive_samples=2,
        clear_consecutive_samples=2,
        clear_hold_seconds=0.5,
    )
    ctl = AdaptivePressureController(config=cfg, monotonic_clock=lambda: 0.0)

    stale = build_snapshot(
        observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        collected_monotonic_ns=1,
        freshness=Freshness.STALE,
        pool_wait_seconds=0.01,
        pool_saturation_ratio=0.2,
        wal_value=0.2,
        disk_value=0.2,
        autovacuum_value=0.2,
    )
    ctl.observe(stale, now_monotonic=0.0)
    assert ctl.mode is OverloadMode.NORMAL

    ctl.observe(stale, now_monotonic=0.1)
    assert ctl.mode is OverloadMode.ENQUEUE_THROTTLE

    unavail = unavailable_snapshot(
        observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        collected_monotonic_ns=2,
    )
    ctl.observe(unavail, now_monotonic=0.2)
    ctl.observe(_clear_below_warn_snapshot(mono=3), now_monotonic=0.3)
    ctl.observe(_clear_below_warn_snapshot(mono=4), now_monotonic=10.0)
    assert ctl.mode is OverloadMode.ENQUEUE_THROTTLE


def test_soft_rates_cannot_raise_hard_deployment_ceilings() -> None:
    with pytest.raises(ValueError, match="hard"):
        AdaptiveEnqueueConfig(
            queue_enqueue_rps=HARD_QUEUE_ENQUEUE_RPS_CEILING + 1,
            instance_enqueue_rps=10,
            throttle_queue_enqueue_rps=1,
            throttle_instance_enqueue_rps=1,
        )
    with pytest.raises(ValueError, match="hard"):
        AdaptiveEnqueueConfig(
            queue_enqueue_rps=10,
            instance_enqueue_rps=HARD_INSTANCE_ENQUEUE_RPS_CEILING + 1,
            throttle_queue_enqueue_rps=1,
            throttle_instance_enqueue_rps=1,
        )
    with pytest.raises(ValueError, match="cannot exceed"):
        AdaptiveEnqueueConfig(
            queue_enqueue_rps=10,
            instance_enqueue_rps=20,
            throttle_queue_enqueue_rps=11,
            throttle_instance_enqueue_rps=1,
        )


def test_replay_resolved_before_throttle_and_claims_keep_draining(
    session_factory: sessionmaker[Session],
    sa_session: Session,
    sa_engine: Engine,
) -> None:
    queue_name = f"adaptive.drain.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)

    cfg = AdaptivePressureConfig(
        enter_consecutive_samples=2,
        clear_consecutive_samples=2,
        clear_hold_seconds=30.0,
    )
    ctl = AdaptivePressureController(config=cfg, monotonic_clock=lambda: 0.0)
    gate = AdaptiveEnqueueGate(
        controller=ctl,
        config=AdaptiveEnqueueConfig(
            queue_enqueue_rps=100,
            instance_enqueue_rps=100,
            throttle_queue_enqueue_rps=0.0,
            throttle_instance_enqueue_rps=0.0,
            retry_after_ms=250,
        ),
        monotonic_clock=lambda: 0.0,
    )
    service = EnqueueService(
        session_factory=session_factory,
        adaptive_gate=gate,
    )

    body, body_bytes = _body({"seed": 1})
    key = f"idem-{uuid.uuid4().hex}"
    first = service.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes
    )
    assert first.replayed is False

    ctl.observe(_crit_snapshot(mono=1), now_monotonic=0.0)
    ctl.observe(_crit_snapshot(mono=2), now_monotonic=0.1)
    assert ctl.mode is OverloadMode.ENQUEUE_THROTTLE
    assert ctl.claims_allowed is True
    assert ctl.readiness_ok is True

    replay = service.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes
    )
    assert replay.replayed is True
    assert replay.task_id == first.task_id

    new_body, new_bytes = _body({"seed": 2})
    with pytest.raises(IntakeValidationError) as exc_info:
        service.enqueue(
            producer_id="producer-a",
            queue_name=queue_name,
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            body=new_body,
            body_bytes=new_bytes
        )
    err = exc_info.value
    assert err.code == "resource_exhausted"
    assert err.retryable is True
    assert err.retry_after_ms is not None and err.retry_after_ms > 0
    assert err.details.get("hint") == "enqueue_throttled_pressure"

    claim_service = ClaimService(session_factory=session_factory)
    claimed = claim_service.claim(
        queue_name=queue_name,
        worker_id="worker-adaptive-1",
        lease_seconds=30,
    )
    assert claimed.empty is False
    assert claimed.task_id == first.task_id

    ctl.observe(_crit_snapshot(mono=3), now_monotonic=0.2)
    ctl.observe(_crit_snapshot(mono=4), now_monotonic=0.3)
    assert ctl.mode is OverloadMode.READINESS_FAILURE
    assert ctl.claims_allowed is True
    assert ctl.readiness_ok is False

    with pytest.raises(IntakeValidationError) as ready_exc:
        service.enqueue(
            producer_id="producer-a",
            queue_name=queue_name,
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            body=new_body,
            body_bytes=new_bytes
        )
    assert ready_exc.value.code == "dependency_unavailable"
    assert ready_exc.value.retryable is True
    assert ready_exc.value.details.get("hint") == "readiness_pressure"

    status = check_readiness(sa_engine, pressure_controller=ctl)
    assert status.ok is False
    assert status.reason_code == ReasonCode.OVERLOAD_PRESSURE


def test_warning_then_throttle_progression_emits_bounded_transitions() -> None:
    transitions: list[str] = []
    cfg = AdaptivePressureConfig(
        enter_consecutive_samples=2,
        clear_consecutive_samples=2,
        clear_hold_seconds=1.0,
        on_transition=lambda prev, new, reason: transitions.append(
            f"{prev.value}->{new.value}:{reason}"
        ),
    )
    ctl = AdaptivePressureController(config=cfg, monotonic_clock=lambda: 0.0)

    ctl.observe(_warn_snapshot(mono=1), now_monotonic=0.0)
    ctl.observe(_warn_snapshot(mono=2), now_monotonic=0.1)
    assert ctl.mode is OverloadMode.WARNING

    ctl.observe(_crit_snapshot(mono=3, signal="disk"), now_monotonic=0.2)
    ctl.observe(_crit_snapshot(mono=4, signal="disk"), now_monotonic=0.3)
    assert ctl.mode is OverloadMode.ENQUEUE_THROTTLE

    ctl.observe(_crit_snapshot(mono=5, signal="autovacuum"), now_monotonic=0.4)
    ctl.observe(_crit_snapshot(mono=6, signal="autovacuum"), now_monotonic=0.5)
    assert ctl.mode is OverloadMode.READINESS_FAILURE

    assert any("warning" in item for item in transitions)
    assert any("enqueue_throttle" in item for item in transitions)
    assert any("readiness_failure" in item for item in transitions)
    blob = " ".join(transitions).lower()
    for forbidden in ("select ", "/var/", "task_id", "payload"):
        assert forbidden not in blob
