"""Durable break-glass elevation across replicas (D-10..D-12 / OPS-09 / REC-03).

Elevations are durable in the Queue store and visible to a second
``BulkReplayRateGate`` admit path until TTL, then auto-revert.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import Session, sessionmaker

from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    DomainValidationError,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.operations.break_glass import BreakGlassAck, raise_replay_limit
from queue_service.operations.bulk import BulkReplayRateGate, DurableReplayElevation
from queue_service.storage.models import BreakGlassElevation

pytest_plugins = ["tests.integration.conftest"]


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _require_durable_elevation_surface() -> None:
    """Fail closed until a durable elevation read path exists for admit."""
    gate = BulkReplayRateGate(queue_rps=1.0, instance_rps=10.0)
    if not hasattr(gate, "load_durable_elevation") and not hasattr(
        BulkReplayRateGate, "from_durable_store"
    ):
        pytest.fail("durable elevation surface missing after 14-04 wiring")


@pytest.fixture
def sa_engine(migrated_schema):
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for tests/integration/recovery")
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

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def sa_session(sa_engine) -> Iterator[Session]:
    factory = sessionmaker(bind=sa_engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()


def _seed_queue(session: Session, *, name: str) -> None:
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
            metadata=AdminRequestMetadata(
                actor_id="break-glass-elev",
                request_id=str(uuid.uuid4()),
                idempotency_key=f"seed-{uuid.uuid4().hex}",
            ),
        ),
    )
    session.commit()


def _ack() -> BreakGlassAck:
    return BreakGlassAck(
        reason="incident remediation for replay elevation",
        incident_reference="INC-2026-0921-ELEV",
        risk_acknowledged=True,
    )


def test_raise_replay_limit_visible_to_second_gate_instance(sa_session: Session) -> None:
    """Elevation written via domain must be observed by a fresh gate until TTL."""
    _require_durable_elevation_surface()
    queue_name = _unique("elev.visible")
    _seed_queue(sa_session, name=queue_name)

    writer = BulkReplayRateGate(queue_rps=1.0, instance_rps=100.0)
    result = raise_replay_limit(
        sa_session,
        queue_name=queue_name,
        actor_id="break-glass-elev",
        request_id=str(uuid.uuid4()),
        ack=_ack(),
        rate_gate=writer,
        factor=3.0,
        ttl_seconds=120,
    )
    sa_session.commit()
    assert result.outcome == "raised"
    assert float(result.effective_rps) == pytest.approx(3.0)

    row = sa_session.execute(
        select(BreakGlassElevation).where(
            BreakGlassElevation.queue_name == queue_name
        )
    ).scalar_one()
    assert float(row.factor) == pytest.approx(3.0)

    reader = BulkReplayRateGate(queue_rps=1.0, instance_rps=100.0)
    loaded = reader.load_durable_elevation(sa_session, queue_name)
    assert isinstance(loaded, DurableReplayElevation)
    assert loaded.factor == pytest.approx(3.0)
    # Fresh gate with no temporarily_raise still admits under elevated rate.
    reader.admit(queue_name, units=2.0, session=sa_session)
    bucket = reader._queue_buckets[queue_name]  # noqa: SLF001
    assert bucket.rate_per_second == pytest.approx(3.0)


def test_durable_elevation_reverts_after_ttl_on_second_gate(sa_session: Session) -> None:
    """After TTL (Queue-store time) a second gate must not apply the raised rate."""
    _require_durable_elevation_surface()
    queue_name = _unique("elev.ttl")
    _seed_queue(sa_session, name=queue_name)

    writer = BulkReplayRateGate(queue_rps=1.0, instance_rps=100.0)
    raise_replay_limit(
        sa_session,
        queue_name=queue_name,
        actor_id="break-glass-elev",
        request_id=str(uuid.uuid4()),
        ack=_ack(),
        rate_gate=writer,
        factor=4.0,
        ttl_seconds=60,
    )
    sa_session.commit()

    # Force expiry under Queue-store time (keep expires_at > raised_at CHECK).
    sa_session.execute(
        text(
            """
            UPDATE break_glass_elevations
            SET raised_at = transaction_timestamp() - interval '2 seconds',
                expires_at = transaction_timestamp() - interval '1 second'
            WHERE queue_name = :q
            """
        ),
        {"q": queue_name},
    )
    sa_session.commit()

    reader = BulkReplayRateGate(queue_rps=1.0, instance_rps=100.0)
    assert reader.load_durable_elevation(sa_session, queue_name) is None
    reader.admit(queue_name, units=1.0, session=sa_session)
    bucket = reader._queue_buckets[queue_name]  # noqa: SLF001
    assert bucket.rate_per_second == pytest.approx(1.0)
    remaining = sa_session.execute(
        select(BreakGlassElevation).where(
            BreakGlassElevation.queue_name == queue_name
        )
    ).scalar_one_or_none()
    assert remaining is None


def test_durable_elevation_read_failure_fail_closed_on_admit() -> None:
    """If durable read fails mid-admit, do not silently admit as unlimited (D-11)."""
    _require_durable_elevation_surface()
    gate = BulkReplayRateGate(queue_rps=1.0, instance_rps=100.0)
    broken = MagicMock()
    broken.execute.side_effect = RuntimeError("simulated store failure")

    with pytest.raises(DomainValidationError) as exc_info:
        gate.admit("any-queue", units=1.0, session=broken)
    assert exc_info.value.code == "dependency_unavailable"

    with pytest.raises(DomainValidationError) as exc_info:
        gate.load_durable_elevation(broken, "any-queue")
    assert exc_info.value.code == "dependency_unavailable"
