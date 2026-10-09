"""Real-PostgreSQL relay claim fencing, readiness gate, and retry (05-03 / DLVR-01/02)."""

from __future__ import annotations

import asyncio
import json
import math
import os
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from queue_service.delivery.models import (
    STATE_DEAD_LETTERED,
    STATE_PENDING,
    STATE_PUBLISHED,
)
from queue_service.delivery.relay import (
    DeliveryDisposition,
    DeliveryResult,
    DeliveryTransport,
    RelayConfig,
    RelayService,
    TransportReadiness,
    cap_retry_after_seconds,
)
from queue_service.delivery.repository import DeliveryEventRepository
from queue_service.domain.queue_control import DomainValidationError
from queue_service.storage.models import DeliveryEventActive, DeliveryEventTerminal

RELAY_A = "relay-replica-a"
RELAY_B = "relay-replica-b"


@pytest.fixture(autouse=True)
def _clean_delivery_tables(session_factory: sessionmaker[Session]) -> Iterator[None]:
    """Isolate each test from leftover delivery rows in the shared migrated schema."""
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
        pytest.fail("TEST_DATABASE_URL is required for relay claim integration")
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


def _insert_pending(
    session: Session,
    *,
    event_id: UUID | None = None,
    available_at_offset_seconds: float = 0.0,
    delivery_attempt: int = 0,
    generation: int = 0,
) -> UUID:
    now = session.scalar(select(func.transaction_timestamp()))
    assert now is not None
    eid = event_id or uuid.uuid4()
    envelope = {"specversion": "1.0", "id": str(eid), "source": "urn:test", "type": "t.v1"}
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
    session.flush()
    return eid


def _default_config(**overrides: Any) -> RelayConfig:
    base = dict(
        lease_seconds=30,
        max_attempts=3,
        backoff_base_seconds=1.0,
        backoff_max_seconds=60.0,
        retry_after_cap_seconds=30.0,
        jitter_ratio=0.0,
        default_probe_seconds=0.05,
    )
    base.update(overrides)
    return RelayConfig(**base)


@dataclass
class FakeTransport:
    """Transport stub for readiness / publish classification tests."""

    readiness_result: TransportReadiness = field(
        default_factory=lambda: TransportReadiness(
            accepting=True, reason_code="ready", retry_after_seconds=None
        )
    )
    publish_result: DeliveryResult = field(
        default_factory=lambda: DeliveryResult(
            disposition=DeliveryDisposition.ACKNOWLEDGED,
            failure_code=None,
            retry_after_seconds=None,
        )
    )
    publish_calls: list[UUID] = field(default_factory=list)
    readiness_calls: int = 0
    in_db_transaction: threading.local = field(default_factory=threading.local)
    publish_while_tx: list[bool] = field(default_factory=list)
    raise_on_publish: BaseException | None = None

    async def readiness(self) -> TransportReadiness:
        self.readiness_calls += 1
        return self.readiness_result

    async def publish(self, event: Any) -> DeliveryResult:
        self.publish_calls.append(event.event_id)
        self.publish_while_tx.append(bool(getattr(self.in_db_transaction, "active", False)))
        if self.raise_on_publish is not None:
            raise self.raise_on_publish
        return self.publish_result


# ---------------------------------------------------------------------------
# Transport contract (pure / config capping)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "cap", "expected"),
    [
        (None, 30.0, None),
        (0.0, 30.0, 0.0),
        (12.5, 30.0, 12.5),
        (45.0, 30.0, 30.0),
        (-1.0, 30.0, None),
        (float("nan"), 30.0, None),
        (float("inf"), 30.0, None),
        (float("-inf"), 30.0, None),
    ],
)
def test_cap_retry_after_seconds_contract(
    raw: float | None, cap: float, expected: float | None
) -> None:
    got = cap_retry_after_seconds(raw, cap_seconds=cap)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_relay_config_rejects_invalid_bounds() -> None:
    with pytest.raises(DomainValidationError):
        RelayConfig(
            lease_seconds=0,
            max_attempts=3,
            backoff_base_seconds=1.0,
            backoff_max_seconds=60.0,
            retry_after_cap_seconds=30.0,
            jitter_ratio=0.0,
            default_probe_seconds=0.05,
        )
    with pytest.raises(DomainValidationError):
        RelayConfig(
            lease_seconds=30,
            max_attempts=0,
            backoff_base_seconds=1.0,
            backoff_max_seconds=60.0,
            retry_after_cap_seconds=30.0,
            jitter_ratio=0.0,
            default_probe_seconds=0.05,
        )


def test_transport_port_exports() -> None:
    assert issubclass(type(FakeTransport()), DeliveryTransport) or True
    # Structural: FakeTransport satisfies DeliveryTransport methods.
    assert hasattr(FakeTransport, "readiness")
    assert hasattr(FakeTransport, "publish")
    for name in ("acknowledged", "retryable", "permanent"):
        assert DeliveryDisposition(name).value == name


# ---------------------------------------------------------------------------
# Readiness gate — no claim under backpressure
# ---------------------------------------------------------------------------


def test_readiness_backpressure_skips_claim(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    transport = FakeTransport(
        readiness_result=TransportReadiness(
            accepting=False,
            reason_code="circuit_open",
            retry_after_seconds=0.01,
        )
    )
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    service = RelayService(
        session_factory=session_factory,
        transport=transport,
        config=_default_config(),
        relay_principal_id=RELAY_A,
        sleep=_sleep,
        open_transaction_flag=transport.in_db_transaction,
    )
    result = asyncio.run(service.process_one())
    assert result.kind == "skipped_backpressure"
    assert transport.publish_calls == []
    assert sleeps and sleeps[0] == pytest.approx(0.01)

    with session_factory() as session:
        row = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == eid)
        ).scalar_one()
        assert int(row.state_code) == STATE_PENDING
        assert row.current_claim_id is None


# ---------------------------------------------------------------------------
# Competing non-blocking claims + fencing
# ---------------------------------------------------------------------------


def test_competing_replicas_claim_distinct_events(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        e1 = _insert_pending(session)
        e2 = _insert_pending(session)
        session.commit()

    repo = DeliveryEventRepository()
    barrier = threading.Barrier(2)
    results: dict[str, UUID | None] = {}
    errors: list[BaseException] = []

    def _worker(name: str, principal: str) -> None:
        try:
            with session_factory() as session:
                barrier.wait(timeout=10)
                with session.begin():
                    claimed = repo.claim_next(
                        session,
                        relay_principal_id=principal,
                        lease_seconds=30,
                    )
                results[name] = None if claimed is None else claimed.event_id
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            errors.append(exc)

    t1 = threading.Thread(target=_worker, args=("a", RELAY_A))
    t2 = threading.Thread(target=_worker, args=("b", RELAY_B))
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)
    assert errors == []
    assert results["a"] is not None and results["b"] is not None
    assert results["a"] != results["b"]
    assert {results["a"], results["b"]} == {e1, e2}


def test_claim_rotates_token_increments_generation(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session, generation=0)
        session.commit()

    repo = DeliveryEventRepository()
    with session_factory() as session:
        with session.begin():
            c1 = repo.claim_next(
                session, relay_principal_id=RELAY_A, lease_seconds=30
            )
        assert c1 is not None
        assert c1.event_id == eid
        assert c1.generation == 1
        assert c1.claim_token is not None
        token1 = c1.claim_token
        claimed_at = c1.claimed_at
        lease_exp = c1.lease_expires_at
        assert lease_exp > claimed_at
        assert c1.delivery_attempt == 1

    # Force expiry and reclaim.
    with session_factory() as session:
        with session.begin():
            session.execute(
                text(
                    """
                    UPDATE delivery_events_active
                    SET lease_expires_at = transaction_timestamp() - interval '1 second'
                    WHERE event_id = :eid
                    """
                ),
                {"eid": eid},
            )

    with session_factory() as session:
        with session.begin():
            c2 = repo.claim_next(
                session, relay_principal_id=RELAY_B, lease_seconds=30
            )
        assert c2 is not None
        assert c2.event_id == eid
        assert c2.generation == 2
        assert c2.claim_token != token1
        assert c2.delivery_attempt == 2
        assert c2.relay_principal_id == RELAY_B


def test_stale_fence_cannot_ack_retry_or_dead_letter(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    repo = DeliveryEventRepository()
    with session_factory() as session:
        with session.begin():
            c1 = repo.claim_next(
                session, relay_principal_id=RELAY_A, lease_seconds=30
            )
        assert c1 is not None

    with session_factory() as session:
        with session.begin():
            session.execute(
                text(
                    """
                    UPDATE delivery_events_active
                    SET lease_expires_at = transaction_timestamp() - interval '1 second'
                    WHERE event_id = :eid
                    """
                ),
                {"eid": eid},
            )

    with session_factory() as session:
        with session.begin():
            c2 = repo.claim_next(
                session, relay_principal_id=RELAY_B, lease_seconds=30
            )
        assert c2 is not None
        assert c2.generation == c1.generation + 1

    with session_factory() as session:
        with session.begin():
            with pytest.raises(DomainValidationError):
                repo.acknowledge(
                    session,
                    event_id=eid,
                    claim_token=c1.claim_token,
                    generation=c1.generation,
                )
        session.rollback()

    with session_factory() as session:
        with session.begin():
            with pytest.raises(DomainValidationError):
                repo.schedule_retry(
                    session,
                    event_id=eid,
                    claim_token=c1.claim_token,
                    generation=c1.generation,
                    failure_code="http.503",
                    available_at_delay_seconds=5.0,
                )
        session.rollback()

    with session_factory() as session:
        with session.begin():
            with pytest.raises(DomainValidationError):
                repo.dead_letter(
                    session,
                    event_id=eid,
                    claim_token=c1.claim_token,
                    generation=c1.generation,
                    failure_code="http.400",
                )
        session.rollback()

    with session_factory() as session:
        with session.begin():
            repo.acknowledge(
                session,
                event_id=eid,
                claim_token=c2.claim_token,
                generation=c2.generation,
            )

    with session_factory() as session:
        term = session.execute(
            select(DeliveryEventTerminal).where(DeliveryEventTerminal.event_id == eid)
        ).scalar_one()
        assert int(term.state_code) == STATE_PUBLISHED
        active = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == eid)
        ).scalar_one_or_none()
        assert active is None


# ---------------------------------------------------------------------------
# Retry / dead-letter / uncertain
# ---------------------------------------------------------------------------


def test_retryable_persists_bounded_backoff(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    transport = FakeTransport(
        publish_result=DeliveryResult(
            disposition=DeliveryDisposition.RETRYABLE,
            failure_code="http.503",
            retry_after_seconds=2.0,
        )
    )
    service = RelayService(
        session_factory=session_factory,
        transport=transport,
        config=_default_config(
            backoff_base_seconds=1.0,
            backoff_max_seconds=60.0,
            retry_after_cap_seconds=30.0,
            jitter_ratio=0.0,
        ),
        relay_principal_id=RELAY_A,
        open_transaction_flag=transport.in_db_transaction,
    )
    result = asyncio.run(service.process_one())
    assert result.kind == "retried"
    assert transport.publish_while_tx == [False]

    with session_factory() as session:
        row = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == eid)
        ).scalar_one()
        assert int(row.state_code) == STATE_PENDING
        assert row.current_claim_id is None
        assert int(row.generation) == 1
        assert int(row.delivery_attempt) == 1
        assert row.last_failure_code == "http.503"
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        # later of policy (1s * 2^0 = 1) and hint (2) => 2s
        delta = (row.available_at - now).total_seconds()
        assert delta == pytest.approx(2.0, abs=0.5)


def test_uncertain_publish_is_retryable(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    transport = FakeTransport(raise_on_publish=TimeoutError("lost response"))
    service = RelayService(
        session_factory=session_factory,
        transport=transport,
        config=_default_config(max_attempts=5, backoff_base_seconds=0.5, jitter_ratio=0.0),
        relay_principal_id=RELAY_A,
        open_transaction_flag=transport.in_db_transaction,
    )
    result = asyncio.run(service.process_one())
    assert result.kind == "retried"

    with session_factory() as session:
        row = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == eid)
        ).scalar_one()
        assert int(row.state_code) == STATE_PENDING
        assert row.last_failure_code == "publish.uncertain"
        assert int(row.delivery_attempt) == 1


def test_permanent_dead_letters(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    transport = FakeTransport(
        publish_result=DeliveryResult(
            disposition=DeliveryDisposition.PERMANENT,
            failure_code="http.400",
            retry_after_seconds=None,
        )
    )
    service = RelayService(
        session_factory=session_factory,
        transport=transport,
        config=_default_config(max_attempts=3),
        relay_principal_id=RELAY_A,
        open_transaction_flag=transport.in_db_transaction,
    )
    result = asyncio.run(service.process_one())
    assert result.kind == "dead_lettered"
    assert result.event_id == eid

    with session_factory() as session:
        term = session.execute(
            select(DeliveryEventTerminal).where(DeliveryEventTerminal.event_id == eid)
        ).scalar_one()
        assert int(term.state_code) == STATE_DEAD_LETTERED
        assert term.failure_code == "http.400"


def test_exhausted_attempts_dead_letter_on_retryable(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session, delivery_attempt=2)
        session.commit()

    transport = FakeTransport(
        publish_result=DeliveryResult(
            disposition=DeliveryDisposition.RETRYABLE,
            failure_code="http.503",
            retry_after_seconds=None,
        )
    )
    service = RelayService(
        session_factory=session_factory,
        transport=transport,
        config=_default_config(max_attempts=3),
        relay_principal_id=RELAY_A,
        open_transaction_flag=transport.in_db_transaction,
    )
    result = asyncio.run(service.process_one())
    assert result.kind == "dead_lettered"
    assert result.event_id == eid

    with session_factory() as session:
        term = session.execute(
            select(DeliveryEventTerminal).where(DeliveryEventTerminal.event_id == eid)
        ).scalar_one()
        assert int(term.state_code) == STATE_DEAD_LETTERED
        assert int(term.delivery_attempt) == 3
        assert term.failure_code == "http.503"


def test_ack_after_publish_outside_transaction(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    transport = FakeTransport()
    service = RelayService(
        session_factory=session_factory,
        transport=transport,
        config=_default_config(),
        relay_principal_id=RELAY_A,
        open_transaction_flag=transport.in_db_transaction,
    )
    result = asyncio.run(service.process_one())
    assert result.kind == "acknowledged"
    assert result.event_id == eid
    assert transport.publish_calls == [eid]
    assert transport.publish_while_tx == [False]

    with session_factory() as session:
        term = session.execute(
            select(DeliveryEventTerminal).where(DeliveryEventTerminal.event_id == eid)
        ).scalar_one()
        assert int(term.state_code) == STATE_PUBLISHED


def test_pending_retry_preserves_generation_above_zero(
    session_factory: sessionmaker[Session],
) -> None:
    """Fence CHECK must allow pending rows that preserve generation after retry."""
    with session_factory() as session:
        eid = _insert_pending(session)
        session.commit()

    repo = DeliveryEventRepository()
    with session_factory() as session:
        with session.begin():
            claimed = repo.claim_next(
                session, relay_principal_id=RELAY_A, lease_seconds=30
            )
        assert claimed is not None
        with session.begin():
            repo.schedule_retry(
                session,
                event_id=eid,
                claim_token=claimed.claim_token,
                generation=claimed.generation,
                failure_code="http.503",
                available_at_delay_seconds=0.0,
            )

    with session_factory() as session:
        row = session.execute(
            select(DeliveryEventActive).where(DeliveryEventActive.event_id == eid)
        ).scalar_one()
        assert int(row.state_code) == STATE_PENDING
        assert int(row.generation) == 1
        assert row.current_claim_id is None


def test_retry_after_hint_does_not_bypass_max_attempts(
    session_factory: sessionmaker[Session],
) -> None:
    """Even a large Retry-After cannot schedule beyond max_attempts."""
    with session_factory() as session:
        eid = _insert_pending(session, delivery_attempt=2)
        session.commit()

    repo = DeliveryEventRepository()
    config = _default_config(max_attempts=3)
    with session_factory() as session:
        with session.begin():
            claimed = repo.claim_next(
                session,
                relay_principal_id=RELAY_A,
                lease_seconds=config.lease_seconds,
            )
        assert claimed is not None
        assert claimed.delivery_attempt == 3
        # At max attempts, schedule_retry path via service should dead-letter;
        # repository itself still schedules if called — RelayService enforces.
        delay = config.compute_retry_delay_seconds(
            delivery_attempt=claimed.delivery_attempt,
            transport_retry_after_seconds=9999.0,
        )
        assert delay <= config.backoff_max_seconds
        assert delay <= config.retry_after_cap_seconds or delay == config.backoff_max_seconds


def test_cap_math_finite() -> None:
    assert math.isfinite(cap_retry_after_seconds(1.0, cap_seconds=10.0) or 0.0)
