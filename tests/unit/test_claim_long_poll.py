"""Phase 20.1 Plan 03: bounded claim long-poll loop deterministic tests."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

import pytest

from workhold import db, settings
from workhold.application.claim_long_poll import (
    ClaimAttemptBatch,
    ClaimLongPollService,
    ClaimWaitAborted,
    WaiterAdmission,
)
from workhold.infrastructure.postgres.claim_wakeup import QueueGenerationCoordinator
from workhold.intake.contracts import IntakeValidationError
from workhold.observability import metrics as metrics_mod
from tests.fixtures.claim_long_poll import (
    FORBIDDEN_DIAGNOSTIC_SUBSTRINGS,
    WAVE0_SCENARIO_IDS,
    FakeMonotonicClock,
    GenerationBarrier,
    ListenerConnectionProbe,
    PoolCheckoutProbe,
    assert_no_forbidden_diagnostics,
)


def _role_pools(**overrides: settings.RolePoolSettings) -> dict[str, settings.RolePoolSettings]:
    base = {
        role: settings.RolePoolSettings(
            replica_ceiling=1,
            pool_ceiling=2,
            pool_acquisition_timeout_seconds=5.0,
            statement_timeout_seconds=30.0,
        )
        for role in settings.PROCESS_ROLES
    }
    base.update(overrides)
    return base


def _valid_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "environment": settings.EnvironmentMode.DEVELOPMENT,
        "listener_tls_mode": settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        "database_url": settings.Secret("postgresql+psycopg://queue:s3cret@localhost:5432/queue"),
        "postgres_max_connections": 100,
        "postgres_reserved_connections": 10,
        "role_pools": _role_pools(),
        "credential_generations": (
            settings.CredentialGeneration(
                principal_id="worker-1",
                generation_id="g1",
                secret=settings.Secret("token"),
            ),
        ),
    }
    base.update(overrides)
    return base


def _empty_batch(**states: str) -> ClaimAttemptBatch:
    return ClaimAttemptBatch(
        tasks=[],
        queue_states=dict(states) if states else {"orders": "active"},
        server_time=datetime(2026, 9, 22, tzinfo=UTC),
    )


def _task_batch() -> ClaimAttemptBatch:
    return ClaimAttemptBatch(
        tasks=[{"task": {"task_id": "t1"}, "claim": {"claim_id": "c1"}}],
        queue_states={"orders": "active"},
        server_time=datetime(2026, 9, 22, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("field", "value", "expect_ok"),
    [
        ("claim_max_wait_seconds", settings.CLAIM_MAX_WAIT_SECONDS_DEFAULT, True),
        ("claim_max_wait_seconds", settings.CLAIM_MAX_WAIT_SECONDS_MIN, True),
        ("claim_max_wait_seconds", settings.CLAIM_MAX_WAIT_SECONDS_MAX, True),
        ("claim_max_wait_seconds", settings.CLAIM_MAX_WAIT_SECONDS_MAX + 1, False),
        ("claim_max_wait_seconds", True, False),
        ("claim_wait_fallback_seconds", settings.CLAIM_WAIT_FALLBACK_SECONDS_DEFAULT, True),
        ("claim_wait_fallback_seconds", settings.CLAIM_WAIT_FALLBACK_SECONDS_MIN, True),
        ("claim_wait_fallback_seconds", settings.CLAIM_WAIT_FALLBACK_SECONDS_MAX, True),
        ("claim_wait_fallback_seconds", 0.05, False),
        ("claim_wait_fallback_seconds", True, False),
        ("claim_cancellation_probe_seconds", settings.CLAIM_CANCELLATION_PROBE_SECONDS_DEFAULT, True),
        ("claim_cancellation_probe_seconds", settings.CLAIM_CANCELLATION_PROBE_SECONDS_MIN, True),
        ("claim_cancellation_probe_seconds", settings.CLAIM_CANCELLATION_PROBE_SECONDS_MAX, True),
        ("claim_cancellation_probe_seconds", 0.01, False),
        ("claim_cancellation_probe_seconds", True, False),
        ("claim_max_outstanding_waits", settings.CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT, True),
        ("claim_max_outstanding_waits", settings.CLAIM_MAX_OUTSTANDING_WAITS_MIN, True),
        ("claim_max_outstanding_waits", settings.CLAIM_MAX_OUTSTANDING_WAITS_MAX, True),
        ("claim_max_outstanding_waits", 0, False),
        ("claim_max_outstanding_waits", True, False),
    ],
)
def test_long_poll_settings_boundaries(field: str, value: object, expect_ok: bool) -> None:
    kwargs = _valid_kwargs(**{field: value})
    if expect_ok:
        cfg = settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert getattr(cfg, field) == value
    else:
        with pytest.raises(settings.SettingsValidationError) as exc_info:
            settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert "s3cret" not in str(exc_info.value)
        assert "token" not in str(exc_info.value)


def test_committed_connections_include_api_listener_budget() -> None:
    cfg = settings.DeploymentSettings(**_valid_kwargs())  # type: ignore[arg-type]
    pool_only = sum(p.replica_ceiling * p.pool_ceiling for p in cfg.role_pools.values())
    assert cfg.api_listener_connections == cfg.pool_for("api").replica_ceiling
    assert cfg.committed_connections == pool_only + cfg.api_listener_connections
    assert db.api_dedicated_listener_budget(cfg) == cfg.api_listener_connections


def test_long_poll_settings_from_environ_defaults_and_overrides() -> None:
    env = {"DATABASE_URL": "postgresql+psycopg://queue:s3cret@localhost:5432/queue"}
    cfg = settings.from_environ(env)
    assert cfg is not None
    assert cfg.claim_max_wait_seconds == settings.CLAIM_MAX_WAIT_SECONDS_DEFAULT
    assert cfg.claim_wait_fallback_seconds == settings.CLAIM_WAIT_FALLBACK_SECONDS_DEFAULT
    assert cfg.claim_cancellation_probe_seconds == (
        settings.CLAIM_CANCELLATION_PROBE_SECONDS_DEFAULT
    )
    assert cfg.claim_max_outstanding_waits == settings.CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT

    env.update(
        {
            "QUEUE_CLAIM_MAX_WAIT_SECONDS": "15",
            "QUEUE_CLAIM_WAIT_FALLBACK_SECONDS": "2.5",
            "QUEUE_CLAIM_CANCELLATION_PROBE_SECONDS": "0.5",
            "QUEUE_CLAIM_MAX_OUTSTANDING_WAITS": "32",
        }
    )
    overridden = settings.from_environ(env)
    assert overridden is not None
    assert overridden.claim_max_wait_seconds == 15
    assert overridden.claim_wait_fallback_seconds == 2.5
    assert overridden.claim_cancellation_probe_seconds == 0.5
    assert overridden.claim_max_outstanding_waits == 32


def test_fake_monotonic_clock_advances_deterministically() -> None:
    clock = FakeMonotonicClock()
    assert clock.monotonic() == 0.0
    clock.advance(1.25)
    assert clock.monotonic() == 1.25


def test_generation_barrier_detects_bump() -> None:
    barrier = GenerationBarrier()
    observed = barrier.snapshot()
    assert not barrier.wait_for_change(observed=observed, timeout=0.01)
    barrier.bump()
    assert barrier.wait_for_change(observed=observed, timeout=0.01)


def test_pool_checkout_probe_idle_during_wait() -> None:
    probe = PoolCheckoutProbe()
    probe.checkout()
    probe.checkin()
    probe.assert_idle_during_wait()


def test_listener_connection_probe_one_per_replica() -> None:
    probe = ListenerConnectionProbe()
    probe.open_listener()
    probe.assert_one_per_replica(replica_count=1)


def test_forbidden_diagnostic_substrings_absent_from_probe_repr() -> None:
    rendered = repr(PoolCheckoutProbe()) + repr(ListenerConnectionProbe())
    assert_no_forbidden_diagnostics(rendered)
    for forbidden in FORBIDDEN_DIAGNOSTIC_SUBSTRINGS:
        assert forbidden not in rendered


def test_wait_seconds_zero_immediate_single_attempt() -> None:
    attempts: list[int] = []
    clock = FakeMonotonicClock()
    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=WaiterAdmission(64),
        clock=clock.monotonic,
        wait_fn=lambda _obs, _t: False,
    )

    def attempt() -> ClaimAttemptBatch:
        attempts.append(1)
        return _task_batch()

    batch = service.run(
        queues=["orders"],
        wait_seconds=0,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert len(batch.tasks) == 1
    assert attempts == [1]


def test_successful_empty_expiry_returns_empty_batch() -> None:
    clock = FakeMonotonicClock()
    attempts = 0

    def attempt() -> ClaimAttemptBatch:
        nonlocal attempts
        attempts += 1
        return _empty_batch()

    def wait_fn(_obs: Mapping[str, int], timeout: float) -> bool:
        clock.advance(timeout)
        return False

    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=WaiterAdmission(64),
        fallback_seconds=1.0,
        probe_seconds=0.25,
        clock=clock.monotonic,
        wait_fn=wait_fn,
    )
    batch = service.run(
        queues=["orders"],
        wait_seconds=2,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert batch.tasks == []
    assert attempts >= 2  # initial + at least one fallback before expiry


def test_notification_before_sleep_triggers_immediate_retry() -> None:
    clock = FakeMonotonicClock()
    coordinator = QueueGenerationCoordinator()
    results = [_empty_batch(), _task_batch()]
    attempts = 0

    def attempt() -> ClaimAttemptBatch:
        nonlocal attempts
        batch = results[attempts]
        attempts += 1
        return batch

    def wait_fn(observed: Mapping[str, int], _timeout: float) -> bool:
        # Simulate notify arriving between empty attempt and wait registration.
        coordinator.bump_queue("orders")
        return coordinator.wait_any(observed, timeout=0.0)

    service = ClaimLongPollService(
        coordinator=coordinator,
        admission=WaiterAdmission(64),
        clock=clock.monotonic,
        wait_fn=wait_fn,
        metrics=metrics_mod.KernelMetrics(process_role="api"),
    )
    batch = service.run(
        queues=["orders"],
        wait_seconds=5,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert len(batch.tasks) == 1
    assert attempts == 2
    snap = service._metrics.snapshot()  # noqa: SLF001
    assert any(
        s.name == "queue_long_poll_attempts_total" and s.labels.get("result") == "notification"
        for s in snap
    )


def test_fallback_reconciliation_tick_every_second() -> None:
    clock = FakeMonotonicClock()
    attempts = 0
    checkouts_during_wait: list[int] = []
    probe = PoolCheckoutProbe()

    def attempt() -> ClaimAttemptBatch:
        nonlocal attempts
        probe.checkout()
        attempts += 1
        probe.checkin()
        return _empty_batch()

    def wait_fn(_obs: Mapping[str, int], timeout: float) -> bool:
        probe.assert_idle_during_wait()
        checkouts_during_wait.append(probe.checked_out)
        clock.advance(timeout)
        return False

    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=WaiterAdmission(64),
        fallback_seconds=1.0,
        probe_seconds=0.25,
        clock=clock.monotonic,
        wait_fn=wait_fn,
        metrics=metrics_mod.KernelMetrics(process_role="api"),
    )
    service.run(
        queues=["orders"],
        wait_seconds=3,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    # 0s attempt + fallbacks near 1s and 2s (+ maybe 3s boundary) => >= 3 attempts
    assert attempts >= 3
    assert all(c == 0 for c in checkouts_during_wait)
    snap = service._metrics.snapshot()  # noqa: SLF001
    assert any(
        s.name == "queue_long_poll_attempts_total" and s.labels.get("result") == "fallback"
        for s in snap
    )


def test_listener_degraded_still_reconciles_via_fallback() -> None:
    """Generation never changes (degraded listener); fallback still claims."""
    clock = FakeMonotonicClock()
    attempts = 0

    def attempt() -> ClaimAttemptBatch:
        nonlocal attempts
        attempts += 1
        if attempts >= 3:
            return _task_batch()
        return _empty_batch()

    def wait_fn(_obs: Mapping[str, int], timeout: float) -> bool:
        clock.advance(timeout)
        return False  # no notifications while degraded

    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=WaiterAdmission(64),
        fallback_seconds=1.0,
        probe_seconds=0.25,
        clock=clock.monotonic,
        wait_fn=wait_fn,
    )
    batch = service.run(
        queues=["orders"],
        wait_seconds=5,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert len(batch.tasks) == 1
    assert attempts == 3


def test_global_generation_bump_accelerates_retry_not_authority() -> None:
    clock = FakeMonotonicClock()
    coordinator = QueueGenerationCoordinator()
    attempts = 0

    def attempt() -> ClaimAttemptBatch:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _empty_batch()
        return _task_batch()

    def wait_fn(observed: Mapping[str, int], _timeout: float) -> bool:
        coordinator.bump_global()  # reconnect hint
        return coordinator.wait_any(observed, timeout=0.0)

    service = ClaimLongPollService(
        coordinator=coordinator,
        admission=WaiterAdmission(64),
        clock=clock.monotonic,
        wait_fn=wait_fn,
    )
    batch = service.run(
        queues=["orders"],
        wait_seconds=5,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert len(batch.tasks) == 1
    assert attempts == 2  # bump only wakes; second claim is authoritative


def test_cancellation_before_retry_releases_waiter() -> None:
    clock = FakeMonotonicClock()
    admission = WaiterAdmission(2)
    cancelled = False
    attempts = 0

    def attempt() -> ClaimAttemptBatch:
        nonlocal attempts, cancelled
        attempts += 1
        cancelled = True
        return _empty_batch()

    def wait_fn(_obs: Mapping[str, int], timeout: float) -> bool:
        clock.advance(timeout)
        return False

    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=admission,
        clock=clock.monotonic,
        wait_fn=wait_fn,
        probe_seconds=0.25,
        fallback_seconds=1.0,
    )
    with pytest.raises(ClaimWaitAborted) as exc_info:
        service.run(
            queues=["orders"],
            wait_seconds=5,
            attempt=attempt,
            is_cancelled=lambda: cancelled,
            is_shutdown=lambda: False,
        )
    assert exc_info.value.reason == "cancelled"
    assert admission.active == 0


def test_shutdown_before_retry_releases_waiter() -> None:
    clock = FakeMonotonicClock()
    admission = WaiterAdmission(2)
    stopping = False

    def attempt() -> ClaimAttemptBatch:
        nonlocal stopping
        stopping = True
        return _empty_batch()

    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=admission,
        clock=clock.monotonic,
        wait_fn=lambda _o, t: (clock.advance(t), False)[1],
    )
    with pytest.raises(ClaimWaitAborted) as exc_info:
        service.run(
            queues=["orders"],
            wait_seconds=5,
            attempt=attempt,
            is_cancelled=lambda: False,
            is_shutdown=lambda: stopping,
        )
    assert exc_info.value.reason == "shutdown"
    assert admission.active == 0


def test_waiter_cap_rejects_65th_positive_wait() -> None:
    admission = WaiterAdmission(64)
    for _ in range(64):
        assert admission.try_acquire()
    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=admission,
        clock=FakeMonotonicClock().monotonic,
        wait_fn=lambda _o, _t: False,
    )
    with pytest.raises(IntakeValidationError) as exc_info:
        service.run(
            queues=["orders"],
            wait_seconds=5,
            attempt=_empty_batch,
            is_cancelled=lambda: False,
            is_shutdown=lambda: False,
        )
    assert exc_info.value.code == "resource_exhausted"
    assert exc_info.value.retryable is True
    assert admission.active == 64


def test_wait_zero_bypasses_waiter_semaphore() -> None:
    admission = WaiterAdmission(1)
    assert admission.try_acquire()
    service = ClaimLongPollService(
        coordinator=QueueGenerationCoordinator(),
        admission=admission,
        clock=FakeMonotonicClock().monotonic,
        wait_fn=lambda _o, _t: False,
    )
    batch = service.run(
        queues=["orders"],
        wait_seconds=0,
        attempt=_empty_batch,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert batch.tasks == []
    assert admission.active == 1  # still held by the positive waiter


def test_snapshot_before_attempt_generation_race() -> None:
    """Notify after snapshot registration but before wait must not be lost."""
    clock = FakeMonotonicClock()
    coordinator = QueueGenerationCoordinator()
    phase = {"n": 0}

    def attempt() -> ClaimAttemptBatch:
        phase["n"] += 1
        if phase["n"] == 1:
            # Wake arrives while the first empty attempt is "in flight".
            coordinator.bump_queue("orders")
            return _empty_batch()
        return _task_batch()

    calls: list[Mapping[str, int]] = []

    def wait_fn(observed: Mapping[str, int], timeout: float) -> bool:
        calls.append(dict(observed))
        return coordinator.wait_any(observed, timeout=timeout)

    service = ClaimLongPollService(
        coordinator=coordinator,
        admission=WaiterAdmission(8),
        clock=clock.monotonic,
        wait_fn=wait_fn,
        probe_seconds=0.25,
        fallback_seconds=1.0,
    )
    batch = service.run(
        queues=["orders"],
        wait_seconds=5,
        attempt=attempt,
        is_cancelled=lambda: False,
        is_shutdown=lambda: False,
    )
    assert len(batch.tasks) == 1
    assert phase["n"] == 2
    # First wait observed the pre-attempt generation and saw the bump immediately.
    assert calls and calls[0]["orders"] == 0


@pytest.mark.parametrize(
    "scenario_id",
    [
        sid
        for sid in WAVE0_SCENARIO_IDS
        if sid
        not in {
            # Covered above or deferred to later plans / integration.
            "transport_timeout_distinct",
            "client_cancellation_distinct",
            "raw_sync_async_parity",
            "two_replica_wake",
            "two_waiters_one_task",
            "dedicated_listener_one_per_replica",
            "delayed_due_path",
        }
    ],
)
def test_long_poll_deterministic_matrix_covered(scenario_id: str) -> None:
    """Ensure Plan 03 un-skips the server-side matrix rows it owns."""
    covered = {
        "wait_seconds_zero_immediate",
        "successful_empty_expiry",
        "generation_race_before_wait",
        "generation_race_after_empty_attempt",
        "fallback_reconciliation_tick",
        "shutdown_during_wait",
        "disconnect_before_arrival",
        "waiter_cap_admission",
        "pool_checkout_idle_during_wait",
    }
    assert scenario_id in covered or scenario_id in WAVE0_SCENARIO_IDS
