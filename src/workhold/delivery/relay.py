"""Transport-neutral Delivery Relay claim/publish/ack orchestration (Phase 5).

Readiness is probed before any database claim. Publication runs outside every
Queue-store transaction. Outcomes require the current unexpired fence.
"""

from __future__ import annotations

import asyncio
import math
import random
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Final, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from workhold.delivery.models import FAILURE_CODE_MAX, RELAY_PRINCIPAL_ID_MAX
from workhold.delivery.repository import (
    ClaimedDeliveryEvent,
    DeliveryEventRepository,
)
from workhold.delivery.telemetry import DeliveryTelemetry
from workhold.domain.queue_control import DomainValidationError

# Match task lease hard ceiling (lease_repository).
LEASE_SECONDS_MIN: Final[int] = 1
LEASE_SECONDS_MAX: Final[int] = 3600
UNCERTAIN_FAILURE_CODE: Final[str] = "publish.uncertain"


class DeliveryDisposition(str, Enum):
    """Transport-neutral publish outcome classification."""

    ACKNOWLEDGED = "acknowledged"
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


@dataclass(frozen=True, slots=True)
class TransportReadiness:
    """Side-effect-free pre-claim probe result."""

    accepting: bool
    reason_code: str
    retry_after_seconds: float | None


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """Transport-neutral publish classification (no adapter body crosses the port)."""

    disposition: DeliveryDisposition
    failure_code: str | None
    retry_after_seconds: float | None


@runtime_checkable
class DeliveryTransport(Protocol):
    """Pluggable delivery adapter boundary (HTTP first in 05-04)."""

    async def readiness(self) -> TransportReadiness:
        """Return whether the transport will accept new work right now."""

    async def publish(self, event: ClaimedDeliveryEvent) -> DeliveryResult:
        """Publish one claimed event; classify the outcome without returning bodies."""


def cap_retry_after_seconds(
    raw: float | None, *, cap_seconds: float
) -> float | None:
    """Cap or discard a transport Retry-After hint.

    Absent, negative, and non-finite values become ``None``. Valid values are
    clamped to ``[0, cap_seconds]``.
    """
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    if not math.isfinite(value) or value < 0:
        return None
    if not math.isfinite(cap_seconds) or cap_seconds < 0:
        raise DomainValidationError(
            "validation_failed",
            "retry_after cap must be a finite non-negative duration",
        )
    return min(value, float(cap_seconds))


@dataclass(frozen=True, slots=True)
class RelayConfig:
    """Validated hard bounds for relay leases, attempts, and backoff."""

    lease_seconds: int
    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    retry_after_cap_seconds: float
    jitter_ratio: float
    default_probe_seconds: float

    def __post_init__(self) -> None:
        if (
            isinstance(self.lease_seconds, bool)
            or not isinstance(self.lease_seconds, int)
            or not (LEASE_SECONDS_MIN <= self.lease_seconds <= LEASE_SECONDS_MAX)
        ):
            raise DomainValidationError(
                "validation_failed",
                "lease_seconds is outside the deployment hard ceiling",
            )
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise DomainValidationError(
                "validation_failed",
                "max_attempts must be an integer >= 1",
            )
        for name, value in (
            ("backoff_base_seconds", self.backoff_base_seconds),
            ("backoff_max_seconds", self.backoff_max_seconds),
            ("retry_after_cap_seconds", self.retry_after_cap_seconds),
            ("default_probe_seconds", self.default_probe_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0
            ):
                raise DomainValidationError(
                    "validation_failed",
                    f"{name} must be a finite non-negative duration",
                )
        if float(self.backoff_max_seconds) < float(self.backoff_base_seconds):
            raise DomainValidationError(
                "validation_failed",
                "backoff_max_seconds must be >= backoff_base_seconds",
            )
        if (
            isinstance(self.jitter_ratio, bool)
            or not isinstance(self.jitter_ratio, (int, float))
            or not math.isfinite(float(self.jitter_ratio))
            or not (0.0 <= float(self.jitter_ratio) <= 1.0)
        ):
            raise DomainValidationError(
                "validation_failed",
                "jitter_ratio must be in [0, 1]",
            )

    def cap_retry_after(self, raw: float | None) -> float | None:
        return cap_retry_after_seconds(
            raw, cap_seconds=float(self.retry_after_cap_seconds)
        )

    def compute_retry_delay_seconds(
        self,
        *,
        delivery_attempt: int,
        transport_retry_after_seconds: float | None,
        rng: random.Random | None = None,
    ) -> float:
        """Later of capped exponential backoff-with-jitter and capped Retry-After."""
        delay, _source = self.compute_retry_delay_with_source(
            delivery_attempt=delivery_attempt,
            transport_retry_after_seconds=transport_retry_after_seconds,
            rng=rng,
        )
        return delay

    def compute_retry_delay_with_source(
        self,
        *,
        delivery_attempt: int,
        transport_retry_after_seconds: float | None,
        rng: random.Random | None = None,
    ) -> tuple[float, str]:
        """Return ``(delay_seconds, delay_source)`` where source is policy|retry_after."""
        if (
            isinstance(delivery_attempt, bool)
            or not isinstance(delivery_attempt, int)
            or delivery_attempt < 1
        ):
            raise DomainValidationError(
                "validation_failed",
                "delivery_attempt must be an integer >= 1",
            )
        exponent = max(delivery_attempt - 1, 0)
        base = float(self.backoff_base_seconds) * (2**exponent)
        policy = min(base, float(self.backoff_max_seconds))
        ratio = float(self.jitter_ratio)
        if ratio > 0:
            source = rng if rng is not None else random
            jitter = 1.0 + source.uniform(-ratio, ratio)
            policy = max(0.0, policy * jitter)
            policy = min(policy, float(self.backoff_max_seconds))
        hint = self.cap_retry_after(transport_retry_after_seconds)
        hint_delay = 0.0 if hint is None else hint
        if hint is not None and hint_delay >= policy:
            return hint_delay, "retry_after"
        return max(policy, hint_delay), "policy"


@dataclass(frozen=True, slots=True)
class RelayCycleResult:
    """One relay process_one outcome."""

    kind: str
    event_id: UUID | None = None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class RelayFaultHooks:
    """Optional test-only seams for publish/ack failure-window chaos.

    Production packaged runtime must leave both hooks unset. Callers that arm
    these hooks are responsible for ensuring they never reach production.
    """

    before_publish: Callable[[ClaimedDeliveryEvent], None] | None = None
    after_publish_before_outcome: (
        Callable[[ClaimedDeliveryEvent, DeliveryResult], None] | None
    ) = None


class RelayService:
    """Orchestrate readiness → short claim → publish → short fenced outcome."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        transport: DeliveryTransport,
        config: RelayConfig,
        relay_principal_id: str,
        repository: DeliveryEventRepository | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        open_transaction_flag: threading.local | None = None,
        rng: random.Random | None = None,
        telemetry: DeliveryTelemetry | None = None,
        fault_hooks: RelayFaultHooks | None = None,
    ) -> None:
        if not isinstance(relay_principal_id, str) or not (
            1 <= len(relay_principal_id) <= RELAY_PRINCIPAL_ID_MAX
        ):
            raise DomainValidationError(
                "validation_failed",
                "relay_principal_id must be 1..128 characters",
            )
        self._session_factory = session_factory
        self._transport = transport
        self._config = config
        self._relay_principal_id = relay_principal_id
        self._repo = repository or DeliveryEventRepository()
        self._sleep = sleep or asyncio.sleep
        self._tx_flag = open_transaction_flag
        self._rng = rng
        self._telemetry = telemetry
        self._fault_hooks = fault_hooks or RelayFaultHooks()

    async def process_one(self) -> RelayCycleResult:
        ready_started = time.perf_counter()
        readiness = await self._transport.readiness()
        ready_elapsed = time.perf_counter() - ready_started
        if self._telemetry is not None:
            self._telemetry.record_readiness(
                result="accepting" if readiness.accepting else "rejected",
                reason_code=readiness.reason_code,
                duration_seconds=ready_elapsed,
            )
        if not readiness.accepting:
            wait = self._config.cap_retry_after(readiness.retry_after_seconds)
            if wait is None:
                wait = float(self._config.default_probe_seconds)
            if self._telemetry is not None:
                self._telemetry.record_claim_skip(
                    reason_code=readiness.reason_code,
                    delay_seconds=wait,
                )
            await self._sleep(wait)
            return RelayCycleResult(
                kind="skipped_backpressure",
                reason_code=readiness.reason_code,
            )

        claim_started = time.perf_counter()
        try:
            claimed = self._claim()
        except Exception:
            if self._telemetry is not None:
                self._telemetry.record_claim(
                    result="error",
                    duration_seconds=time.perf_counter() - claim_started,
                )
            raise
        claim_elapsed = time.perf_counter() - claim_started
        if claimed is None:
            if self._telemetry is not None:
                self._telemetry.record_claim(
                    result="empty", duration_seconds=claim_elapsed
                )
            return RelayCycleResult(kind="empty")

        if self._telemetry is not None:
            self._telemetry.record_claim(
                result="success",
                duration_seconds=claim_elapsed,
                reclaimed=claimed.reclaimed,
                event_id=str(claimed.event_id),
                source_task_id=str(claimed.source_task_id),
                generation=claimed.generation,
            )
            self._telemetry.record_publish_start(
                event_id=str(claimed.event_id),
                source_task_id=str(claimed.source_task_id),
                generation=claimed.generation,
            )

        before = self._fault_hooks.before_publish
        if before is not None:
            before(claimed)

        result = await self._publish_safe(claimed)

        after = self._fault_hooks.after_publish_before_outcome
        if after is not None:
            after(claimed, result)

        return self._persist_outcome(claimed, result)

    def _claim(self) -> ClaimedDeliveryEvent | None:
        with self._session_factory() as session:
            self._set_tx(True)
            try:
                with session.begin():
                    return self._repo.claim_next(
                        session,
                        relay_principal_id=self._relay_principal_id,
                        lease_seconds=self._config.lease_seconds,
                    )
            finally:
                self._set_tx(False)

    async def _publish_safe(self, claimed: ClaimedDeliveryEvent) -> DeliveryResult:
        started = time.perf_counter()
        try:
            result = await self._transport.publish(claimed)
        except (TimeoutError, asyncio.TimeoutError, OSError) as exc:
            elapsed = time.perf_counter() - started
            if self._telemetry is not None:
                self._telemetry.record_publish_result(
                    result="exception",
                    duration_seconds=elapsed,
                    failure_code=UNCERTAIN_FAILURE_CODE,
                    event_id=str(claimed.event_id),
                    source_task_id=str(claimed.source_task_id),
                    generation=claimed.generation,
                )
            del exc
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code=UNCERTAIN_FAILURE_CODE,
                retry_after_seconds=None,
            )
        elapsed = time.perf_counter() - started
        disposition = result.disposition
        if not isinstance(disposition, DeliveryDisposition):
            disposition = DeliveryDisposition(str(disposition))
        if self._telemetry is not None:
            self._telemetry.record_publish_result(
                result=disposition.value,
                duration_seconds=elapsed,
                failure_code=result.failure_code,
                event_id=str(claimed.event_id),
                source_task_id=str(claimed.source_task_id),
                generation=claimed.generation,
            )
        return result

    def _persist_outcome(
        self, claimed: ClaimedDeliveryEvent, result: DeliveryResult
    ) -> RelayCycleResult:
        disposition = result.disposition
        if not isinstance(disposition, DeliveryDisposition):
            disposition = DeliveryDisposition(str(disposition))

        if disposition is DeliveryDisposition.ACKNOWLEDGED:
            started = time.perf_counter()
            try:
                with self._session_factory() as session:
                    self._set_tx(True)
                    try:
                        with session.begin():
                            self._repo.acknowledge(
                                session,
                                event_id=claimed.event_id,
                                claim_token=claimed.claim_token,
                                generation=claimed.generation,
                            )
                    finally:
                        self._set_tx(False)
            except DomainValidationError as exc:
                if self._telemetry is not None:
                    self._telemetry.record_ack(
                        result="rejected",
                        duration_seconds=time.perf_counter() - started,
                        event_id=str(claimed.event_id),
                        source_task_id=str(claimed.source_task_id),
                        generation=claimed.generation,
                        code=exc.code,
                    )
                raise
            if self._telemetry is not None:
                self._telemetry.record_ack(
                    result="success",
                    duration_seconds=time.perf_counter() - started,
                    event_id=str(claimed.event_id),
                    source_task_id=str(claimed.source_task_id),
                    generation=claimed.generation,
                    code="ok",
                )
            return RelayCycleResult(kind="acknowledged", event_id=claimed.event_id)

        failure_code = _normalize_failure_code(result.failure_code)
        exhausted = claimed.delivery_attempt >= self._config.max_attempts
        if disposition is DeliveryDisposition.PERMANENT or exhausted:
            code = failure_code or (
                "attempts_exhausted" if exhausted else "delivery.permanent"
            )
            started = time.perf_counter()
            with self._session_factory() as session:
                self._set_tx(True)
                try:
                    with session.begin():
                        self._repo.dead_letter(
                            session,
                            event_id=claimed.event_id,
                            claim_token=claimed.claim_token,
                            generation=claimed.generation,
                            failure_code=code,
                        )
                finally:
                    self._set_tx(False)
            if self._telemetry is not None:
                self._telemetry.record_dead_letter(
                    failure_code=code,
                    event_id=str(claimed.event_id),
                    source_task_id=str(claimed.source_task_id),
                    generation=claimed.generation,
                    duration_seconds=time.perf_counter() - started,
                )
            return RelayCycleResult(kind="dead_lettered", event_id=claimed.event_id)

        delay, delay_source = self._config.compute_retry_delay_with_source(
            delivery_attempt=claimed.delivery_attempt,
            transport_retry_after_seconds=result.retry_after_seconds,
            rng=self._rng,
        )
        code = failure_code or "delivery.retryable"
        started = time.perf_counter()
        with self._session_factory() as session:
            self._set_tx(True)
            try:
                with session.begin():
                    self._repo.schedule_retry(
                        session,
                        event_id=claimed.event_id,
                        claim_token=claimed.claim_token,
                        generation=claimed.generation,
                        failure_code=code,
                        available_at_delay_seconds=delay,
                    )
            finally:
                self._set_tx(False)
        if self._telemetry is not None:
            self._telemetry.record_retry(
                failure_code=code,
                delay_source=delay_source,
                attempt=claimed.delivery_attempt,
                event_id=str(claimed.event_id),
                source_task_id=str(claimed.source_task_id),
                generation=claimed.generation,
                duration_seconds=time.perf_counter() - started,
            )
        return RelayCycleResult(kind="retried", event_id=claimed.event_id)

    def _set_tx(self, active: bool) -> None:
        if self._tx_flag is not None:
            self._tx_flag.active = active


def _normalize_failure_code(raw: str | None) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not (1 <= len(raw) <= FAILURE_CODE_MAX):
        raise DomainValidationError(
            "validation_failed",
            f"failure_code must be 1..{FAILURE_CODE_MAX} characters",
        )
    return raw
