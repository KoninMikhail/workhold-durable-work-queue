"""Supported application-outbox bridge runner (BRDG-02 / Plan 06-04).

Claims app-owned intents, enqueues through the normative ProducerClient, and
acknowledges lifecycle only after Queue success. Never holds an app-DB
transaction across network I/O. Does not claim exactly-once execution.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import fields
from typing import Any, Protocol

from workhold_producer.bridge.compatibility import (
    SUPPORTED_CAPABILITIES,
    BridgeCompatibility,
    CompatibilityStatus,
)
from workhold_producer.bridge.idempotency import bridge_idempotency_key
from workhold_producer.bridge.observability import BridgeTelemetry, safe_observe
from workhold_producer.bridge.store import OutboxIntent, OutboxStore
from _workhold_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _workhold_client_core.models import EnqueueResponse
from _workhold_client_core.priority import validate_priority
from workhold_producer.client import _AVAILABLE_AT_OMITTED

SUPPORTED_SCHEMA_MAJOR = 1

# Permanent Queue / local invariant codes → terminal_operator_action.
_TERMINAL_ERROR_CODES = frozenset(
    {
        "idempotency_conflict",
        "permission_denied",
        "queue_not_found",
        "validation_failed",
        "payload_too_large",
        "idempotency_key_required",
        "unauthenticated",
    }
)

ClockFn = Callable[[], float]
SleepFn = Callable[[float], None]
JitterFn = Callable[[float, float], float]
CapabilitiesFetcher = Callable[[], Mapping[str, Any] | None]


def _capabilities_as_mapping(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    if hasattr(raw, "protocol_major"):
        out: dict[str, Any] = {}
        for field in fields(raw):
            if field.name == "extra":
                continue
            out[field.name] = getattr(raw, field.name)
        extra = getattr(raw, "extra", None)
        if isinstance(extra, Mapping):
            out.update(extra)
        return out
    raise TypeError(f"unsupported capabilities type: {type(raw)!r}")


def _live_capabilities_fetcher(producer: EnqueueClient) -> CapabilitiesFetcher:
    get_caps = getattr(producer, "get_capabilities", None)
    if get_caps is None:

        def _missing() -> Mapping[str, Any] | None:
            return None

        return _missing

    def _fetch() -> Mapping[str, Any] | None:
        return _capabilities_as_mapping(get_caps())

    return _fetch


class EnqueueClient(Protocol):
    """Minimal producer surface used by the bridge (ProducerClient raw path)."""

    def _enqueue_with_available_at_raw(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = 0,
        available_at: str | None | object = ...,
    ) -> EnqueueResponse: ...


class BridgeRunner:
    """Bounded poll/relay loop over :class:`~workhold_producer.bridge.store.OutboxStore`.

    Outcome classification (Plan 01 / this plan):

    | Queue / local outcome                         | App lifecycle                |
    |-----------------------------------------------|------------------------------|
    | Enqueue success (new or matching replay)      | ``mark_delivered``           |
    | Timeout / transport / retryable ProtocolError | ``schedule_retry`` (backoff) |
    | MalformedResponseError (uncertain)            | ``schedule_retry``           |
    | ``idempotency_conflict`` / forbidden queue    | ``mark_terminal_operator_action`` |
    | Unsupported schema major / malformed intent   | ``mark_terminal_operator_action`` |

    Shutdown stops new claims, finishes the current in-flight batch under
    ``max_in_flight``, and abandons remaining leased rows for reclaim.
    """

    def __init__(
        self,
        *,
        store: OutboxStore,
        producer: EnqueueClient,
        batch_size: int = 16,
        lease_seconds: int = 30,
        max_in_flight: int = 1,
        idle_poll_seconds: float = 0.05,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        backoff_jitter_ratio: float = 0.1,
        clock: ClockFn | None = None,
        sleep: SleepFn | None = None,
        jitter: JitterFn | None = None,
        logger: logging.Logger | None = None,
        telemetry: BridgeTelemetry | None = None,
        capabilities_fetcher: CapabilitiesFetcher | None = None,
        compatibility: BridgeCompatibility | None = None,
        intent_schema_major: int = SUPPORTED_SCHEMA_MAJOR,
        intent_schema_minor: int = 0,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be >= 1")
        if initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must be >= 0")
        if max_backoff_seconds < 0:
            raise ValueError("max_backoff_seconds must be >= 0")
        if not 0.0 <= backoff_jitter_ratio < 1.0:
            raise ValueError("backoff_jitter_ratio must be in [0, 1)")

        self._store = store
        self._producer = producer
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_in_flight = max_in_flight
        self._idle_poll_seconds = idle_poll_seconds
        self._initial_backoff_seconds = initial_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._backoff_jitter_ratio = backoff_jitter_ratio
        self._clock: ClockFn = clock or time.monotonic
        self._sleep: SleepFn = sleep or time.sleep
        self._jitter: JitterFn = jitter or random.uniform
        self._log = logger or logging.getLogger(__name__)
        self._telemetry = telemetry
        self._shutdown = threading.Event()
        # Default: live GET /v1/capabilities via ``producer.get_capabilities`` when
        # present; otherwise fail closed (no static snapshot). Tests use
        # :meth:`for_tests` or an explicit ``capabilities_fetcher``.
        self._capabilities_fetcher = (
            capabilities_fetcher
            if capabilities_fetcher is not None
            else _live_capabilities_fetcher(producer)
        )
        self._compatibility = compatibility or BridgeCompatibility()
        self._intent_schema_major = int(intent_schema_major)
        self._intent_schema_minor = int(intent_schema_minor)
        self._last_compatibility_status: CompatibilityStatus | None = None

    @classmethod
    def for_tests(
        cls,
        *,
        store: OutboxStore,
        producer: EnqueueClient,
        capabilities: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> BridgeRunner:
        """Construct a runner with a static supported-capabilities snapshot.

        Production entrypoints must inject live discovery or pass a producer
        implementing ``get_capabilities``.
        """
        snapshot = dict(SUPPORTED_CAPABILITIES if capabilities is None else capabilities)

        return cls(
            store=store,
            producer=producer,
            capabilities_fetcher=lambda: snapshot,
            **kwargs,
        )

    def request_shutdown(self) -> None:
        """Stop claiming immediately; finish in-flight work then exit ``run``."""

        self._shutdown.set()
        tel = self._telemetry
        if tel is not None:
            safe_observe(tel.record_shutdown)

    def run(self) -> None:
        """Blocking poll loop until shutdown."""

        while not self._shutdown.is_set():
            processed = self.poll_once()
            if self._shutdown.is_set():
                return
            if processed == 0:
                self._sleep(self._idle_poll_seconds)

    def poll_once(self) -> int:
        """Claim up to ``batch_size`` intents and process them. Returns count handled."""

        if self._shutdown.is_set():
            return 0

        tel = self._telemetry
        if not self._compatibility_allows_poll():
            return 0

        # Claim commits before return — no app-DB transaction across enqueue.
        try:
            intents = list(
                self._store.claim(
                    limit=self._batch_size, lease_seconds=self._lease_seconds
                )
            )
        except Exception:
            if tel is not None:
                safe_observe(lambda: tel.refresh_from_store(self._store))
            raise

        if tel is not None:
            safe_observe(tel.note_successful_poll)
            if intents:
                queue = intents[0].target_queue
                safe_observe(
                    lambda: tel.record_claimed(queue=queue, count=len(intents))
                )
                for intent in intents:
                    if intent.generation > 1:
                        safe_observe(
                            lambda q=intent.target_queue: tel.record_lease_reclaim(
                                queue=q
                            )
                        )
            safe_observe(lambda: tel.refresh_from_store(self._store))

        if not intents:
            return 0

        return self._process_batch(intents)

    def _compatibility_allows_poll(self) -> bool:
        """Fail-closed Queue capability gate before any app-store claim."""
        tel = self._telemetry
        discovery_error: str | None = None
        caps: Mapping[str, Any] | None
        try:
            caps = self._capabilities_fetcher()
        except Exception as exc:  # noqa: BLE001 — discovery must fail closed
            caps = None
            discovery_error = f"{type(exc).__name__}:{exc}"

        result = self._compatibility.evaluate(
            capabilities=caps,
            intent_schema_major=self._intent_schema_major,
            intent_schema_minor=self._intent_schema_minor,
            intent_extensions={},
            bridge_policy="current",
            discovery_error=discovery_error,
        )
        self._last_compatibility_status = result.status
        if result.allows_poll and result.status is CompatibilityStatus.SUPPORTED:
            if tel is not None:
                safe_observe(tel.note_capability_ok)
            return True

        self._log.warning(
            "bridge compatibility gate blocked poll reason=%s status=%s",
            result.reason,
            result.status.value,
        )
        if tel is not None:
            safe_observe(tel.note_capability_mismatch)
            safe_observe(lambda: tel.refresh_from_store(self._store))
        return False

    def _process_batch(self, intents: list[OutboxIntent]) -> int:
        """Process a claimed batch under ``max_in_flight``.

        Mid-batch shutdown finishes in-flight work and abandons unsubmitted
        leased rows for reclaim (no force-deliver / no rewrite).
        """
        processed = 0
        with ThreadPoolExecutor(max_workers=self._max_in_flight) as pool:
            pending = list(intents)
            futures: set[Any] = set()

            while pending or futures:
                while (
                    pending
                    and len(futures) < self._max_in_flight
                    and not self._shutdown.is_set()
                ):
                    intent = pending.pop(0)
                    futures.add(pool.submit(self._process_intent, intent))

                if self._shutdown.is_set() and not futures:
                    pending.clear()
                    break

                if not futures:
                    break

                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                processed += len(done)
                for fut in done:
                    fut.result()

                if self._shutdown.is_set():
                    pending.clear()

        return processed

    def _process_intent(self, intent: OutboxIntent) -> None:
        tel = self._telemetry
        if tel is not None and (intent.traceparent or intent.tracestate):
            safe_observe(
                lambda: tel.project_trace_context(
                    traceparent=intent.traceparent,
                    tracestate=intent.tracestate,
                )
            )

        terminal = self._validate_intent(intent)
        if terminal is not None:
            self._store.mark_terminal_operator_action(
                source_namespace=intent.source_namespace,
                source_row_id=intent.source_row_id,
                ownership_token=intent.ownership_token,
                reason=terminal,
            )
            self._log.info(
                "bridge intent terminal reason=%s queue=%s",
                terminal,
                intent.target_queue,
            )
            if tel is not None:
                safe_observe(
                    lambda: tel.record_malformed_intent(
                        queue=intent.target_queue, result=terminal
                    )
                )
            return

        key = bridge_idempotency_key(intent.source_namespace, intent.source_row_id)
        request = intent.enqueue_request
        payload = request["payload"]
        priority = request["priority"]
        if "available_at" in request:
            available_at_wire = request["available_at"]
            if available_at_wire is not None and not isinstance(available_at_wire, str):
                self._store.mark_terminal_operator_action(
                    source_namespace=intent.source_namespace,
                    source_row_id=intent.source_row_id,
                    ownership_token=intent.ownership_token,
                    reason="malformed_enqueue_request",
                )
                if tel is not None:
                    safe_observe(
                        lambda: tel.record_malformed_intent(
                            queue=intent.target_queue,
                            result="malformed_enqueue_request",
                        )
                    )
                return
        else:
            available_at_wire = _AVAILABLE_AT_OMITTED

        try:
            response = self._producer._enqueue_with_available_at_raw(
                intent.target_queue,
                idempotency_key=key,
                payload=payload,
                priority=priority,
                available_at=available_at_wire,
            )
        except Exception as exc:
            self._handle_enqueue_failure(intent, exc)
            return

        if tel is not None:
            safe_observe(tel.note_queue_reachable)

        task_id = response.task.task_id
        delivered = self._store.mark_delivered(
            source_namespace=intent.source_namespace,
            source_row_id=intent.source_row_id,
            ownership_token=intent.ownership_token,
            queue_task_id=task_id,
        )
        if delivered:
            self._log.info(
                "bridge intent delivered queue=%s task_id=%s replayed=%s",
                intent.target_queue,
                task_id,
                response.replayed,
            )
            if tel is not None:
                result = "replay" if response.replayed else "new"
                safe_observe(
                    lambda: tel.record_delivered(
                        queue=intent.target_queue, result=result
                    )
                )
                safe_observe(
                    lambda: tel.emit_delivery_log(
                        queue=intent.target_queue,
                        task_id=task_id,
                        result=result,
                        replayed=response.replayed,
                    )
                )
        else:
            # Stale lease — another replica owns the row; do not rewrite.
            self._log.info(
                "bridge deliver ack lost lease queue=%s task_id=%s",
                intent.target_queue,
                task_id,
            )
            if tel is not None:
                safe_observe(
                    lambda: tel.record_lease_loss(queue=intent.target_queue)
                )
                safe_observe(
                    lambda: tel.emit_delivery_log(
                        queue=intent.target_queue,
                        task_id=task_id,
                        result="lease_lost",
                    )
                )

    def _validate_intent(self, intent: OutboxIntent) -> str | None:
        if intent.schema_version != SUPPORTED_SCHEMA_MAJOR:
            return "unsupported_schema_version"
        request = intent.enqueue_request
        if not isinstance(request, Mapping):
            return "malformed_enqueue_request"
        if "payload" not in request or "priority" not in request:
            return "malformed_enqueue_request"
        priority = request["priority"]
        try:
            validate_priority(priority)
        except ValueError:
            return "malformed_enqueue_request"
        return None

    def _handle_enqueue_failure(self, intent: OutboxIntent, exc: BaseException) -> None:
        tel = self._telemetry
        if isinstance(exc, AuthenticationError):
            self._terminal(intent, "unauthenticated")
            return

        if isinstance(exc, ProtocolError):
            code = exc.code.value
            # Known permanent conflicts / forbidden outcomes never hot-retry,
            # even if a misbehaving server marked retryable=true.
            if code in _TERMINAL_ERROR_CODES or not exc.retryable:
                self._terminal(intent, code)
                return
            self._retry(intent, failure_code=code)
            return

        if isinstance(exc, (ClientTimeoutError, TransportError, MalformedResponseError)):
            failure = (
                "timeout"
                if isinstance(exc, ClientTimeoutError)
                else "transport_error"
                if isinstance(exc, TransportError)
                else "uncertain"
            )
            if tel is not None and isinstance(exc, (ClientTimeoutError, TransportError)):
                safe_observe(tel.note_queue_unreachable)
            self._retry(intent, failure_code=failure)
            return

        # Unexpected local errors: leave replayable rather than silently drop.
        self._retry(intent, failure_code="uncertain")

    def _terminal(self, intent: OutboxIntent, reason: str) -> None:
        self._store.mark_terminal_operator_action(
            source_namespace=intent.source_namespace,
            source_row_id=intent.source_row_id,
            ownership_token=intent.ownership_token,
            reason=reason,
        )
        self._log.info(
            "bridge intent terminal reason=%s queue=%s",
            reason,
            intent.target_queue,
        )
        tel = self._telemetry
        if tel is not None:
            if reason in {
                "unsupported_schema_version",
                "malformed_enqueue_request",
            }:
                safe_observe(
                    lambda: tel.record_malformed_intent(
                        queue=intent.target_queue, result=reason
                    )
                )
            else:
                safe_observe(
                    lambda: tel.record_permanent_conflict(
                        queue=intent.target_queue, result=reason
                    )
                )

    def _retry(self, intent: OutboxIntent, *, failure_code: str) -> None:
        delay = self._backoff_delay_seconds(intent.generation)
        self._store.schedule_retry(
            source_namespace=intent.source_namespace,
            source_row_id=intent.source_row_id,
            ownership_token=intent.ownership_token,
            available_at_delay_seconds=delay,
            failure_code=failure_code,
        )
        self._log.info(
            "bridge intent retryable failure_code=%s queue=%s delay_s=%.3f",
            failure_code,
            intent.target_queue,
            delay,
        )
        tel = self._telemetry
        if tel is not None:
            safe_observe(
                lambda: tel.record_retryable_error(
                    queue=intent.target_queue, result=failure_code
                )
            )

    def _backoff_delay_seconds(self, generation: int) -> float:
        attempt = max(0, int(generation) - 1)
        base = self._initial_backoff_seconds * (2**attempt)
        capped = min(base, self._max_backoff_seconds)
        if self._backoff_jitter_ratio <= 0.0 or capped <= 0.0:
            return capped
        spread = capped * self._backoff_jitter_ratio
        low = max(0.0, capped - spread)
        high = capped + spread
        return float(self._jitter(low, high))
