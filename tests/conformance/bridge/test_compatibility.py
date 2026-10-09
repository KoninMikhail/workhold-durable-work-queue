"""Bridge rolling-upgrade compatibility conformance (BRDG-02 / Plan 06-07)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from queue_service_producer.bridge.compatibility import (
    SUPPORTED_CAPABILITIES,
    BridgeCompatibility,
    CompatibilityResult,
    CompatibilityStatus,
)
from queue_service_producer.bridge.idempotency import bridge_idempotency_key
from queue_service_producer.bridge.observability import BridgeTelemetry
from queue_service_producer.bridge.runner import BridgeRunner
from queue_service_producer.bridge.store import (
    AppStoreHealthSnapshot,
    BoundedPendingDepth,
    OldestPendingSnapshot,
    OutboxIntent,
)
from _queue_service_client_core.models import EnqueueResponse, ErrorCode, Task, TaskState

MATRIX_PATH = Path(__file__).with_name("compatibility_matrix.yaml")


def _parse_scalar(raw: str) -> Any:
    text = raw.strip()
    if text in {"null", "~", ""}:
        return None
    if text == "{}":
        return {}
    if text in {"true", "True"}:
        return True
    if text in {"false", "False"}:
        return False
    if (text.startswith('"') and text.endswith('"')) or (
        text.startswith("'") and text.endswith("'")
    ):
        return text[1:-1]
    try:
        return int(text)
    except ValueError:
        return text


def _load_simple_yaml_matrix(text: str) -> dict[str, Any]:
    """Minimal YAML subset loader for the checked-in matrix (no PyYAML; T-06-SC)."""
    cases: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    nested_key: str | None = None
    nested: dict[str, Any] | None = None
    in_cases = False

    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        if line == "cases:":
            in_cases = True
            continue
        if not in_cases:
            continue
        if indent == 2 and line.startswith("- id:"):
            if current is not None:
                cases.append(current)
            current = {"id": _parse_scalar(line.split(":", 1)[1])}
            nested_key = None
            nested = None
            continue
        if current is None:
            continue
        if indent == 4 and line.endswith(":") and line.count(":") == 1:
            nested_key = line[:-1]
            nested = {}
            current[nested_key] = nested
            continue
        if indent == 6 and nested is not None and ":" in line:
            key, value = line.split(":", 1)
            nested[key.strip()] = _parse_scalar(value)
            continue
        if indent == 4 and ":" in line:
            nested_key = None
            nested = None
            key, value = line.split(":", 1)
            current[key.strip()] = _parse_scalar(value)
            continue
    if current is not None:
        cases.append(current)
    return {"cases": cases}


def _load_matrix() -> list[dict[str, Any]]:
    raw = _load_simple_yaml_matrix(MATRIX_PATH.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    cases = raw["cases"]
    assert isinstance(cases, list) and cases
    return cases


def _caps_for_case(case: Mapping[str, Any]) -> Mapping[str, Any] | None:
    discovery = case.get("discovery")
    if discovery == "unavailable":
        raise ConnectionError("capabilities discovery unavailable")
    if discovery == "malformed":
        return {"not": "a capabilities document"}
    if discovery == "unknown":
        return None

    body = dict(SUPPORTED_CAPABILITIES)
    body["protocol_major"] = int(case["queue_protocol_major"])
    flag = case.get("durable_idempotent_enqueue")
    if flag is True:
        body["enqueue_dedup_ttl_seconds"] = 7776000
        body["durable_idempotent_enqueue"] = True
    elif flag is False:
        body["enqueue_dedup_ttl_seconds"] = 0
        body["durable_idempotent_enqueue"] = False
    elif flag is None:
        body.pop("enqueue_dedup_ttl_seconds", None)
        body.pop("durable_idempotent_enqueue", None)
    return body


def _expected_status(case: Mapping[str, Any]) -> CompatibilityStatus:
    expected = case["expected"]
    if expected == "supported":
        return CompatibilityStatus.SUPPORTED
    if expected == "unknown_extensible":
        return CompatibilityStatus.UNKNOWN
    if expected == "rejected":
        return CompatibilityStatus.INCOMPATIBLE
    raise AssertionError(f"unknown expected={expected!r}")


def test_matrix_file_declares_required_rows() -> None:
    ids = {c["id"] for c in _load_matrix()}
    required = {
        "intent-1.0_queue-1_cap-present_supported",
        "intent-1.1-additive_queue-1_cap-present_supported",
        "intent-2.0_queue-1_cap-present_rejected",
        "intent-1.0_queue-2_cap-present_rejected",
        "intent-1.0_queue-1_cap-missing_rejected",
    }
    assert required <= ids


@pytest.mark.parametrize(
    "case",
    _load_matrix(),
    ids=lambda c: c["id"] if isinstance(c, dict) else str(c),
)
def test_matrix(case: dict[str, Any]) -> None:
    compat = BridgeCompatibility()
    discovery_error: str | None = None
    caps: Mapping[str, Any] | None
    try:
        caps = _caps_for_case(case)
    except ConnectionError as exc:
        caps = None
        discovery_error = str(exc)

    result = compat.evaluate(
        capabilities=caps,
        intent_schema_major=int(case["intent_schema_major"]),
        intent_schema_minor=int(case["intent_schema_minor"]),
        intent_extensions=dict(case.get("intent_extensions") or {}),
        bridge_policy=str(case["bridge_policy"]),
        unknown_extensible_error=case.get("unknown_extensible_error"),
        discovery_error=discovery_error,
    )
    assert result.status is _expected_status(case)
    if case["expected"] == "supported":
        assert result.allows_poll is True
    if case["expected"] == "rejected":
        assert result.allows_poll is False
    if case.get("preserve_pending"):
        assert result.preserves_pending_intents is True
        assert result.mutates_persisted_intent is False


def _utc() -> datetime:
    return datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone.utc)


def _task(task_id: str = "task-1") -> Task:
    return Task(
        task_id=task_id,
        queue_name="orders",
        producer_id="producer-1",
        state=TaskState("ready"),
        priority=0,
        available_at="2026-09-19T12:00:00Z",
        retry_policy_version=1,
        created_at="2026-09-19T12:00:00Z",
        spawned_task_ids=(),
        delivery_event_ids=(),
        payload={"order_id": 1},
    )


def _intent(
    *,
    namespace: str = "orders.checkout",
    row_id: str = "row-1",
    schema_version: int = 1,
    extensions: Mapping[str, Any] | None = None,
) -> OutboxIntent:
    return OutboxIntent(
        source_namespace=namespace,
        source_row_id=row_id,
        schema_version=schema_version,
        target_queue="orders",
        enqueue_request={"payload": {"order_id": 1}, "priority": 0},
        created_at=_utc(),
        ownership_token="lease-token-1",
        generation=1,
        lease_expires_at=_utc() + timedelta(seconds=30),
        extensions=dict(extensions) if extensions else None,
    )


@dataclass
class _RecordingStore:
    rows: dict[tuple[str, str], OutboxIntent] = field(default_factory=dict)
    claim_calls: int = 0
    delivered: list[str] = field(default_factory=list)

    def seed(self, intent: OutboxIntent) -> None:
        self.rows[(intent.source_namespace, intent.source_row_id)] = intent

    def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
        del lease_seconds
        self.claim_calls += 1
        claimed: list[OutboxIntent] = []
        for key, intent in list(self.rows.items()):
            if len(claimed) >= limit:
                break
            claimed.append(intent)
            del self.rows[key]
        return claimed

    def mark_delivered(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        queue_task_id: str | None = None,
    ) -> bool:
        del ownership_token, queue_task_id
        self.delivered.append(f"{source_namespace}:{source_row_id}")
        return True

    def schedule_retry(self, **kwargs: Any) -> bool:
        del kwargs
        return True

    def mark_terminal_operator_action(self, **kwargs: Any) -> bool:
        del kwargs
        return True

    def get_pending_depth(self, depth_cap: int) -> BoundedPendingDepth:
        count = min(len(self.rows), depth_cap)
        return BoundedPendingDepth(
            count=count,
            depth_cap=depth_cap,
            capped=len(self.rows) > depth_cap,
            as_of=_utc(),
        )

    def get_oldest_pending_created_at(self) -> OldestPendingSnapshot:
        if not self.rows:
            return OldestPendingSnapshot(created_at=None, as_of=_utc())
        oldest = min(i.created_at for i in self.rows.values())
        return OldestPendingSnapshot(created_at=oldest, as_of=_utc())

    def get_health_snapshot(self, depth_cap: int) -> AppStoreHealthSnapshot:
        depth = self.get_pending_depth(depth_cap)
        oldest = self.get_oldest_pending_created_at()
        return AppStoreHealthSnapshot(
            as_of=_utc(),
            connected=True,
            query_ok=True,
            pending_count=depth.count,
            pending_capped=depth.capped,
            oldest_pending_created_at=oldest.created_at,
        )


@dataclass
class _RecordingProducer:
    enqueue_calls: int = 0
    keys: list[str] = field(default_factory=list)

    def _enqueue_with_available_at_raw(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = 0,
        available_at: str | None | object = ...,
    ) -> EnqueueResponse:
        del queue_name, payload, priority, available_at
        self.enqueue_calls += 1
        self.keys.append(idempotency_key)
        return EnqueueResponse(
            task=_task(task_id=f"task-{self.enqueue_calls}"),
            replayed=self.enqueue_calls > 1,
        )


def _runner(
    store: _RecordingStore,
    producer: _RecordingProducer,
    *,
    capabilities_fetcher: Callable[[], Mapping[str, Any] | None],
    telemetry: BridgeTelemetry | None = None,
) -> BridgeRunner:
    return BridgeRunner(
        store=store,
        producer=producer,
        batch_size=8,
        lease_seconds=30,
        max_in_flight=1,
        idle_poll_seconds=0.0,
        initial_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        backoff_jitter_ratio=0.0,
        telemetry=telemetry,
        capabilities_fetcher=capabilities_fetcher,
    )


def test_pre_poll_fail_closed_missing_capability() -> None:
    store = _RecordingStore()
    store.seed(_intent())
    producer = _RecordingProducer()
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=50,
        wall_clock=_utc,
        clock=lambda: 0.0,
    )
    caps = dict(SUPPORTED_CAPABILITIES)
    caps["enqueue_dedup_ttl_seconds"] = 0
    caps["durable_idempotent_enqueue"] = False
    runner = _runner(store, producer, capabilities_fetcher=lambda: caps, telemetry=tel)
    assert runner.poll_once() == 0
    assert store.claim_calls == 0
    assert producer.enqueue_calls == 0
    health = tel.refresh_from_store(store)
    assert health.queue_compatible is False
    assert health.ready is False


def test_pre_poll_fail_closed_malformed_capabilities() -> None:
    store = _RecordingStore()
    store.seed(_intent())
    producer = _RecordingProducer()
    runner = _runner(store, producer, capabilities_fetcher=lambda: {"bogus": True})
    assert runner.poll_once() == 0
    assert store.claim_calls == 0
    assert producer.enqueue_calls == 0


def test_pre_poll_fail_closed_discovery_unavailable() -> None:
    store = _RecordingStore()
    store.seed(_intent())
    producer = _RecordingProducer()

    def boom() -> Mapping[str, Any]:
        raise TimeoutError("capabilities unreachable")

    runner = _runner(store, producer, capabilities_fetcher=boom)
    assert runner.poll_once() == 0
    assert store.claim_calls == 0
    assert producer.enqueue_calls == 0


def test_unknown_required_capability_zero_store_claims() -> None:
    store = _RecordingStore()
    store.seed(_intent())
    producer = _RecordingProducer()
    runner = _runner(store, producer, capabilities_fetcher=lambda: None)
    assert runner.poll_once() == 0
    assert store.claim_calls == 0
    assert producer.enqueue_calls == 0


def test_supported_caps_allow_claim_and_enqueue() -> None:
    store = _RecordingStore()
    store.seed(_intent())
    producer = _RecordingProducer()
    runner = _runner(
        store, producer, capabilities_fetcher=lambda: dict(SUPPORTED_CAPABILITIES)
    )
    assert runner.poll_once() == 1
    assert store.claim_calls == 1
    assert producer.enqueue_calls == 1
    assert store.delivered == ["orders.checkout:row-1"]


def test_unknown_extensible_error_is_not_success() -> None:
    compat = BridgeCompatibility()
    result = compat.classify_extensible_error(ErrorCode.parse("brand_new_error"))
    assert result.status is CompatibilityStatus.UNKNOWN
    assert result.treat_as_success is False


def test_additive_intent_fields_ignored_by_older_tolerant_policy() -> None:
    compat = BridgeCompatibility()
    result = compat.evaluate(
        capabilities=dict(SUPPORTED_CAPABILITIES),
        intent_schema_major=1,
        intent_schema_minor=1,
        intent_extensions={"new_optional": "x"},
        bridge_policy="older_tolerant",
    )
    assert result.status is CompatibilityStatus.SUPPORTED
    assert "new_optional" in result.ignored_extension_keys


def test_downgrade_preserves_pending_and_does_not_mutate_intent() -> None:
    compat = BridgeCompatibility()
    result = compat.evaluate(
        capabilities=dict(SUPPORTED_CAPABILITIES),
        intent_schema_major=2,
        intent_schema_minor=0,
        intent_extensions={},
        bridge_policy="current",
    )
    assert result.status is CompatibilityStatus.INCOMPATIBLE
    assert result.preserves_pending_intents is True
    assert result.mutates_persisted_intent is False


def test_mixed_supported_replicas_50_replays_one_task() -> None:
    """Mixed old/new bridge replicas preserve Plan 02 golden key + one Queue identity."""
    ns, row = "orders.checkout", "outbox-row-42"
    key_a = bridge_idempotency_key(ns, row)
    key_b = bridge_idempotency_key(ns, row)
    assert key_a == key_b
    assert key_a.startswith("bridge:v1:")

    seen_keys: list[str] = []
    for i in range(50):
        store = _RecordingStore()
        store.seed(
            _intent(
                namespace=ns,
                row_id=row,
                extensions={"additive": True} if i % 2 else None,
            )
        )
        producer = _RecordingProducer()
        runner = _runner(
            store,
            producer,
            capabilities_fetcher=lambda: dict(SUPPORTED_CAPABILITIES),
        )
        assert runner.poll_once() == 1
        assert producer.enqueue_calls == 1
        seen_keys.append(producer.keys[0])

    assert len(set(seen_keys)) == 1
    assert seen_keys[0] == key_a


def test_version_axes_remain_distinct() -> None:
    compat = BridgeCompatibility()
    axes = compat.version_axes(
        capabilities=dict(SUPPORTED_CAPABILITIES),
        intent_schema_major=1,
        intent_schema_minor=1,
        bridge_package_version="0.6.7",
    )
    assert axes["queue_protocol_major"] == 1
    assert axes["queue_schema_revision"] == "0001"
    assert axes["intent_schema_major"] == 1
    assert axes["intent_schema_minor"] == 1
    assert axes["bridge_package_version"] == "0.6.7"
    assert axes["queue_protocol_major"] != axes["bridge_package_version"]
    assert axes["queue_schema_revision"] != axes["bridge_package_version"]


def test_compatibility_exports() -> None:
    assert CompatibilityResult is not None
    assert BridgeCompatibility is not None
    assert CompatibilityStatus.SUPPORTED.value == "supported"


# ---------------------------------------------------------------------------
# Phase 12 Wave 0 scaffolds (WORK-16 priority capability mirrors)
# ---------------------------------------------------------------------------


def test_zero_priority_major_1_intent_supported_with_priority_capability_true() -> None:
    compat = BridgeCompatibility()
    result = compat.evaluate(
        capabilities=dict(SUPPORTED_CAPABILITIES),
        intent_schema_major=1,
        intent_schema_minor=0,
        intent_extensions={},
        bridge_policy="current",
    )
    assert result.status is CompatibilityStatus.SUPPORTED
    assert SUPPORTED_CAPABILITIES["priority"] is True


def test_mixed_replica_zero_priority_intent_preserves_queue_identity() -> None:
    """Rolling upgrade: major-1 zero-priority intents stay compatible."""
    ns, row = "orders.checkout", "priority-rollout-row"
    for _ in range(10):
        store = _RecordingStore()
        store.seed(
            OutboxIntent(
                source_namespace=ns,
                source_row_id=row,
                schema_version=1,
                target_queue="orders",
                enqueue_request={"payload": {"order_id": 1}, "priority": 0},
                created_at=_utc(),
                ownership_token="lease-token-1",
                generation=1,
                lease_expires_at=_utc() + timedelta(seconds=30),
            )
        )
        producer = _RecordingProducer()
        runner = _runner(
            store,
            producer,
            capabilities_fetcher=lambda: dict(SUPPORTED_CAPABILITIES),
        )
        assert runner.poll_once() == 1
        assert producer.enqueue_calls == 1


def test_live_openapi_harness_and_bridge_priority_true_equality() -> None:
    """Plan 12-11 activates priority=true across every authenticated mirror."""
    import json

    from queue_service.api.v1.capabilities import LIVE_CAPABILITIES
    from tests.conformance.test_harness_self import CAPABILITIES_BODY
    from tests.contracts.test_openapi_contract import CAPABILITIES_CONSTS

    openapi_path = Path(__file__).resolve().parents[3] / "openapi" / "queue.openapi.json"
    caps = json.loads(openapi_path.read_text(encoding="utf-8"))["components"]["schemas"][
        "Capabilities"
    ]["properties"]

    assert SUPPORTED_CAPABILITIES["priority"] is True
    assert LIVE_CAPABILITIES["priority"] is True
    assert CAPABILITIES_BODY["priority"] is True
    assert CAPABILITIES_CONSTS["priority"] is True
    assert caps["priority"]["const"] is True
    assert (
        SUPPORTED_CAPABILITIES["priority"]
        == LIVE_CAPABILITIES["priority"]
        == CAPABILITIES_BODY["priority"]
        == CAPABILITIES_CONSTS["priority"]
        == caps["priority"]["const"]
    )
    assert SUPPORTED_CAPABILITIES["scheduling"] is True
    assert SUPPORTED_CAPABILITIES["protocol_major"] == 1
    assert SUPPORTED_CAPABILITIES["schema_revision"] == "0001"
