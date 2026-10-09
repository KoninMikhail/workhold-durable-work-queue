"""Fail-closed capability guards for reserved batch/long-poll/events shapes."""

from __future__ import annotations

import json
from typing import Any

import pytest

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.capability_guard import (
    require_batch_claim,
    require_delivery_events,
    require_long_polling,
)
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_consumer import ConsumerClient


def _caps(**overrides: object) -> Capabilities:
    body: dict[str, object] = {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": False,
        "max_claim_tasks": 1,
        "max_wait_seconds": 0,
        "payload_runtime_max_bytes": 262144,
        "payload_hard_max_bytes": 1048576,
        "enqueue_dedup_ttl_seconds": 7776000,
        "enqueue_dedup_ttl_min_seconds": 2592000,
        "enqueue_dedup_ttl_max_seconds": 31536000,
        "terminal_replay_ttl_seconds": 604800,
        "terminal_replay_ttl_min_seconds": 86400,
        "terminal_replay_ttl_max_seconds": 2592000,
        "admin_replay_ttl_seconds": 2592000,
        "admin_replay_ttl_min_seconds": 604800,
        "admin_replay_ttl_max_seconds": 7776000,
    }
    body.update(overrides)
    return Capabilities.parse(body)


def test_zero_wait_bypasses_long_polling_guard() -> None:
    require_long_polling(_caps(long_polling=False, max_wait_seconds=0), 0)


def test_positive_wait_requires_live_long_polling() -> None:
    with pytest.raises(ValueError, match="long_polling"):
        require_long_polling(_caps(long_polling=False, max_wait_seconds=0), 15)


def test_positive_wait_requires_positive_exact_maximum() -> None:
    with pytest.raises(ValueError, match="max_wait_seconds"):
        require_long_polling(_caps(long_polling=True, max_wait_seconds=0), 1)


def test_wait_cannot_exceed_advertised_maximum() -> None:
    with pytest.raises(ValueError, match="exceeds"):
        require_long_polling(_caps(long_polling=True, max_wait_seconds=20), 21)


def test_wait_within_advertised_maximum_passes() -> None:
    require_long_polling(_caps(long_polling=True, max_wait_seconds=20), 15)
    require_long_polling(_caps(long_polling=True, max_wait_seconds=20), 20)


def test_bool_wait_fails_closed() -> None:
    with pytest.raises(ValueError, match="integer"):
        require_long_polling(_caps(long_polling=True, max_wait_seconds=20), True)  # type: ignore[arg-type]


def test_batch_claim_disabled_rejects_max_tasks_gt_one() -> None:
    require_batch_claim(_caps(batch_claim=False, max_claim_tasks=1), 1)
    with pytest.raises(ValueError, match="batch_claim"):
        require_batch_claim(_caps(batch_claim=False, max_claim_tasks=1), 2)


def test_batch_claim_enabled_allows_within_advertised_maximum() -> None:
    caps = _caps(batch_claim=True, max_claim_tasks=4)
    require_batch_claim(caps, 1)
    require_batch_claim(caps, 4)
    with pytest.raises(ValueError, match="exceeds"):
        require_batch_claim(caps, 5)


def test_delivery_events_empty_bypasses_guard() -> None:
    require_delivery_events(_caps(delivery_events=False), None)
    require_delivery_events(_caps(delivery_events=False), [])


def test_delivery_events_require_live_capability() -> None:
    with pytest.raises(ValueError, match="delivery_events"):
        require_delivery_events(
            _caps(delivery_events=False),
            [{"specversion": "1.0", "type": "com.example.done", "source": "/app", "id": "1"}],
        )


def test_delivery_events_enabled_requires_object_items() -> None:
    caps = _caps(delivery_events=True)
    require_delivery_events(
        caps,
        [{"specversion": "1.0", "type": "com.example.done", "source": "/app", "id": "1"}],
    )
    with pytest.raises(ValueError, match="events\\[0\\]"):
        require_delivery_events(caps, ["not-an-object"])  # type: ignore[list-item]


class _CountingTransport:
    def __init__(self) -> None:
        self.calls = 0

    def request(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        raise AssertionError("HTTP must not be issued when capability is disabled")


def test_mvp_max_tasks_gt_one_makes_zero_http_requests() -> None:
    transport = _CountingTransport()
    client = ConsumerClient(transport, bearer_token="tok")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_tasks=1"):
        client.claim(
            queues=["orders"],
            worker_id="w1",
            lease_seconds=30,
            max_tasks=2,
            capabilities=_caps(batch_claim=True, max_claim_tasks=4),
        )
    assert transport.calls == 0


def test_disabled_events_make_zero_http_requests() -> None:
    claim_body = {
        "tasks": [
            {
                "task": {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "queue_name": "orders",
                    "producer_id": "p1",
                    "state": "leased",
                    "priority": 0,
                    "available_at": "2026-09-19T00:00:00Z",
                    "retry_policy_version": 1,
                    "created_at": "2026-09-19T00:00:00Z",
                    "spawned_task_ids": [],
                    "delivery_event_ids": [],
                    "payload": {},
                },
                "claim": {
                    "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    "generation": 1,
                    "claimed_at": "2026-09-19T00:01:00Z",
                    "lease_expires_at": "2026-09-19T00:02:00Z",
                    "worker_id": "w1",
                    "cancel_requested": False,
                    "claim_token": "claim-secret",
                },
            }
        ],
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }

    class _Transport:
        def __init__(self) -> None:
            self.paths: list[str] = []

        def request(self, method: str, path: str, **kwargs: Any) -> Any:
            self.paths.append(path)
            if path == "/v1/claims":

                class _Resp:
                    status_code = 200
                    body = claim_body

                return _Resp()
            raise AssertionError(f"unexpected HTTP {method} {path}")

    transport = _Transport()
    client = ConsumerClient(transport, bearer_token="tok")  # type: ignore[arg-type]
    claim = client.claim(queues=["orders"], worker_id="w1", lease_seconds=30)[0]
    with pytest.raises(ValueError, match="delivery_events"):
        claim.complete(
            events=[{"specversion": "1.0", "type": "t", "source": "/s", "id": "1"}],
            capabilities=_caps(delivery_events=False),
        )
    assert transport.paths == ["/v1/claims"]


def test_enabled_events_serialize_reserved_shapes_with_mvp_claim() -> None:
    recorded: list[dict[str, Any]] = []
    caps = _caps(
        batch_claim=True,
        max_claim_tasks=3,
        delivery_events=True,
        long_polling=False,
        max_wait_seconds=0,
    )
    claim_body = {
        "tasks": [
            {
                "task": {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "queue_name": "orders",
                    "producer_id": "p1",
                    "state": "leased",
                    "priority": 0,
                    "available_at": "2026-09-19T00:00:00Z",
                    "retry_policy_version": 1,
                    "created_at": "2026-09-19T00:00:00Z",
                    "spawned_task_ids": [],
                    "delivery_event_ids": [],
                    "payload": {},
                },
                "claim": {
                    "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    "generation": 2,
                    "claimed_at": "2026-09-19T00:01:00Z",
                    "lease_expires_at": "2026-09-19T00:02:00Z",
                    "worker_id": "w1",
                    "cancel_requested": False,
                    "claim_token": "claim-secret",
                },
            }
        ],
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
        "future_hint": True,
    }

    class _Transport:
        def request(self, method: str, path: str, **kwargs: Any) -> Any:
            recorded.append({"path": path, "body": kwargs.get("json_body")})
            if path == "/v1/claims":

                class _ClaimResp:
                    status_code = 200
                    body = claim_body

                return _ClaimResp()

            class _CompleteResp:
                status_code = 200
                body = {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "state": "succeeded",
                    "spawned_task_ids": [],
                    "replayed": False,
                    "events": [{"id": "evt-1"}],
                }

            return _CompleteResp()

    client = ConsumerClient(_Transport(), bearer_token="tok")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_tasks=1"):
        client.claim(
            queues=["orders"],
            worker_id="w1",
            lease_seconds=30,
            max_tasks=2,
            capabilities=caps,
        )
    assert recorded == []

    claims = client.claim(
        queues=["orders"],
        worker_id="w1",
        lease_seconds=30,
        max_tasks=1,
        capabilities=caps,
    )
    assert len(claims) == 1
    event = {
        "specversion": "1.0",
        "type": "com.example.done",
        "source": "/app",
        "id": "evt-1",
    }
    result = claims[0].complete(events=[event], capabilities=caps)
    assert result.extra.get("events") == [{"id": "evt-1"}]
    assert recorded[0]["body"]["max_tasks"] == 1
    assert recorded[1]["body"]["events"] == [event]
    # Ensure reserved keys are JSON-serializable OpenAPI-shaped objects.
    json.dumps(recorded[0]["body"])
    json.dumps(recorded[1]["body"])
