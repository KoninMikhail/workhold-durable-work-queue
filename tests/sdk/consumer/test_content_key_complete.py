"""Sync complete seals spawn payload with the consumer content key."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.codecs import measure_json_bytes
from _workhold_client_core.content_key import (
    ContentDecryptError,
    is_content_envelope,
    open_task_payload,
)
from _workhold_client_core.errors import TerminalConflictError
from workhold_consumer import ConsumerClient

KEY = b"\x11" * 32
WRONG_KEY = b"\x22" * 32
PLAINTEXT = {"sku": "spawn-plain-enc09"}
BEARER = "worker-bearer"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _claim_response(payload: Any) -> dict[str, Any]:
    return {
        "tasks": [
            {
                "task": {
                    "task_id": TASK_ID,
                    "queue_name": "orders",
                    "producer_id": "producer-1",
                    "state": "leased",
                    "priority": 0,
                    "available_at": "2026-09-19T00:00:00Z",
                    "retry_policy_version": 1,
                    "created_at": "2026-09-19T00:00:00Z",
                    "spawned_task_ids": [],
                    "delivery_event_ids": [],
                    "payload": payload,
                },
                "claim": {
                    "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    "generation": 1,
                    "claimed_at": "2026-09-19T00:01:00Z",
                    "lease_expires_at": "2026-09-19T00:02:00Z",
                    "worker_id": "worker-1",
                    "cancel_requested": False,
                    "claim_token": "claim-token",
                },
            }
        ],
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }


class _Response:
    def __init__(self, body: Any) -> None:
        self.status_code = 200
        self.body = body


class _Transport:
    """One claim grant, then record complete bodies. Later claims echo a payload."""

    def __init__(
        self,
        complete_bodies: list[dict[str, Any]],
        *,
        claim_payload: Any | None = None,
    ) -> None:
        self._complete_bodies = complete_bodies
        self._claim_payload: Any = {"seed": True} if claim_payload is None else claim_payload

    def request(self, method: str, path: str, **kwargs: Any) -> _Response:
        if path == "/v1/claims":
            return _Response(_claim_response(self._claim_payload))
        if path.endswith(":complete"):
            self._complete_bodies.append(kwargs["json_body"])
            return _Response(
                {
                    "task_id": TASK_ID,
                    "state": "succeeded",
                    "spawned_task_ids": [],
                    "replayed": False,
                }
            )
        raise AssertionError(f"unexpected path {path}")


def _spawn_item(payload: Any) -> dict[str, Any]:
    return {
        "queue_name": "orders",
        "idempotency_key": "spawn-enc09",
        "payload": payload,
        "priority": 4,
        "available_at": "2026-11-23T00:00:00Z",
    }


def _complete(
    item: dict[str, Any],
    complete_bodies: list[dict[str, Any]],
    *,
    content_key: bytes | None = None,
    max_payload_bytes: int | None = None,
) -> None:
    kwargs: dict[str, Any] = {}
    if content_key is not None:
        kwargs["content_key"] = content_key
    client = ConsumerClient(_Transport(complete_bodies), bearer_token=BEARER, **kwargs)
    claim = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]
    claim.complete(spawn=[item], max_payload_bytes=max_payload_bytes)


def _claim_with(payload: Any, *, content_key: bytes | None = None) -> Any:
    kwargs: dict[str, Any] = {}
    if content_key is not None:
        kwargs["content_key"] = content_key
    client = ConsumerClient(
        _Transport([], claim_payload=payload),
        bearer_token=BEARER,
        **kwargs,
    )
    return client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]


def _assert_identity(wire: dict[str, Any], item: dict[str, Any]) -> None:
    assert wire["queue_name"] == item["queue_name"]
    assert wire["idempotency_key"] == item["idempotency_key"]
    assert wire["priority"] == item["priority"]
    assert wire["available_at"] == item["available_at"]


def test_spawn_payload_plaintext_without_key() -> None:
    payload = {"sku": "spawn-plain-enc09"}
    item = _spawn_item(payload)
    complete_bodies: list[dict[str, Any]] = []

    _complete(item, complete_bodies)

    wire = complete_bodies[0]["spawn"][0]
    assert wire["payload"] is payload
    _assert_identity(wire, item)
    assert item["payload"] is payload


def test_spawn_payload_round_trip() -> None:
    first_payload = {"sku": "spawn-plain-enc09"}
    second_payload = {"sku": "spawn-plain-enc09"}
    first_item = _spawn_item(first_payload)
    second_item = _spawn_item(second_payload)
    first_bodies: list[dict[str, Any]] = []
    second_bodies: list[dict[str, Any]] = []

    _complete(first_item, first_bodies, content_key=KEY)
    _complete(second_item, second_bodies, content_key=KEY)

    recorded = first_bodies[0]["spawn"][0]["payload"]
    assert is_content_envelope(recorded)
    body_text = json.dumps(first_bodies[0])
    assert base64.b64encode(KEY).decode("ascii") not in body_text
    assert "spawn-plain-enc09" not in body_text
    assert recorded == second_bodies[0]["spawn"][0]["payload"]
    _assert_identity(first_bodies[0]["spawn"][0], first_item)
    assert first_item["payload"] is first_payload

    restored = _claim_with(recorded, content_key=KEY)
    assert restored.task.payload == PLAINTEXT


def test_spawn_payload_wrong_key() -> None:
    bodies: list[dict[str, Any]] = []
    _complete(_spawn_item({"sku": "spawn-plain-enc09"}), bodies, content_key=KEY)
    envelope = bodies[0]["spawn"][0]["payload"]

    with pytest.raises(ContentDecryptError) as excinfo:
        _claim_with(envelope, content_key=WRONG_KEY)

    err = excinfo.value
    text = f"{err!s}{err!r}"
    assert str(err) == "content_decrypt_failed"
    assert "spawn-plain-enc09" not in text
    assert base64.b64encode(KEY).decode("ascii") not in text
    assert base64.b64encode(WRONG_KEY).decode("ascii") not in text
    assert str(KEY) not in text
    assert str(WRONG_KEY) not in text


def test_spawn_payload_passthrough() -> None:
    plain = {"sku": "kept-plaintext"}
    assert not is_content_envelope(plain)
    keyed = _claim_with(plain, content_key=KEY)
    assert keyed.task.payload == plain

    bodies: list[dict[str, Any]] = []
    _complete(_spawn_item({"sku": "spawn-plain-enc09"}), bodies, content_key=KEY)
    envelope = bodies[0]["spawn"][0]["payload"]
    unkeyed = _claim_with(envelope)
    assert is_content_envelope(unkeyed.task.payload)
    assert unkeyed.task.payload["$workhold_enc"] == 1
    assert unkeyed.task.payload == envelope


def test_spawn_envelope_oversize_rejected_before_http() -> None:
    payload = {"sku": "spawn-plain-enc09"}
    item = _spawn_item(payload)
    complete_bodies: list[dict[str, Any]] = []
    limit = measure_json_bytes(payload)

    with pytest.raises(ValueError, match="payload exceeds"):
        _complete(item, complete_bodies, content_key=KEY, max_payload_bytes=limit)

    assert complete_bodies == []


_MISSING = object()
_EVENT_FIELDS = (
    "source",
    "type",
    "subject",
    "datacontenttype",
    "extensions",
)


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


def _event(data: Any = _MISSING) -> dict[str, Any]:
    event: dict[str, Any] = {
        "source": "/orders",
        "type": "com.example.shipped",
        "subject": "order-1",
        "datacontenttype": "application/json",
        "extensions": {"trace": "abc"},
    }
    if data is not _MISSING:
        event["data"] = data
    return event


def _assert_event_identity(wire: dict[str, Any], event: dict[str, Any]) -> None:
    for field in _EVENT_FIELDS:
        assert wire[field] == event[field]


def _open_claim(
    complete_bodies: list[dict[str, Any]],
    *,
    content_key: bytes | None = None,
) -> Any:
    kwargs: dict[str, Any] = {}
    if content_key is not None:
        kwargs["content_key"] = content_key
    client = ConsumerClient(
        _Transport(complete_bodies),
        bearer_token=BEARER,
        **kwargs,
    )
    return client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]


def _assert_key_absent(text: str) -> None:
    assert base64.b64encode(KEY).decode("ascii") not in text
    assert base64.b64encode(WRONG_KEY).decode("ascii") not in text
    assert str(KEY) not in text
    assert str(WRONG_KEY) not in text


def test_event_data_round_trip_identity_plaintext() -> None:
    sentinel = {"sku": "event-plain-enc10"}
    with_data = _event(sentinel)
    without_data = _event()
    bodies: list[dict[str, Any]] = []

    claim = _open_claim(bodies, content_key=KEY)
    claim.complete(
        events=[with_data, without_data],
        capabilities=_caps(delivery_events=True),
    )

    recorded = bodies[0]["events"]
    assert is_content_envelope(recorded[0]["data"])
    assert open_task_payload(recorded[0]["data"], KEY) == sentinel
    _assert_event_identity(recorded[0], with_data)
    _assert_event_identity(recorded[1], without_data)
    assert "data" not in recorded[1]
    assert with_data["data"] is sentinel
    assert "data" not in without_data
    body_text = json.dumps(bodies[0])
    _assert_key_absent(body_text)
    assert "event-plain-enc10" not in body_text


def test_event_data_plaintext_without_key() -> None:
    sentinel = {"sku": "event-plain-enc10"}
    with_data = _event(sentinel)
    missing = _event()
    null_data = _event(None)
    bodies: list[dict[str, Any]] = []

    claim = _open_claim(bodies)
    claim.complete(
        events=[with_data, missing, null_data],
        capabilities=_caps(delivery_events=True),
    )

    recorded = bodies[0]["events"]
    assert recorded[0]["data"] is sentinel
    assert "data" not in recorded[1]
    assert recorded[2]["data"] is None
    assert with_data["data"] is sentinel
    assert "data" not in missing
    assert null_data["data"] is None


def test_event_data_null_is_sealed() -> None:
    event = _event(None)
    bodies: list[dict[str, Any]] = []

    claim = _open_claim(bodies, content_key=KEY)
    claim.complete(events=[event], capabilities=_caps(delivery_events=True))

    recorded = bodies[0]["events"][0]["data"]
    assert is_content_envelope(recorded)
    assert open_task_payload(recorded, KEY) is None
    assert event["data"] is None


def test_event_data_wrong_key_and_passthrough() -> None:
    sentinel = {"sku": "event-plain-enc10"}
    bodies: list[dict[str, Any]] = []

    claim = _open_claim(bodies, content_key=KEY)
    claim.complete(
        events=[_event(sentinel)],
        capabilities=_caps(delivery_events=True),
    )
    envelope = bodies[0]["events"][0]["data"]

    with pytest.raises(ContentDecryptError) as excinfo:
        open_task_payload(envelope, WRONG_KEY)

    err = excinfo.value
    text = f"{err!s}{err!r}"
    assert str(err) == "content_decrypt_failed"
    assert "event-plain-enc10" not in text
    _assert_key_absent(text)

    plain = {"sku": "kept-plaintext"}
    assert not is_content_envelope(plain)
    assert open_task_payload(plain, KEY) is plain
    assert open_task_payload(envelope, None) is envelope


def test_event_data_oversize_rejected_before_http() -> None:
    sentinel = {"sku": "event-plain-enc10"}
    bodies: list[dict[str, Any]] = []
    claim = _open_claim(bodies, content_key=KEY)
    limit = measure_json_bytes(sentinel)

    with pytest.raises(ValueError, match="payload exceeds"):
        claim.complete(
            events=[_event(sentinel)],
            capabilities=_caps(delivery_events=True),
            max_payload_bytes=limit,
        )

    assert bodies == []


def test_delivery_events_guard_still_blocks_http() -> None:
    sentinel = {"sku": "event-plain-enc10"}
    event = _event(sentinel)
    bodies: list[dict[str, Any]] = []
    claim = _open_claim(bodies, content_key=KEY)

    with pytest.raises(ValueError, match="delivery_events"):
        claim.complete(events=[event], capabilities=_caps(delivery_events=False))

    assert bodies == []
    assert event["data"] is sentinel


def test_complete_body_stable_for_same_key() -> None:
    caps = _caps(delivery_events=True)
    first_bodies: list[dict[str, Any]] = []
    second_bodies: list[dict[str, Any]] = []

    for bodies in (first_bodies, second_bodies):
        claim = _open_claim(bodies, content_key=KEY)
        claim.complete(
            spawn=[_spawn_item({"sku": "spawn-plain-enc09"})],
            events=[_event({"sku": "event-plain-enc10"})],
            capabilities=caps,
        )

    assert first_bodies[0] == second_bodies[0]
    assert is_content_envelope(first_bodies[0]["spawn"][0]["payload"])
    assert is_content_envelope(first_bodies[0]["events"][0]["data"])


def test_complete_retry_fingerprint_stable() -> None:
    bodies: list[dict[str, Any]] = []
    claim = _open_claim(bodies, content_key=KEY)
    caps = _caps(delivery_events=True)

    def complete(spawn_payload: dict[str, Any], event_data: dict[str, Any]) -> None:
        claim.complete(
            spawn=[_spawn_item(spawn_payload)],
            events=[_event(event_data)],
            capabilities=caps,
        )

    complete({"sku": "spawn-plain-enc09"}, {"sku": "event-plain-enc10"})
    complete({"sku": "spawn-plain-enc09"}, {"sku": "event-plain-enc10"})

    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert is_content_envelope(bodies[0]["spawn"][0]["payload"])
    assert is_content_envelope(bodies[0]["events"][0]["data"])

    with pytest.raises(TerminalConflictError) as changed_event:
        complete({"sku": "spawn-plain-enc09"}, {"sku": "event-plain-enc10-other"})

    assert changed_event.value.reason == "changed terminal body or operation"
    assert len(bodies) == 2
    err_text = str(changed_event.value)
    assert "event-plain-enc10" not in err_text
    assert "spawn-plain-enc09" not in err_text
    _assert_key_absent(err_text)

    with pytest.raises(TerminalConflictError) as changed_spawn:
        complete({"sku": "spawn-plain-other"}, {"sku": "event-plain-enc10"})

    assert changed_spawn.value.reason == "changed terminal body or operation"
    assert len(bodies) == 2
    spawn_text = str(changed_spawn.value)
    assert "event-plain-enc10" not in spawn_text
    assert "spawn-plain-enc09" not in spawn_text
    _assert_key_absent(spawn_text)
