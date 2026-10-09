"""Public wire-model fixtures (capabilities, tasks, claims, pages)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.models import Task

from queue_service_client_testing._secrets import (
    register_secret,
    synthetic_bearer_token,
    synthetic_claim_token,
    synthetic_idempotency_key,
)


def capabilities_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
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
    return body


def capabilities(**overrides: Any) -> Capabilities:
    return Capabilities.parse(capabilities_body(**overrides))


def task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
        "payload": {"example": True},
    }
    body.update(overrides)
    return body


def task(**overrides: Any) -> Task:
    return Task.parse(task_body(**overrides))


def claim_grant_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "generation": 1,
        "claimed_at": "2026-09-19T00:01:00Z",
        "lease_expires_at": "2026-09-19T00:02:00Z",
        "worker_id": "worker-1",
        "cancel_requested": False,
        "claim_token": synthetic_claim_token(),
    }
    body.update(overrides)
    token = body.get("claim_token")
    if isinstance(token, str):
        register_secret(token)
    return body


def claim_summary_body(**overrides: Any) -> dict[str, Any]:
    grant = claim_grant_body(**overrides)
    grant.pop("claim_token", None)
    return grant


def claim_response_body(
    *,
    empty: bool = False,
    task_overrides: Mapping[str, Any] | None = None,
    claim_overrides: Mapping[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    if not empty:
        tasks.append(
            {
                "task": task_body(**dict(task_overrides or {}), state="leased"),
                "claim": claim_grant_body(**dict(claim_overrides or {})),
            }
        )
    body: dict[str, Any] = {
        "tasks": tasks,
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }
    body.update(overrides)
    return body


def enqueue_response_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task": task_body(),
        "replayed": False,
    }
    body.update(overrides)
    return body


def page_body(
    items: Sequence[Mapping[str, Any]],
    *,
    next_cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "items": [dict(item) for item in items],
        "next_cursor": next_cursor,
    }


def auth_headers(*, bearer_token: str | None = None) -> dict[str, str]:
    token = bearer_token if bearer_token is not None else synthetic_bearer_token()
    register_secret(token)
    return {"Authorization": f"Bearer {token}"}


def idempotency_headers(*, key: str | None = None) -> dict[str, str]:
    value = key if key is not None else synthetic_idempotency_key()
    register_secret(value)
    return {"Idempotency-Key": value}
