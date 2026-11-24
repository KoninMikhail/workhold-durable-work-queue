"""Sealed task payloads keep enqueue idempotency intact at the public `/v1` edge.

The server fingerprints the JSON it stores. A client that seals a payload with a
shared content key must therefore get a replay for an identical request and an
`idempotency_conflict` when the plaintext changes.
"""

from __future__ import annotations

import json
from typing import Any

from _workhold_client_core.content_key import seal_task_payload
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from tests.conformance.harness import ConformanceHarness
from tests.conformance.test_enqueue_http import (
    OPENAPI_PATH,
    _assert_error,
    _asgi_http_call,
    _enqueue_body,
    _producer_headers,
    _seed_queue,
    _validate_enqueue_response,
)

# Reuse the enqueue HTTP fixtures (they override the conformance conftest versions,
# which use different principals and queue names).
from tests.conformance.test_enqueue_http import (  # noqa: F401, E402
    app,
    authorizer,
    queue_name,
    session_factory,
)
from workhold.storage.models import TaskActive, TaskPayloadActive

# Fixture-only key; never a real secret.
KEY_BYTES = bytes(range(32))
KEY_BASE64 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
PLAINTEXT_SENTINEL = "plaintext-sentinel"


def _post(app: Any, *, path: str, headers: dict[str, str], body: dict[str, Any]):
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    return _asgi_http_call(app, method="POST", path=path, headers=headers, body=raw)


def test_sealed_enqueue_replays_same_task(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    plaintext = {"order_id": 7, "sku": PLAINTEXT_SENTINEL}
    envelope = seal_task_payload(plaintext, KEY_BYTES)
    path = f"/v1/queues/{queue_name}/tasks"
    headers = _producer_headers(idempotency_key="idem-sealed-replay-1")
    body = _enqueue_body(payload=envelope)

    status, resp_headers, resp_body = _post(app, path=path, headers=headers, body=body)
    assert status == 201
    first = _validate_enqueue_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    assert first["replayed"] is False
    task_id = first["task"]["task_id"]

    status2, resp_headers2, resp_body2 = _post(
        app, path=path, headers=headers, body=_enqueue_body(payload=envelope)
    )
    assert status2 == 200
    second = _validate_enqueue_response(
        harness, status=status2, headers=resp_headers2, body=resp_body2
    )
    assert second["replayed"] is True
    assert second["task"]["task_id"] == task_id

    for raw_response in (resp_body, resp_body2):
        text = raw_response.decode("utf-8")
        assert KEY_BASE64 not in text
        assert PLAINTEXT_SENTINEL not in text

    session = session_factory()
    try:
        stored = session.execute(
            select(TaskPayloadActive.payload)
            .join(TaskActive, TaskActive.id == TaskPayloadActive.task_id)
            .where(TaskActive.task_id == task_id)
        ).scalar_one()
    finally:
        session.close()
    assert stored == envelope
    assert stored != plaintext


def test_sealed_enqueue_changed_plaintext_conflicts(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    headers = _producer_headers(idempotency_key="idem-sealed-conflict-1")

    status, resp_headers, resp_body = _post(
        app,
        path=path,
        headers=headers,
        body=_enqueue_body(payload=seal_task_payload({"order_id": 7}, KEY_BYTES)),
    )
    assert status == 201
    _validate_enqueue_response(harness, status=status, headers=resp_headers, body=resp_body)

    status2, _resp_headers2, resp_body2 = _post(
        app,
        path=path,
        headers=headers,
        body=_enqueue_body(payload=seal_task_payload({"order_id": 8}, KEY_BYTES)),
    )
    assert status2 == 409
    _assert_error(resp_body2, code="idempotency_conflict", retryable=False)
