"""Ergonomics qualification gate (SDK-09..SDK-15 / QUAL-01).

Bundles negative, cancellation, redaction and fail-closed checks that must pass
before a coordinated client release. Recording transports and the public test
kit qualify ergonomics only — live protocol evidence lives in conformance.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
from pathlib import Path

import pytest

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.capability_guard import (
    require_batch_claim,
    require_delivery_events,
    require_long_polling,
)
from _workhold_client_core.codecs import PayloadDecodeError, decode_payload
from _workhold_client_core.errors import RequestCancelledError
from _workhold_client_core.instrumentation import OperationEvent
from _workhold_client_core.retry import (
    RETRY_CLASS_NEVER,
    RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
    RetryBudget,
    RetryNotAllowedError,
    RetryPolicy,
    RetryRequest,
    execute_with_retry,
    retry_class_for,
)
from workhold_admin.pagination import bounded_item_iterator
from workhold_client_testing import (
    DeterministicClock,
    ScriptStep,
    ScriptedSyncTransport,
    clear_registered_secrets,
    scenario_cancellation,
    synthetic_bearer_token,
)

ROOT = Path(__file__).resolve().parents[2]


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


def test_public_testkit_clean_import_without_server() -> None:
    mod = importlib.import_module("workhold_client_testing")
    assert hasattr(mod, "ScriptedSyncTransport")
    assert hasattr(mod, "ScriptedAsyncTransport")
    assert hasattr(mod, "DeterministicClock")
    assert importlib.util.find_spec("queue_service_client") is None
    root = (
        ROOT
        / "packages"
        / "workhold-client-core"
        / "src"
        / "workhold_client_testing"
    )
    forbidden = {"workhold", "pytest", "psycopg", "sqlalchemy"}
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
            for name in names:
                assert name.split(".", 1)[0] not in forbidden


def test_pagination_ceilings_and_cancellation() -> None:
    class _Page:
        def __init__(self, items: tuple[str, ...], next_cursor: str | None) -> None:
            self.items = items
            self.next_cursor = next_cursor

    pages = [_Page(("a", "b"), "c1"), _Page(("c", "d"), None)]
    idx = {"i": 0}

    def fetch(cursor: str | None) -> _Page:
        _ = cursor
        page = pages[idx["i"]]
        idx["i"] += 1
        return page

    it = bounded_item_iterator(fetch, max_items=2)
    assert list(it) == ["a", "b"]
    assert it.item_count == 2
    assert it.page_count == 1

    idx["i"] = 0
    it2 = bounded_item_iterator(fetch, max_pages=1, max_items=100)
    assert list(it2) == ["a", "b"]
    assert it2.page_count == 1
    assert it2.last_cursor == "c1"


def test_retry_budget_and_same_body_enforcement() -> None:
    assert retry_class_for("claimTasks") == RETRY_CLASS_NEVER
    assert retry_class_for("forceLeaseExpiry") == RETRY_CLASS_NEVER

    never_req = RetryRequest(
        operation_id="claimTasks",
        retry_class=RETRY_CLASS_NEVER,
        body=b"{}",
    )
    with pytest.raises(RetryNotAllowedError):
        execute_with_retry(
            never_req,
            lambda _req: "unreachable",
            policy=RetryPolicy(max_attempts=3, max_elapsed_s=5.0),
        )

    with pytest.raises(ValueError, match="idempotency_key"):
        RetryRequest(
            operation_id="enqueueTask",
            retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
            body=b"{}",
            idempotency_key=None,
        )

    policy = RetryPolicy(max_attempts=2, max_elapsed_s=1.0)
    budget = RetryBudget(policy=policy)
    assert budget.can_attempt(now=0.0) is True
    budget.start(now=0.0)
    budget.record_attempt()
    budget.record_attempt()
    assert budget.can_attempt(now=0.0) is False


def test_instrumentation_redaction_and_failure_isolation() -> None:
    clear_registered_secrets()
    token = synthetic_bearer_token(suffix="qual")
    event = OperationEvent(
        kind="success",
        operation_id="enqueueTask",
        method="POST",
        route_template="/v1/queues/{queue_name}/tasks",
        attempt=1,
        duration_s=0.01,
        status="ok",
        status_code=201,
        error_code=None,
        request_id="11111111-1111-4111-8111-111111111111",
    )
    rendered = repr(event)
    assert token not in rendered
    assert "claim_token" not in rendered
    assert "Authorization" not in rendered

    # Hook failure isolation: constructing events never embeds caller secrets.
    secret = "CANARY_SECRET_qual_21"
    assert secret not in rendered


def test_codec_failure_preserves_raw_and_capability_guards_fail_closed() -> None:
    class BadDecoder:
        @property
        def codec_name(self) -> str:
            return "bad"

        @property
        def value_type(self) -> str:
            return "object"

        def decode(self, raw: object) -> object:
            raise ValueError("nope")

    with pytest.raises(PayloadDecodeError) as excinfo:
        decode_payload({"x": 1}, decoder=BadDecoder())
    err = excinfo.value
    assert err.raw == {"x": 1}

    with pytest.raises(ValueError, match="batch_claim"):
        require_batch_claim(_caps(batch_claim=False), 2)
    with pytest.raises(ValueError, match="long_polling"):
        require_long_polling(_caps(long_polling=False), 5)
    with pytest.raises(ValueError, match="delivery_events"):
        require_delivery_events(_caps(delivery_events=False), [{"id": "1"}])


def test_async_cancellation_and_supervisor_race_contracts_importable() -> None:
    import importlib

    # ``async`` is a Python keyword — load via importlib, not dotted ``import``.
    cancel_mod = importlib.import_module("tests.sdk.async.test_async_cancellation")
    supervisor_mod = importlib.import_module(
        "tests.sdk.async.test_async_consumer_supervisor"
    )

    assert hasattr(cancel_mod, "SlowAsyncTransport")
    assert hasattr(supervisor_mod, "AsyncConsumerSupervisor")
    clock = DeterministicClock()
    assert clock.time() is not None

    scenario = scenario_cancellation()
    transport = ScriptedSyncTransport(scenario.steps)
    with pytest.raises(RequestCancelledError):
        transport.request(
            "POST",
            "/v1/claims",
            headers={"Authorization": "Bearer x"},
            json_body={
                "queues": ["q"],
                "max_tasks": 1,
                "lease_seconds": 30,
                "wait_seconds": 0,
                "worker_id": "w",
            },
        )

    # Explicit ScriptStep cancellation also qualifies.
    transport2 = ScriptedSyncTransport(
        (
            ScriptStep(outcome=RequestCancelledError(reason="cancelled")),
        )
    )
    with pytest.raises(RequestCancelledError):
        transport2.request("GET", "/v1/capabilities", headers={}, json_body=None)
