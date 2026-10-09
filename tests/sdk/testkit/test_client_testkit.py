"""Public client test kit acceptance tests (SDK-13)."""

from __future__ import annotations

import ast
import asyncio
import importlib
from pathlib import Path

import pytest

from _workhold_client_core.errors import RequestCancelledError, TransportError
from _workhold_client_core.transport import AsyncTransport, SyncTransport
from workhold_admin import AdminClient, BreakGlassClient, ObserverClient
from workhold_admin.async_client import (
    AsyncAdminClient,
    AsyncBreakGlassClient,
    AsyncObserverClient,
)
from workhold_consumer import ConsumerClient
from workhold_consumer.async_client import AsyncConsumerClient
from workhold_producer import ProducerClient
from workhold_producer.async_client import AsyncProducerClient
from workhold_client_testing import (
    DeterministicClock,
    RedactedAssertionError,
    RequestMatcher,
    ScriptStep,
    ScriptedAsyncTransport,
    ScriptedScenario,
    ScriptedSyncTransport,
    auth_headers,
    capabilities_body,
    claim_response_body,
    clear_registered_secrets,
    enqueue_response_body,
    explain_calls,
    idempotency_headers,
    ok,
    protocol_error,
    recording_instrumentation,
    redact_failure_text,
    registered_secrets,
    scenario_cancellation,
    scenario_instrumentation_enqueue,
    scenario_lease_lost_on_complete,
    scenario_pagination,
    scenario_retry_budget,
    scenario_uncertain_enqueue,
    synthetic_bearer_token,
    synthetic_claim_token,
    synthetic_idempotency_key,
)


@pytest.fixture(autouse=True)
def _reset_secrets() -> None:
    clear_registered_secrets()
    yield
    clear_registered_secrets()


def test_public_import_surface() -> None:
    mod = importlib.import_module("workhold_client_testing")
    assert hasattr(mod, "ScriptedSyncTransport")
    assert hasattr(mod, "ScriptedAsyncTransport")
    assert hasattr(mod, "DeterministicClock")


def test_testkit_has_no_server_db_or_pytest_imports() -> None:
    root = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "workhold-client-core"
        / "src"
        / "workhold_client_testing"
    )
    forbidden_roots = {
        "workhold",
        "pytest",
        "psycopg",
        "psycopg2",
        "sqlalchemy",
        "asyncpg",
    }
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
            for name in names:
                top = name.split(".", 1)[0]
                assert top not in forbidden_roots, f"{path.name} imports {name}"


def test_sync_and_async_share_scenario_definitions() -> None:
    scenario = ScriptedScenario(
        name="shared",
        steps=(
            ScriptStep(
                outcome=ok(capabilities_body()),
                match=RequestMatcher(method="GET", path="/v1/capabilities"),
            ),
            ok(enqueue_response_body()),
        ),
    )
    sync = scenario.sync_transport()
    async_transport = scenario.async_transport()
    assert isinstance(sync, SyncTransport)
    assert isinstance(async_transport, AsyncTransport)

    sync_body = sync.request("GET", "/v1/capabilities").body
    async_body = asyncio.run(async_transport.request("GET", "/v1/capabilities")).body
    assert sync_body == async_body == capabilities_body()
    assert sync.request("POST", "/v1/queues/orders/tasks").body == enqueue_response_body()
    assert (
        asyncio.run(async_transport.request("POST", "/v1/queues/orders/tasks")).body
        == enqueue_response_body()
    )
    sync.assert_request_count(2)
    async_transport.assert_request_order(
        [("GET", "/v1/capabilities"), ("POST", "/v1/queues/orders/tasks")]
    )


def test_failure_output_redacts_synthetic_secrets() -> None:
    bearer = synthetic_bearer_token(suffix="abc")
    claim = synthetic_claim_token(suffix="xyz")
    idem = synthetic_idempotency_key(suffix="42")
    text = (
        f"Authorization: Bearer {bearer}; "
        f"X-Queue-Claim-Token: {claim}; "
        f"Idempotency-Key: {idem}; "
        'payload={"secret":1}'
    )
    redacted = redact_failure_text(text)
    assert bearer not in redacted
    assert claim not in redacted
    assert idem not in redacted
    assert "[REDACTED]" in redacted

    with pytest.raises(RedactedAssertionError) as exc_info:
        raise RedactedAssertionError(f"leaked {bearer} {claim} {idem}")
    rendered = str(exc_info.value)
    assert bearer not in rendered
    assert claim not in rendered
    assert idem not in rendered


def test_recorded_requests_hide_headers_and_bodies_by_default() -> None:
    token = synthetic_bearer_token()
    transport = ScriptedSyncTransport([ok({"ok": True})])
    transport.request(
        "POST",
        "/v1/queues/orders/tasks",
        headers=auth_headers(bearer_token=token),
        json_body={
            "payload": {"n": 1},
            "idempotency_key": synthetic_idempotency_key(),
        },
        query={"cursor": "secret-cursor"},
    )
    recorded = transport.calls[0]
    assert token not in repr(recorded)
    assert token not in str(dict(recorded.headers()))
    body = recorded.json_body()
    assert isinstance(body, dict)
    assert body.get("payload") == "[REDACTED]"
    assert body.get("idempotency_key") == "[REDACTED]"
    query = recorded.query()
    assert query is not None
    assert query.get("cursor") == "[REDACTED]"
    assert recorded.headers(include_secrets=True)["Authorization"].endswith(token)


def test_default_headers_never_expose_raw_idempotency_key() -> None:
    caller_idem = "caller-supplied-idem-secret"
    headers = {
        **auth_headers(bearer_token="caller-bearer-secret"),
        **idempotency_headers(key=caller_idem),
        "IDEMPOTENCY-KEY": "upper-case-idem-secret",
    }
    assert caller_idem in registered_secrets()
    assert "caller-bearer-secret" in registered_secrets()

    transport = ScriptedSyncTransport([ok({"ok": True})])
    transport.request(
        "POST",
        "/v1/queues/orders/tasks",
        headers=headers,
        json_body={"idempotency_key": "body-idem-secret", "payload": {"n": 1}},
    )
    recorded = transport.calls[0]
    safe = dict(recorded.headers())
    safe_blob = str(safe)
    assert safe.get("Idempotency-Key") == "<redacted>"
    assert safe.get("IDEMPOTENCY-KEY") == "<redacted>"
    assert caller_idem not in safe_blob
    assert "upper-case-idem-secret" not in safe_blob
    assert "caller-bearer-secret" not in safe_blob

    body = recorded.json_body()
    assert isinstance(body, dict)
    assert body.get("idempotency_key") == "[REDACTED]"
    assert "body-idem-secret" not in str(body)

    raw = dict(recorded.headers(include_secrets=True))
    assert raw["Idempotency-Key"] == caller_idem
    assert raw["IDEMPOTENCY-KEY"] == "upper-case-idem-secret"


def test_every_role_client_constructs_against_scripted_transports() -> None:
    token = synthetic_bearer_token()
    sync = ScriptedSyncTransport([ok(capabilities_body())])
    async_transport = ScriptedAsyncTransport([ok(capabilities_body())])

    ProducerClient(sync, bearer_token=token)
    ConsumerClient(sync, bearer_token=token)
    AdminClient(sync, bearer_token=token)
    ObserverClient(sync, bearer_token=token, admin_transport=sync)
    BreakGlassClient(sync, bearer_token=token)

    AsyncProducerClient(async_transport, bearer_token=token)
    AsyncConsumerClient(async_transport, bearer_token=token)
    AsyncAdminClient(async_transport, bearer_token=token)
    AsyncObserverClient(
        async_transport, bearer_token=token, admin_transport=async_transport
    )
    AsyncBreakGlassClient(async_transport, bearer_token=token)


def test_builtin_scenarios_cover_required_modes() -> None:
    uncertain = scenario_uncertain_enqueue().sync_transport()
    with pytest.raises(TransportError):
        uncertain.request("POST", "/v1/queues/orders/tasks")

    lease = scenario_lease_lost_on_complete().sync_transport()
    assert lease.request("POST", "/v1/claims").body == claim_response_body()
    lost = lease.request(
        "POST", "/v1/claims/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/complete"
    )
    assert isinstance(lost.body, dict)
    assert lost.body["code"] == "lease_lost"

    cancel = scenario_cancellation().sync_transport()
    with pytest.raises(RequestCancelledError):
        cancel.request("GET", "/v1/capabilities")

    pages = scenario_pagination(pages=2).sync_transport()
    first = pages.request("GET", "/v1/tasks")
    second = pages.request("GET", "/v1/tasks", query={"cursor": "cursor-1"})
    assert isinstance(first.body, dict)
    assert isinstance(second.body, dict)
    assert first.body["next_cursor"] == "cursor-1"
    assert second.body["next_cursor"] is None

    retry = scenario_retry_budget(failures_before_success=1).sync_transport()
    first_retry = retry.request("POST", "/v1/queues/orders/tasks")
    assert first_retry.status_code == 429
    assert retry.request("POST", "/v1/queues/orders/tasks").body == enqueue_response_body()

    instrumented = scenario_instrumentation_enqueue().sync_transport()
    assert instrumented.request("GET", "/v1/capabilities").body == capabilities_body()
    assert (
        instrumented.request("POST", "/v1/queues/orders/tasks").body
        == enqueue_response_body()
    )
    _instrumentation, hooks = recording_instrumentation()
    assert hooks.events == []


def test_deterministic_clock_sleep_and_jitter() -> None:
    clock = DeterministicClock(start_s=10.0)
    clock.reseed(7)
    first = clock.jitter(0.5)
    clock.reseed(7)
    second = clock.jitter(0.5)
    assert first == second
    clock.sleep(1.25)
    assert clock.time() == 11.25
    assert clock.sleep_log == (1.25,)


def test_explain_calls_is_redacted() -> None:
    token = synthetic_bearer_token()
    transport = ScriptedSyncTransport([ok({"ok": True})])
    transport.request(
        "GET",
        "/v1/capabilities",
        headers={"Authorization": f"Bearer {token}"},
    )
    summary = explain_calls(transport.calls)
    assert token not in summary
    assert "GET /v1/capabilities" in summary


def test_matcher_mismatch_raises_redacted_assertion() -> None:
    transport = ScriptedSyncTransport(
        [
            ScriptStep(
                outcome=ok({"ok": True}),
                match=RequestMatcher(method="GET", path="/v1/capabilities"),
            )
        ]
    )
    with pytest.raises(RedactedAssertionError):
        transport.request("POST", "/v1/queues/orders/tasks")


def test_protocol_error_builder_shape() -> None:
    response = protocol_error(
        429,
        code="resource_exhausted",
        message="slow down",
        retryable=True,
        retry_after_ms=25,
    )
    assert response.status_code == 429
    assert isinstance(response.body, dict)
    assert response.body["code"] == "resource_exhausted"
    assert response.body["retryable"] is True
    assert response.body["retry_after_ms"] == 25
    assert response.body["request_id"] == "00000000-0000-4000-8000-000000000001"
    again = protocol_error(500, code="internal_error")
    assert isinstance(again.body, dict)
    assert again.body["request_id"] == response.body["request_id"]
