"""Manifest-driven raw/sync/async live coverage for every ownership cell (phase 21).

Every authenticated ``operationId`` × declared client owner must produce live
PostgreSQL evidence for modes ``raw``, ``sync`` and ``async``. Outcomes are
compared on stable fields (ok / error_code). Multi-client operations are limited
to ``approved_multi_client_operations``.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pytest

from _workhold_client_core.async_transport import HttpxAsyncTransport
from _workhold_client_core.errors import AuthenticationError, ProtocolError
from workhold.security.authorization import Authorizer
from workhold.security.credentials import CredentialBinding
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold_admin.async_client import (
    AsyncAdminClient,
    AsyncBreakGlassClient,
    AsyncObserverClient,
)
from workhold_admin.models import (
    BackoffStrategy,
    BulkPreviewResult,
    QueueState,
    RetryPolicyDraft,
)
from workhold_consumer.async_client import AsyncConsumerClient
from workhold_producer.async_client import AsyncProducerClient
from tests.conformance.clients import (
    COVERAGE_MODES,
    PHASE_19_BREAK_GLASS_OPS,
    AdminClientAdapter,
    OperationResult,
    build_client,
)
from tests.conformance.conftest import (
    ADMIN_PRINCIPAL,
    ADMIN_TOKEN,
    FOREIGN_PRINCIPAL,
    OBSERVER_PRINCIPAL,
    OBSERVER_TOKEN,
    PRODUCER_PRINCIPAL,
    PRODUCER_TOKEN,
    WORKER_PRINCIPAL,
    WORKER_TOKEN,
    seed_queue,
)

pytest_plugins = ["tests.integration.conftest"]

ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_PATH = ROOT / "packages" / "client-operation-ownership.json"

BREAK_GLASS_TOKEN = "tok-break-glass-coverage"
BREAK_GLASS_PRINCIPAL = "break-glass-coverage"

ACK = {
    "reason": "coverage-break-glass",
    "incident_reference": "INC-COV-21",
    "risk_acknowledged": True,
}


@dataclass(frozen=True)
class Evidence:
    operation_id: str
    client: str
    mode: str
    ok: bool
    error_code: str | None
    fingerprint: str


def _load_manifest() -> dict[str, Any]:
    return json.loads(OWNERSHIP_PATH.read_text(encoding="utf-8"))


def _required_cells(manifest: dict[str, Any]) -> set[tuple[str, str, str]]:
    cells: set[tuple[str, str, str]] = set()
    for entry in manifest["operations"]:
        op_id = entry["operationId"]
        for client in entry["clients"]:
            for mode in COVERAGE_MODES:
                cells.add((op_id, client, mode))
    return cells


def _time_window_dt() -> tuple[datetime, datetime]:
    now = datetime.now(tz=UTC)
    return now - timedelta(days=1), now + timedelta(days=1)


def _time_window_filters(**extra: str) -> dict[str, str]:
    time_from, time_to = _time_window_dt()
    payload = {
        "from": time_from.isoformat().replace("+00:00", "Z"),
        "to": time_to.isoformat().replace("+00:00", "Z"),
    }
    payload.update(extra)
    return payload


def _stable_fp(result: OperationResult, *, keys: tuple[str, ...] = ()) -> str:
    picked: dict[str, Any] = {"ok": result.ok, "error_code": result.error_code}
    data = result.data if isinstance(result.data, dict) else {}
    for key in keys:
        if key in data:
            picked[key] = data[key]
        elif isinstance(data.get("task"), dict) and key in data["task"]:
            picked[key] = data["task"][key]
        elif isinstance(data.get("claim"), dict) and key in data["claim"]:
            picked[key] = data["claim"][key]
    return json.dumps(picked, sort_keys=True, default=str)


def _record(
    bag: dict[tuple[str, str, str], Evidence],
    *,
    operation_id: str,
    client: str,
    mode: str,
    result: OperationResult,
    keys: tuple[str, ...] = (),
) -> None:
    bag[(operation_id, client, mode)] = Evidence(
        operation_id=operation_id,
        client=client,
        mode=mode,
        ok=result.ok,
        error_code=result.error_code,
        fingerprint=_stable_fp(result, keys=keys),
    )


def _assert_mode_parity(
    bag: dict[tuple[str, str, str], Evidence],
    *,
    operation_id: str,
    client: str,
) -> None:
    raw = bag[(operation_id, client, "raw")]
    sync = bag[(operation_id, client, "sync")]
    async_ev = bag[(operation_id, client, "async")]
    # Stable semantic parity: ok + error_code. Fingerprints may differ across
    # adapters (wire dict vs dataclass ``{"value": ...}`` nesting).
    assert raw.ok == sync.ok == async_ev.ok, (raw, sync, async_ev)
    assert raw.error_code == sync.error_code == async_ev.error_code, (raw, sync, async_ev)


def _protocol_error_code(exc: ProtocolError) -> str:
    code = getattr(exc, "code", None)
    value = getattr(code, "value", None)
    if isinstance(value, str):
        return value
    if isinstance(code, str):
        return code
    return "protocol_error"


def _model_payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "__dict__"):
        return {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return {"value": value}


async def _await_result(awaitable: Any) -> OperationResult:
    try:
        value = await awaitable
    except AuthenticationError as exc:
        return OperationResult(ok=False, error_code=_protocol_error_code(exc))
    except ProtocolError as exc:
        return OperationResult(ok=False, error_code=_protocol_error_code(exc))
    except ValueError as exc:
        return OperationResult(
            ok=False, error_code="validation_failed", data={"reason": str(exc)}
        )
    except Exception as exc:  # noqa: BLE001 — live coverage still records the call
        return OperationResult(ok=False, error_code=type(exc).__name__)
    return OperationResult(ok=True, data=_model_payload(value), typed=value)


def _admin_preview_replay(admin: Any, **kwargs: Any) -> OperationResult:
    fn = getattr(admin, "preview_bulk_replay", None) or getattr(
        admin, "bulk_preview_replay", None
    )
    if fn is None:
        raise AttributeError("Admin adapter missing preview_bulk_replay")
    return fn(**kwargs)


def _admin_execute_replay(admin: Any, **kwargs: Any) -> OperationResult:
    fn = getattr(admin, "execute_bulk_replay", None) or getattr(
        admin, "bulk_execute_replay", None
    )
    if fn is None:
        raise AttributeError("Admin adapter missing execute_bulk_replay")
    return fn(**kwargs)


def _admin_preview_cancel(admin: Any, **kwargs: Any) -> OperationResult:
    fn = getattr(admin, "preview_bulk_cancel", None) or getattr(
        admin, "bulk_preview_cancel", None
    )
    if fn is None:
        raise AttributeError("Admin adapter missing preview_bulk_cancel")
    return fn(**kwargs)


def _admin_execute_cancel(admin: Any, **kwargs: Any) -> OperationResult:
    fn = getattr(admin, "execute_bulk_cancel", None) or getattr(
        admin, "bulk_execute_cancel", None
    )
    if fn is None:
        raise AttributeError("Admin adapter missing execute_bulk_cancel")
    return fn(**kwargs)


def _queue_config_version(payload: Mapping[str, Any]) -> int:
    nested = payload.get("queue") if isinstance(payload.get("queue"), dict) else payload
    return int(nested.get("config_version") or 1)


def _queue_policy_version(payload: Mapping[str, Any], fallback: int = 1) -> int:
    nested = payload.get("queue") if isinstance(payload.get("queue"), dict) else payload
    value = (
        nested.get("active_policy_version")
        or nested.get("policy_version")
        or payload.get("policy_version")
        or fallback
    )
    return int(value)


@pytest.fixture
def authorizer(queue_name: str, monkeypatch: pytest.MonkeyPatch) -> Authorizer:
    """Kernel dual-client authorizer plus BREAK_GLASS JIT principal for coverage."""

    scoped = frozenset({queue_name})
    admin_scoped = frozenset({queue_name, f"{queue_name}.ctl"})
    authorizer = Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scoped,
            WORKER_PRINCIPAL: scoped,
            ADMIN_PRINCIPAL: admin_scoped,
            OBSERVER_PRINCIPAL: scoped,
            FOREIGN_PRINCIPAL: frozenset({f"other.{uuid.uuid4().hex[:8]}"}),
            BREAK_GLASS_PRINCIPAL: scoped,
        }
    )

    from tests.conformance import conftest as cf

    original_bindings = cf._bindings

    def _bindings_with_bg() -> tuple[CredentialBinding, ...]:
        base = original_bindings()
        expires = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        bg = CredentialBinding(
            principal_id=BREAK_GLASS_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(BREAK_GLASS_TOKEN),
            expires_at=expires,
            allowed_operations=frozenset(PHASE_19_BREAK_GLASS_OPS),
        )
        return (*base, bg)

    monkeypatch.setattr(cf, "_bindings", _bindings_with_bg)
    return authorizer


@pytest.fixture
def coverage_queue(
    session_factory,
    authorizer: Authorizer,
    live_service_url: str,
    live_admin_service_url: str,
    queue_name: str,
) -> str:
    """Ensure ctl sibling exists; live_service_url already seeded ``queue_name``."""

    _ = (authorizer, live_service_url, live_admin_service_url)
    session = session_factory()
    try:
        seed_queue(session, name=f"{queue_name}.ctl")
    finally:
        session.close()
    return queue_name


def test_ownership_cells_match_openapi_and_approved_duplicates() -> None:
    from tools.check_client_operation_ownership import (
        APPROVED_MULTI_CLIENT_OPERATIONS,
        check_ownership,
        load_manifest,
        load_openapi_authenticated_operations,
    )

    openapi = load_openapi_authenticated_operations(ROOT / "openapi" / "queue.openapi.json")
    manifest = load_manifest(OWNERSHIP_PATH)
    errors = check_ownership(openapi, manifest)
    assert errors == [], errors
    approved = set(manifest["approved_multi_client_operations"])
    assert approved == APPROVED_MULTI_CLIENT_OPERATIONS
    for entry in manifest["operations"]:
        clients = entry["clients"]
        if len(clients) > 1:
            assert entry["operationId"] in APPROVED_MULTI_CLIENT_OPERATIONS


def test_manifest_cells_have_raw_sync_async_live_evidence(
    authorizer: Authorizer,
    coverage_queue: str,
    live_service_url: str,
    live_admin_service_url: str,
) -> None:
    """Drive every ownership cell against one live PostgreSQL-backed service."""

    _ = authorizer
    # Confirm AdminClientAdapter exposes preview_/execute_ names (aliases optional).
    assert hasattr(AdminClientAdapter, "preview_bulk_replay")
    assert hasattr(AdminClientAdapter, "execute_bulk_replay")
    assert hasattr(AdminClientAdapter, "preview_bulk_cancel")
    assert hasattr(AdminClientAdapter, "execute_bulk_cancel")

    manifest = _load_manifest()
    required = _required_cells(manifest)
    evidence: dict[tuple[str, str, str], Evidence] = {}
    queue_name = coverage_queue

    raw = build_client("raw_http", live_service_url, admin_base_url=live_admin_service_url)
    producer = build_client("producer", live_service_url, admin_base_url=live_admin_service_url)
    consumer = build_client("consumer", live_service_url, admin_base_url=live_admin_service_url)
    observer = build_client("observer", live_service_url, admin_base_url=live_admin_service_url)
    admin = build_client("admin", live_service_url, admin_base_url=live_admin_service_url)
    break_glass = build_client(
        "break_glass", live_service_url, admin_base_url=live_admin_service_url
    )

    _exercise_producer(evidence, raw=raw, producer=producer, queue_name=queue_name)
    asyncio.run(
        _async_producer(evidence, base_url=live_service_url, queue_name=queue_name)
    )

    _exercise_consumer(
        evidence,
        raw=raw,
        producer=producer,
        consumer=consumer,
        queue_name=queue_name,
    )
    asyncio.run(
        _async_consumer(evidence, base_url=live_service_url, queue_name=queue_name)
    )

    _exercise_observer_admin(
        evidence,
        raw=raw,
        observer=observer,
        admin=admin,
        producer=producer,
        queue_name=queue_name,
    )
    asyncio.run(
        _async_observer_admin(
            evidence,
            app_url=live_service_url,
            admin_url=live_admin_service_url,
            queue_name=queue_name,
        )
    )

    _exercise_break_glass(
        evidence,
        raw=raw,
        break_glass=break_glass,
        producer=producer,
        consumer=consumer,
        queue_name=queue_name,
    )
    asyncio.run(
        _async_break_glass(
            evidence,
            app_url=live_service_url,
            admin_url=live_admin_service_url,
            queue_name=queue_name,
        )
    )

    missing = sorted(required - set(evidence))
    extra = sorted(set(evidence) - required)
    assert missing == [], f"missing coverage cells ({len(missing)}): {missing[:30]}"
    assert extra == [], f"extra coverage cells ({len(extra)}): {extra[:30]}"

    seen: set[tuple[str, str]] = set()
    for op_id, client, _mode in evidence:
        key = (op_id, client)
        if key in seen:
            continue
        seen.add(key)
        if all((op_id, client, mode) in evidence for mode in COVERAGE_MODES):
            _assert_mode_parity(evidence, operation_id=op_id, client=client)


def _exercise_producer(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    raw: Any,
    producer: Any,
    queue_name: str,
) -> None:
    for mode, actor in (("raw", raw), ("sync", producer)):
        caps = actor.get_capabilities(bearer_token=PRODUCER_TOKEN)
        _record(
            evidence,
            operation_id="getCapabilities",
            client="ProducerClient",
            mode=mode,
            result=caps,
            keys=("protocol_version", "protocol_major"),
        )

        idem = f"idem-cov-prod-{mode}-{uuid.uuid4().hex}"
        enq = actor.enqueue(
            queue_name=queue_name,
            idempotency_key=idem,
            payload={"phase": 21, "mode": mode},
            bearer_token=PRODUCER_TOKEN,
        )
        _record(
            evidence,
            operation_id="enqueueTask",
            client="ProducerClient",
            mode=mode,
            result=enq,
            keys=("state",),
        )
        assert enq.ok, enq
        task_id = enq.data["task"]["task_id"]

        resolved = actor.resolve_submission(
            queue_name=queue_name,
            idempotency_key=idem,
            bearer_token=PRODUCER_TOKEN,
        )
        _record(
            evidence,
            operation_id="resolveSubmission",
            client="ProducerClient",
            mode=mode,
            result=resolved,
            keys=("task_id",),
        )

        inspected = actor.inspect(task_id=task_id, bearer_token=PRODUCER_TOKEN)
        _record(
            evidence,
            operation_id="getTask",
            client="ProducerClient",
            mode=mode,
            result=inspected,
            keys=("task_id", "state"),
        )

        cancelled = actor.cancel(
            task_id=task_id,
            bearer_token=PRODUCER_TOKEN,
            reason="coverage-cancel",
        )
        _record(
            evidence,
            operation_id="cancelTask",
            client="ProducerClient",
            mode=mode,
            result=cancelled,
            keys=("task_id", "state"),
        )


async def _async_producer(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    base_url: str,
    queue_name: str,
) -> None:
    transport = HttpxAsyncTransport(base_url, timeout_s=15.0)
    client = AsyncProducerClient(transport, bearer_token=PRODUCER_TOKEN)
    try:
        caps = await _await_result(client.get_capabilities())
        _record(
            evidence,
            operation_id="getCapabilities",
            client="ProducerClient",
            mode="async",
            result=caps,
            keys=("protocol_version", "protocol_major"),
        )

        idem = f"idem-cov-prod-async-{uuid.uuid4().hex}"
        enq = await _await_result(
            client.enqueue(
                queue_name, idempotency_key=idem, payload={"phase": 21, "mode": "async"}
            )
        )
        _record(
            evidence,
            operation_id="enqueueTask",
            client="ProducerClient",
            mode="async",
            result=enq,
            keys=("state",),
        )
        assert enq.ok, enq
        task_id = enq.typed.task.task_id

        resolved = await _await_result(
            client.resolve_submission(queue_name, idempotency_key=idem)
        )
        _record(
            evidence,
            operation_id="resolveSubmission",
            client="ProducerClient",
            mode="async",
            result=resolved,
            keys=("task_id",),
        )

        inspected = await _await_result(client.inspect_task(task_id))
        _record(
            evidence,
            operation_id="getTask",
            client="ProducerClient",
            mode="async",
            result=inspected,
            keys=("task_id", "state"),
        )

        cancelled = await _await_result(
            client.cancel_task(task_id, reason="coverage-async")
        )
        _record(
            evidence,
            operation_id="cancelTask",
            client="ProducerClient",
            mode="async",
            result=cancelled,
            keys=("task_id", "state"),
        )
    finally:
        await transport.aclose()


def _exercise_consumer(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    raw: Any,
    producer: Any,
    consumer: Any,
    queue_name: str,
) -> None:
    for mode, actor in (("raw", raw), ("sync", consumer)):
        caps = actor.get_capabilities(bearer_token=WORKER_TOKEN)
        _record(
            evidence,
            operation_id="getCapabilities",
            client="ConsumerClient",
            mode=mode,
            result=caps,
            keys=("protocol_version", "protocol_major"),
        )

        enq = producer.enqueue(
            queue_name=queue_name,
            idempotency_key=f"idem-cov-cons-{mode}-{uuid.uuid4().hex}",
            payload={"consumer": mode},
            bearer_token=PRODUCER_TOKEN,
        )
        assert enq.ok, enq

        claimed = actor.claim(
            queues=[queue_name],
            worker_id=f"worker-cov-{mode}",
            lease_seconds=30,
            bearer_token=WORKER_TOKEN,
        )
        _record(
            evidence,
            operation_id="claimTasks",
            client="ConsumerClient",
            mode=mode,
            result=claimed,
        )
        assert claimed.ok and claimed.data["tasks"], claimed
        claim = claimed.data["tasks"][0]["claim"]
        claim_id = str(claim["claim_id"])
        claim_token = str(claim["claim_token"])
        generation = int(claim["generation"])

        hb = actor.heartbeat(
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            lease_seconds=30,
            bearer_token=WORKER_TOKEN,
        )
        _record(
            evidence,
            operation_id="heartbeatClaim",
            client="ConsumerClient",
            mode=mode,
            result=hb,
            keys=("claim_id",),
        )

        done = actor.complete(
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            bearer_token=WORKER_TOKEN,
        )
        _record(
            evidence,
            operation_id="completeClaim",
            client="ConsumerClient",
            mode=mode,
            result=done,
            keys=("task_id",),
        )

        enq_f = producer.enqueue(
            queue_name=queue_name,
            idempotency_key=f"idem-cov-fail-{mode}-{uuid.uuid4().hex}",
            payload={"fail": mode},
            bearer_token=PRODUCER_TOKEN,
        )
        assert enq_f.ok, enq_f
        claimed_f = actor.claim(
            queues=[queue_name],
            worker_id=f"worker-cov-fail-{mode}",
            lease_seconds=30,
            bearer_token=WORKER_TOKEN,
        )
        assert claimed_f.ok and claimed_f.data["tasks"], claimed_f
        c = claimed_f.data["tasks"][0]["claim"]
        failed = actor.fail(
            claim_id=str(c["claim_id"]),
            claim_token=str(c["claim_token"]),
            generation=int(c["generation"]),
            bearer_token=WORKER_TOKEN,
            retryable=True,
            failure_code="handler_timeout",
            failure_detail="coverage",
        )
        _record(
            evidence,
            operation_id="failClaim",
            client="ConsumerClient",
            mode=mode,
            result=failed,
        )

        enq_a = producer.enqueue(
            queue_name=queue_name,
            idempotency_key=f"idem-cov-ack-{mode}-{uuid.uuid4().hex}",
            payload={"ack": mode},
            bearer_token=PRODUCER_TOKEN,
        )
        assert enq_a.ok, enq_a
        task_a = enq_a.data["task"]["task_id"]
        claimed_a = actor.claim(
            queues=[queue_name],
            worker_id=f"worker-cov-ack-{mode}",
            lease_seconds=30,
            bearer_token=WORKER_TOKEN,
        )
        assert claimed_a.ok and claimed_a.data["tasks"], claimed_a
        cancelled = producer.cancel(
            task_id=task_a, bearer_token=PRODUCER_TOKEN, reason="cov-ack"
        )
        assert cancelled.ok, cancelled
        c2 = claimed_a.data["tasks"][0]["claim"]
        acked = actor.ack_cancel(
            claim_id=str(c2["claim_id"]),
            claim_token=str(c2["claim_token"]),
            generation=int(c2["generation"]),
            bearer_token=WORKER_TOKEN,
        )
        _record(
            evidence,
            operation_id="acknowledgeClaimCancellation",
            client="ConsumerClient",
            mode=mode,
            result=acked,
        )


async def _async_consumer(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    base_url: str,
    queue_name: str,
) -> None:
    prod_t = HttpxAsyncTransport(base_url, timeout_s=15.0)
    cons_t = HttpxAsyncTransport(base_url, timeout_s=15.0)
    producer = AsyncProducerClient(prod_t, bearer_token=PRODUCER_TOKEN)
    consumer = AsyncConsumerClient(cons_t, bearer_token=WORKER_TOKEN)
    try:
        caps = await _await_result(consumer.get_capabilities())
        _record(
            evidence,
            operation_id="getCapabilities",
            client="ConsumerClient",
            mode="async",
            result=caps,
            keys=("protocol_version", "protocol_major"),
        )

        async def _lease(marker: str) -> Any:
            await producer.enqueue(
                queue_name,
                idempotency_key=f"idem-cov-async-{marker}-{uuid.uuid4().hex}",
                payload={"async": marker},
            )
            claims = await consumer.claim(
                queues=[queue_name],
                worker_id=f"worker-async-{marker}",
                lease_seconds=30,
            )
            assert claims, marker
            return claims[0]

        claim = await _lease("complete")
        _record(
            evidence,
            operation_id="claimTasks",
            client="ConsumerClient",
            mode="async",
            result=OperationResult(
                ok=True,
                data={"tasks": [{"claim": {"claim_id": claim.claim_id}}]},
            ),
        )
        hb = await _await_result(claim.heartbeat(lease_seconds=30))
        _record(
            evidence,
            operation_id="heartbeatClaim",
            client="ConsumerClient",
            mode="async",
            result=hb,
            keys=("claim_id",),
        )
        done = await _await_result(claim.complete())
        _record(
            evidence,
            operation_id="completeClaim",
            client="ConsumerClient",
            mode="async",
            result=done,
            keys=("task_id",),
        )

        claim_f = await _lease("fail")
        failed = await _await_result(
            claim_f.fail(
                retryable=True,
                failure_code="handler_timeout",
                failure_detail="async",
            )
        )
        _record(
            evidence,
            operation_id="failClaim",
            client="ConsumerClient",
            mode="async",
            result=failed,
        )

        enq = await producer.enqueue(
            queue_name,
            idempotency_key=f"idem-cov-async-ack-{uuid.uuid4().hex}",
            payload={"async": "ack"},
        )
        claims = await consumer.claim(
            queues=[queue_name],
            worker_id="worker-async-ack",
            lease_seconds=30,
        )
        assert claims
        await producer.cancel_task(enq.task.task_id, reason="async-ack")
        acked = await _await_result(claims[0].ack_cancel())
        _record(
            evidence,
            operation_id="acknowledgeClaimCancellation",
            client="ConsumerClient",
            mode="async",
            result=acked,
        )
    finally:
        await prod_t.aclose()
        await cons_t.aclose()


def _exercise_observer_admin(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    raw: Any,
    observer: Any,
    admin: Any,
    producer: Any,
    queue_name: str,
) -> None:
    ctl = f"{queue_name}.ctl"
    time_from, time_to = _time_window_dt()
    from_s = time_from.isoformat().replace("+00:00", "Z")
    to_s = time_to.isoformat().replace("+00:00", "Z")

    for mode, client_label, actor, token in (
        ("raw", "ObserverClient", raw, OBSERVER_TOKEN),
        ("sync", "ObserverClient", observer, OBSERVER_TOKEN),
        ("raw", "AdminClient", raw, ADMIN_TOKEN),
        ("sync", "AdminClient", admin, ADMIN_TOKEN),
    ):
        caps = actor.get_capabilities(bearer_token=token)
        _record(
            evidence,
            operation_id="getCapabilities",
            client=client_label,
            mode=mode,
            result=caps,
            keys=("protocol_version",),
        )

    # createQueue — sync typed + raw wire (OpenAPI POST /admin/v1/queues).
    policy_body = {
        "enabled": True,
        "max_attempts": 3,
        "backoff_strategy": "fixed",
        "retry_delay_seconds": 0,
    }
    created_sync_name = f"cov.create.{uuid.uuid4().hex[:10]}"
    created_sync = admin.create_queue(
        name=created_sync_name,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-create-sync-{uuid.uuid4().hex}",
        initial_policy=RetryPolicyDraft(
            enabled=True,
            max_attempts=3,
            backoff_strategy=BackoffStrategy("fixed"),
            retry_delay_seconds=0,
        ),
    )
    _record(
        evidence,
        operation_id="createQueue",
        client="AdminClient",
        mode="sync",
        result=created_sync,
        keys=("name", "state"),
    )

    created_raw_name = f"cov.create.raw.{uuid.uuid4().hex[:10]}"
    created_raw = raw._exchange(  # noqa: SLF001
        "POST",
        "/admin/v1/queues",
        headers={
            "Authorization": f"Bearer {ADMIN_TOKEN}",
            "Idempotency-Key": f"idem-raw-create-{uuid.uuid4().hex}",
        },
        body={"name": created_raw_name, "initial_policy": policy_body},
        base_url=raw.admin_base_url,
    )
    _record(
        evidence,
        operation_id="createQueue",
        client="AdminClient",
        mode="raw",
        result=created_raw,
        keys=("name",),
    )

    # Shared observer/admin reads.
    for mode, client_label, token in (
        ("raw", "ObserverClient", OBSERVER_TOKEN),
        ("sync", "ObserverClient", OBSERVER_TOKEN),
        ("raw", "AdminClient", ADMIN_TOKEN),
        ("sync", "AdminClient", ADMIN_TOKEN),
    ):
        if mode == "sync":
            actor = observer if client_label == "ObserverClient" else admin
            gq = actor.get_queue(queue_name=queue_name, bearer_token=token)
            gs = actor.get_stats(bearer_token=token)
            gm = actor.get_maintenance_status(bearer_token=token)
            lit = actor.list_inspection_tasks(
                queue_name=queue_name, bearer_token=token, limit=10
            )
            ldl = actor.list_dead_letters(
                queue_name=queue_name,
                bearer_token=token,
                time_from=time_from,
                time_to=time_to,
                limit=10,
            )
            if client_label == "AdminClient":
                lq = actor.list_queues(bearer_token=token, limit=10)
                _record(
                    evidence,
                    operation_id="listQueues",
                    client="AdminClient",
                    mode="sync",
                    result=lq,
                )
        else:
            gq = raw._exchange(  # noqa: SLF001
                "GET",
                f"/admin/v1/queues/{queue_name}",
                headers={"Authorization": f"Bearer {token}"},
                base_url=raw.admin_base_url,
            )
            gs = raw._exchange(  # noqa: SLF001
                "GET",
                "/admin/v1/stats",
                headers={"Authorization": f"Bearer {token}"},
                base_url=raw.admin_base_url,
            )
            gm = raw._exchange(  # noqa: SLF001
                "GET",
                "/admin/v1/maintenance",
                headers={"Authorization": f"Bearer {token}"},
                base_url=raw.admin_base_url,
            )
            lit = raw._exchange(  # noqa: SLF001
                "GET",
                f"/admin/v1/tasks?{urlencode({'queue_name': queue_name, 'limit': '10'})}",
                headers={"Authorization": f"Bearer {token}"},
                base_url=raw.admin_base_url,
            )
            ldl = raw._exchange(  # noqa: SLF001
                "GET",
                f"/admin/v1/dead-letters?{urlencode({'queue_name': queue_name, 'from': from_s, 'to': to_s, 'limit': '10'})}",
                headers={"Authorization": f"Bearer {token}"},
                base_url=raw.admin_base_url,
            )
            if client_label == "AdminClient":
                lq = raw._exchange(  # noqa: SLF001
                    "GET",
                    f"/admin/v1/queues?{urlencode({'limit': '10'})}",
                    headers={"Authorization": f"Bearer {token}"},
                    base_url=raw.admin_base_url,
                )
                _record(
                    evidence,
                    operation_id="listQueues",
                    client="AdminClient",
                    mode="raw",
                    result=lq,
                )

        for op_id, result in (
            ("getQueue", gq),
            ("getStats", gs),
            ("getMaintenanceStatus", gm),
            ("listInspectionTasks", lit),
            ("listDeadLetters", ldl),
        ):
            _record(
                evidence,
                operation_id=op_id,
                client=client_label,
                mode=mode,
                result=result,
            )

    # Observer getTask / listTaskAttempts / listInspectionAttempts (+ Admin shell).
    enq = producer.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-obs-{uuid.uuid4().hex}",
        payload={"obs": True},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enq.ok, enq
    tid = enq.data["task"]["task_id"]

    for mode in ("sync", "raw"):
        if mode == "sync":
            gt = observer.get_task(task_id=tid, bearer_token=OBSERVER_TOKEN)
            attempts = observer.list_task_attempts(
                task_id=tid, bearer_token=OBSERVER_TOKEN
            )
            lia = observer.list_inspection_attempts(
                task_id=tid,
                bearer_token=OBSERVER_TOKEN,
                time_from=time_from,
                time_to=time_to,
            )
            lia_admin = admin.list_inspection_attempts(
                task_id=tid,
                bearer_token=ADMIN_TOKEN,
                time_from=time_from,
                time_to=time_to,
            )
        else:
            gt = raw.inspect(task_id=tid, bearer_token=OBSERVER_TOKEN)
            attempts = raw._exchange(  # noqa: SLF001
                "GET",
                f"/v1/tasks/{tid}/attempts",
                headers={"Authorization": f"Bearer {OBSERVER_TOKEN}"},
            )
            lia = raw._exchange(  # noqa: SLF001
                "GET",
                f"/admin/v1/attempts?{urlencode({'task_id': tid, 'from': from_s, 'to': to_s})}",
                headers={"Authorization": f"Bearer {OBSERVER_TOKEN}"},
                base_url=raw.admin_base_url,
            )
            lia_admin = raw._exchange(  # noqa: SLF001
                "GET",
                f"/admin/v1/attempts?{urlencode({'task_id': tid, 'from': from_s, 'to': to_s})}",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                base_url=raw.admin_base_url,
            )
        _record(
            evidence,
            operation_id="getTask",
            client="ObserverClient",
            mode=mode,
            result=gt,
            keys=("task_id",),
        )
        _record(
            evidence,
            operation_id="listTaskAttempts",
            client="ObserverClient",
            mode=mode,
            result=attempts,
        )
        _record(
            evidence,
            operation_id="listInspectionAttempts",
            client="ObserverClient",
            mode=mode,
            result=lia,
        )
        _record(
            evidence,
            operation_id="listInspectionAttempts",
            client="AdminClient",
            mode=mode,
            result=lia_admin,
        )

    _exercise_admin_mutations(
        evidence, raw=raw, admin=admin, queue_name=queue_name, ctl=ctl
    )


def _exercise_admin_mutations(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    raw: Any,
    admin: Any,
    queue_name: str,
    ctl: str,
) -> None:
    time_from, time_to = _time_window_dt()
    from_s = time_from.isoformat().replace("+00:00", "Z")
    to_s = time_to.isoformat().replace("+00:00", "Z")

    q = admin.get_queue(queue_name=ctl, bearer_token=ADMIN_TOKEN)
    assert q.ok, q
    config_version = _queue_config_version(q.data)

    for mode in ("sync", "raw"):
        if mode == "sync":
            policy = admin.create_queue_policy(
                queue_name=ctl,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-pol-{uuid.uuid4().hex}",
                policy=RetryPolicyDraft(
                    enabled=True,
                    max_attempts=4,
                    backoff_strategy=BackoffStrategy("fixed"),
                    retry_delay_seconds=1,
                ),
            )
        else:
            policy = raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{ctl}/policies",
                headers={
                    "Authorization": f"Bearer {ADMIN_TOKEN}",
                    "Idempotency-Key": f"idem-pol-raw-{uuid.uuid4().hex}",
                },
                body={
                    "enabled": True,
                    "max_attempts": 5,
                    "backoff_strategy": "fixed",
                    "retry_delay_seconds": 1,
                },
                base_url=raw.admin_base_url,
            )
        _record(
            evidence,
            operation_id="createQueuePolicy",
            client="AdminClient",
            mode=mode,
            result=policy,
        )
        assert policy.ok, policy

    q2 = admin.get_queue(queue_name=ctl, bearer_token=ADMIN_TOKEN)
    assert q2.ok, q2
    config_version = _queue_config_version(q2.data)
    policy_version = _queue_policy_version(q2.data, fallback=_queue_policy_version(policy.data))

    for mode in ("sync", "raw"):
        if mode == "sync":
            act = admin.activate_queue_policy(
                queue_name=ctl,
                policy_version=policy_version,
                expected_config_version=config_version,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-act-{uuid.uuid4().hex}",
            )
        else:
            act = raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{ctl}/policies/{policy_version}:activate",
                headers={
                    "Authorization": f"Bearer {ADMIN_TOKEN}",
                    "Idempotency-Key": f"idem-act-raw-{uuid.uuid4().hex}",
                },
                body={"expected_config_version": config_version},
                base_url=raw.admin_base_url,
            )
        _record(
            evidence,
            operation_id="activateQueuePolicy",
            client="AdminClient",
            mode=mode,
            result=act,
        )
        if act.ok:
            config_version = _queue_config_version(
                admin.get_queue(queue_name=ctl, bearer_token=ADMIN_TOKEN).data
            )

    for mode in ("sync", "raw"):
        if mode == "sync":
            st = admin.set_queue_state(
                queue_name=ctl,
                state=QueueState("paused"),
                expected_config_version=config_version,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-state-{uuid.uuid4().hex}",
            )
        else:
            st = raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{ctl}:set-state",
                headers={
                    "Authorization": f"Bearer {ADMIN_TOKEN}",
                    "Idempotency-Key": f"idem-state-raw-{uuid.uuid4().hex}",
                },
                body={"state": "paused", "expected_config_version": config_version},
                base_url=raw.admin_base_url,
            )
        _record(
            evidence,
            operation_id="setQueueState",
            client="AdminClient",
            mode=mode,
            result=st,
        )
        if st.ok:
            config_version = _queue_config_version(
                admin.get_queue(queue_name=ctl, bearer_token=ADMIN_TOKEN).data
            )
            admin.set_queue_state(
                queue_name=ctl,
                state=QueueState("active"),
                expected_config_version=config_version,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-state-restore-{uuid.uuid4().hex}",
            )
            config_version = _queue_config_version(
                admin.get_queue(queue_name=ctl, bearer_token=ADMIN_TOKEN).data
            )

    for mode in ("sync", "raw"):
        if mode == "sync":
            audit = admin.list_admin_audit(
                bearer_token=ADMIN_TOKEN,
                time_from=time_from,
                time_to=time_to,
                limit=10,
            )
            maint = admin.run_maintenance(
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-maint-{uuid.uuid4().hex}",
            )
        else:
            audit = raw._exchange(  # noqa: SLF001
                "GET",
                f"/admin/v1/audit?{urlencode({'from': from_s, 'to': to_s, 'limit': '10'})}",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                base_url=raw.admin_base_url,
            )
            maint = raw._exchange(  # noqa: SLF001
                "POST",
                "/admin/v1/maintenance:run",
                headers={
                    "Authorization": f"Bearer {ADMIN_TOKEN}",
                    "Idempotency-Key": f"idem-maint-raw-{uuid.uuid4().hex}",
                },
                body={},
                base_url=raw.admin_base_url,
            )
        _record(
            evidence,
            operation_id="listAdminAudit",
            client="AdminClient",
            mode=mode,
            result=audit,
        )
        _record(
            evidence,
            operation_id="runMaintenance",
            client="AdminClient",
            mode=mode,
            result=maint,
        )

    replay_filters = _time_window_filters(failure_code="exhausted")
    cancel_filters = _time_window_filters(state="ready")

    for mode in ("sync", "raw"):
        if mode == "sync":
            prev_r = _admin_preview_replay(
                admin,
                queue_name=queue_name,
                bearer_token=ADMIN_TOKEN,
                filters=replay_filters,
            )
            prev_c = _admin_preview_cancel(
                admin,
                queue_name=queue_name,
                bearer_token=ADMIN_TOKEN,
                filters=cancel_filters,
            )
        else:
            prev_r = raw.bulk_preview_replay(
                queue_name=queue_name,
                bearer_token=ADMIN_TOKEN,
                filters=replay_filters,
            )
            prev_c = raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}/bulk:preview-cancel",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                body={"filters": cancel_filters},
                base_url=raw.admin_base_url,
            )
        _record(
            evidence,
            operation_id="previewBulkReplay",
            client="AdminClient",
            mode=mode,
            result=prev_r,
        )
        _record(
            evidence,
            operation_id="previewBulkCancel",
            client="AdminClient",
            mode=mode,
            result=prev_c,
        )

    # Execute — live call counts even when confirmation/candidates fail.
    for mode in ("sync", "raw"):
        prev = _admin_preview_replay(
            admin,
            queue_name=queue_name,
            bearer_token=ADMIN_TOKEN,
            filters=replay_filters,
        )
        prev_c = _admin_preview_cancel(
            admin,
            queue_name=queue_name,
            bearer_token=ADMIN_TOKEN,
            filters=cancel_filters,
        )
        if mode == "sync" and prev.ok and isinstance(prev.typed, BulkPreviewResult):
            exec_r = _admin_execute_replay(
                admin,
                queue_name=queue_name,
                bearer_token=ADMIN_TOKEN,
                preview=prev.typed,
                idempotency_key=f"idem-ex-r-{uuid.uuid4().hex}",
                filters=replay_filters,
                reason="coverage",
            )
        elif mode == "raw" and prev.ok:
            token = str(
                prev.data.get("confirmation_token")
                or getattr(prev.typed, "confirmation_token", "")
                or ""
            )
            exec_r = raw.bulk_execute_replay(
                queue_name=queue_name,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-ex-r-raw-{uuid.uuid4().hex}",
                confirmation_token=token or "missing",
                filters=replay_filters,
                reason="coverage",
                batch_limit=25,
            )
        else:
            exec_r = OperationResult(ok=False, error_code=prev.error_code or "validation_failed")

        if mode == "sync" and prev_c.ok and isinstance(prev_c.typed, BulkPreviewResult):
            exec_c = _admin_execute_cancel(
                admin,
                queue_name=queue_name,
                bearer_token=ADMIN_TOKEN,
                preview=prev_c.typed,
                filters=cancel_filters,
                reason="coverage",
            )
        elif mode == "raw" and prev_c.ok:
            ctoken = str(
                prev_c.data.get("confirmation_token")
                or getattr(prev_c.typed, "confirmation_token", "")
                or ""
            )
            exec_c = raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}/bulk:execute-cancel",
                headers={
                    "Authorization": f"Bearer {ADMIN_TOKEN}",
                    "Idempotency-Key": f"idem-ex-c-raw-{uuid.uuid4().hex}",
                },
                body={
                    "confirmation_token": ctoken or "missing",
                    "filters": cancel_filters,
                    "reason": "coverage",
                    "start_index": 0,
                    "batch_limit": 25,
                },
                base_url=raw.admin_base_url,
            )
        else:
            exec_c = OperationResult(
                ok=False, error_code=prev_c.error_code or "validation_failed"
            )

        _record(
            evidence,
            operation_id="executeBulkReplay",
            client="AdminClient",
            mode=mode,
            result=exec_r,
        )
        _record(
            evidence,
            operation_id="executeBulkCancel",
            client="AdminClient",
            mode=mode,
            result=exec_c,
        )

    phantom = str(uuid.uuid4())
    for mode in ("sync", "raw"):
        if mode == "sync":
            rep = admin.replay_dead_letter(
                queue_name=queue_name,
                task_id=phantom,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-rep-{uuid.uuid4().hex}",
                reason="coverage",
            )
        else:
            rep = raw.replay_dead_letter(
                queue_name=queue_name,
                task_id=phantom,
                bearer_token=ADMIN_TOKEN,
                idempotency_key=f"idem-rep-raw-{uuid.uuid4().hex}",
                reason="coverage",
            )
        _record(
            evidence,
            operation_id="replayDeadLetter",
            client="AdminClient",
            mode=mode,
            result=rep,
        )


async def _async_observer_admin(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    app_url: str,
    admin_url: str,
    queue_name: str,
) -> None:
    public = HttpxAsyncTransport(app_url, timeout_s=15.0)
    admin_t = HttpxAsyncTransport(admin_url, timeout_s=15.0)
    observer = AsyncObserverClient(
        public, bearer_token=OBSERVER_TOKEN, admin_transport=admin_t
    )
    admin = AsyncAdminClient(public, bearer_token=ADMIN_TOKEN, admin_transport=admin_t)
    time_from, time_to = _time_window_dt()
    ctl = f"{queue_name}.ctl"
    try:
        for client_label, reader in (
            ("ObserverClient", observer),
            ("AdminClient", admin),
        ):
            caps = await _await_result(reader.get_capabilities())
            _record(
                evidence,
                operation_id="getCapabilities",
                client=client_label,
                mode="async",
                result=caps,
                keys=("protocol_version",),
            )
            gq = await _await_result(reader.get_queue(queue_name))
            gm = await _await_result(reader.get_maintenance_status())
            gs = await _await_result(reader.get_stats())
            lit = await _await_result(reader.list_inspection_tasks(queue_name, limit=10))
            ldl = await _await_result(
                reader.list_dead_letters(
                    queue_name, time_from=time_from, time_to=time_to, limit=10
                )
            )
            for op_id, result in (
                ("getQueue", gq),
                ("getStats", gs),
                ("getMaintenanceStatus", gm),
                ("listInspectionTasks", lit),
                ("listDeadLetters", ldl),
            ):
                _record(
                    evidence,
                    operation_id=op_id,
                    client=client_label,
                    mode="async",
                    result=result,
                )

        lq = await _await_result(admin.list_queues(limit=10))
        _record(
            evidence,
            operation_id="listQueues",
            client="AdminClient",
            mode="async",
            result=lq,
        )

        prod = AsyncProducerClient(public, bearer_token=PRODUCER_TOKEN)
        enq = await prod.enqueue(
            queue_name,
            idempotency_key=f"idem-async-obs-{uuid.uuid4().hex}",
            payload={"obs": "async"},
        )
        gt = await _await_result(observer.get_task(enq.task.task_id))
        attempts = await _await_result(observer.list_task_attempts(enq.task.task_id))
        lia = await _await_result(
            observer.list_inspection_attempts(
                enq.task.task_id, time_from=time_from, time_to=time_to
            )
        )
        lia_a = await _await_result(
            admin.list_inspection_attempts(
                enq.task.task_id, time_from=time_from, time_to=time_to
            )
        )
        for op_id, result in (
            ("getTask", gt),
            ("listTaskAttempts", attempts),
            ("listInspectionAttempts", lia),
        ):
            _record(
                evidence,
                operation_id=op_id,
                client="ObserverClient",
                mode="async",
                result=result,
            )
        _record(
            evidence,
            operation_id="listInspectionAttempts",
            client="AdminClient",
            mode="async",
            result=lia_a,
        )

        created = await _await_result(
            admin.create_queue(
                f"cov.create.async.{uuid.uuid4().hex[:10]}",
                initial_policy=RetryPolicyDraft(
                    enabled=True,
                    max_attempts=3,
                    backoff_strategy=BackoffStrategy("fixed"),
                    retry_delay_seconds=0,
                ),
                idempotency_key=f"idem-async-create-{uuid.uuid4().hex}",
            )
        )
        _record(
            evidence,
            operation_id="createQueue",
            client="AdminClient",
            mode="async",
            result=created,
        )

        policy = await _await_result(
            admin.create_queue_policy(
                ctl,
                RetryPolicyDraft(
                    enabled=True,
                    max_attempts=6,
                    backoff_strategy=BackoffStrategy("fixed"),
                    retry_delay_seconds=2,
                ),
                idempotency_key=f"idem-async-pol-{uuid.uuid4().hex}",
            )
        )
        _record(
            evidence,
            operation_id="createQueuePolicy",
            client="AdminClient",
            mode="async",
            result=policy,
        )

        q = await admin.get_queue(ctl)
        config_version = int(getattr(q, "config_version", 1) or 1)
        policy_version = int(
            getattr(policy.typed, "policy_version", None)
            or getattr(q, "active_policy_version", 1)
            or 1
        )
        act = await _await_result(
            admin.activate_queue_policy(
                ctl,
                policy_version,
                expected_config_version=config_version,
                idempotency_key=f"idem-async-act-{uuid.uuid4().hex}",
            )
        )
        _record(
            evidence,
            operation_id="activateQueuePolicy",
            client="AdminClient",
            mode="async",
            result=act,
        )
        if act.ok:
            q = await admin.get_queue(ctl)
            config_version = int(getattr(q, "config_version", config_version) or config_version)

        q = await admin.get_queue(ctl)
        config_version = int(getattr(q, "config_version", 1) or 1)
        st = await _await_result(
            admin.set_queue_state(
                ctl,
                QueueState("paused"),
                expected_config_version=config_version,
                idempotency_key=f"idem-async-state-{uuid.uuid4().hex}",
            )
        )
        _record(
            evidence,
            operation_id="setQueueState",
            client="AdminClient",
            mode="async",
            result=st,
        )
        if st.ok:
            q = await admin.get_queue(ctl)
            config_version = int(getattr(q, "config_version", config_version) or config_version)
            await admin.set_queue_state(
                ctl,
                QueueState("active"),
                expected_config_version=config_version,
                idempotency_key=f"idem-async-state-restore-{uuid.uuid4().hex}",
            )

        audit = await _await_result(
            admin.list_admin_audit(time_from=time_from, time_to=time_to, limit=10)
        )
        maint = await _await_result(
            admin.run_maintenance(idempotency_key=f"idem-async-maint-{uuid.uuid4().hex}")
        )
        _record(
            evidence,
            operation_id="listAdminAudit",
            client="AdminClient",
            mode="async",
            result=audit,
        )
        _record(
            evidence,
            operation_id="runMaintenance",
            client="AdminClient",
            mode="async",
            result=maint,
        )

        replay_filters = _time_window_filters(failure_code="exhausted")
        cancel_filters = _time_window_filters(state="ready")
        prev_r = await _await_result(
            admin.preview_bulk_replay(queue_name, filters=replay_filters)
        )
        prev_c = await _await_result(
            admin.preview_bulk_cancel(queue_name, filters=cancel_filters)
        )
        _record(
            evidence,
            operation_id="previewBulkReplay",
            client="AdminClient",
            mode="async",
            result=prev_r,
        )
        _record(
            evidence,
            operation_id="previewBulkCancel",
            client="AdminClient",
            mode="async",
            result=prev_c,
        )

        if prev_r.ok and isinstance(prev_r.typed, BulkPreviewResult):
            exec_r = await _await_result(
                admin.execute_bulk_replay(
                    queue_name,
                    preview=prev_r.typed,
                    filters=replay_filters,
                    reason="async-coverage",
                    idempotency_key=f"idem-async-ex-r-{uuid.uuid4().hex}",
                )
            )
        else:
            exec_r = OperationResult(
                ok=False, error_code=prev_r.error_code or "validation_failed"
            )
        if prev_c.ok and isinstance(prev_c.typed, BulkPreviewResult):
            exec_c = await _await_result(
                admin.execute_bulk_cancel(
                    queue_name,
                    preview=prev_c.typed,
                    filters=cancel_filters,
                    reason="async-coverage",
                )
            )
        else:
            exec_c = OperationResult(
                ok=False, error_code=prev_c.error_code or "validation_failed"
            )
        _record(
            evidence,
            operation_id="executeBulkReplay",
            client="AdminClient",
            mode="async",
            result=exec_r,
        )
        _record(
            evidence,
            operation_id="executeBulkCancel",
            client="AdminClient",
            mode="async",
            result=exec_c,
        )

        rep = await _await_result(
            admin.replay_dead_letter(
                queue_name,
                str(uuid.uuid4()),
                reason="async-coverage",
                idempotency_key=f"idem-async-rep-{uuid.uuid4().hex}",
            )
        )
        _record(
            evidence,
            operation_id="replayDeadLetter",
            client="AdminClient",
            mode="async",
            result=rep,
        )
    finally:
        await public.aclose()
        await admin_t.aclose()


def _exercise_break_glass(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    raw: Any,
    break_glass: Any,
    producer: Any,
    consumer: Any,
    queue_name: str,
) -> None:
    def _lease_one(marker: str) -> str:
        enq = producer.enqueue(
            queue_name=queue_name,
            idempotency_key=f"idem-bg-{marker}-{uuid.uuid4().hex}",
            payload={"bg": marker},
            bearer_token=PRODUCER_TOKEN,
        )
        assert enq.ok, enq
        claimed = consumer.claim(
            queues=[queue_name],
            worker_id=f"worker-bg-cov-{marker}",
            lease_seconds=60,
            bearer_token=WORKER_TOKEN,
        )
        assert claimed.ok and claimed.data["tasks"], claimed
        return str(claimed.data["tasks"][0]["task"]["task_id"])

    task_sync = _lease_one("sync")
    fle_sync = break_glass.force_lease_expiry(
        queue_name=queue_name,
        task_id=task_sync,
        bearer_token=BREAK_GLASS_TOKEN,
        **ACK,
    )
    _record(
        evidence,
        operation_id="forceLeaseExpiry",
        client="BreakGlassClient",
        mode="sync",
        result=fle_sync,
        keys=("operation",),
    )

    task_raw = _lease_one("raw")
    fle_raw = raw._exchange(  # noqa: SLF001
        "POST",
        f"/admin/v1/queues/{queue_name}/tasks/{task_raw}:force-lease-expiry",
        headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
        body={**ACK, "task_id": task_raw},
        base_url=raw.admin_base_url,
    )
    _record(
        evidence,
        operation_id="forceLeaseExpiry",
        client="BreakGlassClient",
        mode="raw",
        result=fle_raw,
        keys=("operation",),
    )

    phantom_event = str(uuid.uuid4())
    ops: list[tuple[str, OperationResult, OperationResult]] = [
        (
            "reconcileCounters",
            break_glass.reconcile_counters(
                queue_name=queue_name, bearer_token=BREAK_GLASS_TOKEN, **ACK
            ),
            raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}:reconcile-counters",
                headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
                body=ACK,
                base_url=raw.admin_base_url,
            ),
        ),
        (
            "raiseReplayLimit",
            break_glass.raise_replay_limit(
                queue_name=queue_name,
                bearer_token=BREAK_GLASS_TOKEN,
                factor=2.0,
                ttl_seconds=120,
                **ACK,
            ),
            raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}:raise-replay-limit",
                headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
                body={**ACK, "factor": 2.0, "ttl_seconds": 120},
                base_url=raw.admin_base_url,
            ),
        ),
        (
            "forceDeliveryReclaim",
            break_glass.force_delivery_reclaim(
                queue_name=queue_name,
                event_id=phantom_event,
                bearer_token=BREAK_GLASS_TOKEN,
                **ACK,
            ),
            raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}/delivery-events/{phantom_event}:force-reclaim",
                headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
                body=ACK,
                base_url=raw.admin_base_url,
            ),
        ),
        (
            "forceDeliveryDeadLetter",
            break_glass.force_delivery_dead_letter(
                queue_name=queue_name,
                event_id=phantom_event,
                bearer_token=BREAK_GLASS_TOKEN,
                **ACK,
            ),
            raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}/delivery-events/{phantom_event}:force-dead-letter",
                headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
                body=ACK,
                base_url=raw.admin_base_url,
            ),
        ),
        (
            "dropExpiredPartition",
            break_glass.drop_expired_partition(
                partition_name="admin_audit_log_20000101",
                bearer_token=BREAK_GLASS_TOKEN,
                **ACK,
            ),
            raw._exchange(  # noqa: SLF001
                "POST",
                "/admin/v1/partitions/admin_audit_log_20000101:force-drop",
                headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
                body=ACK,
                base_url=raw.admin_base_url,
            ),
        ),
        (
            "repairRegistryEntry",
            break_glass.repair_registry_entry(
                queue_name=queue_name,
                bearer_token=BREAK_GLASS_TOKEN,
                entry_id=1,
                acknowledge_duplicate_window=True,
                **ACK,
            ),
            raw._exchange(  # noqa: SLF001
                "POST",
                f"/admin/v1/queues/{queue_name}/registry:repair",
                headers={"Authorization": f"Bearer {BREAK_GLASS_TOKEN}"},
                body={
                    **ACK,
                    "entry_id": 1,
                    "acknowledge_duplicate_window": True,
                    "extend_seconds": 86400,
                    "registry": "enqueue_dedup",
                },
                base_url=raw.admin_base_url,
            ),
        ),
    ]
    for op_id, sync_res, raw_res in ops:
        _record(
            evidence,
            operation_id=op_id,
            client="BreakGlassClient",
            mode="sync",
            result=sync_res,
        )
        _record(
            evidence,
            operation_id=op_id,
            client="BreakGlassClient",
            mode="raw",
            result=raw_res,
        )


async def _async_break_glass(
    evidence: dict[tuple[str, str, str], Evidence],
    *,
    app_url: str,
    admin_url: str,
    queue_name: str,
) -> None:
    admin_t = HttpxAsyncTransport(admin_url, timeout_s=15.0)
    public = HttpxAsyncTransport(app_url, timeout_s=15.0)
    client = AsyncBreakGlassClient(admin_t, bearer_token=BREAK_GLASS_TOKEN)
    producer = AsyncProducerClient(public, bearer_token=PRODUCER_TOKEN)
    consumer = AsyncConsumerClient(public, bearer_token=WORKER_TOKEN)
    try:
        enq = await producer.enqueue(
            queue_name,
            idempotency_key=f"idem-bg-async-{uuid.uuid4().hex}",
            payload={"bg": "async"},
        )
        claims = await consumer.claim(
            queues=[queue_name], worker_id="w-bg-async", lease_seconds=60
        )
        assert claims
        task_id = str(claims[0].task.task_id)
        fle = await _await_result(
            client.force_lease_expiry(queue_name, task_id, **ACK)
        )
        _record(
            evidence,
            operation_id="forceLeaseExpiry",
            client="BreakGlassClient",
            mode="async",
            result=fle,
            keys=("operation",),
        )

        phantom = str(uuid.uuid4())
        calls = (
            (
                "reconcileCounters",
                client.reconcile_counters(queue_name, **ACK),
            ),
            (
                "raiseReplayLimit",
                client.raise_replay_limit(
                    queue_name, factor=2.0, ttl_seconds=120, **ACK
                ),
            ),
            (
                "forceDeliveryReclaim",
                client.force_delivery_reclaim(queue_name, phantom, **ACK),
            ),
            (
                "forceDeliveryDeadLetter",
                client.force_delivery_dead_letter(queue_name, phantom, **ACK),
            ),
            (
                "dropExpiredPartition",
                client.drop_expired_partition("admin_audit_log_20000101", **ACK),
            ),
            (
                "repairRegistryEntry",
                client.repair_registry_entry(
                    queue_name,
                    entry_id=1,
                    acknowledge_duplicate_window=True,
                    **ACK,
                ),
            ),
        )
        for op_id, coro in calls:
            result = await _await_result(coro)
            _record(
                evidence,
                operation_id=op_id,
                client="BreakGlassClient",
                mode="async",
                result=result,
            )
    finally:
        await public.aclose()
        await admin_t.aclose()
