"""Deterministic sync retry helpers (SDK-11)."""

from __future__ import annotations

import json
import random
import subprocess
import zipfile
from importlib.resources import files
from pathlib import Path

import pytest

from _queue_service_client_core.errors import (
    AuthenticationError,
    LeaseLostError,
    ProtocolError,
    RequestCancelledError,
    TerminalConflictError,
    TimeoutError,
    TransportError,
)
from _queue_service_client_core.models import ErrorCode, ProtocolErrorBody
from _queue_service_client_core.retry import (
    RETRY_CLASS_NEVER,
    RETRY_CLASS_SAFE_READ,
    RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
    RETRY_CLASS_SAME_RESOURCE_IDENTITY,
    RETRY_CLASS_SAME_TERMINAL_BODY,
    RETRY_CLASSES,
    RetryBudget,
    RetryNotAllowedError,
    RetryPolicy,
    RetryRequest,
    RetryRequestChangedError,
    _assert_request_unchanged,
    execute_with_retry,
    load_operation_retry_classes,
    retry_class_for,
)

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "packages" / "client-operation-ownership.json"


def _protocol(
    *,
    code: str = "internal_error",
    retryable: bool = True,
    status_code: int = 500,
    retry_after_ms: int | None = None,
) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse(code),
        message="boom",
        retryable=retryable,
        request_id="req-1",
        details={},
        retry_after_ms=retry_after_ms,
    )
    return ProtocolError(status_code=status_code, body=body)


def test_bundled_manifest_loads_without_monorepo_path() -> None:
    ref = (
        files("_queue_service_client_core")
        / "data"
        / "client-operation-ownership.json"
    )
    assert ref.is_file()
    classes = load_operation_retry_classes()
    assert classes["claimTasks"] == RETRY_CLASS_NEVER
    assert classes["cancelTask"] == RETRY_CLASS_SAME_RESOURCE_IDENTITY
    assert classes == load_operation_retry_classes(MANIFEST)


def test_wheel_includes_ownership_manifest(tmp_path: Path) -> None:
    pkg_root = ROOT / "packages" / "queue-service-client-core"
    out_dir = tmp_path / "dist"
    subprocess.run(
        ["uv", "build", "--wheel", "-o", str(out_dir)],
        cwd=pkg_root,
        check=True,
        capture_output=True,
    )
    wheel = next(out_dir.glob("queue_service_client_core-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        bundled = next(
            name
            for name in names
            if name.endswith("data/client-operation-ownership.json")
        )
        payload = json.loads(archive.read(bundled).decode("utf-8"))
    assert any(
        name.endswith("data/client-operation-ownership.json") for name in names
    )
    cancel = next(
        entry for entry in payload["operations"] if entry["operationId"] == "cancelTask"
    )
    assert cancel["retry_class"] == RETRY_CLASS_SAME_RESOURCE_IDENTITY


def test_every_manifest_operation_has_audited_retry_class() -> None:
    classes = load_operation_retry_classes(MANIFEST)
    raw = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert set(classes) == {entry["operationId"] for entry in raw["operations"]}
    assert set(classes.values()) <= RETRY_CLASSES
    for op_id, retry_class in classes.items():
        assert retry_class_for(op_id) == retry_class


def test_break_glass_and_claim_are_never() -> None:
    assert retry_class_for("claimTasks") == RETRY_CLASS_NEVER
    assert retry_class_for("forceLeaseExpiry") == RETRY_CLASS_NEVER
    assert retry_class_for("dropExpiredPartition") == RETRY_CLASS_NEVER
    assert retry_class_for("executeBulkCancel") == RETRY_CLASS_NEVER
    assert retry_class_for("createQueue") == RETRY_CLASS_NEVER


def test_safe_and_idempotent_classes() -> None:
    assert retry_class_for("getTask") == RETRY_CLASS_SAFE_READ
    assert retry_class_for("enqueueTask") == RETRY_CLASS_SAME_IDEMPOTENCY_KEY
    assert retry_class_for("cancelTask") == RETRY_CLASS_SAME_RESOURCE_IDENTITY
    assert retry_class_for("heartbeatClaim") == RETRY_CLASS_SAME_IDEMPOTENCY_KEY
    assert retry_class_for("completeClaim") == RETRY_CLASS_SAME_TERMINAL_BODY
    assert retry_class_for("failClaim") == RETRY_CLASS_SAME_TERMINAL_BODY


def test_same_idempotency_key_requires_idempotency_key() -> None:
    with pytest.raises(ValueError, match="idempotency_key is required"):
        RetryRequest(
            operation_id="enqueueTask",
            retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
            body=b"{}",
        )
    with pytest.raises(ValueError, match="idempotency_key is required"):
        RetryRequest(
            operation_id="enqueueTask",
            retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
            body=b"{}",
            idempotency_key="",
        )


def test_same_resource_identity_requires_non_empty_identity() -> None:
    with pytest.raises(ValueError, match="resource_identity is required"):
        RetryRequest(
            operation_id="cancelTask",
            retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
            body=b"{}",
        )
    with pytest.raises(ValueError, match="resource_identity is required"):
        RetryRequest(
            operation_id="cancelTask",
            retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
            body=b"{}",
            resource_identity="",
        )
    with pytest.raises(ValueError, match="resource_identity is required"):
        RetryRequest(
            operation_id="cancelTask",
            retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
            body=b"{}",
            resource_identity="   \t\n",
        )
    with pytest.raises(ValueError, match="resource_identity is required"):
        RetryRequest(
            operation_id="cancelTask",
            retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
            body=b"{}",
            resource_identity=b"task-42",  # type: ignore[arg-type]
        )


def test_same_resource_identity_replay_reuses_request() -> None:
    seen: list[RetryRequest] = []

    def op(req: RetryRequest) -> str:
        seen.append(req)
        if len(seen) < 2:
            raise TransportError(reason="reset")
        return "cancelled"

    request = RetryRequest(
        operation_id="cancelTask",
        retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
        body=b'{"reason":"user revoked"}',
        resource_identity="task-42",
    )
    policy = RetryPolicy(
        max_attempts=3,
        max_elapsed_s=5.0,
        initial_backoff_s=0.0,
        jitter_ratio=0.0,
    )
    assert (
        execute_with_retry(
            request,
            op,
            policy=policy,
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
        == "cancelled"
    )
    assert len(seen) == 2
    assert seen[0] is request and seen[1] is request
    assert seen[0].resource_identity == "task-42"
    assert seen[0].idempotency_key is None


def test_changed_resource_identity_is_rejected() -> None:
    original = RetryRequest(
        operation_id="cancelTask",
        retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
        body=b"{}",
        resource_identity="task-a",
    )
    mutated = RetryRequest(
        operation_id="cancelTask",
        retry_class=RETRY_CLASS_SAME_RESOURCE_IDENTITY,
        body=b"{}",
        resource_identity="task-b",
    )
    with pytest.raises(RetryRequestChangedError, match="resource_identity"):
        _assert_request_unchanged(original, mutated)


def test_never_class_fails_locally_on_execute() -> None:
    request = RetryRequest(
        operation_id="claimTasks",
        retry_class=RETRY_CLASS_NEVER,
        body=b"{}",
    )
    with pytest.raises(RetryNotAllowedError):
        execute_with_retry(
            request,
            lambda _req: None,
            policy=RetryPolicy(max_attempts=3, max_elapsed_s=1.0),
        )


def test_retry_class_must_match_manifest() -> None:
    with pytest.raises(ValueError, match="does not match audited"):
        RetryRequest(
            operation_id="enqueueTask",
            retry_class=RETRY_CLASS_SAFE_READ,
            body=b"{}",
            idempotency_key="k1",
        )


def test_attempt_budget_is_hard_capped() -> None:
    calls: list[bytes] = []

    def op(req: RetryRequest) -> str:
        calls.append(req.body)
        raise TransportError(reason="reset")

    request = RetryRequest(
        operation_id="getTask",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b'{"task_id":"t1"}',
    )
    policy = RetryPolicy(
        max_attempts=3,
        max_elapsed_s=10.0,
        initial_backoff_s=0.0,
        jitter_ratio=0.0,
    )
    with pytest.raises(TransportError):
        execute_with_retry(
            request,
            op,
            policy=policy,
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
    assert calls == [b'{"task_id":"t1"}'] * 3


def test_elapsed_budget_stops_before_extra_attempt() -> None:
    calls: list[int] = []
    clock = {"t": 0.0}

    def op(_req: RetryRequest) -> str:
        calls.append(1)
        raise TransportError(reason="reset")

    def sleep(delay: float) -> None:
        clock["t"] += delay

    request = RetryRequest(
        operation_id="getCapabilities",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b"",
    )
    policy = RetryPolicy(
        max_attempts=10,
        max_elapsed_s=0.2,
        initial_backoff_s=0.15,
        max_backoff_s=0.15,
        jitter_ratio=0.0,
    )
    with pytest.raises(TransportError):
        execute_with_retry(
            request,
            op,
            policy=policy,
            sleep=sleep,
            monotonic=lambda: clock["t"],
            rng=random.Random(0),
        )
    assert len(calls) == 2


def test_same_key_replay_is_byte_identical() -> None:
    seen: list[RetryRequest] = []

    def op(req: RetryRequest) -> str:
        seen.append(req)
        if len(seen) < 2:
            raise _protocol(retryable=True, retry_after_ms=0)
        return "ok"

    request = RetryRequest(
        operation_id="enqueueTask",
        retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
        body=b'{"payload":{"a":1}}',
        idempotency_key="idem-1",
    )
    policy = RetryPolicy(
        max_attempts=3,
        max_elapsed_s=5.0,
        initial_backoff_s=0.0,
        jitter_ratio=0.0,
    )
    assert (
        execute_with_retry(
            request,
            op,
            policy=policy,
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
        == "ok"
    )
    assert len(seen) == 2
    assert seen[0] is request and seen[1] is request
    assert seen[0].body == b'{"payload":{"a":1}}'
    assert seen[0].idempotency_key == "idem-1"


def test_terminal_body_replay_preserves_fingerprint() -> None:
    seen: list[str] = []

    def op(req: RetryRequest) -> str:
        seen.append(req.terminal_fingerprint or "")
        if len(seen) < 2:
            raise TransportError(reason="reset")
        return "done"

    request = RetryRequest(
        operation_id="completeClaim",
        retry_class=RETRY_CLASS_SAME_TERMINAL_BODY,
        body=b'{"generation":1,"spawn":[]}',
        terminal_fingerprint='{"generation":1,"spawn":[]}',
    )
    policy = RetryPolicy(
        max_attempts=3,
        max_elapsed_s=5.0,
        initial_backoff_s=0.0,
        jitter_ratio=0.0,
    )
    assert (
        execute_with_retry(
            request,
            op,
            policy=policy,
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
        == "done"
    )
    assert seen == ['{"generation":1,"spawn":[]}', '{"generation":1,"spawn":[]}']


def test_lease_loss_stops_with_zero_later_request() -> None:
    calls = {"n": 0}

    def op(_req: RetryRequest) -> str:
        calls["n"] += 1
        raise LeaseLostError(claim_id="c1")

    request = RetryRequest(
        operation_id="heartbeatClaim",
        retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
        body=b'{"generation":1}',
        idempotency_key="claim:c1",
    )
    with pytest.raises(LeaseLostError):
        execute_with_retry(
            request,
            op,
            policy=RetryPolicy(max_attempts=5, max_elapsed_s=10.0),
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
        )
    assert calls["n"] == 1


def test_cancellation_stops_before_next_request() -> None:
    calls = {"n": 0}
    cancelled = {"v": False}

    def op(_req: RetryRequest) -> str:
        calls["n"] += 1
        cancelled["v"] = True
        raise TransportError(reason="reset")

    request = RetryRequest(
        operation_id="getQueue",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b"",
    )
    with pytest.raises(RequestCancelledError):
        execute_with_retry(
            request,
            op,
            policy=RetryPolicy(
                max_attempts=5,
                max_elapsed_s=10.0,
                initial_backoff_s=0.0,
                jitter_ratio=0.0,
            ),
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
            is_cancelled=lambda: cancelled["v"],
            rng=random.Random(0),
        )
    assert calls["n"] == 1


def test_auth_and_permission_errors_are_not_retried() -> None:
    auth = AuthenticationError(
        status_code=401,
        body=ProtocolErrorBody(
            code=ErrorCode.parse("unauthenticated"),
            message="no",
            retryable=True,
            request_id="r",
            details={},
        ),
    )
    cases: list[BaseException] = [
        auth,
        _protocol(code="permission_denied", retryable=True, status_code=403),
        TerminalConflictError(claim_id="c1", reason="changed"),
        _protocol(retryable=False),
    ]
    for exc in cases:
        calls = {"n": 0}

        def op(_req: RetryRequest, _exc: BaseException = exc) -> str:
            calls["n"] += 1
            raise _exc

        request = RetryRequest(
            operation_id="getTask",
            retry_class=RETRY_CLASS_SAFE_READ,
            body=b"",
        )
        with pytest.raises(type(exc)):
            execute_with_retry(
                request,
                op,
                policy=RetryPolicy(max_attempts=5, max_elapsed_s=10.0),
                sleep=lambda _d: None,
                monotonic=lambda: 0.0,
            )
        assert calls["n"] == 1


def test_retryable_hint_alone_does_not_retry_never_ops() -> None:
    request = RetryRequest(
        operation_id="runMaintenance",
        retry_class=RETRY_CLASS_NEVER,
        body=b"{}",
    )
    with pytest.raises(RetryNotAllowedError):
        execute_with_retry(
            request,
            lambda _req: (_ for _ in ()).throw(
                _protocol(retryable=True, retry_after_ms=10)
            ),
            policy=RetryPolicy(max_attempts=5, max_elapsed_s=10.0),
        )


def test_server_retry_after_is_bounded() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def op(_req: RetryRequest) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _protocol(retryable=True, retry_after_ms=60_000)
        return "ok"

    request = RetryRequest(
        operation_id="getStats",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b"",
    )
    policy = RetryPolicy(
        max_attempts=3,
        max_elapsed_s=100.0,
        initial_backoff_s=0.01,
        max_backoff_s=0.01,
        jitter_ratio=0.0,
        max_server_hint_s=0.5,
    )
    assert (
        execute_with_retry(
            request,
            op,
            policy=policy,
            sleep=lambda d: sleeps.append(d),
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
        == "ok"
    )
    assert sleeps == [0.5]


def test_changed_request_identity_is_rejected() -> None:
    original = RetryRequest(
        operation_id="enqueueTask",
        retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
        body=b'{"payload":1}',
        idempotency_key="k1",
    )
    mutated = RetryRequest(
        operation_id="enqueueTask",
        retry_class=RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
        body=b'{"payload":2}',
        idempotency_key="k1",
    )
    with pytest.raises(RetryRequestChangedError, match="body"):
        _assert_request_unchanged(original, mutated)


def test_budget_tracks_attempts() -> None:
    policy = RetryPolicy(max_attempts=2, max_elapsed_s=1.0)
    budget = RetryBudget(policy=policy)
    budget.start(now=0.0)
    assert budget.can_attempt(now=0.0)
    budget.record_attempt()
    budget.record_attempt()
    assert not budget.can_attempt(now=0.0)


def test_timeout_error_is_retryable_for_safe_read() -> None:
    calls = {"n": 0}

    def op(_req: RetryRequest) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError(timeout_s=1.0)
        return "ok"

    request = RetryRequest(
        operation_id="listQueues",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b"",
    )
    assert (
        execute_with_retry(
            request,
            op,
            policy=RetryPolicy(
                max_attempts=3,
                max_elapsed_s=5.0,
                initial_backoff_s=0.0,
                jitter_ratio=0.0,
            ),
            sleep=lambda _d: None,
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
        == "ok"
    )
    assert calls["n"] == 2
