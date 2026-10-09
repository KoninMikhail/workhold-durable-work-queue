"""Deterministic async retry helpers (SDK-11)."""

from __future__ import annotations

import asyncio
import random

import pytest

from _workhold_client_core.errors import (
    LeaseLostError,
    ProtocolError,
    RequestCancelledError,
    TransportError,
)
from _workhold_client_core.models import ErrorCode, ProtocolErrorBody
from _workhold_client_core.retry import (
    RETRY_CLASS_NEVER,
    RETRY_CLASS_SAFE_READ,
    RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
    RETRY_CLASS_SAME_RESOURCE_IDENTITY,
    RETRY_CLASS_SAME_TERMINAL_BODY,
    RetryBudget,
    RetryNotAllowedError,
    RetryPolicy,
    RetryRequest,
    async_execute_with_retry,
)


def _protocol(
    *,
    retryable: bool = True,
    retry_after_ms: int | None = None,
) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse("internal_error"),
        message="boom",
        retryable=retryable,
        request_id="req-1",
        details={},
        retry_after_ms=retry_after_ms,
    )
    return ProtocolError(status_code=500, body=body)


@pytest.mark.asyncio
async def test_async_attempt_budget_is_hard_capped() -> None:
    calls: list[bytes] = []

    async def op(req: RetryRequest) -> str:
        calls.append(req.body)
        raise TransportError(reason="reset")

    request = RetryRequest(
        operation_id="getTask",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b'{"task_id":"t1"}',
    )
    with pytest.raises(TransportError):
        await async_execute_with_retry(
            request,
            op,
            policy=RetryPolicy(
                max_attempts=3,
                max_elapsed_s=10.0,
                initial_backoff_s=0.0,
                jitter_ratio=0.0,
            ),
            sleep=lambda _d: asyncio.sleep(0),
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
    assert calls == [b'{"task_id":"t1"}'] * 3


@pytest.mark.asyncio
async def test_async_same_key_replay_is_byte_identical() -> None:
    seen: list[RetryRequest] = []

    async def op(req: RetryRequest) -> str:
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
    result = await async_execute_with_retry(
        request,
        op,
        policy=RetryPolicy(
            max_attempts=3,
            max_elapsed_s=5.0,
            initial_backoff_s=0.0,
            jitter_ratio=0.0,
        ),
        sleep=lambda _d: asyncio.sleep(0),
        monotonic=lambda: 0.0,
        rng=random.Random(0),
    )
    assert result == "ok"
    assert len(seen) == 2
    assert seen[0] is seen[1] is request
    assert seen[0].body == b'{"payload":{"a":1}}'


@pytest.mark.asyncio
async def test_async_same_resource_identity_replay_reuses_request() -> None:
    seen: list[RetryRequest] = []

    async def op(req: RetryRequest) -> str:
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
    result = await async_execute_with_retry(
        request,
        op,
        policy=RetryPolicy(
            max_attempts=3,
            max_elapsed_s=5.0,
            initial_backoff_s=0.0,
            jitter_ratio=0.0,
        ),
        sleep=lambda _d: asyncio.sleep(0),
        monotonic=lambda: 0.0,
        rng=random.Random(0),
    )
    assert result == "cancelled"
    assert len(seen) == 2
    assert seen[0] is seen[1] is request
    assert seen[0].resource_identity == "task-42"
    assert seen[0].idempotency_key is None


@pytest.mark.asyncio
async def test_async_terminal_body_replay_preserves_fingerprint() -> None:
    fingerprints: list[str] = []

    async def op(req: RetryRequest) -> str:
        fingerprints.append(req.terminal_fingerprint or "")
        if len(fingerprints) < 2:
            raise TransportError(reason="reset")
        return "done"

    request = RetryRequest(
        operation_id="failClaim",
        retry_class=RETRY_CLASS_SAME_TERMINAL_BODY,
        body=b'{"generation":1,"retryable":false,"failure_code":"x"}',
        terminal_fingerprint='{"failure_code":"x","generation":1,"retryable":false}',
    )
    assert (
        await async_execute_with_retry(
            request,
            op,
            policy=RetryPolicy(
                max_attempts=3,
                max_elapsed_s=5.0,
                initial_backoff_s=0.0,
                jitter_ratio=0.0,
            ),
            sleep=lambda _d: asyncio.sleep(0),
            monotonic=lambda: 0.0,
            rng=random.Random(0),
        )
        == "done"
    )
    assert fingerprints == [
        '{"failure_code":"x","generation":1,"retryable":false}',
        '{"failure_code":"x","generation":1,"retryable":false}',
    ]


@pytest.mark.asyncio
async def test_async_lease_loss_stops_immediately() -> None:
    calls = {"n": 0}

    async def op(_req: RetryRequest) -> str:
        calls["n"] += 1
        raise LeaseLostError(claim_id="c1")

    with pytest.raises(LeaseLostError):
        await async_execute_with_retry(
            RetryRequest(
                operation_id="completeClaim",
                retry_class=RETRY_CLASS_SAME_TERMINAL_BODY,
                body=b'{"generation":1}',
                terminal_fingerprint="fp",
            ),
            op,
            policy=RetryPolicy(max_attempts=5, max_elapsed_s=10.0),
            sleep=lambda _d: asyncio.sleep(0),
            monotonic=lambda: 0.0,
        )
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_async_cancellation_before_retry_skips_later_request() -> None:
    calls = {"n": 0}
    cancelled = {"v": False}

    async def op(_req: RetryRequest) -> str:
        calls["n"] += 1
        cancelled["v"] = True
        raise TransportError(reason="reset")

    with pytest.raises(RequestCancelledError):
        await async_execute_with_retry(
            RetryRequest(
                operation_id="getCapabilities",
                retry_class=RETRY_CLASS_SAFE_READ,
                body=b"",
            ),
            op,
            policy=RetryPolicy(
                max_attempts=5,
                max_elapsed_s=10.0,
                initial_backoff_s=0.0,
                jitter_ratio=0.0,
            ),
            sleep=lambda _d: asyncio.sleep(0),
            monotonic=lambda: 0.0,
            is_cancelled=lambda: cancelled["v"],
            rng=random.Random(0),
        )
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_async_task_cancellation_raises_cancelled_error() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def op(_req: RetryRequest) -> str:
        started.set()
        await release.wait()
        raise TransportError(reason="reset")

    task = asyncio.create_task(
        async_execute_with_retry(
            RetryRequest(
                operation_id="getStats",
                retry_class=RETRY_CLASS_SAFE_READ,
                body=b"",
            ),
            op,
            policy=RetryPolicy(max_attempts=5, max_elapsed_s=10.0),
            sleep=lambda _d: asyncio.sleep(0),
            monotonic=lambda: 0.0,
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_async_budget_policy_must_match_policy_argument() -> None:
    policy = RetryPolicy(max_attempts=3, max_elapsed_s=10.0)
    mismatched = RetryBudget(policy=RetryPolicy(max_attempts=5, max_elapsed_s=10.0))
    request = RetryRequest(
        operation_id="getTask",
        retry_class=RETRY_CLASS_SAFE_READ,
        body=b"",
    )
    with pytest.raises(ValueError, match="budget policy must match"):
        await async_execute_with_retry(
            request,
            lambda _req: asyncio.sleep(0),
            policy=policy,
            budget=mismatched,
        )


@pytest.mark.asyncio
async def test_async_never_ops_fail_locally() -> None:
    with pytest.raises(RetryNotAllowedError):
        await async_execute_with_retry(
            RetryRequest(
                operation_id="reconcileCounters",
                retry_class=RETRY_CLASS_NEVER,
                body=b"{}",
            ),
            lambda _req: asyncio.sleep(0),
            policy=RetryPolicy(max_attempts=3, max_elapsed_s=1.0),
        )
