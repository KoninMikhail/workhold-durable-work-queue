"""Conformance adapters: raw HTTP and role SDK surfaces.

Adapters translate call syntax only. Fixtures, assertions, the live Queue
service, and PostgreSQL are shared. SDK mocks and in-memory stores are forbidden.

Producer operations go through ``workhold_producer.ProducerClient``.
Admin recovery / break-glass live cases use ``AdminClient`` and
``BreakGlassClient``. Worker surfaces on the producer adapter still delegate to
raw HTTP where dual-client delayed/priority cases need claim/complete.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from _workhold_client_core.errors import (
    AuthenticationError,
    ProtocolError,
)
from _workhold_client_core.priority import validate_priority
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin import AdminClient, BreakGlassClient, ObserverClient
from workhold_admin.models import (
    BackoffStrategy,
    BulkPreviewResult,
    QueueState,
    RetryPolicyDraft,
)
from workhold_consumer import ConsumerClient
from workhold_consumer.client import Claim as ConsumerClaim
from workhold_producer import ProducerClient

ClientKind = Literal[
    "raw_http",
    "producer",
    "consumer",
    "observer",
    "admin",
    "break_glass",
]

# Kernel dual-client scenarios (SDK-05 interim surface).
CLIENT_KINDS: tuple[ClientKind, ...] = ("raw_http", "producer")
ROLE_CLIENT_KINDS: tuple[ClientKind, ...] = ("observer", "admin")
# Phase 21 ownership coverage — every sync role adapter kind.
COVERAGE_CLIENT_KINDS: tuple[ClientKind, ...] = (
    "raw_http",
    "producer",
    "consumer",
    "observer",
    "admin",
    "break_glass",
)
COVERAGE_MODES: tuple[str, ...] = ("raw", "sync", "async")

# Manifest client class → sync adapter kind (raw is always available separately).
MANIFEST_CLIENT_TO_KIND: dict[str, ClientKind] = {
    "ProducerClient": "producer",
    "ConsumerClient": "consumer",
    "ObserverClient": "observer",
    "AdminClient": "admin",
    "BreakGlassClient": "break_glass",
}

# Phase 19 recovery + emergency surfaces from client-operation-ownership.json.
PHASE_19_ADMIN_RECOVERY_OPS: tuple[str, ...] = (
    "replayDeadLetter",
    "previewBulkReplay",
    "executeBulkReplay",
    "previewBulkCancel",
    "executeBulkCancel",
)
PHASE_19_BREAK_GLASS_OPS: tuple[str, ...] = (
    "forceLeaseExpiry",
    "forceDeliveryReclaim",
    "forceDeliveryDeadLetter",
    "reconcileCounters",
    "raiseReplayLimit",
    "dropExpiredPartition",
    "repairRegistryEntry",
)

REQUIRED_KERNEL_SCENARIOS: tuple[str, ...] = (
    "enqueue_idempotency",
    "claim_fencing",
    "stale_heartbeat_complete",
    "uncertain_complete_replay",
    "cancellation",
    "queue_active_paused_draining_gates",
    "structured_failures",
    "authorization",
)

CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"


@dataclass(frozen=True)
class WireExchange:
    """Raw HTTP observation retained so SDK mapping cannot hide protocol drift."""

    status_code: int
    headers: Mapping[str, str]
    body: Any | None
    raw_body: bytes = b""


@dataclass
class OperationResult:
    """Shared semantic outcome; ``wire`` is always set for raw_http."""

    ok: bool
    data: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    wire: WireExchange | None = None
    typed: Any | None = None


class ConformanceClient(Protocol):
    """Typed transport surface for black-box kernel scenarios."""

    kind: ClientKind
    base_url: str

    def enqueue(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        payload: Any,
        bearer_token: str,
        priority: int = 0,
        available_at: datetime | None = None,
    ) -> OperationResult: ...

    def resolve_submission(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        bearer_token: str,
    ) -> OperationResult: ...

    def get_capabilities(
        self,
        *,
        bearer_token: str,
    ) -> OperationResult: ...

    def inspect(
        self,
        *,
        task_id: str,
        bearer_token: str,
    ) -> OperationResult: ...

    def replay_dead_letter(
        self,
        *,
        queue_name: str,
        task_id: str,
        bearer_token: str,
        idempotency_key: str,
        reason: str,
    ) -> OperationResult: ...

    def bulk_preview_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        filters: Mapping[str, Any],
    ) -> OperationResult: ...

    def bulk_execute_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        idempotency_key: str,
        confirmation_token: str,
        filters: Mapping[str, Any],
        reason: str,
        start_index: int = 0,
        batch_limit: int = 100,
    ) -> OperationResult: ...

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        bearer_token: str,
        wait_seconds: int = 0,
    ) -> OperationResult: ...

    def heartbeat(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        lease_seconds: int,
        bearer_token: str,
    ) -> OperationResult: ...

    def complete(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        spawn: Sequence[Mapping[str, Any]] | None = None,
    ) -> OperationResult: ...

    def fail(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> OperationResult: ...

    def cancel(
        self,
        *,
        task_id: str,
        bearer_token: str,
        reason: str | None = None,
    ) -> OperationResult: ...

    def ack_cancel(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
    ) -> OperationResult: ...


def _parse_json_body(raw: bytes) -> Any | None:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _error_code_from_body(body: Any | None) -> str | None:
    if isinstance(body, Mapping):
        code = body.get("code")
        if isinstance(code, str):
            return code
    return None


def _protocol_error_code(exc: ProtocolError) -> str:
    code = getattr(exc, "code", None)
    value = getattr(code, "value", None)
    if isinstance(value, str):
        return value
    if isinstance(code, str):
        return code
    return "protocol_error"


def _task_dict(task: Any) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "queue_name": task.queue_name,
        "state": getattr(task.state, "value", str(task.state)),
        "priority": task.priority,
        "payload": task.payload,
        "producer_id": getattr(task, "producer_id", None),
        "available_at": getattr(task, "available_at", None),
        "terminal_at": getattr(task, "terminal_at", None),
        "source_task_id": getattr(task, "source_task_id", None),
    }


class RawHttpClientAdapter:
    """stdlib urllib adapter with full wire retention."""

    kind: ClientKind = "raw_http"

    def __init__(
        self,
        base_url: str,
        *,
        admin_base_url: str | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self.admin_base_url = (admin_base_url or "").rstrip("/")
        self._timeout_s = timeout_s

    def _exchange(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str],
        body: Mapping[str, Any] | None = None,
        base_url: str | None = None,
    ) -> OperationResult:
        raw_body = b""
        if body is not None:
            raw_body = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode(
                "utf-8"
            )
        req_headers = {str(k): str(v) for k, v in headers.items()}
        if body is not None:
            req_headers.setdefault("Content-Type", "application/json")
        url = f"{(base_url or self.base_url).rstrip('/')}{path}"
        request = Request(url, data=raw_body or None, headers=req_headers, method=method)
        try:
            with urlopen(request, timeout=self._timeout_s) as response:
                raw = response.read()
                status = int(getattr(response, "status", response.getcode()))
                resp_headers = {k.lower(): v for k, v in response.headers.items()}
        except HTTPError as exc:
            raw = exc.read() if exc.fp is not None else b""
            status = int(exc.code)
            resp_headers = (
                {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
            )
        except (URLError, TimeoutError, OSError) as exc:
            return OperationResult(
                ok=False,
                error_code="transport_error",
                data={"reason": type(exc).__name__},
            )

        parsed = _parse_json_body(raw)
        wire = WireExchange(
            status_code=status,
            headers=resp_headers,
            body=parsed,
            raw_body=raw,
        )
        ok = 200 <= status < 300
        return OperationResult(
            ok=ok,
            data=dict(parsed) if isinstance(parsed, dict) else {},
            error_code=None if ok else _error_code_from_body(parsed),
            wire=wire,
        )

    def enqueue(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        payload: Any,
        bearer_token: str,
        priority: Any = 0,
        available_at: datetime | None = None,
    ) -> OperationResult:
        body: dict[str, Any] = {"payload": payload, "priority": priority}
        if available_at is not None:
            body["available_at"] = available_at.isoformat()
        return self._exchange(
            "POST",
            f"/v1/queues/{queue_name}/tasks",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "Idempotency-Key": idempotency_key,
            },
            body=body,
        )

    def resolve_submission(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        bearer_token: str,
    ) -> OperationResult:
        return self._exchange(
            "POST",
            f"/v1/queues/{queue_name}/submissions:resolve",
            headers={"Authorization": f"Bearer {bearer_token}"},
            body={"idempotency_key": idempotency_key},
        )

    def get_capabilities(
        self,
        *,
        bearer_token: str,
    ) -> OperationResult:
        return self._exchange(
            "GET",
            "/v1/capabilities",
            headers={"Authorization": f"Bearer {bearer_token}"},
        )

    def inspect(
        self,
        *,
        task_id: str,
        bearer_token: str,
    ) -> OperationResult:
        return self._exchange(
            "GET",
            f"/v1/tasks/{task_id}",
            headers={"Authorization": f"Bearer {bearer_token}"},
        )

    def replay_dead_letter(
        self,
        *,
        queue_name: str,
        task_id: str,
        bearer_token: str,
        idempotency_key: str,
        reason: str,
    ) -> OperationResult:
        if not self.admin_base_url:
            raise RuntimeError("admin_base_url is required for replay_dead_letter")
        return self._exchange(
            "POST",
            f"/admin/v1/queues/{queue_name}/dead-letters/{task_id}:replay",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "Idempotency-Key": idempotency_key,
            },
            body={"reason": reason},
            base_url=self.admin_base_url,
        )

    def bulk_preview_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        filters: Mapping[str, Any],
    ) -> OperationResult:
        if not self.admin_base_url:
            raise RuntimeError("admin_base_url is required for bulk_preview_replay")
        return self._exchange(
            "POST",
            f"/admin/v1/queues/{queue_name}/bulk:preview-replay",
            headers={"Authorization": f"Bearer {bearer_token}"},
            body={"filters": dict(filters)},
            base_url=self.admin_base_url,
        )

    def bulk_execute_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        idempotency_key: str,
        confirmation_token: str,
        filters: Mapping[str, Any],
        reason: str,
        start_index: int = 0,
        batch_limit: int = 100,
    ) -> OperationResult:
        if not self.admin_base_url:
            raise RuntimeError("admin_base_url is required for bulk_execute_replay")
        return self._exchange(
            "POST",
            f"/admin/v1/queues/{queue_name}/bulk:execute-replay",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "Idempotency-Key": idempotency_key,
            },
            body={
                "confirmation_token": confirmation_token,
                "filters": dict(filters),
                "reason": reason,
                "start_index": start_index,
                "batch_limit": batch_limit,
            },
            base_url=self.admin_base_url,
        )

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        bearer_token: str,
        wait_seconds: int = 0,
    ) -> OperationResult:
        # Preserve caller timeout for immediate claim; stretch for positive waits
        # using the documented wait+10 total budget (proxy floor remains >=30s).
        previous = self._timeout_s
        if wait_seconds > 0:
            self._timeout_s = max(previous, float(wait_seconds) + 10.0)
        try:
            return self._exchange(
                "POST",
                "/v1/claims",
                headers={"Authorization": f"Bearer {bearer_token}"},
                body={
                    "queues": list(queues),
                    "max_tasks": 1,
                    "lease_seconds": lease_seconds,
                    "wait_seconds": wait_seconds,
                    "worker_id": worker_id,
                },
            )
        finally:
            self._timeout_s = previous

    def heartbeat(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        lease_seconds: int,
        bearer_token: str,
    ) -> OperationResult:
        return self._exchange(
            "POST",
            f"/v1/claims/{claim_id}:heartbeat",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                CLAIM_TOKEN_HEADER: claim_token,
            },
            body={"generation": generation, "lease_seconds": lease_seconds},
        )

    def complete(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        spawn: Sequence[Mapping[str, Any]] | None = None,
    ) -> OperationResult:
        return self._exchange(
            "POST",
            f"/v1/claims/{claim_id}:complete",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                CLAIM_TOKEN_HEADER: claim_token,
            },
            body={"generation": generation, "spawn": [dict(x) for x in (spawn or ())]},
        )

    def fail(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> OperationResult:
        body: dict[str, Any] = {
            "generation": generation,
            "retryable": retryable,
            "failure_code": failure_code,
        }
        if failure_detail is not None:
            body["failure_detail"] = failure_detail
        return self._exchange(
            "POST",
            f"/v1/claims/{claim_id}:fail",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                CLAIM_TOKEN_HEADER: claim_token,
            },
            body=body,
        )

    def cancel(
        self,
        *,
        task_id: str,
        bearer_token: str,
        reason: str | None = None,
    ) -> OperationResult:
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        return self._exchange(
            "POST",
            f"/v1/tasks/{task_id}:cancel",
            headers={"Authorization": f"Bearer {bearer_token}"},
            body=body,
        )

    def ack_cancel(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
    ) -> OperationResult:
        return self._exchange(
            "POST",
            f"/v1/claims/{claim_id}:ack-cancel",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                CLAIM_TOKEN_HEADER: claim_token,
            },
            body={"generation": generation},
        )


class ProducerClientAdapter:
    """``workhold_producer.ProducerClient`` adapter — syntax translation only.

    Producer-owned OpenAPI operations use the role SDK. Claim/heartbeat/complete/
    fail/ack_cancel and admin dead-letter/bulk paths delegate to
    :class:`RawHttpClientAdapter` until consumer/admin clients exist.
    """

    kind: ClientKind = "producer"

    def __init__(
        self,
        base_url: str,
        *,
        admin_base_url: str | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self.admin_base_url = (admin_base_url or "").rstrip("/")
        self._timeout_s = timeout_s
        self._transport = HttpJsonTransport(self.base_url, timeout_s=timeout_s)
        self._raw = RawHttpClientAdapter(
            self.base_url,
            admin_base_url=admin_base_url,
            timeout_s=timeout_s,
        )

    def __repr__(self) -> str:
        return "ProducerClientAdapter(base_url=..., bearer_token=<redacted>)"

    def __str__(self) -> str:
        return self.__repr__()

    def _producer(self, bearer_token: str) -> ProducerClient:
        return ProducerClient(self._transport, bearer_token=bearer_token)

    def enqueue(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        payload: Any,
        bearer_token: str,
        priority: Any = 0,
        available_at: datetime | None = None,
    ) -> OperationResult:
        try:
            validate_priority(priority)
        except (TypeError, ValueError):
            return OperationResult(ok=False, error_code="validation_failed")
        try:
            response = self._producer(bearer_token).enqueue(
                queue_name,
                idempotency_key=idempotency_key,
                payload=payload,
                priority=priority,
                available_at=available_at,
            )
        except ValueError:
            return OperationResult(ok=False, error_code="validation_failed")
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(
            ok=True,
            data={
                "task": _task_dict(response.task),
                "replayed": bool(response.replayed),
            },
        )

    def resolve_submission(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        bearer_token: str,
    ) -> OperationResult:
        try:
            response = self._producer(bearer_token).resolve_submission(
                queue_name,
                idempotency_key=idempotency_key,
            )
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(
            ok=True,
            data={
                "task": _task_dict(response.task),
                "dedup_expires_at": response.dedup_expires_at,
            },
        )

    def get_capabilities(
        self,
        *,
        bearer_token: str,
    ) -> OperationResult:
        try:
            caps = self._producer(bearer_token).get_capabilities()
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        data = asdict(caps)
        # ``extra`` is already a mapping; keep protocol_version for assertions.
        return OperationResult(ok=True, data=data)

    def inspect(
        self,
        *,
        task_id: str,
        bearer_token: str,
    ) -> OperationResult:
        try:
            task = self._producer(bearer_token).inspect_task(task_id)
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(ok=True, data=_task_dict(task))

    def cancel(
        self,
        *,
        task_id: str,
        bearer_token: str,
        reason: str | None = None,
    ) -> OperationResult:
        try:
            response = self._producer(bearer_token).cancel_task(task_id, reason=reason)
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        task = response.task
        return OperationResult(
            ok=True,
            data={
                "task_id": task.task_id,
                "task": _task_dict(task),
                "state": getattr(task.state, "value", str(task.state)),
            },
        )

    def replay_dead_letter(
        self,
        *,
        queue_name: str,
        task_id: str,
        bearer_token: str,
        idempotency_key: str,
        reason: str,
    ) -> OperationResult:
        return self._raw.replay_dead_letter(
            queue_name=queue_name,
            task_id=task_id,
            bearer_token=bearer_token,
            idempotency_key=idempotency_key,
            reason=reason,
        )

    def bulk_preview_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        filters: Mapping[str, Any],
    ) -> OperationResult:
        return self._raw.bulk_preview_replay(
            queue_name=queue_name,
            bearer_token=bearer_token,
            filters=filters,
        )

    def bulk_execute_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        idempotency_key: str,
        confirmation_token: str,
        filters: Mapping[str, Any],
        reason: str,
        start_index: int = 0,
        batch_limit: int = 100,
    ) -> OperationResult:
        return self._raw.bulk_execute_replay(
            queue_name=queue_name,
            bearer_token=bearer_token,
            idempotency_key=idempotency_key,
            confirmation_token=confirmation_token,
            filters=filters,
            reason=reason,
            start_index=start_index,
            batch_limit=batch_limit,
        )

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        bearer_token: str,
        wait_seconds: int = 0,
    ) -> OperationResult:
        return self._raw.claim(
            queues=queues,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            bearer_token=bearer_token,
            wait_seconds=wait_seconds,
        )

    def heartbeat(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        lease_seconds: int,
        bearer_token: str,
    ) -> OperationResult:
        return self._raw.heartbeat(
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            lease_seconds=lease_seconds,
            bearer_token=bearer_token,
        )

    def complete(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        spawn: Sequence[Mapping[str, Any]] | None = None,
    ) -> OperationResult:
        return self._raw.complete(
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            bearer_token=bearer_token,
            spawn=spawn,
        )

    def fail(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> OperationResult:
        return self._raw.fail(
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            bearer_token=bearer_token,
            retryable=retryable,
            failure_code=failure_code,
            failure_detail=failure_detail,
        )

    def ack_cancel(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
    ) -> OperationResult:
        return self._raw.ack_cancel(
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            bearer_token=bearer_token,
        )


class ConsumerClientAdapter:
    """``workhold_consumer.ConsumerClient`` adapter — lease ops via SDK.

    Producer/admin paths used for fixture setup still delegate to raw HTTP.
    """

    kind: ClientKind = "consumer"

    def __init__(
        self,
        base_url: str,
        *,
        admin_base_url: str | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self.admin_base_url = (admin_base_url or "").rstrip("/")
        self._timeout_s = timeout_s
        self._transport = HttpJsonTransport(self.base_url, timeout_s=timeout_s)
        self._raw = RawHttpClientAdapter(
            self.base_url,
            admin_base_url=admin_base_url,
            timeout_s=timeout_s,
        )
        self._claims: dict[str, ConsumerClaim] = {}

    def __repr__(self) -> str:
        return "ConsumerClientAdapter(base_url=..., bearer_token=<redacted>)"

    def __str__(self) -> str:
        return self.__repr__()

    def _consumer(self, bearer_token: str) -> ConsumerClient:
        return ConsumerClient(self._transport, bearer_token=bearer_token)

    def enqueue(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        payload: Any,
        bearer_token: str,
        priority: Any = 0,
        available_at: datetime | None = None,
    ) -> OperationResult:
        return self._raw.enqueue(
            queue_name=queue_name,
            idempotency_key=idempotency_key,
            payload=payload,
            bearer_token=bearer_token,
            priority=priority,
            available_at=available_at,
        )

    def resolve_submission(
        self,
        *,
        queue_name: str,
        idempotency_key: str,
        bearer_token: str,
    ) -> OperationResult:
        return self._raw.resolve_submission(
            queue_name=queue_name,
            idempotency_key=idempotency_key,
            bearer_token=bearer_token,
        )

    def get_capabilities(self, *, bearer_token: str) -> OperationResult:
        try:
            caps = self._consumer(bearer_token).get_capabilities()
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(ok=True, data=asdict(caps))

    def inspect(self, *, task_id: str, bearer_token: str) -> OperationResult:
        return self._raw.inspect(task_id=task_id, bearer_token=bearer_token)

    def cancel(
        self,
        *,
        task_id: str,
        bearer_token: str,
        reason: str | None = None,
    ) -> OperationResult:
        return self._raw.cancel(task_id=task_id, bearer_token=bearer_token, reason=reason)

    def replay_dead_letter(
        self,
        *,
        queue_name: str,
        task_id: str,
        bearer_token: str,
        idempotency_key: str,
        reason: str,
    ) -> OperationResult:
        return self._raw.replay_dead_letter(
            queue_name=queue_name,
            task_id=task_id,
            bearer_token=bearer_token,
            idempotency_key=idempotency_key,
            reason=reason,
        )

    def bulk_preview_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        filters: Mapping[str, Any],
    ) -> OperationResult:
        return self._raw.bulk_preview_replay(
            queue_name=queue_name,
            bearer_token=bearer_token,
            filters=filters,
        )

    def bulk_execute_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        idempotency_key: str,
        confirmation_token: str,
        filters: Mapping[str, Any],
        reason: str,
        start_index: int = 0,
        batch_limit: int = 100,
    ) -> OperationResult:
        return self._raw.bulk_execute_replay(
            queue_name=queue_name,
            bearer_token=bearer_token,
            idempotency_key=idempotency_key,
            confirmation_token=confirmation_token,
            filters=filters,
            reason=reason,
            start_index=start_index,
            batch_limit=batch_limit,
        )

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        bearer_token: str,
        wait_seconds: int = 0,
    ) -> OperationResult:
        try:
            claims = self._consumer(bearer_token).claim(
                queues=queues,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                wait_seconds=wait_seconds,
            )
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ValueError as exc:
            return OperationResult(
                ok=False, error_code="validation_failed", data={"reason": str(exc)}
            )
        tasks: list[dict[str, Any]] = []
        for handle in claims:
            self._claims[handle.claim_id] = handle
            tasks.append(
                {
                    "task": _task_dict(handle.task),
                    "claim": {
                        "claim_id": handle.claim_id,
                        "generation": handle.generation,
                        "claimed_at": handle.claimed_at,
                        "lease_expires_at": handle.lease_expires_at,
                        "worker_id": handle.worker_id,
                        "cancel_requested": handle.cancel_requested,
                        "claim_token": handle._claim_token,
                    },
                }
            )
        return OperationResult(
            ok=True,
            data={
                "tasks": tasks,
                "server_time": claims[0].server_time if claims else None,
                "recommended_heartbeat_seconds": (
                    claims[0].recommended_heartbeat_seconds if claims else None
                ),
                "queue_states": {},
            },
        )

    def heartbeat(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        lease_seconds: int,
        bearer_token: str,
    ) -> OperationResult:
        handle = self._claims.get(claim_id)
        if handle is None or handle._claim_token != claim_token:
            return self._raw.heartbeat(
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                lease_seconds=lease_seconds,
                bearer_token=bearer_token,
            )
        try:
            result = handle.heartbeat(lease_seconds=lease_seconds)
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(
            ok=True,
            data={
                "claim": {
                    "claim_id": result.claim.claim_id,
                    "generation": result.claim.generation,
                    "lease_expires_at": result.claim.lease_expires_at,
                    "cancel_requested": result.claim.cancel_requested,
                },
                "server_time": result.server_time,
                "recommended_heartbeat_seconds": result.recommended_heartbeat_seconds,
            },
            typed=result,
        )

    def complete(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        spawn: Sequence[Mapping[str, Any]] | None = None,
    ) -> OperationResult:
        handle = self._claims.get(claim_id)
        if handle is None or handle._claim_token != claim_token:
            return self._raw.complete(
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                bearer_token=bearer_token,
                spawn=spawn,
            )
        try:
            result = handle.complete(spawn=spawn)
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        data = _model_data(result)
        if "task_id" not in data and hasattr(result, "task_id"):
            data["task_id"] = result.task_id
        return OperationResult(ok=True, data=data, typed=result)

    def fail(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> OperationResult:
        handle = self._claims.get(claim_id)
        if handle is None or handle._claim_token != claim_token:
            return self._raw.fail(
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                bearer_token=bearer_token,
                retryable=retryable,
                failure_code=failure_code,
                failure_detail=failure_detail,
            )
        try:
            result = handle.fail(
                retryable=retryable,
                failure_code=failure_code,
                failure_detail=failure_detail,
            )
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def ack_cancel(
        self,
        *,
        claim_id: str,
        claim_token: str,
        generation: int,
        bearer_token: str,
    ) -> OperationResult:
        handle = self._claims.get(claim_id)
        if handle is None or handle._claim_token != claim_token:
            return self._raw.ack_cancel(
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                bearer_token=bearer_token,
            )
        try:
            result = handle.ack_cancel()
        except AuthenticationError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        except ProtocolError as exc:
            return OperationResult(ok=False, error_code=_protocol_error_code(exc))
        return OperationResult(ok=True, data=_model_data(result), typed=result)


def _sdk_error_result(exc: BaseException) -> OperationResult:
    if isinstance(exc, AuthenticationError):
        return OperationResult(ok=False, error_code=_protocol_error_code(exc))
    if isinstance(exc, ProtocolError):
        return OperationResult(ok=False, error_code=_protocol_error_code(exc))
    if isinstance(exc, ValueError):
        return OperationResult(ok=False, error_code="validation_failed", data={"reason": str(exc)})
    raise exc


def _model_data(model: Any) -> dict[str, Any]:
    raw = asdict(model)
    # Drop empty extras that dataclasses include by default.
    extra = raw.pop("extra", None)
    if isinstance(extra, dict) and extra:
        raw["extra"] = extra
    return raw



def _sdk_call(fn: Any) -> OperationResult:
    try:
        result = fn()
    except (AuthenticationError, ProtocolError, ValueError) as exc:
        return _sdk_error_result(exc)
    data = _model_data(result)
    if not isinstance(data, dict):
        data = {"value": data}
    return OperationResult(ok=True, data=data, typed=result)


def default_retry_policy_draft(**overrides: Any) -> RetryPolicyDraft:
    payload: dict[str, Any] = {
        "enabled": True,
        "max_attempts": 3,
        "backoff_strategy": BackoffStrategy("fixed"),
        "retry_delay_seconds": 5,
    }
    payload.update(overrides)
    return RetryPolicyDraft(**payload)


class ObserverClientAdapter:
    """``workhold_admin.ObserverClient`` live adapter — read-only syntax bridge."""

    kind: ClientKind = "observer"

    def __init__(
        self,
        base_url: str,
        *,
        admin_base_url: str | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if not admin_base_url:
            raise ValueError("admin_base_url is required for ObserverClientAdapter")
        self.base_url = base_url.rstrip("/")
        self.admin_base_url = admin_base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._public = HttpJsonTransport(self.base_url, timeout_s=timeout_s)
        self._admin_transport = HttpJsonTransport(self.admin_base_url, timeout_s=timeout_s)

    def __repr__(self) -> str:
        return "ObserverClientAdapter(base_url=..., admin_base_url=..., bearer_token=<redacted>)"

    def _client(self, bearer_token: str) -> ObserverClient:
        return ObserverClient(
            self._public,
            bearer_token=bearer_token,
            admin_transport=self._admin_transport,
        )

    def get_capabilities(self, *, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_capabilities())

    def get_task(self, *, task_id: str, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_task(task_id))

    def list_task_attempts(
        self,
        *,
        task_id: str,
        bearer_token: str,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_task_attempts(
                task_id, cursor=cursor, limit=limit
            )
        )

    def get_queue(self, *, queue_name: str, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_queue(queue_name))

    def get_stats(self, *, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_stats())

    def get_maintenance_status(self, *, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_maintenance_status())

    def list_inspection_tasks(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_inspection_tasks(
                queue_name, cursor=cursor, limit=limit
            )
        )

    def list_inspection_attempts(
        self,
        *,
        task_id: str,
        bearer_token: str,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_inspection_attempts(
                task_id,
                time_from=time_from,
                time_to=time_to,
                cursor=cursor,
                limit=limit,
            )
        )

    def list_dead_letters(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_dead_letters(
                queue_name,
                time_from=time_from,
                time_to=time_to,
                cursor=cursor,
                limit=limit,
            )
        )


class AdminClientAdapter:
    """``workhold_admin.AdminClient`` adapter for Phase 18 routine + Phase 19 recovery."""

    kind: ClientKind = "admin"

    def __init__(
        self,
        base_url: str | None = None,
        *,
        admin_base_url: str | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        resolved_admin = admin_base_url or base_url
        if not resolved_admin:
            raise ValueError("admin_base_url is required")
        resolved_public = (base_url or resolved_admin).rstrip("/")
        self.base_url = resolved_public
        self.admin_base_url = resolved_admin.rstrip("/")
        self._timeout_s = timeout_s
        self._public = HttpJsonTransport(self.base_url, timeout_s=timeout_s)
        self._admin_transport = HttpJsonTransport(self.admin_base_url, timeout_s=timeout_s)
        self._transport = self._admin_transport

    def __repr__(self) -> str:
        return "AdminClientAdapter(base_url=..., admin_base_url=..., bearer_token=<redacted>)"

    def _admin(self, bearer_token: str) -> AdminClient:
        return AdminClient(
            self._public,
            bearer_token=bearer_token,
            admin_transport=self._admin_transport,
        )

    def _client(self, bearer_token: str) -> AdminClient:
        return self._admin(bearer_token)

    def get_capabilities(self, *, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_capabilities())

    def list_queues(
        self,
        *,
        bearer_token: str,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_queues(cursor=cursor, limit=limit)
        )

    def create_queue(
        self,
        *,
        name: str,
        bearer_token: str,
        idempotency_key: str,
        initial_policy: RetryPolicyDraft | None = None,
    ) -> OperationResult:
        policy = initial_policy or default_retry_policy_draft()
        return _sdk_call(
            lambda: self._client(bearer_token).create_queue(
                name,
                initial_policy=policy,
                idempotency_key=idempotency_key,
            )
        )

    def get_queue(self, *, queue_name: str, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_queue(queue_name))

    def create_queue_policy(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        idempotency_key: str,
        policy: RetryPolicyDraft | None = None,
    ) -> OperationResult:
        draft = policy or default_retry_policy_draft(max_attempts=4)
        return _sdk_call(
            lambda: self._client(bearer_token).create_queue_policy(
                queue_name,
                draft,
                idempotency_key=idempotency_key,
            )
        )

    def activate_queue_policy(
        self,
        *,
        queue_name: str,
        policy_version: int,
        expected_config_version: int,
        bearer_token: str,
        idempotency_key: str,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).activate_queue_policy(
                queue_name,
                policy_version,
                expected_config_version=expected_config_version,
                idempotency_key=idempotency_key,
            )
        )

    def set_queue_state(
        self,
        *,
        queue_name: str,
        state: str | QueueState,
        expected_config_version: int,
        bearer_token: str,
        idempotency_key: str,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).set_queue_state(
                queue_name,
                state,
                expected_config_version=expected_config_version,
                idempotency_key=idempotency_key,
            )
        )

    def get_stats(self, *, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_stats())

    def get_maintenance_status(self, *, bearer_token: str) -> OperationResult:
        return _sdk_call(lambda: self._client(bearer_token).get_maintenance_status())

    def list_inspection_tasks(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_inspection_tasks(
                queue_name, cursor=cursor, limit=limit
            )
        )

    def list_inspection_attempts(
        self,
        *,
        task_id: str,
        bearer_token: str,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_inspection_attempts(
                task_id,
                time_from=time_from,
                time_to=time_to,
                cursor=cursor,
                limit=limit,
            )
        )

    def list_dead_letters(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_dead_letters(
                queue_name,
                time_from=time_from,
                time_to=time_to,
                cursor=cursor,
                limit=limit,
            )
        )

    def list_admin_audit(
        self,
        *,
        bearer_token: str,
        time_from: datetime,
        time_to: datetime,
        queue_name: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).list_admin_audit(
                time_from=time_from,
                time_to=time_to,
                queue_name=queue_name,
                cursor=cursor,
                limit=limit,
            )
        )

    def run_maintenance(
        self,
        *,
        bearer_token: str,
        idempotency_key: str,
    ) -> OperationResult:
        return _sdk_call(
            lambda: self._client(bearer_token).run_maintenance(
                idempotency_key=idempotency_key
            )
        )

    def replay_dead_letter(
        self,
        *,
        queue_name: str,
        task_id: str,
        bearer_token: str,
        idempotency_key: str,
        reason: str,
    ) -> OperationResult:
        try:
            result = self._admin(bearer_token).replay_dead_letter(
                queue_name,
                task_id,
                idempotency_key=idempotency_key,
                reason=reason,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def preview_bulk_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        filters: Mapping[str, str] | None = None,
    ) -> OperationResult:
        try:
            result = self._admin(bearer_token).preview_bulk_replay(
                queue_name,
                filters=filters,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def execute_bulk_replay(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        preview: BulkPreviewResult,
        idempotency_key: str,
        reason: str,
        filters: Mapping[str, str],
        start_index: int = 0,
        batch_limit: int | None = None,
    ) -> OperationResult:
        try:
            result = self._admin(bearer_token).execute_bulk_replay(
                queue_name,
                preview=preview,
                idempotency_key=idempotency_key,
                reason=reason,
                filters=dict(filters),
                start_index=start_index,
                batch_limit=batch_limit,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def preview_bulk_cancel(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        filters: Mapping[str, str] | None = None,
    ) -> OperationResult:
        try:
            result = self._admin(bearer_token).preview_bulk_cancel(
                queue_name,
                filters=filters,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def execute_bulk_cancel(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        preview: BulkPreviewResult,
        reason: str,
        filters: Mapping[str, str],
        start_index: int = 0,
        batch_limit: int | None = None,
    ) -> OperationResult:
        try:
            result = self._admin(bearer_token).execute_bulk_cancel(
                queue_name,
                preview=preview,
                reason=reason,
                filters=dict(filters),
                start_index=start_index,
                batch_limit=batch_limit,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)


class BreakGlassClientAdapter:
    """``workhold_admin.BreakGlassClient`` adapter for Phase 19 emergency ops."""

    kind: ClientKind = "break_glass"

    def __init__(self, admin_base_url: str, *, timeout_s: float = 15.0) -> None:
        if not admin_base_url:
            raise ValueError("admin_base_url is required")
        self.base_url = admin_base_url.rstrip("/")
        self.admin_base_url = self.base_url
        self._timeout_s = timeout_s
        self._transport = HttpJsonTransport(self.base_url, timeout_s=timeout_s)

    def __repr__(self) -> str:
        return "BreakGlassClientAdapter(admin_base_url=..., bearer_token=<redacted>)"

    def _client(self, bearer_token: str) -> BreakGlassClient:
        return BreakGlassClient(self._transport, bearer_token=bearer_token)

    def force_lease_expiry(
        self,
        *,
        queue_name: str,
        task_id: str,
        bearer_token: str,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).force_lease_expiry(
                queue_name,
                task_id,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def force_delivery_reclaim(
        self,
        *,
        queue_name: str,
        event_id: str,
        bearer_token: str,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).force_delivery_reclaim(
                queue_name,
                event_id,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def force_delivery_dead_letter(
        self,
        *,
        queue_name: str,
        event_id: str,
        bearer_token: str,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        failure_code: str = "break_glass_force_dead_letter",
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).force_delivery_dead_letter(
                queue_name,
                event_id,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
                failure_code=failure_code,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def reconcile_counters(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).reconcile_counters(
                queue_name,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def raise_replay_limit(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        factor: float = 2.0,
        ttl_seconds: int = 60,
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).raise_replay_limit(
                queue_name,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
                factor=factor,
                ttl_seconds=ttl_seconds,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def drop_expired_partition(
        self,
        *,
        partition_name: str,
        bearer_token: str,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).drop_expired_partition(
                partition_name,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)

    def repair_registry_entry(
        self,
        *,
        queue_name: str,
        bearer_token: str,
        entry_id: int,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        acknowledge_duplicate_window: bool,
        extend_seconds: int = 86400,
        registry: str = "enqueue_dedup",
    ) -> OperationResult:
        try:
            result = self._client(bearer_token).repair_registry_entry(
                queue_name,
                entry_id=entry_id,
                reason=reason,
                incident_reference=incident_reference,
                risk_acknowledged=risk_acknowledged,
                acknowledge_duplicate_window=acknowledge_duplicate_window,
                extend_seconds=extend_seconds,
                registry=registry,
            )
        except (AuthenticationError, ProtocolError, ValueError) as exc:
            return _sdk_error_result(exc)
        return OperationResult(ok=True, data=_model_data(result), typed=result)


def build_client(
    kind: ClientKind,
    base_url: str,
    *,
    admin_base_url: str | None = None,
) -> (
    ConformanceClient
    | ConsumerClientAdapter
    | ObserverClientAdapter
    | AdminClientAdapter
    | BreakGlassClientAdapter
):
    if kind == "raw_http":
        return RawHttpClientAdapter(base_url, admin_base_url=admin_base_url)
    if kind == "producer":
        return ProducerClientAdapter(base_url, admin_base_url=admin_base_url)
    if kind == "consumer":
        return ConsumerClientAdapter(base_url, admin_base_url=admin_base_url)
    if kind == "observer":
        if not admin_base_url:
            raise ValueError("admin_base_url is required for observer client")
        return ObserverClientAdapter(base_url, admin_base_url=admin_base_url)
    if kind == "admin":
        if not admin_base_url:
            raise ValueError("admin_base_url is required for admin client")
        return AdminClientAdapter(base_url, admin_base_url=admin_base_url)
    if kind == "break_glass":
        if not admin_base_url:
            raise ValueError("admin_base_url is required for break_glass client")
        return BreakGlassClientAdapter(admin_base_url)
    raise ValueError(f"unknown client kind: {kind!r}")
