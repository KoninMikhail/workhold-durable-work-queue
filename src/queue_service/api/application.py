"""Public application-plane ASGI composition (`/v1` only)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import (
    Handler,
    ListenerBind,
    RequestContext,
    create_plane_app,
    error_envelope,
    send_json,
)
from queue_service.api.routes.complete import build_complete_handler
from queue_service.api.routes.fail import build_fail_handler
from queue_service.api.routes.tasks import (
    build_cancel_handler,
    build_get_task_handler,
    build_list_attempts_handler,
)
from queue_service.api.routes.worker import build_ack_cancel_handler
from queue_service.api.v1.capabilities import build_capabilities_handler
from queue_service.api.v1.claims import build_claim_handler, build_heartbeat_handler
from queue_service.api.v1.enqueue import build_enqueue_handler
from queue_service.api.v1.submissions import build_resolve_submission_handler
from queue_service.application.cancellation import CancellationService
from queue_service.application.claim_long_poll import ClaimLongPollService
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionService
from queue_service.application.lease_service import LeaseService
from queue_service.application.task_inspection import TaskInspectionService
from queue_service.application.worker_terminal import WorkerTerminalService
from queue_service.intake.admission import (
    DEFAULT_REQUEST_MAX_BYTES,
    EnqueueAdmissionLimits,
)
from queue_service.intake.service import EnqueueService
from queue_service.scheduling import SchedulingPolicy
from queue_service.security.authorization import Authorizer, Operation
from queue_service.security.credentials import IdentityAuthenticator
from queue_service.security.payload_policy import (
    PayloadHandlingPolicy,
    PayloadRetentionPolicy,
)
from queue_service.security.redaction import sanitize_for_diagnostics
from queue_service.settings import CLAIM_MAX_WAIT_SECONDS_DEFAULT

SKELETON_CODE = "skeleton_operation_unsupported"
SKELETON_MESSAGE = "operation is not implemented"


def _skeleton_handler(
    *,
    payload_policy: PayloadHandlingPolicy,
) -> Handler:
    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        # Metadata-only default: never project opaque payload into diagnostics.
        _ = payload_policy.inspect(
            {},
            payload_bytes=len(body),
            include_payload=False,
        )
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "queue_name": context.authorization.queue_name,
            }
        )
        await send_json(
            send,
            status=501,
            payload=error_envelope(
                code=SKELETON_CODE,
                message=SKELETON_MESSAGE,
                retryable=False,
                request_id=context.request_id,
            ),
        )

    return handler


def _dispatch_handler(
    *,
    handlers: dict[Operation, Handler],
    skeleton: Handler,
) -> Handler:
    async def handler(
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        implemented = handlers.get(context.operation)
        if implemented is not None:
            await implemented(scope, receive, send, context, body)
            return
        await skeleton(scope, receive, send, context, body)

    return handler


def create_application_app(
    *,
    authenticator: IdentityAuthenticator,
    authorizer: Authorizer,
    bind: ListenerBind,
    lookup_probe: Any | None = None,
    payload_policy: PayloadHandlingPolicy | None = None,
    session_factory: sessionmaker[Session] | None = None,
    enqueue_service: EnqueueService | None = None,
    claim_service: ClaimService | None = None,
    claim_long_poll_service: ClaimLongPollService | None = None,
    max_wait_seconds: int = CLAIM_MAX_WAIT_SECONDS_DEFAULT,
    lease_service: LeaseService | None = None,
    worker_terminal_service: WorkerTerminalService | None = None,
    completion_service: CompletionService | None = None,
    cancellation_service: CancellationService | None = None,
    inspection_service: TaskInspectionService | None = None,
    retention_policy: PayloadRetentionPolicy | None = None,
    admission_limits: EnqueueAdmissionLimits | None = None,
    schedule_horizon_seconds: int | None = None,
) -> Any:
    """Build the public `/v1` ASGI app bound for a distinct listener.

    When ``session_factory`` (and optionally ``enqueue_service`` /
    ``claim_service`` / ``lease_service`` / ``worker_terminal_service`` /
    ``completion_service`` / ``cancellation_service`` / ``inspection_service``)
    is provided, durable producer intake, worker claim, heartbeat, complete,
    fail, ack_cancel, cancel, and task/attempt inspection handlers are
    registered; remaining application operations stay on the Phase 3.2
    skeleton until later plans.
    """

    policy = payload_policy or PayloadHandlingPolicy()
    skeleton = _skeleton_handler(payload_policy=policy)
    handlers: dict[Operation, Handler] = {
        Operation.GET_CAPABILITIES: build_capabilities_handler(
            max_wait_seconds=max_wait_seconds,
        ),
    }
    if session_factory is not None:
        scheduling_policy = (
            SchedulingPolicy(horizon_seconds=schedule_horizon_seconds)
            if schedule_horizon_seconds is not None
            else None
        )
        service = enqueue_service or EnqueueService(
            session_factory=session_factory,
            scheduling_policy=scheduling_policy,
        )
        handlers[Operation.ENQUEUE_TASK] = build_enqueue_handler(
            enqueue_service=service,
            session_factory=session_factory,
            admission_limits=admission_limits,
        )
        handlers[Operation.RESOLVE_SUBMISSION] = build_resolve_submission_handler(
            session_factory=session_factory,
            payload_policy=policy,
        )
        claim = claim_service or ClaimService(session_factory=session_factory)
        handlers[Operation.CLAIM_TASKS] = build_claim_handler(
            claim_service=claim,
            session_factory=session_factory,
            max_request_bytes=(
                admission_limits.max_request_bytes
                if admission_limits is not None
                else DEFAULT_REQUEST_MAX_BYTES
            ),
            max_wait_seconds=max_wait_seconds,
            long_poll_service=claim_long_poll_service,
        )
        lease = lease_service or LeaseService(session_factory=session_factory)
        handlers[Operation.HEARTBEAT_CLAIM] = build_heartbeat_handler(
            lease_service=lease,
            authorizer=authorizer,
            max_request_bytes=(
                admission_limits.max_request_bytes
                if admission_limits is not None
                else DEFAULT_REQUEST_MAX_BYTES
            ),
        )
        terminal = worker_terminal_service or WorkerTerminalService(
            session_factory=session_factory
        )
        completion = completion_service or CompletionService(
            session_factory=session_factory,
            scheduling_policy=scheduling_policy,
        )
        handlers[Operation.COMPLETE_CLAIM] = build_complete_handler(
            completion_service=completion,
            authorizer=authorizer,
            max_request_bytes=(
                admission_limits.max_request_bytes
                if admission_limits is not None
                else DEFAULT_REQUEST_MAX_BYTES
            ),
        )
        handlers[Operation.FAIL_CLAIM] = build_fail_handler(
            worker_terminal_service=terminal,
            authorizer=authorizer,
            max_request_bytes=(
                admission_limits.max_request_bytes
                if admission_limits is not None
                else DEFAULT_REQUEST_MAX_BYTES
            ),
        )
        handlers[Operation.ACKNOWLEDGE_CLAIM_CANCELLATION] = build_ack_cancel_handler(
            worker_terminal_service=terminal,
            authorizer=authorizer,
            max_request_bytes=(
                admission_limits.max_request_bytes
                if admission_limits is not None
                else DEFAULT_REQUEST_MAX_BYTES
            ),
        )
        cancel = cancellation_service or CancellationService(
            session_factory=session_factory
        )
        handlers[Operation.CANCEL_TASK] = build_cancel_handler(
            cancellation_service=cancel,
            authorizer=authorizer,
            max_request_bytes=(
                admission_limits.max_request_bytes
                if admission_limits is not None
                else DEFAULT_REQUEST_MAX_BYTES
            ),
        )
        inspection = inspection_service or TaskInspectionService(
            session_factory=session_factory,
            retention_policy=retention_policy,
        )
        handlers[Operation.GET_TASK] = build_get_task_handler(
            inspection_service=inspection,
            authorizer=authorizer,
        )
        handlers[Operation.LIST_TASK_ATTEMPTS] = build_list_attempts_handler(
            inspection_service=inspection,
            authorizer=authorizer,
        )
    return create_plane_app(
        plane="application",
        authenticator=authenticator,
        authorizer=authorizer,
        bind=bind,
        handler=_dispatch_handler(handlers=handlers, skeleton=skeleton),
        lookup_probe=lookup_probe,
    )
