"""Deny-by-default operation and named-queue authorization (OPS-06 / SEC-02).

Authenticated :class:`~workhold.security.principals.Principal` identity is
trusted; requested operation and queue name are not. Diagnostic ``worker_id`` is
never an authorization input. Failures expose only ``permission_denied``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Final

from workhold.security.principals import Principal, ServiceRole

_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_QUEUE_NAME_MAX_LEN: Final[int] = 128


class Operation(str, Enum):
    """Closed Phase 3 operation catalog.

    HTTP values match OpenAPI ``operationId``s. Deployment operations are
    process-role capabilities outside the HTTP catalog.
    """

    # Public application plane
    GET_CAPABILITIES = "getCapabilities"
    ENQUEUE_TASK = "enqueueTask"
    RESOLVE_SUBMISSION = "resolveSubmission"
    GET_TASK = "getTask"
    CANCEL_TASK = "cancelTask"
    LIST_TASK_ATTEMPTS = "listTaskAttempts"
    CLAIM_TASKS = "claimTasks"
    HEARTBEAT_CLAIM = "heartbeatClaim"
    COMPLETE_CLAIM = "completeClaim"
    FAIL_CLAIM = "failClaim"
    ACKNOWLEDGE_CLAIM_CANCELLATION = "acknowledgeClaimCancellation"

    # Private admin plane
    LIST_QUEUES = "listQueues"
    CREATE_QUEUE = "createQueue"
    GET_QUEUE = "getQueue"
    CREATE_QUEUE_POLICY = "createQueuePolicy"
    ACTIVATE_QUEUE_POLICY = "activateQueuePolicy"
    SET_QUEUE_STATE = "setQueueState"
    LIST_ADMIN_AUDIT = "listAdminAudit"
    LIST_INSPECTION_TASKS = "listInspectionTasks"
    LIST_INSPECTION_ATTEMPTS = "listInspectionAttempts"
    LIST_DEAD_LETTERS = "listDeadLetters"
    GET_STATS = "getStats"
    GET_MAINTENANCE_STATUS = "getMaintenanceStatus"
    RUN_MAINTENANCE = "runMaintenance"
    REPLAY_DEAD_LETTER = "replayDeadLetter"
    PREVIEW_BULK_REPLAY = "previewBulkReplay"
    EXECUTE_BULK_REPLAY = "executeBulkReplay"
    PREVIEW_BULK_CANCEL = "previewBulkCancel"
    EXECUTE_BULK_CANCEL = "executeBulkCancel"
    FORCE_LEASE_EXPIRY = "forceLeaseExpiry"
    RECONCILE_COUNTERS = "reconcileCounters"
    RAISE_REPLAY_LIMIT = "raiseReplayLimit"
    DROP_EXPIRED_PARTITION = "dropExpiredPartition"
    REPAIR_REGISTRY_ENTRY = "repairRegistryEntry"
    FORCE_DELIVERY_RECLAIM = "forceDeliveryReclaim"
    FORCE_DELIVERY_DEAD_LETTER = "forceDeliveryDeadLetter"

    # Deployment process roles (non-HTTP)
    APPLY_MIGRATIONS = "applyMigrations"
    RUN_PARTITION_MAINTENANCE = "runPartitionMaintenance"


QUEUE_SCOPED_OPERATIONS: Final[frozenset[Operation]] = frozenset(
    {
        Operation.ENQUEUE_TASK,
        Operation.RESOLVE_SUBMISSION,
        Operation.GET_TASK,
        Operation.CANCEL_TASK,
        Operation.LIST_TASK_ATTEMPTS,
        Operation.CLAIM_TASKS,
        Operation.HEARTBEAT_CLAIM,
        Operation.COMPLETE_CLAIM,
        Operation.FAIL_CLAIM,
        Operation.ACKNOWLEDGE_CLAIM_CANCELLATION,
        Operation.GET_QUEUE,
        Operation.CREATE_QUEUE_POLICY,
        Operation.ACTIVATE_QUEUE_POLICY,
        Operation.SET_QUEUE_STATE,
        Operation.REPLAY_DEAD_LETTER,
        Operation.PREVIEW_BULK_REPLAY,
        Operation.EXECUTE_BULK_REPLAY,
        Operation.PREVIEW_BULK_CANCEL,
        Operation.EXECUTE_BULK_CANCEL,
        Operation.FORCE_LEASE_EXPIRY,
        Operation.RECONCILE_COUNTERS,
        Operation.RAISE_REPLAY_LIMIT,
        Operation.REPAIR_REGISTRY_ENTRY,
        Operation.FORCE_DELIVERY_RECLAIM,
        Operation.FORCE_DELIVERY_DEAD_LETTER,
    }
)

ROLE_OPERATION_GRANTS: Final[dict[ServiceRole, frozenset[Operation]]] = {
    ServiceRole.PRODUCER: frozenset(
        {
            Operation.GET_CAPABILITIES,
            Operation.ENQUEUE_TASK,
            Operation.RESOLVE_SUBMISSION,
            Operation.GET_TASK,
            Operation.CANCEL_TASK,
        }
    ),
    ServiceRole.WORKER: frozenset(
        {
            Operation.GET_CAPABILITIES,
            Operation.CLAIM_TASKS,
            Operation.HEARTBEAT_CLAIM,
            Operation.COMPLETE_CLAIM,
            Operation.FAIL_CLAIM,
            Operation.ACKNOWLEDGE_CLAIM_CANCELLATION,
        }
    ),
    ServiceRole.OBSERVER: frozenset(
        {
            Operation.GET_CAPABILITIES,
            Operation.GET_TASK,
            Operation.LIST_TASK_ATTEMPTS,
            Operation.GET_STATS,
            Operation.GET_QUEUE,
            Operation.GET_MAINTENANCE_STATUS,
            Operation.LIST_INSPECTION_TASKS,
            Operation.LIST_INSPECTION_ATTEMPTS,
            Operation.LIST_DEAD_LETTERS,
        }
    ),
    ServiceRole.RELAY: frozenset(),
    ServiceRole.ADMIN: frozenset(
        {
            Operation.GET_CAPABILITIES,
            Operation.LIST_QUEUES,
            Operation.CREATE_QUEUE,
            Operation.GET_QUEUE,
            Operation.CREATE_QUEUE_POLICY,
            Operation.ACTIVATE_QUEUE_POLICY,
            Operation.SET_QUEUE_STATE,
            Operation.LIST_ADMIN_AUDIT,
            Operation.LIST_INSPECTION_TASKS,
            Operation.LIST_INSPECTION_ATTEMPTS,
            Operation.LIST_DEAD_LETTERS,
            Operation.GET_STATS,
            Operation.GET_MAINTENANCE_STATUS,
            Operation.RUN_MAINTENANCE,
            Operation.REPLAY_DEAD_LETTER,
            Operation.PREVIEW_BULK_REPLAY,
            Operation.EXECUTE_BULK_REPLAY,
            Operation.PREVIEW_BULK_CANCEL,
            Operation.EXECUTE_BULK_CANCEL,
        }
    ),
    ServiceRole.BREAK_GLASS: frozenset(
        {
            Operation.FORCE_LEASE_EXPIRY,
            Operation.RECONCILE_COUNTERS,
            Operation.RAISE_REPLAY_LIMIT,
            Operation.DROP_EXPIRED_PARTITION,
            Operation.REPAIR_REGISTRY_ENTRY,
            Operation.FORCE_DELIVERY_RECLAIM,
            Operation.FORCE_DELIVERY_DEAD_LETTER,
        }
    ),
    ServiceRole.MIGRATOR: frozenset({Operation.APPLY_MIGRATIONS}),
    ServiceRole.MAINTAINER: frozenset({Operation.RUN_PARTITION_MAINTENANCE}),
}


@dataclass(frozen=True, slots=True)
class AuthorizationDenied:
    """Uniform authorization failure for HTTP 403 projection.

    Contains no queue name, task/claim identifiers, or existence hints.
    """

    code: str = "permission_denied"

    def __repr__(self) -> str:
        return "AuthorizationDenied(code='permission_denied')"

    def __str__(self) -> str:
        return "permission_denied"


@dataclass(frozen=True, slots=True)
class AuthorizationContext:
    """Successful authorization decision for middleware/handlers."""

    principal: Principal
    operation: Operation
    queue_name: str | None
    producer_id: str | None = None


AuthorizationResult = AuthorizationContext | AuthorizationDenied

_DENIED: Final[AuthorizationDenied] = AuthorizationDenied()


def _is_exact_queue_name(queue_name: str) -> bool:
    if len(queue_name) > _QUEUE_NAME_MAX_LEN:
        return False
    return _QUEUE_NAME_RE.fullmatch(queue_name) is not None


def _resolve_operation(operation: Operation | str) -> Operation | None:
    if isinstance(operation, Operation):
        return operation
    try:
        return Operation(operation)
    except ValueError:
        return None


class Authorizer:
    """Pure authorization kernel: role grant ∩ exact named-queue scope.

    Queue scopes are keyed by stable ``principal_id``. Operation grants are a
    closed role table with no inheritance.
    """

    __slots__ = ("_queue_scopes",)

    def __init__(self, queue_scopes: Mapping[str, frozenset[str]]) -> None:
        normalized: dict[str, frozenset[str]] = {}
        for principal_id, queues in queue_scopes.items():
            if not principal_id:
                raise ValueError("principal_id in queue_scopes must be non-empty")
            cleaned: set[str] = set()
            for name in queues:
                if not _is_exact_queue_name(name):
                    raise ValueError(f"invalid queue scope name: {name!r}")
                cleaned.add(name)
            normalized[principal_id] = frozenset(cleaned)
        self._queue_scopes = normalized

    def allows_queue(self, principal: Principal, queue_name: str) -> bool:
        """Return True when ``queue_name`` is an exact grant for the principal.

        Admin principals are instance-scoped for operational lists and may
        inspect any syntactically valid named queue. Observer (and other
        scoped roles) require an exact ``queue_scopes`` membership.
        """

        if not isinstance(principal, Principal):
            return False
        if not _is_exact_queue_name(queue_name):
            return False
        if principal.role is ServiceRole.ADMIN:
            return True
        allowed = self._queue_scopes.get(principal.principal_id, frozenset())
        return queue_name in allowed

    def authorize(
        self,
        principal: Principal,
        operation: Operation | str,
        *,
        queue_name: str | None = None,
    ) -> AuthorizationResult:
        """Allow only when role grants the operation and queue scope matches.

        Unknown operations, missing/malformed queue names for scoped operations,
        out-of-scope queues, and cross-role requests all return the same
        :class:`AuthorizationDenied` value.
        """
        if not isinstance(principal, Principal):
            return _DENIED

        # BREAK_GLASS is JIT-only: missing expiry or audience fails closed (D-01/D-02).
        if principal.role is ServiceRole.BREAK_GLASS:
            if principal.credential_expires_at is None:
                return _DENIED
            if not principal.operation_audience:
                return _DENIED

        if principal.credential_expires_at is not None:
            now = datetime.now(timezone.utc)
            expires = principal.credential_expires_at
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            if now >= expires:
                return _DENIED

        resolved = _resolve_operation(operation)
        if resolved is None:
            return _DENIED

        if (
            principal.operation_audience is not None
            and resolved.value not in principal.operation_audience
        ):
            return _DENIED

        granted = ROLE_OPERATION_GRANTS.get(principal.role, frozenset())
        if resolved not in granted:
            return _DENIED

        if resolved in QUEUE_SCOPED_OPERATIONS:
            if queue_name is None or not _is_exact_queue_name(queue_name):
                return _DENIED
            # ADMIN is instance-scoped: queue-targeted control operations still
            # require a syntactically exact name, but not a per-queue scope.
            # Other HTTP roles remain exact-membership scoped.
            if principal.role is not ServiceRole.ADMIN:
                allowed = self._queue_scopes.get(
                    principal.principal_id, frozenset()
                )
                if queue_name not in allowed:
                    return _DENIED
            effective_queue: str | None = queue_name
        else:
            # Instance-scoped: role grant alone; ignore/absent queue is fine.
            # A malformed queue name still fails closed to avoid wildcard smuggling.
            if queue_name is not None and not _is_exact_queue_name(queue_name):
                return _DENIED
            effective_queue = None

        producer_id: str | None = None
        if resolved is Operation.ENQUEUE_TASK and principal.role is ServiceRole.PRODUCER:
            producer_id = principal.principal_id

        return AuthorizationContext(
            principal=principal,
            operation=resolved,
            queue_name=effective_queue,
            producer_id=producer_id,
        )
