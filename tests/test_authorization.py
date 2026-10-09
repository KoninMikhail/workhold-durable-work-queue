"""Deny-by-default authorization matrix (OPS-06 / SEC-01 / SEC-02).

Plan 03.2-04: every role × operation family is explicit; queue scope is exact;
failures are uniform ``permission_denied`` without resource enumeration.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import pytest

from queue_service.security.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    Authorizer,
    Operation,
    QUEUE_SCOPED_OPERATIONS,
    ROLE_OPERATION_GRANTS,
)
from queue_service.security.principals import Principal, ServiceRole

# Closed operation families for the exhaustive role matrix.
OPERATION_FAMILIES: dict[str, frozenset[Operation]] = {
    "producer_public": frozenset(
        {
            Operation.ENQUEUE_TASK,
            Operation.RESOLVE_SUBMISSION,
            Operation.GET_TASK,
            Operation.CANCEL_TASK,
        }
    ),
    "worker_public": frozenset(
        {
            Operation.CLAIM_TASKS,
            Operation.HEARTBEAT_CLAIM,
            Operation.COMPLETE_CLAIM,
            Operation.FAIL_CLAIM,
            Operation.ACKNOWLEDGE_CLAIM_CANCELLATION,
        }
    ),
    "observer_reads": frozenset(
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
    "admin_private": frozenset(
        {
            Operation.LIST_QUEUES,
            Operation.CREATE_QUEUE,
            Operation.CREATE_QUEUE_POLICY,
            Operation.ACTIVATE_QUEUE_POLICY,
            Operation.SET_QUEUE_STATE,
            Operation.LIST_ADMIN_AUDIT,
            Operation.RUN_MAINTENANCE,
            Operation.REPLAY_DEAD_LETTER,
            Operation.PREVIEW_BULK_REPLAY,
            Operation.EXECUTE_BULK_REPLAY,
            Operation.PREVIEW_BULK_CANCEL,
            Operation.EXECUTE_BULK_CANCEL,
        }
    ),
    "break_glass": frozenset(
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
    "deployment": frozenset(
        {
            Operation.APPLY_MIGRATIONS,
            Operation.RUN_PARTITION_MAINTENANCE,
        }
    ),
}

ALL_OPERATIONS = frozenset().union(*OPERATION_FAMILIES.values())


def _principal(role: ServiceRole, principal_id: str = "principal-a") -> Principal:
    if role is ServiceRole.BREAK_GLASS:
        return Principal(
            principal_id=principal_id,
            role=role,
            credential_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            operation_audience=frozenset(
                op.value for op in OPERATION_FAMILIES["break_glass"]
            ),
        )
    return Principal(principal_id=principal_id, role=role)


def _authorizer(
    *,
    principal_id: str = "principal-a",
    queues: Iterable[str] = ("orders", "billing"),
) -> Authorizer:
    return Authorizer(queue_scopes={principal_id: frozenset(queues)})


def _assert_denied(result: object) -> AuthorizationDenied:
    assert isinstance(result, AuthorizationDenied)
    assert result.code == "permission_denied"
    # Non-enumerating: no resource/task/claim existence hints.
    rendered = repr(result) + str(result)
    for forbidden in ("task_id", "claim_id", "exists", "not_found", "orders", "billing"):
        assert forbidden not in rendered
    assert not hasattr(result, "details") or not result.details  # type: ignore[union-attr]
    return result


def _assert_allowed(
    result: object,
    *,
    principal: Principal,
    operation: Operation,
    queue_name: str | None,
) -> AuthorizationContext:
    assert isinstance(result, AuthorizationContext)
    assert result.principal == principal
    assert result.operation is operation
    assert result.queue_name == queue_name
    return result


@pytest.mark.parametrize("role", list(ServiceRole))
@pytest.mark.parametrize("family_name,operations", sorted(OPERATION_FAMILIES.items()))
def test_role_operation_family_matrix(
    role: ServiceRole,
    family_name: str,
    operations: frozenset[Operation],
) -> None:
    """Every role × operation family: explicit allow or cross-role denial."""
    principal = _principal(role)
    authz = _authorizer()
    granted = ROLE_OPERATION_GRANTS[role]

    for operation in sorted(operations, key=lambda op: op.value):
        queue_name = "orders" if operation in QUEUE_SCOPED_OPERATIONS else None
        result = authz.authorize(principal, operation, queue_name=queue_name)
        if operation in granted:
            ctx = _assert_allowed(
                result,
                principal=principal,
                operation=operation,
                queue_name=queue_name,
            )
            if operation is Operation.ENQUEUE_TASK and role is ServiceRole.PRODUCER:
                assert ctx.producer_id == principal.principal_id
        else:
            _assert_denied(result)


def test_matrix_covers_every_operation_and_role_grant() -> None:
    assert set(Operation) == ALL_OPERATIONS
    assert set(ROLE_OPERATION_GRANTS) == set(ServiceRole)
    for role, grants in ROLE_OPERATION_GRANTS.items():
        assert grants <= ALL_OPERATIONS
        # No implicit hierarchy: admin does not inherit producer/worker grants.
        if role is ServiceRole.ADMIN:
            assert Operation.ENQUEUE_TASK not in grants
            assert Operation.CLAIM_TASKS not in grants
            # Admin may share instance-scoped observer reads (getStats + lists).
            assert Operation.GET_STATS in grants
            assert Operation.LIST_INSPECTION_TASKS in grants
            assert Operation.LIST_DEAD_LETTERS in grants
        if role is ServiceRole.PRODUCER:
            assert not (grants & OPERATION_FAMILIES["admin_private"])
            assert not (grants & OPERATION_FAMILIES["worker_public"])
            assert not (grants & OPERATION_FAMILIES["deployment"])
            assert Operation.GET_STATS not in grants
            assert Operation.LIST_INSPECTION_TASKS not in grants
        if role is ServiceRole.WORKER:
            assert not (grants & OPERATION_FAMILIES["admin_private"])
            assert Operation.ENQUEUE_TASK not in grants
            assert Operation.GET_STATS not in grants
            assert Operation.LIST_DEAD_LETTERS not in grants
        if role is ServiceRole.OBSERVER:
            assert grants <= OPERATION_FAMILIES["observer_reads"]
            assert not (grants & OPERATION_FAMILIES["admin_private"])
            assert Operation.GET_STATS in grants
            assert Operation.LIST_INSPECTION_TASKS in grants
            assert Operation.LIST_ADMIN_AUDIT not in grants
        if role is ServiceRole.RELAY:
            assert grants == frozenset()
        if role is ServiceRole.BREAK_GLASS:
            assert grants == OPERATION_FAMILIES["break_glass"]
            assert Operation.LIST_QUEUES not in grants
            assert Operation.EXECUTE_BULK_CANCEL not in grants
        if role is ServiceRole.MIGRATOR:
            assert grants == frozenset({Operation.APPLY_MIGRATIONS})
        if role is ServiceRole.MAINTAINER:
            assert grants == frozenset({Operation.RUN_PARTITION_MAINTENANCE})


def test_exact_queue_match_allows_scoped_operation() -> None:
    principal = _principal(ServiceRole.PRODUCER)
    authz = _authorizer(queues=("orders",))
    result = authz.authorize(principal, Operation.ENQUEUE_TASK, queue_name="orders")
    ctx = _assert_allowed(
        result,
        principal=principal,
        operation=Operation.ENQUEUE_TASK,
        queue_name="orders",
    )
    assert ctx.producer_id == "principal-a"


def test_out_of_scope_queue_denies_with_permission_denied() -> None:
    principal = _principal(ServiceRole.PRODUCER)
    authz = _authorizer(queues=("orders",))
    result = authz.authorize(principal, Operation.ENQUEUE_TASK, queue_name="billing")
    _assert_denied(result)


@pytest.mark.parametrize(
    "queue_name",
    [
        None,
        "",
        "*",
        "Orders",
        "orders/*",
        "../orders",
        "orders?",
        "a" * 129,
        " orders",
        "orders ",
        "ord ers",
    ],
)
def test_absent_or_malformed_queue_scope_denies(queue_name: str | None) -> None:
    principal = _principal(ServiceRole.WORKER)
    authz = _authorizer(queues=("orders",))
    result = authz.authorize(principal, Operation.CLAIM_TASKS, queue_name=queue_name)
    _assert_denied(result)


def test_unknown_operation_denies_closed() -> None:
    principal = _principal(ServiceRole.ADMIN)
    authz = _authorizer()
    result = authz.authorize(principal, "notARealOperation", queue_name=None)
    _assert_denied(result)


def test_admin_ops_denied_for_application_roles() -> None:
    authz = _authorizer()
    for role in (
        ServiceRole.PRODUCER,
        ServiceRole.WORKER,
        ServiceRole.OBSERVER,
        ServiceRole.RELAY,
    ):
        principal = _principal(role)
        for operation in OPERATION_FAMILIES["admin_private"]:
            queue_name = "orders" if operation in QUEUE_SCOPED_OPERATIONS else None
            _assert_denied(authz.authorize(principal, operation, queue_name=queue_name))


def test_application_ops_denied_for_admin_credentials() -> None:
    principal = _principal(ServiceRole.ADMIN)
    authz = _authorizer()
    for operation in (
        Operation.ENQUEUE_TASK,
        Operation.CLAIM_TASKS,
        Operation.GET_TASK,
    ):
        queue_name = "orders" if operation in QUEUE_SCOPED_OPERATIONS else None
        _assert_denied(authz.authorize(principal, operation, queue_name=queue_name))


def test_admin_may_discover_capabilities() -> None:
    """OpenAPI allows ADMIN on getCapabilities; grant table matches (15-CONTEXT)."""
    principal = _principal(ServiceRole.ADMIN)
    authz = _authorizer()
    result = authz.authorize(principal, Operation.GET_CAPABILITIES, queue_name=None)
    _assert_allowed(
        result,
        principal=principal,
        operation=Operation.GET_CAPABILITIES,
        queue_name=None,
    )


def test_producer_cannot_list_task_attempts() -> None:
    """Attempt history is observer/admin-tool only; PRODUCER keeps getTask."""
    principal = _principal(ServiceRole.PRODUCER)
    authz = _authorizer()
    _assert_denied(
        authz.authorize(principal, Operation.LIST_TASK_ATTEMPTS, queue_name="orders")
    )


def test_producer_idempotency_namespace_preserved_on_enqueue_allow() -> None:
    principal = _principal(ServiceRole.PRODUCER, principal_id="producer-stable-9")
    authz = _authorizer(principal_id="producer-stable-9", queues=("orders",))
    result = authz.authorize(principal, Operation.ENQUEUE_TASK, queue_name="orders")
    ctx = _assert_allowed(
        result,
        principal=principal,
        operation=Operation.ENQUEUE_TASK,
        queue_name="orders",
    )
    assert ctx.producer_id == "producer-stable-9"


def test_non_enqueue_allow_has_no_producer_id() -> None:
    principal = _principal(ServiceRole.WORKER)
    authz = _authorizer()
    result = authz.authorize(principal, Operation.CLAIM_TASKS, queue_name="orders")
    ctx = _assert_allowed(
        result,
        principal=principal,
        operation=Operation.CLAIM_TASKS,
        queue_name="orders",
    )
    assert ctx.producer_id is None


def test_missing_queue_scope_binding_denies() -> None:
    """Principal with no configured scopes cannot pass queue-scoped checks."""
    principal = _principal(ServiceRole.PRODUCER, principal_id="unscoped")
    authz = Authorizer(queue_scopes={})
    _assert_denied(authz.authorize(principal, Operation.ENQUEUE_TASK, queue_name="orders"))


def test_instance_scoped_admin_allow_without_queue() -> None:
    principal = _principal(ServiceRole.ADMIN)
    authz = _authorizer()
    result = authz.authorize(principal, Operation.LIST_QUEUES, queue_name=None)
    _assert_allowed(
        result,
        principal=principal,
        operation=Operation.LIST_QUEUES,
        queue_name=None,
    )


def test_worker_id_is_not_an_authorization_input() -> None:
    """Authorize accepts only Principal — no worker_id parameter exists."""
    import inspect

    sig = inspect.signature(Authorizer.authorize)
    assert "worker_id" not in sig.parameters
