"""JIT BREAK_GLASS fail-closed credentials (D-01..D-03 / REC-03 / SEC-01).

``ServiceRole.BREAK_GLASS`` bindings must carry timezone-aware ``expires_at`` and
a non-empty ``allowed_operations`` audience. Misconfig fails at bind time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from queue_service.security.authorization import (
    AuthorizationDenied,
    Authorizer,
    Operation,
)
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
    Unauthenticated,
)
from queue_service.security.principals import Principal, ServiceRole
from queue_service.settings import Secret

_BG_OPS = frozenset({"forceLeaseExpiry", "reconcileCounters"})
_TOKEN = "tok-break-glass-jit"
_ADMIN_TOKEN = "tok-admin-jit"


def _bg_binding(**overrides: object) -> CredentialBinding:
    now = datetime.now(timezone.utc)
    fields: dict[str, object] = {
        "principal_id": "break-glass-jit",
        "role": ServiceRole.BREAK_GLASS,
        "generation_id": "g1",
        "secret": Secret(_TOKEN),
        "expires_at": now + timedelta(hours=1),
        "allowed_operations": _BG_OPS,
    }
    fields.update(overrides)
    return CredentialBinding(**fields)  # type: ignore[arg-type]


def test_break_glass_missing_expires_at_fails_closed() -> None:
    with pytest.raises(ValueError, match="expires_at"):
        _bg_binding(expires_at=None)


def test_break_glass_missing_allowed_operations_fails_closed() -> None:
    with pytest.raises(ValueError, match="allowed_operations"):
        _bg_binding(allowed_operations=None)


def test_break_glass_empty_allowed_operations_fails_closed() -> None:
    with pytest.raises(ValueError, match="allowed_operations"):
        _bg_binding(allowed_operations=frozenset())


def test_break_glass_naive_expires_at_rejected_at_bind() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _bg_binding(expires_at=datetime(2099, 1, 1, 0, 0, 0))


def test_admin_may_omit_expires_at_and_allowed_operations() -> None:
    binding = CredentialBinding(
        principal_id="admin-jit",
        role=ServiceRole.ADMIN,
        generation_id="g1",
        secret=Secret(_ADMIN_TOKEN),
    )
    auth = BearerCredentialAuthenticator.from_bindings((binding,))
    result = auth.authenticate(f"Bearer {_ADMIN_TOKEN}")
    assert isinstance(result, Principal)
    assert result.role is ServiceRole.ADMIN
    assert result.credential_expires_at is None
    assert result.operation_audience is None


def test_expired_break_glass_never_authenticates() -> None:
    now = datetime.now(timezone.utc)
    binding = _bg_binding(expires_at=now - timedelta(seconds=5))
    auth = BearerCredentialAuthenticator.from_bindings(
        (binding,),
        clock=lambda: now,
    )
    result = auth.authenticate(f"Bearer {_TOKEN}")
    assert isinstance(result, Unauthenticated)
    assert result.code == "unauthenticated"


def test_audience_excluding_op_denies_authorize() -> None:
    principal = Principal(
        principal_id="break-glass-narrow",
        role=ServiceRole.BREAK_GLASS,
        credential_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        operation_audience=frozenset({"reconcileCounters"}),
    )
    authorizer = Authorizer(
        queue_scopes={"break-glass-narrow": frozenset({"orders"})}
    )
    result = authorizer.authorize(
        principal,
        Operation.FORCE_LEASE_EXPIRY,
        queue_name="orders",
    )
    assert isinstance(result, AuthorizationDenied)
    assert result.code == "permission_denied"


def test_break_glass_with_jit_fields_authenticates() -> None:
    binding = _bg_binding()
    auth = BearerCredentialAuthenticator.from_bindings((binding,))
    result = auth.authenticate(f"Bearer {_TOKEN}")
    assert isinstance(result, Principal)
    assert result.role is ServiceRole.BREAK_GLASS
    assert result.credential_expires_at is not None
    assert result.operation_audience == _BG_OPS


def test_admin_cannot_authorize_break_glass_operations() -> None:
    principal = Principal(principal_id="admin-no-bg", role=ServiceRole.ADMIN)
    authorizer = Authorizer(queue_scopes={"admin-no-bg": frozenset({"orders"})})
    for operation in (
        Operation.FORCE_LEASE_EXPIRY,
        Operation.RECONCILE_COUNTERS,
        Operation.RAISE_REPLAY_LIMIT,
        Operation.DROP_EXPIRED_PARTITION,
        Operation.REPAIR_REGISTRY_ENTRY,
    ):
        queue_name = "orders"
        result = authorizer.authorize(principal, operation, queue_name=queue_name)
        assert isinstance(result, AuthorizationDenied)
        assert result.code == "permission_denied"
