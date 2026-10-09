"""Authentication and rotation tests for deployment-issued service principals.

Covers OPS-06 / SEC-01 / SEC-04 behaviours for Plan 03.2-03:
role isolation, fail-closed auth, overlap rotation, revocation, ambiguity,
and non-disclosure of credential material.
"""

from __future__ import annotations

import pytest

from workhold.settings import CredentialGeneration, Secret
from workhold.security.credentials import (
    AuthenticationResult,
    BearerCredentialAuthenticator,
    CredentialBinding,
    Unauthenticated,
)
from workhold.security.principals import Principal, ServiceRole


def _binding(
    *,
    principal_id: str,
    role: ServiceRole,
    generation_id: str,
    secret: str,
    enabled: bool = True,
) -> CredentialBinding:
    return CredentialBinding(
        principal_id=principal_id,
        role=role,
        generation_id=generation_id,
        secret=Secret(secret),
        enabled=enabled,
    )


def _auth(*bindings: CredentialBinding) -> BearerCredentialAuthenticator:
    return BearerCredentialAuthenticator.from_bindings(bindings)


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def _assert_unauthenticated(result: AuthenticationResult, *forbidden: str) -> None:
    assert isinstance(result, Unauthenticated)
    assert result.code == "unauthenticated"
    rendered = repr(result) + str(result)
    for value in forbidden:
        assert value not in rendered


def test_all_service_roles_are_distinct() -> None:
    roles = list(ServiceRole)
    assert [role.value for role in roles] == [
        "producer",
        "worker",
        "relay",
        "observer",
        "admin",
        "migrator",
        "maintainer",
    ]
    assert len({role.value for role in roles}) == len(roles)


@pytest.mark.parametrize(
    ("role", "token"),
    [
        (ServiceRole.PRODUCER, "tok-producer"),
        (ServiceRole.WORKER, "tok-worker"),
        (ServiceRole.RELAY, "tok-relay"),
        (ServiceRole.OBSERVER, "tok-observer"),
        (ServiceRole.ADMIN, "tok-admin"),
        (ServiceRole.MIGRATOR, "tok-migrator"),
        (ServiceRole.MAINTAINER, "tok-maintainer"),
    ],
)
def test_valid_bearer_authenticates_to_stable_principal_and_role(
    role: ServiceRole,
    token: str,
) -> None:
    auth = _auth(
        _binding(
            principal_id=f"{role.value}-a",
            role=role,
            generation_id="g1",
            secret=token,
        )
    )
    result = auth.authenticate(_bearer(token))
    assert isinstance(result, Principal)
    assert result.principal_id == f"{role.value}-a"
    assert result.role is role


def test_role_credentials_are_not_interchangeable() -> None:
    producer_token = "producer-secret-aaa"
    admin_token = "admin-secret-bbb"
    auth = _auth(
        _binding(
            principal_id="prod-1",
            role=ServiceRole.PRODUCER,
            generation_id="g-prod",
            secret=producer_token,
        ),
        _binding(
            principal_id="admin-1",
            role=ServiceRole.ADMIN,
            generation_id="g-admin",
            secret=admin_token,
        ),
    )

    producer = auth.authenticate(_bearer(producer_token))
    admin = auth.authenticate(_bearer(admin_token))
    assert isinstance(producer, Principal)
    assert isinstance(admin, Principal)
    assert producer.role is ServiceRole.PRODUCER
    assert admin.role is ServiceRole.ADMIN
    assert producer.principal_id != admin.principal_id

    # Cross-role use of the other credential still authenticates only as its own role.
    assert auth.authenticate(_bearer(producer_token)).role is ServiceRole.PRODUCER
    assert auth.authenticate(_bearer(admin_token)).role is ServiceRole.ADMIN


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "Bearer",
        "Bearer ",
        "bearer tok-valid",
        "Basic tok-valid",
        "Bearer tok-valid extra",
        "Bearer\ttok-valid",
        "TokWithoutScheme",
    ],
)
def test_missing_and_malformed_credentials_are_unauthenticated(header: str | None) -> None:
    auth = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret="tok-valid",
        )
    )
    _assert_unauthenticated(auth.authenticate(header), "tok-valid")


def test_unknown_credential_is_unauthenticated() -> None:
    auth = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret="known-secret",
        )
    )
    _assert_unauthenticated(auth.authenticate(_bearer("unknown-secret")), "unknown-secret")


def test_disabled_credential_is_unauthenticated() -> None:
    auth = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g-old",
            secret="revoked-secret",
            enabled=False,
        ),
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g-new",
            secret="active-secret",
            enabled=True,
        ),
    )
    _assert_unauthenticated(auth.authenticate(_bearer("revoked-secret")), "revoked-secret")
    result = auth.authenticate(_bearer("active-secret"))
    assert isinstance(result, Principal)
    assert result.principal_id == "worker-1"


def test_overlap_rotation_both_generations_authenticate_same_principal() -> None:
    auth = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g-old",
            secret="secret-old",
        ),
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g-new",
            secret="secret-new",
        ),
    )
    old = auth.authenticate(_bearer("secret-old"))
    new = auth.authenticate(_bearer("secret-new"))
    assert isinstance(old, Principal)
    assert isinstance(new, Principal)
    assert old == new
    assert old.principal_id == "worker-1"
    assert old.role is ServiceRole.WORKER


def test_disabling_old_generation_revokes_only_it() -> None:
    rotated = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g-old",
            secret="secret-old",
            enabled=False,
        ),
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g-new",
            secret="secret-new",
            enabled=True,
        ),
    )
    _assert_unauthenticated(rotated.authenticate(_bearer("secret-old")), "secret-old")
    still = rotated.authenticate(_bearer("secret-new"))
    assert isinstance(still, Principal)
    assert still.principal_id == "worker-1"


def test_duplicate_secret_across_principals_is_ambiguous_unauthenticated() -> None:
    shared = "same-material-for-two-principals"
    auth = _auth(
        _binding(
            principal_id="producer-1",
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=shared,
        ),
        _binding(
            principal_id="admin-1",
            role=ServiceRole.ADMIN,
            generation_id="g2",
            secret=shared,
        ),
    )
    _assert_unauthenticated(auth.authenticate(_bearer(shared)), shared)


def test_claim_token_header_is_not_used_as_identity() -> None:
    auth = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret="worker-bearer",
        )
    )
    # Authenticator API accepts only the Authorization header value.
    _assert_unauthenticated(auth.authenticate(None), "worker-bearer")
    result = auth.authenticate(_bearer("worker-bearer"))
    assert isinstance(result, Principal)


def test_authenticator_retains_no_cleartext_secrets() -> None:
    clear = "super-secret-cleartext-xyz"
    auth = _auth(
        _binding(
            principal_id="prod-1",
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=clear,
        )
    )
    assert clear not in repr(auth)
    assert clear not in str(auth)
    # Internal attrs should not expose Secret or cleartext.
    for name, value in vars(auth).items():
        rendered = repr(value) + str(value)
        assert clear not in rendered, name
        assert "Secret(" not in rendered or "***" in rendered


def test_principal_and_failure_never_echo_secrets() -> None:
    clear = "leak-me-please-token"
    auth = _auth(
        _binding(
            principal_id="observer-1",
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=clear,
        )
    )
    ok = auth.authenticate(_bearer(clear))
    bad = auth.authenticate(_bearer("wrong"))
    assert isinstance(ok, Principal)
    assert clear not in repr(ok)
    assert clear not in str(ok)
    _assert_unauthenticated(bad, clear)

    with pytest.raises(Exception) as exc_info:
        raise RuntimeError(f"auth failed: {bad!r} principal={ok!r} auth={auth!r}")
    assert clear not in str(exc_info.value)


def test_from_credential_generations_uses_validated_settings_rows() -> None:
    generations = (
        CredentialGeneration(
            principal_id="relay-1",
            generation_id="g1",
            secret=Secret("relay-token"),
        ),
        CredentialGeneration(
            principal_id="relay-1",
            generation_id="g2",
            secret=Secret("relay-token-new"),
        ),
    )
    auth = BearerCredentialAuthenticator.from_credential_generations(
        generations,
        roles_by_principal={"relay-1": ServiceRole.RELAY},
    )
    first = auth.authenticate(_bearer("relay-token"))
    second = auth.authenticate(_bearer("relay-token-new"))
    assert isinstance(first, Principal)
    assert isinstance(second, Principal)
    assert first == second
    assert first.role is ServiceRole.RELAY


def test_oversized_bearer_token_rejected_before_comparison() -> None:
    auth = _auth(
        _binding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret="short",
        )
    )
    huge = "x" * 10_000
    _assert_unauthenticated(auth.authenticate(_bearer(huge)), huge)
