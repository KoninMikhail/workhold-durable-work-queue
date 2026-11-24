"""JWT verifier behavior for the remote MCP resource server (MCP-15, D-09, D-20).

Every case uses a local key through an injected callable. No JWKS URL is
fetched and no socket is opened.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import urllib.request
from collections.abc import Callable
from typing import Any

import jwt
import pytest

from tests.sdk.mcp.jwks_fixture import (
    ISSUER,
    RESOURCE,
    KeyMaterial,
    base_claims,
    make_ec_key,
    make_rsa_key,
    mint,
    mint_hs256,
    mint_unsigned,
)

SKEW = 60


@pytest.fixture(scope="module")
def rsa_key() -> KeyMaterial:
    return make_rsa_key()


@pytest.fixture(scope="module")
def ec_key() -> KeyMaterial:
    return make_ec_key()


@pytest.fixture(scope="module")
def other_rsa_key() -> KeyMaterial:
    return make_rsa_key(kid="rsa-test")


def _verifier(
    key: KeyMaterial,
    *,
    skew: int = SKEW,
    key_for_token: Callable[[str], Any] | None = None,
) -> Any:
    from workhold_mcp.jwt_verifier import JwtJwksTokenVerifier

    return JwtJwksTokenVerifier(
        issuer=ISSUER,
        resource=RESOURCE,
        key_for_token=key_for_token or (lambda _token: key.pyjwk()),
        clock_skew_seconds=skew,
    )


def _verify(verifier: Any, token: str) -> Any:
    return asyncio.run(verifier.verify_token(token))


def _assert_rejected(result: Any, *forbidden: str) -> None:
    assert result is None
    rendered = repr(result) + str(result)
    for value in forbidden:
        assert value not in rendered


def test_pyjwt_signature_preflight() -> None:
    parameters = inspect.signature(jwt.decode).parameters
    for name in ("algorithms", "audience", "issuer", "leeway"):
        assert name in parameters
    assert hasattr(jwt.PyJWKClient, "get_signing_key_from_jwt")


@pytest.mark.parametrize("key_name", ["rsa_key", "ec_key"])
def test_rs256_and_es256_verify(key_name: str, request: pytest.FixtureRequest) -> None:
    key: KeyMaterial = request.getfixturevalue(key_name)
    claims = base_claims(client_id=None, sub="user-1", azp="client-azp")
    token = mint(key, claims)

    access = _verify(_verifier(key), token)

    assert access is not None
    assert access.token == token
    assert access.client_id == "client-azp"
    assert access.scopes == ["workhold-mcp:read"]
    assert access.resource == RESOURCE
    assert access.subject == "user-1"
    assert access.expires_at == claims["exp"]


def test_client_id_precedence(rsa_key: KeyMaterial) -> None:
    verifier = _verifier(rsa_key)

    explicit = _verify(verifier, mint(rsa_key, base_claims(client_id="explicit", azp="azp")))
    from_azp = _verify(verifier, mint(rsa_key, base_claims(azp="azp")))
    from_sub = _verify(verifier, mint(rsa_key, base_claims(azp=None, sub="subject-only")))

    assert explicit is not None and explicit.client_id == "explicit"
    assert from_azp is not None and from_azp.client_id == "azp"
    assert from_sub is not None and from_sub.client_id == "subject-only"


def test_rejects_none_hs256_and_bad_signature(
    rsa_key: KeyMaterial, other_rsa_key: KeyMaterial
) -> None:
    claims = base_claims()
    verifier = _verifier(rsa_key)

    unsigned = mint_unsigned(claims)
    hmac = mint_hs256(claims)
    forged = mint(other_rsa_key, claims)

    _assert_rejected(_verify(verifier, unsigned), unsigned)
    _assert_rejected(_verify(verifier, hmac), hmac)
    _assert_rejected(_verify(verifier, forged), forged)
    _assert_rejected(_verify(verifier, "not-a-jwt"), "not-a-jwt")


def test_rejects_wrong_iss_aud_and_expiry(rsa_key: KeyMaterial) -> None:
    verifier = _verifier(rsa_key)
    now = int(time.time())

    wrong_iss = mint(rsa_key, base_claims(iss="https://other-idp.example.test"))
    wrong_aud = mint(rsa_key, base_claims(aud="https://other.example.test/mcp"))
    aud_list_without_resource = mint(
        rsa_key, base_claims(aud=["https://a.example.test", "https://b.example.test"])
    )
    missing_aud = mint(rsa_key, base_claims(aud=None))
    missing_exp = mint(rsa_key, base_claims(exp=None))
    expired = mint(rsa_key, base_claims(exp=now - SKEW - 1))

    for token in (
        wrong_iss,
        wrong_aud,
        aud_list_without_resource,
        missing_aud,
        missing_exp,
        expired,
    ):
        _assert_rejected(_verify(verifier, token), token)


def test_audience_list_containing_resource_verifies(rsa_key: KeyMaterial) -> None:
    token = mint(rsa_key, base_claims(aud=["https://other.example.test", RESOURCE]))

    access = _verify(_verifier(rsa_key), token)

    assert access is not None
    assert access.resource == RESOURCE


def test_clock_skew_window(rsa_key: KeyMaterial) -> None:
    verifier = _verifier(rsa_key)
    now = int(time.time())

    future = _verify(verifier, mint(rsa_key, base_claims(exp=now + 1)))
    inside_skew = _verify(verifier, mint(rsa_key, base_claims(exp=now - 30)))

    assert future is not None
    assert future.expires_at == now + 1
    assert inside_skew is not None
    # The SDK bearer middleware rejects expires_at < int(time.time()) with no
    # leeway, so an accepted in-skew token must carry a still-future expiry.
    assert inside_skew.expires_at is not None
    assert inside_skew.expires_at >= int(time.time())

    strict = _verifier(rsa_key, skew=0)
    expired_for_strict = mint(rsa_key, base_claims(exp=now - 30))
    _assert_rejected(_verify(strict, expired_for_strict), expired_for_strict)


def test_scope_and_scp(rsa_key: KeyMaterial) -> None:
    verifier = _verifier(rsa_key)

    space_delimited = _verify(
        verifier, mint(rsa_key, base_claims(scope="workhold-mcp:read workhold-mcp:operate"))
    )
    scp_list = _verify(
        verifier, mint(rsa_key, base_claims(scope=None, scp=["workhold-mcp:read", "other:x"]))
    )
    scp_string = _verify(
        verifier, mint(rsa_key, base_claims(scope=None, scp="workhold-mcp:operate"))
    )
    matching = _verify(
        verifier,
        mint(
            rsa_key,
            base_claims(
                scope="workhold-mcp:read workhold-mcp:operate",
                scp=["workhold-mcp:operate", "workhold-mcp:read"],
            ),
        ),
    )
    non_strings_ignored = _verify(
        verifier,
        mint(rsa_key, base_claims(scope=None, scp=["workhold-mcp:read", 7, None, {"x": 1}])),
    )
    no_scope = _verify(verifier, mint(rsa_key, base_claims(scope=None)))

    assert space_delimited is not None
    assert set(space_delimited.scopes) == {"workhold-mcp:read", "workhold-mcp:operate"}
    assert scp_list is not None
    assert set(scp_list.scopes) == {"workhold-mcp:read", "other:x"}
    assert scp_string is not None
    assert scp_string.scopes == ["workhold-mcp:operate"]
    assert matching is not None
    assert set(matching.scopes) == {"workhold-mcp:read", "workhold-mcp:operate"}
    assert non_strings_ignored is not None
    assert non_strings_ignored.scopes == ["workhold-mcp:read"]
    assert no_scope is not None
    assert no_scope.scopes == []


def test_disagreeing_scope_and_scp_are_rejected(rsa_key: KeyMaterial) -> None:
    verifier = _verifier(rsa_key)
    disagreeing = mint(
        rsa_key,
        base_claims(scope="workhold-mcp:read", scp=["workhold-mcp:operate"]),
    )
    subset_only = mint(
        rsa_key,
        base_claims(scope="workhold-mcp:read workhold-mcp:operate", scp=["workhold-mcp:read"]),
    )

    _assert_rejected(_verify(verifier, disagreeing), disagreeing)
    _assert_rejected(_verify(verifier, subset_only), subset_only)


def test_missing_client_identity_is_rejected(rsa_key: KeyMaterial) -> None:
    token = mint(rsa_key, base_claims(client_id=None, azp=None, sub=None))

    _assert_rejected(_verify(_verifier(rsa_key), token), token)


def test_non_string_subject_is_not_exposed(rsa_key: KeyMaterial) -> None:
    token = mint(rsa_key, base_claims(sub=12345, azp="client-1"))

    access = _verify(_verifier(rsa_key), token)

    assert access is not None
    assert access.client_id == "client-1"
    assert access.subject is None


def test_hostile_jku_does_not_choose_the_key(
    rsa_key: KeyMaterial, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("verifier must not fetch a token-supplied URL")

    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    calls: list[str] = []

    def key_for_token(token: str) -> Any:
        calls.append(token)
        return rsa_key.pyjwk()

    token = mint(
        rsa_key,
        base_claims(),
        headers={"jku": "https://attacker.example/jwks.json", "x5u": "https://attacker.example/x5"},
    )

    access = _verify(_verifier(rsa_key, key_for_token=key_for_token), token)

    assert access is not None
    assert calls == [token]


def test_key_loader_failure_is_rejected_without_leaking(rsa_key: KeyMaterial) -> None:
    def broken(_token: str) -> Any:
        raise RuntimeError("jwks unreachable https://idp.example.test/jwks")

    token = mint(rsa_key, base_claims())

    result = _verify(_verifier(rsa_key, key_for_token=broken), token)

    _assert_rejected(result, token, "jwks unreachable")


def test_unsupported_algorithm_does_not_load_a_key(rsa_key: KeyMaterial) -> None:
    calls: list[str] = []

    def key_for_token(token: str) -> Any:
        calls.append(token)
        return rsa_key.pyjwk()

    verifier = _verifier(rsa_key, key_for_token=key_for_token)

    assert _verify(verifier, mint_unsigned(base_claims())) is None
    assert _verify(verifier, mint_hs256(base_claims())) is None
    assert calls == []
