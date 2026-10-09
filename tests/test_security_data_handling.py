"""Adversarial leakage, retention config, and Phase 3.8 handoff tests (SEC-03, SEC-05).

Phase 3.2 covers redaction/configuration only. Physical payload expiry enforcement
belongs to Phase 3.8 and must consume ``PayloadRetentionPolicy``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from workhold import settings
from workhold.security import payload_policy
from workhold.security import redaction
from workhold.security.payload_policy import (
    PayloadIndexingRejected,
    PayloadRetentionPolicy,
    PayloadView,
)


def _role_pools() -> dict[str, settings.RolePoolSettings]:
    return {
        role: settings.RolePoolSettings(
            replica_ceiling=1,
            pool_ceiling=2,
            pool_acquisition_timeout_seconds=5.0,
            statement_timeout_seconds=30.0,
        )
        for role in settings.PROCESS_ROLES
    }


def _settings(**overrides: object) -> settings.DeploymentSettings:
    base: dict[str, object] = {
        "environment": settings.EnvironmentMode.DEVELOPMENT,
        "listener_tls_mode": settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        "database_url": settings.Secret("postgresql+psycopg://queue:s3cret@localhost/queue"),
        "postgres_max_connections": 100,
        "postgres_reserved_connections": 10,
        "role_pools": _role_pools(),
        "credential_generations": (
            settings.CredentialGeneration(
                principal_id="producer-a",
                generation_id="gen-1",
                secret=settings.Secret("token-old"),
            ),
        ),
        "payload_retention_days": 90,
    }
    base.update(overrides)
    return settings.DeploymentSettings(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Redaction / sanitizer (SEC-03 + SEC-05 logging slice)
# ---------------------------------------------------------------------------


SECRET_MARKERS = (
    "claim-tok-secret-VALUE",
    "Bearer real-credential-xyz",
    "session-cookie=abc",
    '{"file_id":"app-secret-field"}',
    "password-cleartext",
)


def test_sanitize_redacts_nested_secret_and_payload_keys() -> None:
    sentry_sentinel = "SENTRY_DSN_SENTINEL_9z8y"
    raw = {
        "request_id": "req-1",
        "code": "lease_lost",
        "nested": {
            "Authorization": "Bearer real-credential-xyz",
            "X-Queue-Claim-Token": "claim-tok-secret-VALUE",
            "Cookie": "session-cookie=abc",
            "payload": {"file_id": "app-secret-field"},
            "credential": "password-cleartext",
            "secret": "password-cleartext",
            "claim_token": "claim-tok-secret-VALUE",
            "sentry_dsn": sentry_sentinel,
            "SENTRY_DSN": sentry_sentinel,
            "safe_status": "leased",
        },
        "items": [
            {"authorization": "Bearer real-credential-xyz"},
            {"PAYLOAD": {"file_id": "app-secret-field"}},
        ],
    }
    cleaned = redaction.sanitize_for_diagnostics(raw)
    rendered = repr(cleaned) + str(cleaned)

    for marker in SECRET_MARKERS:
        assert marker not in rendered
        assert marker not in str(cleaned)
    assert sentry_sentinel not in rendered
    assert sentry_sentinel not in str(cleaned)

    assert cleaned["request_id"] == "req-1"
    assert cleaned["code"] == "lease_lost"
    nested = cleaned["nested"]
    assert nested["Authorization"] == redaction.REDACTED
    assert nested["X-Queue-Claim-Token"] == redaction.REDACTED
    assert nested["Cookie"] == redaction.REDACTED
    assert nested["payload"] == redaction.REDACTED
    assert nested["credential"] == redaction.REDACTED
    assert nested["secret"] == redaction.REDACTED
    assert nested["claim_token"] == redaction.REDACTED
    assert nested["sentry_dsn"] == redaction.REDACTED
    assert nested["SENTRY_DSN"] == redaction.REDACTED
    assert nested["safe_status"] == "leased"
    assert cleaned["items"][0]["authorization"] == redaction.REDACTED
    assert cleaned["items"][1]["PAYLOAD"] == redaction.REDACTED


def test_sanitize_is_allowlist_based_and_drops_novel_keys() -> None:
    raw = {
        "request_id": "r1",
        "novel_attacker_key": "claim-tok-secret-VALUE",
        "details": {
            "retryable": True,
            "sneaky_payload": {"file_id": "app-secret-field"},
            "unknown_nested": "Bearer real-credential-xyz",
        },
    }
    cleaned = redaction.sanitize_for_diagnostics(raw)
    assert "novel_attacker_key" not in cleaned
    assert cleaned["request_id"] == "r1"
    assert cleaned["details"]["retryable"] is True
    # Secret-shaped novel keys are retained only as fixed markers (never values).
    assert cleaned["details"]["sneaky_payload"] == redaction.REDACTED
    assert "unknown_nested" not in cleaned["details"]
    rendered = repr(cleaned) + str(cleaned)
    for marker in ("claim-tok-secret-VALUE", "Bearer real-credential-xyz", "app-secret-field"):
        assert marker not in rendered


def test_sanitize_bounds_depth_items_and_bytes() -> None:
    deep: object = {"leaf": "x"}
    for _ in range(40):
        deep = {"nested": deep, "request_id": "ok"}
    bounded = redaction.sanitize_for_diagnostics(deep)
    assert redaction.TRUNCATED in repr(bounded) or redaction.REDACTED in repr(bounded)

    huge_list = [{"request_id": f"id-{i}", "code": "ok"} for i in range(10_000)]
    listed = redaction.sanitize_for_diagnostics({"request_id": "root", "items": huge_list})
    assert len(listed["items"]) < 10_000
    assert redaction.TRUNCATED in listed["items"] or len(listed["items"]) <= redaction.MAX_ITEMS

    fat = {"request_id": "r", "code": "x" * 100_000}
    fat_out = redaction.sanitize_for_diagnostics(fat)
    assert fat_out["request_id"] == "r"
    assert fat_out["code"] == redaction.TRUNCATED or len(str(fat_out["code"])) < 100_000


def test_sanitize_and_errors_never_echo_secrets_in_repr() -> None:
    err = redaction.DiagnosticSanitizerError("claim-tok-secret-VALUE leaked?")
    assert "claim-tok-secret-VALUE" not in repr(err)
    assert "claim-tok-secret-VALUE" not in str(err)


# ---------------------------------------------------------------------------
# Payload handling policy (SEC-05 logging/indexing slice)
# ---------------------------------------------------------------------------


def test_payload_metadata_excludes_application_fields() -> None:
    policy = payload_policy.PayloadHandlingPolicy()
    body = {"file_id": "abc", "minio_path": "/x", "nested": {"k": 1}}
    view = policy.inspect(body, payload_bytes=len(str(body).encode()), include_payload=False)
    assert isinstance(view, PayloadView)
    assert view.payload is None
    assert view.metadata == {
        "payload_bytes": len(str(body).encode()),
        "content_type": "application/json",
        "opaque": True,
    }
    assert "file_id" not in view.metadata
    assert "minio_path" not in view.metadata


def test_payload_policy_rejects_index_and_search_derivation() -> None:
    policy = payload_policy.PayloadHandlingPolicy()
    body = {"file_id": "abc", "status": "ready"}
    with pytest.raises(PayloadIndexingRejected) as exc_info:
        policy.derive_index_fields(body)
    assert "file_id" not in str(exc_info.value)
    assert "abc" not in str(exc_info.value)

    with pytest.raises(PayloadIndexingRejected):
        policy.derive_search_fields(body)


def test_authorized_payload_read_returns_opaque_body_only_when_requested() -> None:
    policy = payload_policy.PayloadHandlingPolicy()
    body = {"file_id": "abc"}
    denied = policy.inspect(body, payload_bytes=12, include_payload=False)
    assert denied.payload is None
    allowed = policy.inspect(body, payload_bytes=12, include_payload=True)
    assert allowed.payload == body


def test_oversized_payload_rejected_without_indexing() -> None:
    policy = payload_policy.PayloadHandlingPolicy(
        max_payload_bytes=payload_policy.HARD_PAYLOAD_CEILING_BYTES
    )
    too_big = b"x" * (payload_policy.HARD_PAYLOAD_CEILING_BYTES + 1)
    with pytest.raises(payload_policy.PayloadTooLarge):
        policy.validate_opaque_json_bytes(too_big)


# ---------------------------------------------------------------------------
# Retention configuration + Phase 3.8 handoff contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("days", [30, 90])
def test_settings_accept_retention_boundary_days(days: int) -> None:
    cfg = _settings(payload_retention_days=days)
    assert cfg.payload_retention_days == days
    policy = cfg.payload_retention_policy()
    assert isinstance(policy, PayloadRetentionPolicy)
    assert policy.retention_days == days


@pytest.mark.parametrize("days", [29, 91, 0, -1, 365])
def test_settings_reject_retention_outside_inclusive_range(days: int) -> None:
    with pytest.raises(settings.SettingsValidationError) as exc_info:
        _settings(payload_retention_days=days)
    assert "s3cret" not in str(exc_info.value)
    assert "token-old" not in str(exc_info.value)


def test_payload_retention_policy_expires_at_uses_queue_store_time_only() -> None:
    policy = PayloadRetentionPolicy(retention_days=30)
    created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    expires = policy.expires_at(created)
    assert expires == created + timedelta(days=30)
    assert expires.tzinfo is not None

    naive = datetime(2026, 1, 1, 12, 0, 0)
    with pytest.raises(ValueError):
        policy.expires_at(naive)


def test_payload_retention_policy_is_expired_uses_queue_store_now() -> None:
    policy = PayloadRetentionPolicy(retention_days=90)
    created = datetime(2026, 1, 1, tzinfo=UTC)
    expires = policy.expires_at(created)
    assert policy.is_expired(expires - timedelta(seconds=1), expires) is False
    assert policy.is_expired(expires, expires) is True
    assert policy.is_expired(expires + timedelta(days=1), expires) is True

    with pytest.raises(ValueError):
        policy.is_expired(datetime(2026, 4, 1), expires)


def test_phase_32_does_not_delete_or_detach_payload_rows() -> None:
    """Evidence: this slice ships no maintenance purge / detach APIs."""
    module_path = Path(payload_policy.__file__).resolve()
    source = module_path.read_text(encoding="utf-8")
    forbidden = (
        "DELETE FROM",
        "detach_partition",
        "DROP TABLE",
        "purge_registry",
        "delete_payload",
        "detach_payload",
    )
    lowered = source.lower()
    for needle in forbidden:
        assert needle.lower() not in lowered

    policy = PayloadRetentionPolicy(retention_days=90)
    assert not hasattr(policy, "delete_expired")
    assert not hasattr(policy, "detach_partition")
    assert not hasattr(policy, "purge")
    # Explicit handoff name for Phase 3.8 consumers.
    assert (
        f"{PayloadRetentionPolicy.__module__}.{PayloadRetentionPolicy.__qualname__}"
        == "workhold.security.payload_policy.PayloadRetentionPolicy"
    )
