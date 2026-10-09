"""Secret/payload/DSN redaction for client-core diagnostics."""

from __future__ import annotations

from _workhold_client_core.redaction import (
    REDACTED,
    redact_headers,
    redact_text,
    sanitize_for_diagnostics,
)


def test_redact_headers_authorization_and_claim_token() -> None:
    out = redact_headers(
        {
            "Authorization": "Bearer super-secret",
            "X-Queue-Claim-Token": "claim-token-secret",
            "Idempotency-Key": "idem-secret-value",
            "idempotency-key": "idem-lower-secret",
            "Accept": "application/json",
            "X-Request-ID": "rid-1",
        }
    )
    assert out["Authorization"] == "<redacted>"
    assert out["X-Queue-Claim-Token"] == "<redacted>"
    assert out["Idempotency-Key"] == "<redacted>"
    assert out["idempotency-key"] == "<redacted>"
    assert out["Accept"] == "application/json"
    assert out["X-Request-ID"] == "rid-1"
    assert "super-secret" not in str(out)
    assert "claim-token-secret" not in str(out)
    assert "idem-secret-value" not in str(out)
    assert "idem-lower-secret" not in str(out)


def test_sanitize_for_diagnostics_redacts_payload_and_secret_keys() -> None:
    cleaned = sanitize_for_diagnostics(
        {
            "request_id": "rid-ok",
            "code": "internal_error",
            "payload": {"credit_card": "4111111111111111"},
            "Authorization": "Bearer leak",
            "claim_token": "tok",
            "nested": {"secret": "nope", "queue_name": "orders"},
            "details": {"reason": "safe"},
        }
    )
    assert cleaned["request_id"] == "rid-ok"
    assert cleaned["code"] == "internal_error"
    assert cleaned["payload"] == REDACTED
    assert cleaned["Authorization"] == REDACTED
    assert cleaned["claim_token"] == REDACTED
    assert cleaned["nested"]["secret"] == REDACTED
    assert cleaned["nested"]["queue_name"] == "orders"
    assert cleaned["details"]["reason"] == "safe"
    blob = repr(cleaned)
    assert "4111111111111111" not in blob
    assert "Bearer leak" not in blob
    assert "tok" not in blob or REDACTED in blob


def test_redact_text_masks_dsn_like_values() -> None:
    dsn = "postgresql://user:p4ssw0rd@db.example:5432/queue"
    sentry = "https://abc123@o0.ingest.sentry.io/1"
    text = f"failed connect {dsn} also {sentry} bearer TokenValue"
    cleaned = redact_text(text)
    assert "p4ssw0rd" not in cleaned
    assert "user:p4ssw0rd@" not in cleaned
    assert "abc123@" not in cleaned
    assert "postgresql://" not in cleaned or REDACTED in cleaned or "<redacted>" in cleaned
    assert "TokenValue" not in cleaned or "bearer" not in cleaned.lower()
