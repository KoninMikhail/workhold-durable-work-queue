"""Allowlisted correlation fields for structured logs and traces (OPS-08).

Secrets are excluded by construction: only keys in
:data:`CORRELATION_ALLOWLIST` are projected. Payload bodies, claim tokens,
idempotency keys, and free-text failure detail never enter the projection.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Final

CORRELATION_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "trace_id",
        "queue",
        "operation",
        "task_id",
        "claim_id",
        "event_id",
        "generation",
        "worker_id",
        "config_version",
        "policy_version",
        "result",
        "code",
        # Maintenance diagnostics (OPS-05 / OPS-08 Plan 04-04).
        "process_role",
        "store_now",
        # Routine admin drain/maintenance (CTRL-06 / OPS-08 Plan 04-06).
        "actor_id",
        "maintenance_run_id",
        # Dead-letter replay lineage (CTRL-06 / REC-01 / OPS-08 Plan 04-07).
        "source_task_id",
        # Bulk admin aggregates (REC-02 / OPS-08 Plan 04-08).
        "candidate_count",
        "batch_size",
        "succeeded_count",
        "skipped_count",
        "failed_count",
        # Break-glass emergency ops (REC-03 / OPS-08 Plan 04-09).
        "incident_ref_hash",
        "target_id",
    }
)

# Explicit deny set documents the threat mitigations (T-04-01-I); deny wins
# even if a future allowlist expansion accidentally overlaps.
_CORRELATION_DENY: Final[frozenset[str]] = frozenset(
    {
        "payload",
        "payload_body",
        "task_payload",
        "event_payload",
        "claim_token",
        "claim-token",
        "x-queue-claim-token",
        "idempotency_key",
        "idempotency-key",
        "failure_detail",
        "reason",
        "authorization",
        "cookie",
        "set-cookie",
        "token",
        "secret",
        "password",
        "credential",
        "credentials",
        "database_url",
        "dsn",
        "sentry_dsn",
        "sql",
        "partition_name",
        "child_name",
        "error_detail",
        "confirmation_token",
        "filters",
        "filter",
        "sample",
        "sample_task_ids",
        "candidate_ids",
        "task_ids",
        "outcomes",
        "repair_value",
        "registry_value",
        "raw_registry",
        "incident_reference",
        "incident",
        "ticket",
        "acknowledgement",
        "risk_acknowledgement",
    }
)


def _normalize_key(key: object) -> str:
    return str(key).strip().lower().replace(" ", "_")


def project_correlation(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Return a secret-safe correlation dict for logs and trace attributes.

    The same projector must be used for structured logging and tracing so
    redaction cannot drift between surfaces.
    """
    out: dict[str, Any] = {}
    for raw_key, raw_val in fields.items():
        key = _normalize_key(raw_key)
        if key in _CORRELATION_DENY:
            continue
        if key not in CORRELATION_ALLOWLIST:
            continue
        if isinstance(raw_val, (str, int, float, bool)) or raw_val is None:
            out[key] = raw_val
        # Non-scalar correlation fields are never projected (no nested payloads).
    return out


def current_span() -> Any | None:
    """Return the active trace span when an OTEL SDK is installed; else None."""
    try:
        from opentelemetry import trace  # type: ignore[import-not-found]
    except ImportError:
        return None
    span = trace.get_current_span()
    if span is None:
        return None
    is_recording = getattr(span, "is_recording", None)
    if callable(is_recording) and not is_recording():
        return None
    return span


def emit_correlation(
    logger: logging.Logger,
    event: str,
    projected: Mapping[str, Any],
    *,
    span: Any | None = None,
) -> dict[str, Any]:
    """Emit allowlisted correlation to structured logs and optional span attrs.

    Re-projects through :func:`project_correlation` so callers cannot bypass
    the deny list even if they pass a polluted mapping.
    """
    safe = project_correlation(projected)
    logger.info("%s %s", event, safe)
    target = span if span is not None else current_span()
    if target is not None:
        set_attribute = getattr(target, "set_attribute", None)
        if callable(set_attribute):
            for key, value in safe.items():
                if isinstance(value, (str, int, float, bool)):
                    set_attribute(key, value)
    return safe
