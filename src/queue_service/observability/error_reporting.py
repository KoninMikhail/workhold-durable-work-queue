"""Optional GlitchTip error reporting via official sentry_sdk (OPS-10).

DSN presence is the only enablement switch: unset, empty, or whitespace DSN
never calls ``sentry_sdk.init``. Init is once-per-process, leak-safe, and
fail-open on SDK errors.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.stdlib import StdlibIntegration
from sentry_sdk.scrubber import DEFAULT_DENYLIST, EventScrubber

from queue_service.settings import Secret

_SCRUBBER_DENYLIST: list[str] = list(DEFAULT_DENYLIST) + [
    "claim_token",
    "claim-token",
    "x-queue-claim-token",
    "payload",
    "payload_body",
    "task_payload",
    "event_payload",
    "sentry_dsn",
    "database_url",
    "authorization",
    "bearer",
]

_EXTRA_DROP_KEYS: frozenset[str] = frozenset({"payload", "claim_token", "sentry_dsn"})

_initialized = False


def _before_send(event: dict[str, Any], hint: object) -> dict[str, Any] | None:
    event.pop("request", None)
    extras = event.get("extra")
    if isinstance(extras, dict):
        for key in _EXTRA_DROP_KEYS:
            extras.pop(key, None)
    return event


def _before_breadcrumb(
    crumb: dict[str, Any],
    hint: object,
) -> dict[str, Any] | None:
    if crumb.get("type") == "http":
        return None
    return crumb


def reset_error_reporting_for_tests() -> None:
    """Clear init-once state and deactivate any test client without env auto-read."""
    global _initialized
    _initialized = False
    client = sentry_sdk.get_client()
    if sentry_sdk.is_initialized():
        close = getattr(client, "close", None)
        if callable(close):
            close(timeout=2)
    sentry_sdk.get_global_scope().set_client(None)


def maybe_init_error_reporting(
    *,
    dsn: Secret | None,
    environment: str,
    release: str,
    process_role: str,
    transport=None,
) -> bool:
    """Initialize sentry_sdk once when ``dsn`` is a non-empty secret; else no-op."""
    global _initialized

    if _initialized:
        return True

    if dsn is None:
        return False

    value = dsn.get_secret_value().strip()
    if not value:
        return False

    try:
        sentry_sdk.init(
            dsn=value,
            environment=environment,
            release=release,
            send_default_pii=False,
            debug=False,
            include_local_variables=False,
            max_request_body_size="never",
            traces_sample_rate=None,
            auto_session_tracking=False,
            auto_enabling_integrations=False,
            disabled_integrations=[StdlibIntegration],
            integrations=[
                LoggingIntegration(level=None, event_level=logging.ERROR),
            ],
            event_scrubber=EventScrubber(
                denylist=_SCRUBBER_DENYLIST,
                recursive=True,
            ),
            before_send=_before_send,
            before_breadcrumb=_before_breadcrumb,
            transport=transport,
        )
    except Exception:
        print("sentry: initialization failed", file=sys.stderr)
        return False

    _initialized = True
    sentry_sdk.set_tag("process_role", process_role)
    return True
