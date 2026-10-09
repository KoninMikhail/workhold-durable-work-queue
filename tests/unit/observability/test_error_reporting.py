"""Off/on-path and leak-safe coverage for optional GlitchTip reporting (OPS-10)."""

from __future__ import annotations

from typing import Any

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport

from queue_service import __version__
from queue_service.observability import error_reporting
from queue_service.observability.error_reporting import (
    _before_send,
    maybe_init_error_reporting,
    reset_error_reporting_for_tests,
)
from queue_service.settings import Secret

SENTRY_DSN_SENTINEL = "https://SENTRY_DSN_SENTINEL_9z8y@127.0.0.1/0"


class RecordingTransport(Transport):
    def __init__(self) -> None:
        super().__init__()
        self.envelopes: list[object] = []

    def capture_envelope(self, envelope: object) -> None:
        self.envelopes.append(envelope)


@pytest.fixture(autouse=True)
def _reset_error_reporting() -> None:
    reset_error_reporting_for_tests()
    yield
    reset_error_reporting_for_tests()


def _client_inactive() -> bool:
    return not sentry_sdk.is_initialized()


def test_unset_dsn_does_not_init(monkeypatch: pytest.MonkeyPatch) -> None:
    init_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def _spy_init(*args: Any, **kwargs: Any) -> None:
        init_calls.append((args, kwargs))

    monkeypatch.setattr(sentry_sdk, "init", _spy_init)

    assert maybe_init_error_reporting(
        dsn=None,
        environment="development",
        release=f"queue@{__version__}",
        process_role="api",
    ) is False
    assert init_calls == []
    assert _client_inactive()


def test_empty_dsn_does_not_init(monkeypatch: pytest.MonkeyPatch) -> None:
    init_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def _spy_init(*args: Any, **kwargs: Any) -> None:
        init_calls.append((args, kwargs))

    monkeypatch.setattr(sentry_sdk, "init", _spy_init)

    assert maybe_init_error_reporting(
        dsn=Secret("   "),
        environment="development",
        release=f"queue@{__version__}",
        process_role="api",
    ) is False
    assert init_calls == []
    assert _client_inactive()


def test_init_once_with_recording_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    init_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    real_init = sentry_sdk.init

    def _counting_init(*args: Any, **kwargs: Any) -> None:
        init_calls.append((args, kwargs))
        real_init(*args, **kwargs)

    monkeypatch.setattr(sentry_sdk, "init", _counting_init)
    transport = RecordingTransport()
    kwargs = {
        "dsn": Secret(SENTRY_DSN_SENTINEL),
        "environment": "development",
        "release": f"queue@{__version__}",
        "process_role": "api",
        "transport": transport,
    }

    assert maybe_init_error_reporting(**kwargs) is True
    assert len(init_calls) == 1
    assert sentry_sdk.is_initialized()

    assert maybe_init_error_reporting(**kwargs) is True
    assert len(init_calls) == 1


def test_capture_exception_uses_injected_transport() -> None:
    transport = RecordingTransport()
    assert maybe_init_error_reporting(
        dsn=Secret(SENTRY_DSN_SENTINEL),
        environment="development",
        release=f"queue@{__version__}",
        process_role="api",
        transport=transport,
    ) is True

    try:
        raise RuntimeError("queue-test-error")
    except RuntimeError as exc:
        sentry_sdk.capture_exception(exc)

    sentry_sdk.flush(timeout=2)
    assert len(transport.envelopes) >= 1


def test_dsn_not_leaked(capsys: pytest.CaptureFixture[str]) -> None:
    transport = RecordingTransport()
    secret = Secret(SENTRY_DSN_SENTINEL)

    assert maybe_init_error_reporting(
        dsn=secret,
        environment="development",
        release=f"queue@{__version__}",
        process_role="api",
        transport=transport,
    ) is True

    try:
        raise RuntimeError("queue-test-error")
    except RuntimeError as exc:
        sentry_sdk.capture_exception(exc)

    sentry_sdk.flush(timeout=2)

    rendered_secret = repr(secret) + str(secret)
    assert SENTRY_DSN_SENTINEL not in rendered_secret

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert SENTRY_DSN_SENTINEL not in combined

    for envelope in transport.envelopes:
        serialized = envelope.serialize()
        assert SENTRY_DSN_SENTINEL not in serialized.decode("utf-8", errors="replace")


def test_before_send_strips_secrets() -> None:
    event = {
        "request": {"url": "http://example.test", "headers": {"Authorization": "Bearer x"}},
        "extra": {
            "payload": {"task": "secret-body"},
            "claim_token": "claim-secret",
            "sentry_dsn": SENTRY_DSN_SENTINEL,
            "queue": "orders",
        },
    }

    cleaned = _before_send(event, {})

    assert "request" not in cleaned
    assert "payload" not in cleaned.get("extra", {})
    assert "claim_token" not in cleaned.get("extra", {})
    assert "sentry_dsn" not in cleaned.get("extra", {})
    assert cleaned.get("extra", {}).get("queue") == "orders"


def test_init_failure_is_fail_open_name_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise_init(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError(SENTRY_DSN_SENTINEL)

    monkeypatch.setattr(sentry_sdk, "init", _raise_init)

    assert maybe_init_error_reporting(
        dsn=Secret(SENTRY_DSN_SENTINEL),
        environment="development",
        release=f"queue@{__version__}",
        process_role="api",
        transport=RecordingTransport(),
    ) is False

    captured = capsys.readouterr()
    assert "sentry: initialization failed" in captured.err
    assert SENTRY_DSN_SENTINEL not in captured.err
    assert "RuntimeError" not in captured.err
    assert _client_inactive()

    assert maybe_init_error_reporting(
        dsn=Secret(SENTRY_DSN_SENTINEL),
        environment="development",
        release=f"queue@{__version__}",
        process_role="api",
        transport=RecordingTransport(),
    ) is False
