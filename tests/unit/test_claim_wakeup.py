"""Unit tests for ClaimWakeListener connect timeout and stop semantics."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from workhold.db import psycopg_connect_timeout_seconds
from workhold.infrastructure.postgres.claim_wakeup import (
    ClaimWakeListener,
    ListenerHealth,
    QueueGenerationCoordinator,
)


def test_psycopg_connect_timeout_seconds_matches_pool_budget() -> None:
    assert psycopg_connect_timeout_seconds(5.0) == 5
    assert psycopg_connect_timeout_seconds(0.9) == 1
    assert psycopg_connect_timeout_seconds(12.7) == 12


def test_default_connect_passes_bounded_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_conn = MagicMock()
    fake_conn.autocommit = True

    def fake_connect(dsn: str, **kwargs: Any) -> MagicMock:
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return fake_conn

    monkeypatch.setattr(
        "workhold.infrastructure.postgres.claim_wakeup.psycopg.connect",
        fake_connect,
    )
    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(
        "postgresql://unused",
        coordinator,
        connect_timeout_seconds=3,
    )

    conn = listener._default_connect()

    assert conn is fake_conn
    assert captured["dsn"] == "postgresql://unused"
    assert captured["kwargs"]["autocommit"] is True
    assert captured["kwargs"]["connect_timeout"] == 3


def test_reconnect_default_connect_keeps_timeout_and_stop_is_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every default connect/reconnect is bounded; stop still exits promptly."""
    connect_calls: list[int] = []
    fake_conn = MagicMock()
    fake_conn.autocommit = True
    fake_conn.notifies = MagicMock(side_effect=lambda **_kwargs: iter(()))
    fake_conn.execute = MagicMock()
    fake_conn.close = MagicMock()

    def fake_connect(dsn: str, **kwargs: Any) -> MagicMock:
        assert kwargs.get("connect_timeout") == 2
        connect_calls.append(kwargs["connect_timeout"])
        if len(connect_calls) == 1:
            raise OSError("simulated connect failure")
        return fake_conn

    monkeypatch.setattr(
        "workhold.infrastructure.postgres.claim_wakeup.psycopg.connect",
        fake_connect,
    )
    coordinator = QueueGenerationCoordinator()
    listener = ClaimWakeListener(
        "postgresql://unused",
        coordinator,
        connect_timeout_seconds=2,
        initial_backoff_seconds=0.05,
        max_backoff_seconds=0.05,
        notify_poll_seconds=0.05,
    )
    listener.start()
    try:
        assert _wait_until(
            lambda: listener.health is ListenerHealth.CONNECTED,
            timeout=2.0,
        )
        assert len(connect_calls) >= 2
        assert all(timeout == 2 for timeout in connect_calls)
    finally:
        listener.stop(join_timeout_seconds=1.0)

    assert listener.health is ListenerHealth.DEGRADED
    assert listener.connection is None
    assert listener._thread is not None
    assert not listener._thread.is_alive()


def _wait_until(predicate: Any, *, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False
