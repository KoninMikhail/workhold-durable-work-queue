"""Helpers to drive existing admin ASGI conformance apps through AdminClient."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_admin import AdminClient
from queue_service_admin.models import BackoffStrategy, RetryPolicyDraft
from tests.conformance.conftest import _start_http_server, _stop_http_server


@pytest.fixture
def admin_http_url(admin_app: Any) -> Iterator[str]:
    url, server, thread = _start_http_server(admin_app, thread_name="admin-sdk-asgi")
    try:
        yield url
    finally:
        _stop_http_server(server, thread)


def make_admin_client(base_url: str, *, bearer_token: str) -> AdminClient:
    transport = HttpJsonTransport(base_url, timeout_s=5.0)
    return AdminClient(transport, bearer_token=bearer_token)


def retry_policy_draft() -> RetryPolicyDraft:
    return RetryPolicyDraft(
        enabled=True,
        max_attempts=3,
        backoff_strategy=BackoffStrategy("fixed"),
        retry_delay_seconds=5,
    )
