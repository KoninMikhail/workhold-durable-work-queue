"""ClientConfig normalization, timeout/TLS forwarding and safe repr."""

from __future__ import annotations

import pytest

from _queue_service_client_core.config import ClientConfig
from _queue_service_client_core.transport import HttpJsonTransport


def test_public_base_url_is_normalized() -> None:
    config = ClientConfig.for_public("https://queue.example.com")
    assert config.public_base_url == "https://queue.example.com/"
    assert config.admin_base_url is None


def test_admin_base_url_is_normalized() -> None:
    config = ClientConfig(
        public_base_url="https://queue.example.com",
        admin_base_url="https://admin.queue.example.com/",
    )
    assert config.admin_base_url == "https://admin.queue.example.com/"


def test_sync_timeout_prefers_total() -> None:
    config = ClientConfig.for_public(
        "https://queue.example.com",
        read_timeout_s=30.0,
        total_timeout_s=12.5,
    )
    assert config.sync_timeout_s() == 12.5
    transport = HttpJsonTransport.from_config(config)
    assert transport.timeout_s == 12.5


def test_legacy_transport_constructor_forwards_timeout() -> None:
    transport = HttpJsonTransport("https://queue.example.com", timeout_s=7.5)
    assert transport.timeout_s == 7.5
    assert transport.config.public_base_url == "https://queue.example.com/"


def test_config_repr_redacts_certificate_paths_and_urls_without_secrets() -> None:
    config = ClientConfig(
        public_base_url="https://user:pass@queue.example.com",
        admin_base_url="https://admin.example.com",
        ca_cert_path="/etc/ssl/ca.pem",
        client_cert_path="/etc/ssl/client.pem",
        client_key_path="/etc/ssl/client.key",
    )
    text = repr(config)
    assert "ca.pem" not in text
    assert "client.pem" not in text
    assert "client.key" not in text
    assert "<set>" in text
    assert "pass" not in text
    assert "user:" not in text


def test_invalid_timeouts_are_rejected() -> None:
    with pytest.raises(ValueError, match="connect_timeout_s"):
        ClientConfig.for_public("https://queue.example.com", connect_timeout_s=0)
    with pytest.raises(ValueError, match="read_timeout_s"):
        ClientConfig.for_public("https://queue.example.com", read_timeout_s=-1)
    with pytest.raises(ValueError, match="total_timeout_s"):
        ClientConfig.for_public("https://queue.example.com", total_timeout_s=0)


def test_missing_public_base_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="base_url is required"):
        ClientConfig.for_public("")

def test_long_poll_budgets_are_wait_plus_slack() -> None:
    from _queue_service_client_core.config import (
        long_poll_read_timeout_s,
        long_poll_total_timeout_s,
    )

    assert long_poll_read_timeout_s(15) == 20.0
    assert long_poll_total_timeout_s(15) == 25.0
    assert long_poll_read_timeout_s(20) == 25.0
    assert long_poll_total_timeout_s(20) == 30.0


def test_ensure_long_poll_budgets_rejects_undersized_config() -> None:
    config = ClientConfig.for_public(
        'https://queue.example.com',
        read_timeout_s=10.0,
        total_timeout_s=10.0,
    )
    with pytest.raises(ValueError, match='read_timeout_s'):
        config.ensure_long_poll_budgets(15)


def test_ensure_long_poll_budgets_accepts_sufficient_config() -> None:
    config = ClientConfig.for_public(
        'https://queue.example.com',
        read_timeout_s=30.0,
        total_timeout_s=30.0,
    )
    read_s, total_s = config.ensure_long_poll_budgets(15)
    assert read_s == 20.0
    assert total_s == 25.0
