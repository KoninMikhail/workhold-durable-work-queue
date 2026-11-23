"""MCP startup settings parsed from an explicit env mapping."""

from __future__ import annotations

import pytest

from workhold_mcp.config import (
    ConfigError,
    build_observer,
    configs_for,
    load_settings,
)

_TOKEN = "WHMCP-TOKEN-CANARY"
_PUBLIC = "http://127.0.0.1:9"
_ADMIN = "http://127.0.0.1:10"
_USERINFO_URL = "https://user:secret@host"
_FTP_URL = "ftp://example.com"


def _required_env() -> dict[str, str]:
    return {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
        "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
    }


def test_defaults_and_redaction() -> None:
    settings = load_settings(_required_env())
    public_cfg, admin_cfg = configs_for(settings)

    assert public_cfg.connect_timeout_s == 5.0
    assert public_cfg.read_timeout_s == 30.0
    assert public_cfg.total_timeout_s is None
    assert public_cfg.verify_tls is True
    assert isinstance(public_cfg.connect_timeout_s, float)
    assert isinstance(public_cfg.read_timeout_s, float)
    assert admin_cfg.connect_timeout_s == 5.0
    assert admin_cfg.read_timeout_s == 30.0
    assert admin_cfg.total_timeout_s is None
    assert admin_cfg.verify_tls is True

    rendered = repr(settings)
    assert "<redacted>" in rendered
    assert _TOKEN not in rendered
    assert "<redacted>" in str(settings)
    assert _TOKEN not in str(settings)


def test_admin_origin_is_the_admin_transport_base_url() -> None:
    settings = load_settings(_required_env())
    public_cfg, admin_cfg = configs_for(settings)
    observer = build_observer(settings)

    assert admin_cfg.public_base_url.rstrip("/") == _ADMIN
    assert public_cfg.public_base_url.rstrip("/") == _PUBLIC
    assert admin_cfg.public_base_url != public_cfg.public_base_url

    admin_transport = observer._admin_transport
    public_transport = observer._transport
    assert admin_transport.config.public_base_url.rstrip("/") == _ADMIN
    assert admin_transport._base_url.rstrip("/") == _ADMIN
    assert public_transport.config.public_base_url.rstrip("/") == _PUBLIC
    assert public_transport._base_url.rstrip("/") == _PUBLIC
    assert admin_transport._base_url != public_transport._base_url


def test_verify_tls_false_is_explicit() -> None:
    env = _required_env()
    env["WORKHOLD_MCP_VERIFY_TLS"] = "false"
    _public_cfg, admin_cfg = configs_for(load_settings(env))
    assert admin_cfg.verify_tls is False

    env["WORKHOLD_MCP_VERIFY_TLS"] = "true"
    _public_cfg, admin_cfg = configs_for(load_settings(env))
    assert admin_cfg.verify_tls is True


@pytest.mark.parametrize(
    ("env", "setting_name", "forbidden"),
    [
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
            },
            "WORKHOLD_MCP_OBSERVER_TOKEN",
            (_TOKEN,),
        ),
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": "   ",
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
            },
            "WORKHOLD_MCP_PUBLIC_BASE_URL",
            (_TOKEN, "   "),
        ),
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _USERINFO_URL,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
            },
            "WORKHOLD_MCP_PUBLIC_BASE_URL",
            (_TOKEN, _USERINFO_URL),
        ),
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _FTP_URL,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
            },
            "WORKHOLD_MCP_ADMIN_BASE_URL",
            (_TOKEN, _FTP_URL),
        ),
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
                "WORKHOLD_MCP_CONNECT_TIMEOUT_S": "0",
            },
            "WORKHOLD_MCP_CONNECT_TIMEOUT_S",
            (_TOKEN, "0"),
        ),
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
                "WORKHOLD_MCP_VERIFY_TLS": "yes",
            },
            "WORKHOLD_MCP_VERIFY_TLS",
            (_TOKEN, "yes"),
        ),
        (
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _TOKEN,
                "WORKHOLD_MCP_TOKEN": _TOKEN,
            },
            "WORKHOLD_MCP_TOKEN",
            (_TOKEN,),
        ),
    ],
)
def test_invalid_settings_name_the_key_only(
    env: dict[str, str],
    setting_name: str,
    forbidden: tuple[str, ...],
) -> None:
    with pytest.raises(ConfigError) as caught:
        load_settings(env)
    message = str(caught.value)
    assert setting_name in message
    assert caught.value.setting_name == setting_name
    for value in forbidden:
        assert value not in message


def test_unrelated_env_is_ignored() -> None:
    env = _required_env()
    env["PATH"] = _TOKEN
    settings = load_settings(env)
    assert _TOKEN not in repr(settings)
    assert _TOKEN not in str(settings)
