from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping, Sequence

import pytest


RESOURCE_URL = "https://mcp.example.invalid/mcp"
ISSUER_URL = "https://idp.example.invalid"
JWKS_URL = "https://idp.example.invalid/jwks"


def _complete_http_env() -> dict[str, str]:
    return {
        "WORKHOLD_MCP_TRANSPORT": "streamable-http",
        "WORKHOLD_MCP_RESOURCE_URL": RESOURCE_URL,
        "WORKHOLD_MCP_ISSUER_URL": ISSUER_URL,
        "WORKHOLD_MCP_JWKS_URL": JWKS_URL,
        "WORKHOLD_MCP_ALLOWED_HOSTS": "mcp.example.invalid,127.0.0.1:*",
        "WORKHOLD_MCP_NO_BROWSER_ORIGINS": "true",
    }


def _clean_process_env(overrides: Mapping[str, str]) -> dict[str, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("WORKHOLD_MCP_")
    }
    env.update(overrides)
    return env


def _run_invalid(
    *,
    argv: Sequence[str] = (),
    environ: Mapping[str, str],
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        [sys.executable, "-m", "workhold_mcp", *argv],
        env=_clean_process_env(environ),
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert result.stdout == b""
    lines = result.stderr.splitlines()
    assert len(lines) == 1
    assert lines[0].startswith(b"workhold-mcp: invalid configuration")
    assert RESOURCE_URL.encode() not in result.stdout + result.stderr
    assert JWKS_URL.encode() not in result.stdout + result.stderr
    return result


@pytest.mark.parametrize(
    ("env_name", "flag", "value"),
    [
        ("WORKHOLD_MCP_RESOURCE_URL", "--resource-url", RESOURCE_URL),
        ("WORKHOLD_MCP_ISSUER_URL", "--issuer-url", ISSUER_URL),
        ("WORKHOLD_MCP_JWKS_URL", "--jwks-url", JWKS_URL),
        ("WORKHOLD_MCP_ALLOWED_HOSTS", "--allowed-host", "mcp.example.invalid"),
        (
            "WORKHOLD_MCP_ALLOWED_ORIGINS",
            "--allowed-origin",
            "https://app.example.invalid",
        ),
        ("WORKHOLD_MCP_NO_BROWSER_ORIGINS", "--no-browser-origins", ""),
        ("WORKHOLD_MCP_BIND_HOST", "--bind-host", "127.0.0.1"),
        ("WORKHOLD_MCP_BIND_PORT", "--bind-port", "8000"),
        (
            "WORKHOLD_MCP_FORWARDED_ALLOW_IPS",
            "--forwarded-allow-ip",
            "127.0.0.1",
        ),
        ("WORKHOLD_MCP_CLOCK_SKEW_SECONDS", "--clock-skew-seconds", "60"),
        (
            "WORKHOLD_MCP_DNS_REBINDING_PROTECTION",
            "--disable-dns-rebinding-protection",
            "",
        ),
    ],
)
@pytest.mark.parametrize("explicit_transport", [False, True])
def test_stdio_rejects_http_only_settings(
    env_name: str,
    flag: str,
    value: str,
    explicit_transport: bool,
) -> None:
    env = {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": "https://api.example.invalid",
        "WORKHOLD_MCP_ADMIN_BASE_URL": "https://admin.example.invalid",
        "WORKHOLD_MCP_OBSERVER_TOKEN": "observer-token",
    }
    argv: list[str] = []
    if explicit_transport:
        argv.extend(["--transport", "stdio"])
    if value:
        argv.extend([flag, value])
    else:
        argv.append(flag)
    _run_invalid(argv=argv, environ=env)

    env[env_name] = value or "true"
    _run_invalid(
        argv=["--transport", "stdio"] if explicit_transport else [],
        environ=env,
    )

    from workhold_mcp.config import resolve_http_config

    resolved = resolve_http_config([], {"WORKHOLD_MCP_TRANSPORT": "stdio"})
    assert resolved.transport == "stdio"


@pytest.mark.parametrize(
    "missing_name",
    [
        "WORKHOLD_MCP_RESOURCE_URL",
        "WORKHOLD_MCP_ISSUER_URL",
        "WORKHOLD_MCP_JWKS_URL",
        "WORKHOLD_MCP_ALLOWED_HOSTS",
        "WORKHOLD_MCP_NO_BROWSER_ORIGINS",
    ],
)
def test_http_missing_required_settings(missing_name: str) -> None:
    env = _complete_http_env()
    env.pop(missing_name)
    result = _run_invalid(environ=env)
    assert missing_name.encode() in result.stderr


@pytest.mark.parametrize(
    ("env_name", "flag", "value"),
    [
        ("WORKHOLD_MCP_RESOURCE_URL", "--resource-url", RESOURCE_URL),
        ("WORKHOLD_MCP_ALLOWED_HOSTS", "--allowed-host", "mcp.example.invalid"),
    ],
)
def test_channel_conflict_exits_even_when_values_match(
    env_name: str,
    flag: str,
    value: str,
) -> None:
    env = _complete_http_env()
    env[env_name] = value
    result = _run_invalid(argv=[flag, value], environ=env)
    assert env_name.encode() in result.stderr


@pytest.mark.parametrize(
    "resource_url",
    [
        "https://mcp.example.invalid",
        "https://mcp.example.invalid/mcp/",
        "https://mcp.example.invalid/mcp?query=1",
        "https://mcp.example.invalid/mcp#fragment",
    ],
)
def test_resource_url_shape(resource_url: str) -> None:
    from workhold_mcp.config import ConfigError, resolve_http_config

    env = _complete_http_env()
    env["WORKHOLD_MCP_RESOURCE_URL"] = resource_url
    with pytest.raises(ConfigError, match="^WORKHOLD_MCP_RESOURCE_URL$"):
        resolve_http_config([], env)

    valid = resolve_http_config([], _complete_http_env())
    assert valid.resource_url == RESOURCE_URL
    assert valid.allowed_hosts == ("mcp.example.invalid", "127.0.0.1:*")
    assert RESOURCE_URL not in valid.allowed_hosts

    for issuer_url in (
        "https://idp.example.invalid?query=1",
        "https://idp.example.invalid#fragment",
    ):
        env = _complete_http_env()
        env["WORKHOLD_MCP_ISSUER_URL"] = issuer_url
        with pytest.raises(ConfigError, match="^WORKHOLD_MCP_ISSUER_URL$"):
            resolve_http_config([], env)


@pytest.mark.parametrize(
    "host",
    ["*", ":*", "mcp.*", "https://mcp.example.invalid"],
)
def test_host_and_proxy_bounds(host: str) -> None:
    from workhold_mcp.config import ConfigError, resolve_http_config

    env = _complete_http_env()
    env["WORKHOLD_MCP_ALLOWED_HOSTS"] = host
    with pytest.raises(ConfigError, match="^WORKHOLD_MCP_ALLOWED_HOSTS$"):
        resolve_http_config([], env)

    valid = resolve_http_config([], _complete_http_env())
    assert valid.forwarded_allow_ips == ()
    assert "127.0.0.1:*" in valid.allowed_hosts

    for proxy in ("*", "", "proxy.example.invalid"):
        env = _complete_http_env()
        env["WORKHOLD_MCP_FORWARDED_ALLOW_IPS"] = proxy
        with pytest.raises(
            ConfigError,
            match="^WORKHOLD_MCP_FORWARDED_ALLOW_IPS$",
        ):
            resolve_http_config([], env)


def test_origin_choice() -> None:
    from workhold_mcp.config import ConfigError, resolve_http_config

    env = _complete_http_env()
    env["WORKHOLD_MCP_ALLOWED_ORIGINS"] = "https://app.example.invalid"
    with pytest.raises(ConfigError, match="^WORKHOLD_MCP_ALLOWED_ORIGINS$"):
        resolve_http_config([], env)

    allowlist_env = _complete_http_env()
    allowlist_env.pop("WORKHOLD_MCP_NO_BROWSER_ORIGINS")
    allowlist_env["WORKHOLD_MCP_ALLOWED_ORIGINS"] = "https://app.example.invalid"
    allowlist = resolve_http_config([], allowlist_env)
    assert allowlist.allowed_origins == ("https://app.example.invalid",)
    assert allowlist.no_browser_origins is False

    no_browser = resolve_http_config([], _complete_http_env())
    assert no_browser.allowed_origins == ()
    assert no_browser.no_browser_origins is True

    for value in ("false", "TRUE", "1", ""):
        env = _complete_http_env()
        env["WORKHOLD_MCP_NO_BROWSER_ORIGINS"] = value
        with pytest.raises(ConfigError, match="^WORKHOLD_MCP_NO_BROWSER_ORIGINS$"):
            resolve_http_config([], env)


def test_rebinding_switch_and_skew() -> None:
    from workhold_mcp.config import ConfigError, resolve_http_config

    for argv, env in (
        (["--disable-dns-rebinding-protection"], _complete_http_env()),
        (
            [],
            {
                **_complete_http_env(),
                "WORKHOLD_MCP_DNS_REBINDING_PROTECTION": "true",
            },
        ),
    ):
        with pytest.raises(
            ConfigError,
            match=(
                "^--disable-dns-rebinding-protection$"
                "|^WORKHOLD_MCP_DNS_REBINDING_PROTECTION$"
            ),
        ):
            resolve_http_config(argv, env)

    for value in ("-1", "301"):
        env = _complete_http_env()
        env["WORKHOLD_MCP_CLOCK_SKEW_SECONDS"] = value
        with pytest.raises(ConfigError, match="^WORKHOLD_MCP_CLOCK_SKEW_SECONDS$"):
            resolve_http_config([], env)

    for value in ("0", "300"):
        env = _complete_http_env()
        env["WORKHOLD_MCP_CLOCK_SKEW_SECONDS"] = value
        assert resolve_http_config([], env).clock_skew_seconds == int(value)


def test_bind_defaults() -> None:
    from workhold_mcp.config import resolve_http_config

    resolved = resolve_http_config([], _complete_http_env())
    assert resolved.transport == "streamable-http"
    assert resolved.bind_host == "127.0.0.1"
    assert resolved.bind_port == 8000
    assert resolved.forwarded_allow_ips == ()
    assert resolved.clock_skew_seconds == 60
    assert resolved.dns_rebinding_protection is True
