"""Parse ``WORKHOLD_MCP_*`` settings for the roles a tool selection needs."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from ipaddress import ip_address
from urllib.parse import urlparse

from _workhold_client_core.config import ClientConfig
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin.observer import ObserverClient

from workhold_mcp.registry import load_manifest, preset_names

__all__ = [
    "ConfigError",
    "HttpSettings",
    "McpSettings",
    "SelectionNeeds",
    "build_observer",
    "configs_for",
    "load_settings",
    "origin_config",
    "resolve_http_config",
    "selection_needs",
]

_PREFIX = "WORKHOLD_MCP_"
_PUBLIC_BASE_URL = "WORKHOLD_MCP_PUBLIC_BASE_URL"
_ADMIN_BASE_URL = "WORKHOLD_MCP_ADMIN_BASE_URL"
_OBSERVER_TOKEN = "WORKHOLD_MCP_OBSERVER_TOKEN"
_CONNECT_TIMEOUT_S = "WORKHOLD_MCP_CONNECT_TIMEOUT_S"
_READ_TIMEOUT_S = "WORKHOLD_MCP_READ_TIMEOUT_S"
_TOTAL_TIMEOUT_S = "WORKHOLD_MCP_TOTAL_TIMEOUT_S"
_VERIFY_TLS = "WORKHOLD_MCP_VERIFY_TLS"
_PRESET = "WORKHOLD_MCP_PRESET"
_ALLOWED_TOOLS = "WORKHOLD_MCP_ALLOWED_TOOLS"
_PRODUCER_TOKEN = "WORKHOLD_MCP_PRODUCER_TOKEN"
_ADMIN_TOKEN = "WORKHOLD_MCP_ADMIN_TOKEN"
_CONTENT_KEY = "WORKHOLD_MCP_CONTENT_KEY"
_TRANSPORT = "WORKHOLD_MCP_TRANSPORT"
_RESOURCE_URL = "WORKHOLD_MCP_RESOURCE_URL"
_ISSUER_URL = "WORKHOLD_MCP_ISSUER_URL"
_JWKS_URL = "WORKHOLD_MCP_JWKS_URL"
_ALLOWED_HOSTS = "WORKHOLD_MCP_ALLOWED_HOSTS"
_ALLOWED_ORIGINS = "WORKHOLD_MCP_ALLOWED_ORIGINS"
_NO_BROWSER_ORIGINS = "WORKHOLD_MCP_NO_BROWSER_ORIGINS"
_BIND_HOST = "WORKHOLD_MCP_BIND_HOST"
_BIND_PORT = "WORKHOLD_MCP_BIND_PORT"
_FORWARDED_ALLOW_IPS = "WORKHOLD_MCP_FORWARDED_ALLOW_IPS"
_CLOCK_SKEW_SECONDS = "WORKHOLD_MCP_CLOCK_SKEW_SECONDS"
_DNS_REBINDING_PROTECTION = "WORKHOLD_MCP_DNS_REBINDING_PROTECTION"
_KNOWN = frozenset(
    {
        _PUBLIC_BASE_URL,
        _ADMIN_BASE_URL,
        _OBSERVER_TOKEN,
        _CONNECT_TIMEOUT_S,
        _READ_TIMEOUT_S,
        _TOTAL_TIMEOUT_S,
        _VERIFY_TLS,
        _PRESET,
        _ALLOWED_TOOLS,
        _PRODUCER_TOKEN,
        _ADMIN_TOKEN,
        _CONTENT_KEY,
        _TRANSPORT,
        _RESOURCE_URL,
        _ISSUER_URL,
        _JWKS_URL,
        _ALLOWED_HOSTS,
        _ALLOWED_ORIGINS,
        _NO_BROWSER_ORIGINS,
        _BIND_HOST,
        _BIND_PORT,
        _FORWARDED_ALLOW_IPS,
        _CLOCK_SKEW_SECONDS,
        _DNS_REBINDING_PROTECTION,
    }
)
_HTTP_SCHEMES = frozenset({"http", "https"})
_HTTP_ONLY_NAMES = frozenset(
    {
        _RESOURCE_URL,
        _ISSUER_URL,
        _JWKS_URL,
        _ALLOWED_HOSTS,
        _ALLOWED_ORIGINS,
        _NO_BROWSER_ORIGINS,
        _BIND_HOST,
        _BIND_PORT,
        _FORWARDED_ALLOW_IPS,
        _CLOCK_SKEW_SECONDS,
        _DNS_REBINDING_PROTECTION,
    }
)
_HTTP_VALUE_FLAGS = {
    "--resource-url": (_RESOURCE_URL, False),
    "--issuer-url": (_ISSUER_URL, False),
    "--jwks-url": (_JWKS_URL, False),
    "--allowed-host": (_ALLOWED_HOSTS, True),
    "--allowed-origin": (_ALLOWED_ORIGINS, True),
    "--bind-host": (_BIND_HOST, False),
    "--bind-port": (_BIND_PORT, False),
    "--forwarded-allow-ip": (_FORWARDED_ALLOW_IPS, True),
    "--clock-skew-seconds": (_CLOCK_SKEW_SECONDS, False),
}
_HTTP_BOOLEAN_FLAGS = {
    "--no-browser-origins": _NO_BROWSER_ORIGINS,
    "--disable-dns-rebinding-protection": _DNS_REBINDING_PROTECTION,
}


class ConfigError(Exception):
    """A startup setting was missing or rejected.

    ``str(error)`` is the setting name only, or empty when no name applies.
    The supplied value is never included.
    """

    def __init__(self, setting_name: str | None) -> None:
        self.setting_name = setting_name
        super().__init__(setting_name or "")


@dataclass(frozen=True, slots=True)
class SelectionNeeds:
    """URLs and role clients required by one selected name set."""

    public: bool
    admin_url: bool
    observer: bool
    producer: bool
    admin: bool


@dataclass(frozen=True, slots=True)
class McpSettings:
    """Validated MCP process settings. Tokens and the content key stay redacted."""

    public_base_url: str | None
    admin_base_url: str | None
    observer_token: str | None
    producer_token: str | None = None
    admin_token: str | None = None
    content_key: str | None = None
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 30.0
    total_timeout_s: float | None = None
    verify_tls: bool = True

    def __repr__(self) -> str:
        return (
            "McpSettings("
            f"public_base_url={self.public_base_url!r}, "
            f"admin_base_url={self.admin_base_url!r}, "
            "observer_token=<redacted>, "
            "producer_token=<redacted>, "
            "admin_token=<redacted>, "
            "content_key=<redacted>, "
            f"connect_timeout_s={self.connect_timeout_s!r}, "
            f"read_timeout_s={self.read_timeout_s!r}, "
            f"total_timeout_s={self.total_timeout_s!r}, "
            f"verify_tls={self.verify_tls!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True, slots=True)
class HttpSettings:
    """Validated transport settings resolved before either server can start."""

    transport: str
    resource_url: str | None
    issuer_url: str | None
    jwks_url: str | None
    allowed_hosts: tuple[str, ...]
    allowed_origins: tuple[str, ...]
    no_browser_origins: bool
    bind_host: str
    bind_port: int
    forwarded_allow_ips: tuple[str, ...]
    clock_skew_seconds: int
    dns_rebinding_protection: bool = True
    policy_argv: tuple[str, ...] = ()


def resolve_http_config(
    argv: list[str],
    environ: Mapping[str, str],
) -> HttpSettings:
    """Resolve stdio or fail-closed Streamable HTTP startup settings."""

    cli, policy_argv = _parse_http_argv(argv)
    transport = _one_value(cli, "--transport", _TRANSPORT, environ, "stdio")
    if transport not in {"stdio", "streamable-http"}:
        raise ConfigError(_TRANSPORT if _TRANSPORT in environ else "--transport")

    present_http_flags = [
        flag
        for flag in (*_HTTP_VALUE_FLAGS, *_HTTP_BOOLEAN_FLAGS)
        if flag in cli
    ]
    present_http_names = [name for name in _HTTP_ONLY_NAMES if name in environ]
    if transport == "stdio":
        if present_http_flags:
            raise ConfigError(present_http_flags[0])
        if present_http_names:
            raise ConfigError(present_http_names[0])
        return HttpSettings(
            transport="stdio",
            resource_url=None,
            issuer_url=None,
            jwks_url=None,
            allowed_hosts=(),
            allowed_origins=(),
            no_browser_origins=False,
            bind_host="127.0.0.1",
            bind_port=8000,
            forwarded_allow_ips=(),
            clock_skew_seconds=60,
            policy_argv=policy_argv,
        )

    if "--disable-dns-rebinding-protection" in cli:
        raise ConfigError("--disable-dns-rebinding-protection")
    if _DNS_REBINDING_PROTECTION in environ:
        raise ConfigError(_DNS_REBINDING_PROTECTION)

    resource_url = _required_http_value(cli, "--resource-url", _RESOURCE_URL, environ)
    issuer_url = _required_http_value(cli, "--issuer-url", _ISSUER_URL, environ)
    jwks_url = _required_http_value(cli, "--jwks-url", _JWKS_URL, environ)
    _resource_url(resource_url)
    _url_without_query_or_fragment(issuer_url, _ISSUER_URL)
    _url_without_query_or_fragment(jwks_url, _JWKS_URL)

    allowed_hosts = _list_value(cli, "--allowed-host", _ALLOWED_HOSTS, environ)
    if not allowed_hosts:
        raise ConfigError(_ALLOWED_HOSTS)
    for host in allowed_hosts:
        _allowed_host(host)

    allowed_origins = _list_value(
        cli,
        "--allowed-origin",
        _ALLOWED_ORIGINS,
        environ,
    )
    no_browser_origins = _boolean_true(
        cli,
        "--no-browser-origins",
        _NO_BROWSER_ORIGINS,
        environ,
    )
    if allowed_origins and no_browser_origins:
        raise ConfigError(_ALLOWED_ORIGINS)
    if not allowed_origins and not no_browser_origins:
        raise ConfigError(_NO_BROWSER_ORIGINS)
    for origin in allowed_origins:
        _url_without_query_or_fragment(origin, _ALLOWED_ORIGINS)

    bind_host = _one_value(cli, "--bind-host", _BIND_HOST, environ, "127.0.0.1")
    if not bind_host.strip():
        raise ConfigError(_BIND_HOST)
    bind_port = _bounded_integer(
        _one_value(cli, "--bind-port", _BIND_PORT, environ, "8000"),
        _BIND_PORT,
        minimum=1,
        maximum=65535,
    )
    forwarded_allow_ips = _list_value(
        cli,
        "--forwarded-allow-ip",
        _FORWARDED_ALLOW_IPS,
        environ,
    )
    for value in forwarded_allow_ips:
        try:
            ip_address(value)
        except ValueError:
            raise ConfigError(_FORWARDED_ALLOW_IPS) from None
    clock_skew_seconds = _bounded_integer(
        _one_value(
            cli,
            "--clock-skew-seconds",
            _CLOCK_SKEW_SECONDS,
            environ,
            "60",
        ),
        _CLOCK_SKEW_SECONDS,
        minimum=0,
        maximum=300,
    )
    return HttpSettings(
        transport=transport,
        resource_url=resource_url,
        issuer_url=issuer_url,
        jwks_url=jwks_url,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
        no_browser_origins=no_browser_origins,
        bind_host=bind_host,
        bind_port=bind_port,
        forwarded_allow_ips=forwarded_allow_ips,
        clock_skew_seconds=clock_skew_seconds,
        policy_argv=policy_argv,
    )


def selection_needs(names: frozenset[str]) -> SelectionNeeds:
    """Return the URLs and roles required by ``names``.

    Producer tools need the public origin. Observer tools need both origins.
    Admin-plane tools need the admin origin. ``admin_get_capabilities`` also
    needs the public origin. Names outside the manifest add nothing.
    """

    by_name = {row.name: row for row in load_manifest()}
    public = False
    admin_url = False
    observer = False
    producer = False
    admin = False
    for name in names:
        row = by_name.get(name)
        if row is None:
            continue
        if row.role == "producer":
            producer = True
            public = True
        elif row.role == "observer":
            observer = True
            public = True
            admin_url = True
        elif row.role == "admin":
            admin = True
            admin_url = True
            if row.method == "get_capabilities":
                public = True
    return SelectionNeeds(
        public=public,
        admin_url=admin_url,
        observer=observer,
        producer=producer,
        admin=admin,
    )


def load_settings(
    env: Mapping[str, str] | None = None,
    *,
    names: frozenset[str] | None = None,
) -> McpSettings:
    """Read MCP settings from ``env`` for the selected tool names.

    ``os.environ`` is used only when ``env`` is omitted. Omitted ``names``
    is the observer preset. Unknown ``WORKHOLD_MCP_*`` names and invalid
    values raise ``ConfigError`` before any client exists. A content key
    with no producer tool is rejected here. An empty key is left for the
    producer client to reject.
    """

    source: Mapping[str, str] = os.environ if env is None else env
    _reject_unknown(source)
    selected = frozenset(preset_names("observer")) if names is None else names
    needs = selection_needs(selected)
    public_base_url = (
        _http_origin(_required_text(source, _PUBLIC_BASE_URL), _PUBLIC_BASE_URL)
        if needs.public
        else None
    )
    admin_base_url = (
        _http_origin(_required_text(source, _ADMIN_BASE_URL), _ADMIN_BASE_URL)
        if needs.admin_url
        else None
    )
    if _CONTENT_KEY in source and not needs.producer:
        raise ConfigError(_CONTENT_KEY)
    content_key = source[_CONTENT_KEY] if _CONTENT_KEY in source else None
    return McpSettings(
        public_base_url=public_base_url,
        admin_base_url=admin_base_url,
        observer_token=_required_text(source, _OBSERVER_TOKEN) if needs.observer else None,
        producer_token=_required_text(source, _PRODUCER_TOKEN) if needs.producer else None,
        admin_token=_required_text(source, _ADMIN_TOKEN) if needs.admin else None,
        content_key=content_key,
        connect_timeout_s=_positive_timeout(source, _CONNECT_TIMEOUT_S, 5.0),
        read_timeout_s=_positive_timeout(source, _READ_TIMEOUT_S, 30.0),
        total_timeout_s=_optional_timeout(source, _TOTAL_TIMEOUT_S),
        verify_tls=_verify_tls(source),
    )


def configs_for(settings: McpSettings) -> tuple[ClientConfig, ClientConfig]:
    """Return the public config, then the admin config.

    Both origins are stored on ``public_base_url``. The admin origin is not
    left only on ``admin_base_url``.
    """

    if settings.public_base_url is None:
        raise ConfigError(_PUBLIC_BASE_URL)
    if settings.admin_base_url is None:
        raise ConfigError(_ADMIN_BASE_URL)
    return (
        _config(settings.public_base_url, settings),
        _config(settings.admin_base_url, settings),
    )


def origin_config(settings: McpSettings, base_url: str) -> ClientConfig:
    """Return one transport config whose request root is ``base_url``."""

    return _config(base_url, settings)


def build_observer(settings: McpSettings) -> ObserverClient:
    """Build one observer over the public transport and the admin transport."""

    public_cfg, admin_cfg = configs_for(settings)
    return ObserverClient(
        HttpJsonTransport.from_config(public_cfg),
        bearer_token=settings.observer_token,
        admin_transport=HttpJsonTransport.from_config(admin_cfg),
    )


def _config(base_url: str, settings: McpSettings) -> ClientConfig:
    return ClientConfig(
        public_base_url=base_url,
        connect_timeout_s=settings.connect_timeout_s,
        read_timeout_s=settings.read_timeout_s,
        total_timeout_s=settings.total_timeout_s,
        verify_tls=settings.verify_tls,
    )


def _reject_unknown(source: Mapping[str, str]) -> None:
    for name in source:
        if name.startswith(_PREFIX) and name not in _KNOWN:
            raise ConfigError(name)


def _required_text(source: Mapping[str, str], name: str) -> str:
    if name not in source:
        raise ConfigError(name)
    value = source[name]
    if value.strip() == "":
        raise ConfigError(name)
    return value.strip()


def _http_origin(value: str, setting_name: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme.lower() not in _HTTP_SCHEMES or not parsed.hostname:
        raise ConfigError(setting_name)
    if parsed.username is not None or parsed.password is not None:
        raise ConfigError(setting_name)
    return value


def _positive_timeout(source: Mapping[str, str], name: str, default: float) -> float:
    parsed = _optional_timeout(source, name)
    if parsed is None:
        return default
    return parsed


def _optional_timeout(source: Mapping[str, str], name: str) -> float | None:
    if name not in source:
        return None
    raw = source[name].strip()
    if raw == "":
        raise ConfigError(name)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ConfigError(name) from None
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(name)
    return value


def _verify_tls(source: Mapping[str, str]) -> bool:
    if _VERIFY_TLS not in source:
        return True
    raw = source[_VERIFY_TLS]
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise ConfigError(_VERIFY_TLS)


def _parse_http_argv(
    argv: list[str],
) -> tuple[dict[str, list[str]], tuple[str, ...]]:
    parsed: dict[str, list[str]] = {}
    remaining: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        flag, separator, attached = token.partition("=")
        if flag == "--transport" or flag in _HTTP_VALUE_FLAGS:
            repeatable = flag in _HTTP_VALUE_FLAGS and _HTTP_VALUE_FLAGS[flag][1]
            if flag in parsed and not repeatable:
                raise ConfigError(flag)
            if separator:
                value = attached
            else:
                index += 1
                if index >= len(argv) or argv[index].startswith("--"):
                    raise ConfigError(flag)
                value = argv[index]
            parsed.setdefault(flag, []).append(value)
        elif flag in _HTTP_BOOLEAN_FLAGS:
            if separator or flag in parsed:
                raise ConfigError(flag)
            parsed[flag] = ["true"]
        else:
            remaining.append(token)
        index += 1
    return parsed, tuple(remaining)


def _one_value(
    cli: Mapping[str, list[str]],
    flag: str,
    env_name: str,
    environ: Mapping[str, str],
    default: str,
) -> str:
    if flag in cli and env_name in environ:
        raise ConfigError(env_name)
    if flag in cli:
        value = cli[flag][0]
    elif env_name in environ:
        value = environ[env_name]
    else:
        return default
    if value.strip() == "":
        raise ConfigError(env_name if env_name in environ else flag)
    return value.strip()


def _required_http_value(
    cli: Mapping[str, list[str]],
    flag: str,
    env_name: str,
    environ: Mapping[str, str],
) -> str:
    value = _one_value(cli, flag, env_name, environ, "")
    if value == "":
        raise ConfigError(env_name)
    return value


def _list_value(
    cli: Mapping[str, list[str]],
    flag: str,
    env_name: str,
    environ: Mapping[str, str],
) -> tuple[str, ...]:
    if flag in cli and env_name in environ:
        raise ConfigError(env_name)
    if flag in cli:
        raw_values = cli[flag]
    elif env_name in environ:
        raw_values = environ[env_name].split(",")
    else:
        return ()
    values = tuple(value.strip() for value in raw_values)
    if not values or any(not value for value in values):
        raise ConfigError(env_name if env_name in environ else flag)
    return values


def _boolean_true(
    cli: Mapping[str, list[str]],
    flag: str,
    env_name: str,
    environ: Mapping[str, str],
) -> bool:
    if flag in cli and env_name in environ:
        raise ConfigError(env_name)
    if flag in cli:
        return True
    if env_name not in environ:
        return False
    if environ[env_name] != "true":
        raise ConfigError(env_name)
    return True


def _resource_url(value: str) -> None:
    parsed = _url_without_query_or_fragment(value, _RESOURCE_URL)
    if not parsed.path.endswith("/mcp") or parsed.path.endswith("/mcp/"):
        raise ConfigError(_RESOURCE_URL)


def _url_without_query_or_fragment(value: str, setting_name: str):
    parsed = urlparse(value)
    if (
        parsed.scheme.lower() not in _HTTP_SCHEMES
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ConfigError(setting_name)
    return parsed


def _allowed_host(value: str) -> None:
    if "://" in value or value in {"*", ":*"}:
        raise ConfigError(_ALLOWED_HOSTS)
    if "*" not in value:
        return
    if not value.endswith(":*") or "*" in value[:-2] or not value[:-2]:
        raise ConfigError(_ALLOWED_HOSTS)


def _bounded_integer(
    value: str,
    setting_name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise ConfigError(setting_name) from None
    if parsed < minimum or parsed > maximum:
        raise ConfigError(setting_name)
    return parsed
