"""Parse ``WORKHOLD_MCP_*`` settings for the roles a tool selection needs."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

from _workhold_client_core.config import ClientConfig
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin.observer import ObserverClient

from workhold_mcp.registry import load_manifest, preset_names

__all__ = [
    "ConfigError",
    "McpSettings",
    "SelectionNeeds",
    "build_observer",
    "configs_for",
    "load_settings",
    "origin_config",
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
    }
)
_HTTP_SCHEMES = frozenset({"http", "https"})


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
