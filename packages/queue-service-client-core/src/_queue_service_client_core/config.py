"""Immutable client configuration without credential discovery."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

from _queue_service_client_core.requests import normalize_base_url

__all__ = [
    "ClientConfig",
    "LONG_POLL_READ_SLACK_S",
    "LONG_POLL_TOTAL_SLACK_S",
    "long_poll_read_timeout_s",
    "long_poll_total_timeout_s",
]

# Per-call long-poll budgets relative to requested wait (Phase 20.1 D-04/D-06).
LONG_POLL_READ_SLACK_S = 5.0
LONG_POLL_TOTAL_SLACK_S = 10.0


def long_poll_read_timeout_s(wait_seconds: int) -> float:
    """Read inactivity budget: ``wait_seconds + 5``."""

    return float(wait_seconds) + LONG_POLL_READ_SLACK_S


def long_poll_total_timeout_s(wait_seconds: int) -> float:
    """Total deadline budget: ``wait_seconds + 10``."""

    return float(wait_seconds) + LONG_POLL_TOTAL_SLACK_S


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """Shared wire configuration for sync and async transports.

    Bearer tokens and claim tokens are **not** part of this object; role clients
    pass them explicitly per request/constructor.
    """

    public_base_url: str
    admin_base_url: str | None = None
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 30.0
    total_timeout_s: float | None = None
    verify_tls: bool = True
    ca_cert_path: str | None = None
    client_cert_path: str | None = None
    client_key_path: str | None = None

    def __post_init__(self) -> None:
        if self.connect_timeout_s <= 0:
            raise ValueError("connect_timeout_s must be positive")
        if self.read_timeout_s <= 0:
            raise ValueError("read_timeout_s must be positive")
        if self.total_timeout_s is not None and self.total_timeout_s <= 0:
            raise ValueError("total_timeout_s must be positive when set")

        object.__setattr__(
            self,
            "public_base_url",
            normalize_base_url(self.public_base_url),
        )
        if self.admin_base_url is not None:
            object.__setattr__(
                self,
                "admin_base_url",
                normalize_base_url(self.admin_base_url),
            )

    @classmethod
    def for_public(cls, base_url: str, **kwargs: object) -> ClientConfig:
        """Build a config with only the public API base URL."""

        return cls(public_base_url=base_url, **kwargs)  # type: ignore[arg-type]

    def sync_timeout_s(self) -> float:
        """Single timeout value for stdlib transports."""

        if self.total_timeout_s is not None:
            return self.total_timeout_s
        return self.read_timeout_s

    def ensure_long_poll_budgets(self, wait_seconds: int) -> tuple[float, float]:
        """Validate configured budgets for a positive wait; return (read, total).

        Fails closed before HTTP when ``read_timeout_s < wait+5`` or the effective
        total deadline is ``< wait+10``.
        """

        if isinstance(wait_seconds, bool) or type(wait_seconds) is not int:
            raise ValueError("wait_seconds must be an integer")
        if wait_seconds < 0:
            raise ValueError("wait_seconds must be >= 0")
        if wait_seconds == 0:
            timeout = self.sync_timeout_s()
            return self.read_timeout_s, timeout

        need_read = long_poll_read_timeout_s(wait_seconds)
        need_total = long_poll_total_timeout_s(wait_seconds)
        if self.read_timeout_s < need_read:
            raise ValueError(
                f"read_timeout_s {self.read_timeout_s} is below required "
                f"{need_read} for wait_seconds={wait_seconds}"
            )
        configured_total = (
            self.total_timeout_s
            if self.total_timeout_s is not None
            else self.read_timeout_s
        )
        if configured_total < need_total:
            raise ValueError(
                f"total_timeout_s {configured_total} is below required "
                f"{need_total} for wait_seconds={wait_seconds}"
            )
        return need_read, need_total

    def __repr__(self) -> str:
        return (
            f"ClientConfig("
            f"public_base_url={_base_url_for_repr(self.public_base_url)!r}, "
            f"admin_base_url={_base_url_for_repr(self.admin_base_url)!r}, "
            f"connect_timeout_s={self.connect_timeout_s!r}, "
            f"read_timeout_s={self.read_timeout_s!r}, "
            f"total_timeout_s={self.total_timeout_s!r}, "
            f"verify_tls={self.verify_tls!r}, "
            f"ca_cert_path={('<set>' if self.ca_cert_path else None)!r}, "
            f"client_cert_path={('<set>' if self.client_cert_path else None)!r}, "
            f"client_key_path={('<set>' if self.client_key_path else None)!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()


def _base_url_for_repr(url: str | None) -> str | None:
    if url is None:
        return None
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunparse((parsed.scheme, host, parsed.path or "/", "", "", ""))
