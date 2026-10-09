"""Secured HTTP webhook DeliveryTransport (ADR 018 / DLVR-03 / DLVR-04).

Deployment configuration owns the single webhook destination, credentials,
timeouts, allowlists, and circuit thresholds. Event content never selects
URL, auth, headers, redirects, or transport options.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import math
import socket
import ssl
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Final
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    Request,
    build_opener,
)

from workhold.delivery.cloudevents import CLOUDEVENTS_JSON_MEDIA_TYPE
from workhold.delivery.relay import (
    DeliveryDisposition,
    DeliveryResult,
    TransportReadiness,
    cap_retry_after_seconds,
)
from workhold.delivery.repository import ClaimedDeliveryEvent
from workhold.settings import EnvironmentMode, Secret

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS: Final[frozenset[int]] = frozenset({408, 425, 429})
_DEFAULT_MAX_RESPONSE_BYTES: Final[int] = 65_536


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class _RedirectRejected(Exception):
    """Raised when the peer attempts an HTTP redirect."""


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(  # type: ignore[no-untyped-def]
        self, req, fp, code, msg, headers, newurl
    ):
        raise _RedirectRejected()


def _safe_host(hostname: str | None) -> str:
    return hostname or ""


def _redact_url(url: str) -> str:
    parsed = urlparse(url)
    host = _safe_host(parsed.hostname)
    path = parsed.path or "/"
    return f"{parsed.scheme}://{host}{path}"


@dataclass(frozen=True, slots=True)
class HttpDeliveryConfig:
    """Validated deployment-owned HTTP webhook settings."""

    webhook_url: str
    environment: EnvironmentMode
    allowed_hosts: frozenset[str]
    allowed_cidrs: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]
    connect_timeout_seconds: float
    read_timeout_seconds: float
    total_timeout_seconds: float
    max_response_bytes: int
    bearer_token: Secret | None
    circuit_failure_threshold: int
    circuit_success_threshold: int
    circuit_open_seconds: float
    half_open_max_probes: int
    retry_after_cap_seconds: float
    client_cert_path: str | None = None
    client_key_path: str | None = None
    verify_tls: bool = True

    def __post_init__(self) -> None:
        self._validate()

    def __repr__(self) -> str:
        return (
            "HttpDeliveryConfig("
            f"webhook_url={_redact_url(self.webhook_url)!r}, "
            f"environment={self.environment!r}, "
            f"allowed_hosts={sorted(self.allowed_hosts)!r}, "
            f"bearer_token={'Secret(***)' if self.bearer_token else None}, "
            f"client_cert_path={'***' if self.client_cert_path else None})"
        )

    def __str__(self) -> str:
        return repr(self)

    def _validate(self) -> None:
        parsed = urlparse(self.webhook_url)
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("webhook URL must not contain userinfo credentials")
        if parsed.fragment:
            raise ValueError("webhook URL must not contain a fragment")
        scheme = (parsed.scheme or "").lower()
        if self.environment is EnvironmentMode.PRODUCTION:
            if scheme != "https":
                raise ValueError("production webhook scheme must be https")
        elif scheme not in {"http", "https"}:
            raise ValueError("webhook scheme must be http or https")
        host = _safe_host(parsed.hostname).lower()
        if not host:
            raise ValueError("webhook URL host is required")
        if host not in {h.lower() for h in self.allowed_hosts}:
            raise ValueError("webhook host is not on the deployment allowlist")
        if not self.allowed_cidrs:
            raise ValueError("allowed_cidrs must be non-empty")
        for name, value in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("read_timeout_seconds", self.read_timeout_seconds),
            ("total_timeout_seconds", self.total_timeout_seconds),
            ("circuit_open_seconds", self.circuit_open_seconds),
            ("retry_after_cap_seconds", self.retry_after_cap_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ValueError(f"{name} must be a finite positive duration")
        if float(self.total_timeout_seconds) < float(self.connect_timeout_seconds):
            raise ValueError("total_timeout_seconds must be >= connect_timeout_seconds")
        if (
            isinstance(self.max_response_bytes, bool)
            or not isinstance(self.max_response_bytes, int)
            or self.max_response_bytes < 1
        ):
            raise ValueError("max_response_bytes must be a positive integer")
        for name, value in (
            ("circuit_failure_threshold", self.circuit_failure_threshold),
            ("circuit_success_threshold", self.circuit_success_threshold),
            ("half_open_max_probes", self.half_open_max_probes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be an integer >= 1")
        if self.client_cert_path and not self.client_key_path:
            raise ValueError("client_key_path is required when client_cert_path is set")
        if self.client_key_path and not self.client_cert_path:
            raise ValueError("client_cert_path is required when client_key_path is set")


class _CircuitBreaker:
    """Process-local load protection; never acknowledges or dead-letters."""

    def __init__(
        self,
        *,
        failure_threshold: int,
        success_threshold: int,
        open_seconds: float,
        half_open_max_probes: int,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._open_seconds = open_seconds
        self._half_open_max_probes = half_open_max_probes
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at_mono: float | None = None
        self._half_open_inflight = 0

    def force_open(self) -> None:
        with self._lock:
            self._state = CircuitState.OPEN
            self._opened_at_mono = time.monotonic()
            self._failures = self._failure_threshold
            self._successes = 0
            self._half_open_inflight = 0

    def readiness(self) -> TransportReadiness:
        with self._lock:
            self._maybe_transition_locked()
            if self._state is CircuitState.CLOSED:
                return TransportReadiness(
                    accepting=True,
                    reason_code="circuit_closed",
                    retry_after_seconds=None,
                )
            if self._state is CircuitState.OPEN:
                return TransportReadiness(
                    accepting=False,
                    reason_code="circuit_open",
                    retry_after_seconds=self._remaining_open_locked(),
                )
            if self._half_open_inflight >= self._half_open_max_probes:
                return TransportReadiness(
                    accepting=False,
                    reason_code="circuit_half_open_busy",
                    retry_after_seconds=min(0.05, self._open_seconds),
                )
            return TransportReadiness(
                accepting=True,
                reason_code="circuit_half_open",
                retry_after_seconds=None,
            )

    def begin_probe(self) -> bool:
        with self._lock:
            self._maybe_transition_locked()
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.OPEN:
                return False
            if self._half_open_inflight >= self._half_open_max_probes:
                return False
            self._half_open_inflight += 1
            return True

    def end_probe(self, *, success: bool) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN and self._half_open_inflight > 0:
                self._half_open_inflight -= 1
            if success:
                self._on_success_locked()
            else:
                self._on_failure_locked()

    def _remaining_open_locked(self) -> float:
        if self._opened_at_mono is None:
            return self._open_seconds
        remaining = self._open_seconds - (time.monotonic() - self._opened_at_mono)
        return max(0.0, remaining)

    def _maybe_transition_locked(self) -> None:
        if self._state is CircuitState.OPEN and self._remaining_open_locked() <= 0:
            self._state = CircuitState.HALF_OPEN
            self._successes = 0
            self._half_open_inflight = 0

    def _on_success_locked(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._successes += 1
            if self._successes >= self._success_threshold:
                self._state = CircuitState.CLOSED
                self._failures = 0
                self._successes = 0
                self._opened_at_mono = None
            return
        self._failures = 0

    def _on_failure_locked(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at_mono = time.monotonic()
            self._failures = self._failure_threshold
            self._successes = 0
            return
        self._failures += 1
        if self._failures >= self._failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at_mono = time.monotonic()


class HttpDeliveryTransport:
    """POST stored CloudEvents structured JSON to one deployment webhook."""

    def __init__(self, config: HttpDeliveryConfig) -> None:
        self._config = config
        self._parsed = urlparse(config.webhook_url)
        self._host = _safe_host(self._parsed.hostname).lower()
        self._port = self._parsed.port or (
            443 if self._parsed.scheme == "https" else 80
        )
        self._validate_resolved_addresses()
        self._breaker = _CircuitBreaker(
            failure_threshold=config.circuit_failure_threshold,
            success_threshold=config.circuit_success_threshold,
            open_seconds=config.circuit_open_seconds,
            half_open_max_probes=config.half_open_max_probes,
        )
        self._ssl_context = self._build_ssl_context()
        handlers: list[Any] = [_RejectRedirects()]
        if self._ssl_context is not None:
            handlers.append(HTTPSHandler(context=self._ssl_context))
        self._opener = build_opener(*handlers)

    async def readiness(self) -> TransportReadiness:
        ready = self._breaker.readiness()
        if ready.retry_after_seconds is not None:
            capped = cap_retry_after_seconds(
                ready.retry_after_seconds,
                cap_seconds=float(self._config.retry_after_cap_seconds),
            )
            return TransportReadiness(
                accepting=ready.accepting,
                reason_code=ready.reason_code,
                retry_after_seconds=capped,
            )
        return ready

    async def publish(self, event: ClaimedDeliveryEvent) -> DeliveryResult:
        if not self._breaker.begin_probe():
            remaining = self._breaker.readiness().retry_after_seconds
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code="circuit_open",
                retry_after_seconds=cap_retry_after_seconds(
                    remaining,
                    cap_seconds=float(self._config.retry_after_cap_seconds),
                ),
            )
        success = False
        try:
            result = await asyncio.to_thread(self._publish_sync, event)
            success = result.disposition is DeliveryDisposition.ACKNOWLEDGED
            return result
        except Exception:
            logger.debug(
                "http delivery publish failed destination=%s",
                _redact_url(self._config.webhook_url),
                exc_info=False,
            )
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code="publish.uncertain",
                retry_after_seconds=None,
            )
        finally:
            self._breaker.end_probe(success=success)

    def close(self) -> None:
        """Release transport resources (stdlib opener has nothing durable)."""
        return None

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        if self._parsed.scheme != "https":
            return None
        ctx = ssl.create_default_context()
        if not self._config.verify_tls:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if self._config.client_cert_path and self._config.client_key_path:
            ctx.load_cert_chain(
                self._config.client_cert_path,
                self._config.client_key_path,
            )
        return ctx

    def _validate_resolved_addresses(self) -> list[str]:
        try:
            infos = socket.getaddrinfo(
                self._host,
                self._port,
                type=socket.SOCK_STREAM,
            )
        except OSError as exc:
            raise ValueError("webhook DNS resolution failed") from exc
        if not infos:
            raise ValueError("webhook DNS resolution returned no addresses")
        allowed: list[str] = []
        for info in infos:
            sockaddr = info[4]
            ip_text = sockaddr[0]
            try:
                ip = ipaddress.ip_address(ip_text)
            except ValueError as exc:
                raise ValueError("webhook resolved address is invalid") from exc
            if not any(ip in network for network in self._config.allowed_cidrs):
                raise ValueError(
                    "webhook resolved address is outside the deployment "
                    "CIDR allowlist (mixed or disallowed DNS results)"
                )
            allowed.append(str(ip))
        if not allowed:
            raise ValueError("webhook DNS allowlist check failed")
        return allowed

    def _publish_sync(self, event: ClaimedDeliveryEvent) -> DeliveryResult:
        self._validate_resolved_addresses()
        body = json.dumps(
            dict(event.envelope),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        headers = {
            "Content-Type": CLOUDEVENTS_JSON_MEDIA_TYPE,
            "Accept": CLOUDEVENTS_JSON_MEDIA_TYPE,
            "Content-Length": str(len(body)),
        }
        if self._config.bearer_token is not None:
            token = self._config.bearer_token.get_secret_value()
            headers["Authorization"] = f"Bearer {token}"

        request = Request(
            self._config.webhook_url,
            data=body,
            headers=headers,
            method="POST",
        )
        timeout = min(
            float(self._config.total_timeout_seconds),
            float(self._config.connect_timeout_seconds)
            + float(self._config.read_timeout_seconds),
        )
        try:
            with self._opener.open(request, timeout=timeout) as response:
                status = int(getattr(response, "status", response.getcode()))
                raw_headers = {k.lower(): v for k, v in response.headers.items()}
                self._drain_bounded(response)
                return self._classify(status, raw_headers)
        except _RedirectRejected:
            return DeliveryResult(
                disposition=DeliveryDisposition.PERMANENT,
                failure_code="http.redirect",
                retry_after_seconds=None,
            )
        except HTTPError as exc:
            status = int(exc.code)
            raw_headers = {
                k.lower(): v
                for k, v in (exc.headers.items() if exc.headers else [])
            }
            try:
                if exc.fp is not None:
                    self._drain_bounded(exc.fp)
            except OSError:
                pass
            if 300 <= status < 400:
                return DeliveryResult(
                    disposition=DeliveryDisposition.PERMANENT,
                    failure_code="http.redirect",
                    retry_after_seconds=None,
                )
            return self._classify(status, raw_headers)
        except TimeoutError:
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code="http.timeout",
                retry_after_seconds=None,
            )
        except URLError as exc:
            if _is_timeout(exc.reason):
                return DeliveryResult(
                    disposition=DeliveryDisposition.RETRYABLE,
                    failure_code="http.timeout",
                    retry_after_seconds=None,
                )
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code="http.network",
                retry_after_seconds=None,
            )
        except OSError as exc:
            if _is_timeout(exc):
                return DeliveryResult(
                    disposition=DeliveryDisposition.RETRYABLE,
                    failure_code="http.timeout",
                    retry_after_seconds=None,
                )
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code="http.network",
                retry_after_seconds=None,
            )

    def _drain_bounded(self, stream: Any) -> None:
        remaining = int(self._config.max_response_bytes)
        while remaining > 0:
            chunk = stream.read(min(4096, remaining))
            if not chunk:
                return
            remaining -= len(chunk)
        while True:
            chunk = stream.read(8192)
            if not chunk:
                return

    def _classify(
        self, status: int, headers: Mapping[str, str]
    ) -> DeliveryResult:
        retry_after = _parse_retry_after(
            headers.get("retry-after"),
            cap_seconds=float(self._config.retry_after_cap_seconds),
        )
        if 200 <= status < 300:
            return DeliveryResult(
                disposition=DeliveryDisposition.ACKNOWLEDGED,
                failure_code=None,
                retry_after_seconds=None,
            )
        if status in _RETRYABLE_STATUS or status >= 500:
            return DeliveryResult(
                disposition=DeliveryDisposition.RETRYABLE,
                failure_code=f"http.{status}",
                retry_after_seconds=retry_after,
            )
        if 300 <= status < 400:
            return DeliveryResult(
                disposition=DeliveryDisposition.PERMANENT,
                failure_code="http.redirect",
                retry_after_seconds=None,
            )
        return DeliveryResult(
            disposition=DeliveryDisposition.PERMANENT,
            failure_code=f"http.{status}",
            retry_after_seconds=None,
        )


def _is_timeout(exc: BaseException | object) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    name = type(exc).__name__.lower()
    if "timeout" in name:
        return True
    text = str(exc).lower()
    return "timed out" in text or "timeout" in text


def _parse_retry_after(raw: str | None, *, cap_seconds: float) -> float | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        delta = (when - datetime.now(UTC)).total_seconds()
        return cap_retry_after_seconds(delta, cap_seconds=cap_seconds)
    return cap_retry_after_seconds(seconds, cap_seconds=cap_seconds)


def http_config_from_mapping(
    environ: Mapping[str, str],
    *,
    environment: EnvironmentMode | None = None,
) -> HttpDeliveryConfig:
    """Build HttpDeliveryConfig from environment-like mapping."""
    url = (environ.get("QUEUE_DELIVERY_WEBHOOK_URL") or "").strip()
    if not url:
        raise ValueError("QUEUE_DELIVERY_WEBHOOK_URL is required")

    env_raw = (
        environment.value
        if environment is not None
        else (environ.get("QUEUE_ENVIRONMENT") or "development").strip().lower()
    )
    try:
        env_mode = EnvironmentMode(env_raw)
    except ValueError as exc:
        raise ValueError(f"unknown QUEUE_ENVIRONMENT: {env_raw!r}") from exc

    hosts_raw = (environ.get("QUEUE_DELIVERY_ALLOWED_HOSTS") or "").strip()
    if not hosts_raw:
        raise ValueError("QUEUE_DELIVERY_ALLOWED_HOSTS is required")
    hosts = frozenset(h.strip().lower() for h in hosts_raw.split(",") if h.strip())

    cidrs_raw = (environ.get("QUEUE_DELIVERY_ALLOWED_CIDRS") or "").strip()
    if not cidrs_raw:
        raise ValueError("QUEUE_DELIVERY_ALLOWED_CIDRS is required")
    cidrs: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for part in cidrs_raw.split(","):
        part = part.strip()
        if not part:
            continue
        cidrs.append(ipaddress.ip_network(part, strict=False))

    bearer_raw = (environ.get("QUEUE_DELIVERY_BEARER_TOKEN") or "").strip()
    bearer = Secret(bearer_raw) if bearer_raw else None

    def _float(name: str, default: float) -> float:
        raw = (environ.get(name) or "").strip()
        if not raw:
            return default
        return float(raw)

    def _int(name: str, default: int) -> int:
        raw = (environ.get(name) or "").strip()
        if not raw:
            return default
        return int(raw)

    cert = (environ.get("QUEUE_DELIVERY_CLIENT_CERT_PATH") or "").strip() or None
    key = (environ.get("QUEUE_DELIVERY_CLIENT_KEY_PATH") or "").strip() or None
    verify_raw = (environ.get("QUEUE_DELIVERY_VERIFY_TLS") or "true").strip().lower()
    verify_tls = verify_raw not in {"0", "false", "no"}

    return HttpDeliveryConfig(
        webhook_url=url,
        environment=env_mode,
        allowed_hosts=hosts,
        allowed_cidrs=tuple(cidrs),
        connect_timeout_seconds=_float("QUEUE_DELIVERY_CONNECT_TIMEOUT_SECONDS", 2.0),
        read_timeout_seconds=_float("QUEUE_DELIVERY_READ_TIMEOUT_SECONDS", 5.0),
        total_timeout_seconds=_float("QUEUE_DELIVERY_TOTAL_TIMEOUT_SECONDS", 8.0),
        max_response_bytes=_int(
            "QUEUE_DELIVERY_MAX_RESPONSE_BYTES", _DEFAULT_MAX_RESPONSE_BYTES
        ),
        bearer_token=bearer,
        circuit_failure_threshold=_int("QUEUE_DELIVERY_CIRCUIT_FAILURE_THRESHOLD", 5),
        circuit_success_threshold=_int("QUEUE_DELIVERY_CIRCUIT_SUCCESS_THRESHOLD", 1),
        circuit_open_seconds=_float("QUEUE_DELIVERY_CIRCUIT_OPEN_SECONDS", 30.0),
        half_open_max_probes=_int("QUEUE_DELIVERY_HALF_OPEN_MAX_PROBES", 1),
        retry_after_cap_seconds=_float("QUEUE_DELIVERY_RETRY_AFTER_CAP_SECONDS", 60.0),
        client_cert_path=cert,
        client_key_path=key,
        verify_tls=verify_tls,
    )


__all__ = [
    "HttpDeliveryConfig",
    "HttpDeliveryTransport",
    "http_config_from_mapping",
]
