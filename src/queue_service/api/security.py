"""Authentication, authorization, and sanitized error projection for HTTP planes.

Physically separate application (`/v1`) and admin (`/admin/v1`) ASGI compositions
share this module. Every OpenAPI operation maps to exactly one plane. Requests
authenticate, map to a closed :class:`~queue_service.security.authorization.Operation`,
and authorize (including named-queue scope when present) before any handler or
resource lookup. Diagnostics use :func:`sanitize_for_diagnostics`.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

from queue_service.security.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    Authorizer,
    Operation,
    QUEUE_SCOPED_OPERATIONS,
    ROLE_OPERATION_GRANTS,
)
from queue_service.security.credentials import IdentityAuthenticator, Unauthenticated
from queue_service.security.principals import Principal
from queue_service.security.redaction import sanitize_for_diagnostics

logger = logging.getLogger(__name__)

PlaneName = Literal["application", "admin"]
ASGIApp = Callable[
    [MutableMapping[str, Any], Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]],
    Awaitable[None],
]

_HTTP_METHODS: Final[frozenset[str]] = frozenset(
    {"get", "post", "put", "patch", "delete", "head", "options"}
)
_OPENAPI_CANDIDATES: Final[tuple[Path, ...]] = (
    Path.cwd() / "openapi" / "queue.openapi.json",
    Path(__file__).resolve().parents[3] / "openapi" / "queue.openapi.json",
)


@dataclass(frozen=True, slots=True)
class ListenerBind:
    """Deployment binding retained on each plane app for dual-listener startup."""

    host: str
    port: int

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("listener host must be non-empty")
        if not (0 < self.port < 65536):
            raise ValueError("listener port must be in 1..65535")


@dataclass(frozen=True, slots=True)
class Route:
    method: str
    path_template: str
    path_re: re.Pattern[str]
    operation_id: str
    param_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Stable principal/authz context injected for handlers after security gates."""

    request_id: str
    principal: Principal
    authorization: AuthorizationContext
    path_params: Mapping[str, str]
    operation: Operation


def _default_openapi_path() -> Path:
    for path in _OPENAPI_CANDIDATES:
        if path.is_file():
            return path
    raise FileNotFoundError(
        "openapi/queue.openapi.json not found; Phase 3.1 contract required"
    )


def load_openapi_operation_catalog(
    path: Path | None = None,
) -> list[tuple[str, str, str]]:
    """Return ``(operationId, path_template, METHOD)`` for every OpenAPI operation."""
    openapi_path = path if path is not None else _default_openapi_path()
    spec = json.loads(openapi_path.read_text(encoding="utf-8"))
    schemes = spec.get("components", {}).get("securitySchemes", {})
    for required in (
        "ProducerBearer",
        "WorkerBearer",
        "ObserverBearer",
        "AdminBearer",
    ):
        if required not in schemes:
            raise ValueError(f"OpenAPI missing security scheme {required!r}")

    catalog: list[tuple[str, str, str]] = []
    for path_template, item in spec.get("paths", {}).items():
        if not isinstance(item, dict):
            continue
        for method, operation in item.items():
            if method not in _HTTP_METHODS or not isinstance(operation, dict):
                continue
            operation_id = operation.get("operationId")
            if not isinstance(operation_id, str) or not operation_id:
                raise ValueError(f"OpenAPI operation missing operationId at {path_template}")
            catalog.append((operation_id, path_template, method.upper()))
    if not catalog:
        raise ValueError("OpenAPI defines no HTTP operations")
    return catalog


def _classify_path(path_template: str) -> PlaneName:
    if path_template.startswith("/admin/v1"):
        return "admin"
    if path_template.startswith("/v1"):
        return "application"
    raise ValueError(f"OpenAPI path is outside known planes: {path_template!r}")


def _build_plane_sets(
    catalog: Sequence[tuple[str, str, str]],
) -> tuple[frozenset[str], frozenset[str]]:
    application: set[str] = set()
    admin: set[str] = set()
    by_id: dict[str, PlaneName] = {}
    for operation_id, path_template, _method in catalog:
        plane = _classify_path(path_template)
        prior = by_id.get(operation_id)
        if prior is not None and prior != plane:
            raise ValueError(
                f"operation {operation_id!r} mapped to multiple planes"
            )
        by_id[operation_id] = plane
        if plane == "application":
            application.add(operation_id)
        else:
            admin.add(operation_id)
    if application & admin:
        raise ValueError("application and admin operation sets overlap")
    return frozenset(application), frozenset(admin)


_CATALOG = load_openapi_operation_catalog()
APPLICATION_OPERATIONS, ADMIN_OPERATIONS = _build_plane_sets(_CATALOG)

_HTTP_OPERATION_VALUES: Final[frozenset[str]] = frozenset(
    op.value
    for op in Operation
    if op
    not in {
        Operation.APPLY_MIGRATIONS,
        Operation.RUN_PARTITION_MAINTENANCE,
    }
)
if APPLICATION_OPERATIONS | ADMIN_OPERATIONS != _HTTP_OPERATION_VALUES:
    missing = _HTTP_OPERATION_VALUES - (APPLICATION_OPERATIONS | ADMIN_OPERATIONS)
    extra = (APPLICATION_OPERATIONS | ADMIN_OPERATIONS) - _HTTP_OPERATION_VALUES
    raise RuntimeError(
        "OpenAPI operationIds drift from Operation enum: "
        f"missing={sorted(missing)} extra={sorted(extra)}"
    )


def plane_for_operation(operation_id: str, path: str) -> PlaneName:
    """Resolve the single plane for an operation; fail on unmapped/cross-plane."""
    path_plane = _classify_path(path if path.startswith("/") else f"/{path}")
    if operation_id in APPLICATION_OPERATIONS:
        op_plane: PlaneName = "application"
    elif operation_id in ADMIN_OPERATIONS:
        op_plane = "admin"
    else:
        raise ValueError(f"unmapped operation {operation_id!r}")
    if op_plane != path_plane:
        raise ValueError(
            f"operation {operation_id!r} belongs to {op_plane} plane, "
            f"not path plane {path_plane}"
        )
    return op_plane


def _template_to_regex(template: str) -> tuple[re.Pattern[str], tuple[str, ...]]:
    names: list[str] = []
    parts: list[str] = []
    i = 0
    while i < len(template):
        if template[i] == "{":
            end = template.index("}", i)
            name = template[i + 1 : end]
            names.append(name)
            parts.append(f"(?P<{name}>[^/]+)")
            i = end + 1
        else:
            parts.append(re.escape(template[i]))
            i += 1
    return re.compile("^" + "".join(parts) + "$"), tuple(names)


def build_routes(plane: PlaneName, *, openapi_path: Path | None = None) -> tuple[Route, ...]:
    catalog = load_openapi_operation_catalog(openapi_path)
    routes: list[Route] = []
    for operation_id, path_template, method in catalog:
        if _classify_path(path_template) != plane:
            continue
        path_re, names = _template_to_regex(path_template)
        routes.append(
            Route(
                method=method,
                path_template=path_template,
                path_re=path_re,
                operation_id=operation_id,
                param_names=names,
            )
        )
    return tuple(routes)


def match_route(
    routes: Sequence[Route],
    method: str,
    path: str,
) -> tuple[Route, dict[str, str]] | None:
    method_u = method.upper()
    for route in routes:
        if route.method != method_u:
            continue
        matched = route.path_re.match(path)
        if matched is None:
            continue
        return route, matched.groupdict()
    return None


def error_envelope(
    *,
    code: str,
    message: str,
    retryable: bool,
    request_id: str,
    retry_after_ms: int | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    safe_details = sanitize_for_diagnostics(dict(details or {}))
    if not isinstance(safe_details, dict):
        safe_details = {}
    return {
        "code": code,
        "message": message,
        "retryable": retryable,
        "retry_after_ms": retry_after_ms,
        "request_id": request_id,
        "details": safe_details,
    }


# docs/04-architecture/error-model.md HTTP projection (Phase 3.1/3.2 stable codes).
_PROTOCOL_ERROR_HTTP: Final[Mapping[str, int]] = {
    "validation_failed": 400,
    "idempotency_key_required": 400,
    "payload_too_large": 413,
    "idempotency_conflict": 409,
    "queue_not_found": 404,
    "queue_draining": 409,
    "task_not_found": 404,
    "claim_not_found": 404,
    "lease_lost": 409,
    "task_already_terminal": 409,
    "cancel_race_lost": 409,
    "config_version_conflict": 412,
    "permission_denied": 403,
    "unauthenticated": 401,
    "resource_exhausted": 429,
    "dependency_unavailable": 503,
    "not_accepting": 503,
    "internal_error": 500,
}

_PROTOCOL_RETRYABLE_DEFAULTS: Final[frozenset[str]] = frozenset(
    {
        "queue_draining",
        "config_version_conflict",
        "resource_exhausted",
        "dependency_unavailable",
        "not_accepting",
        "internal_error",
    }
)

# Conservative Retry-After for draining when callers omit retry_after_ms.
_DEFAULT_DRAINING_RETRY_AFTER_MS: Final[int] = 1000


def protocol_error_http_status(code: str) -> int:
    """Map a stable protocol error code to its coarse HTTP status."""

    return int(_PROTOCOL_ERROR_HTTP.get(code, 500))


def protocol_error_retry_after_seconds(
    *,
    code: str,
    retryable: bool,
    retry_after_ms: int | None,
) -> int | None:
    """Return ``Retry-After`` seconds for statuses that advertise the header.

    OpenAPI marks ``Retry-After`` required on enqueue 409/429/503 responses.
    Non-retryable conflicts use ``0``; draining defaults to 1s when unset.
    """

    status = protocol_error_http_status(code)
    if status not in {409, 429, 503}:
        return None
    if retry_after_ms is not None:
        if retry_after_ms <= 0:
            return 0
        return max(1, (int(retry_after_ms) + 999) // 1000)
    if code == "queue_draining" or (retryable and status in {429, 503}):
        return max(1, (_DEFAULT_DRAINING_RETRY_AFTER_MS + 999) // 1000)
    return 0


async def send_protocol_error(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    *,
    request_id: str,
    code: str,
    message: str,
    retryable: bool | None = None,
    retry_after_ms: int | None = None,
    details: Mapping[str, Any] | None = None,
) -> None:
    """Project a protocol error through the shared envelope and HTTP status."""

    effective_retryable = (
        bool(retryable)
        if retryable is not None
        else code in _PROTOCOL_RETRYABLE_DEFAULTS
    )
    effective_retry_after_ms = retry_after_ms
    if effective_retry_after_ms is None and code == "queue_draining":
        effective_retry_after_ms = _DEFAULT_DRAINING_RETRY_AFTER_MS
    status = protocol_error_http_status(code)
    headers: dict[str, str] = {"X-Request-ID": request_id}
    retry_after = protocol_error_retry_after_seconds(
        code=code,
        retryable=effective_retryable,
        retry_after_ms=effective_retry_after_ms,
    )
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    await send_json(
        send,
        status=status,
        payload=error_envelope(
            code=code,
            message=message,
            retryable=effective_retryable,
            request_id=request_id,
            retry_after_ms=effective_retry_after_ms,
            details=details,
        ),
        extra_headers=headers,
    )


def _header_map(scope: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in scope.get("headers", []):
        key = raw_key.decode("latin-1").lower()
        out[key] = raw_val.decode("latin-1")
    return out


async def _read_body(receive: Callable[[], Awaitable[dict[str, Any]]]) -> bytes:
    chunks: list[bytes] = []
    while True:
        message = await receive()
        if message["type"] != "http.request":
            break
        chunks.append(message.get("body", b"") or b"")
        if not message.get("more_body"):
            break
    return b"".join(chunks)


async def send_json(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    *,
    status: int,
    payload: Mapping[str, Any],
    extra_headers: Mapping[str, str] | None = None,
) -> None:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    headers: list[tuple[bytes, bytes]] = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    if extra_headers:
        for key, value in extra_headers.items():
            headers.append(
                (key.lower().encode("latin-1"), value.encode("latin-1"))
            )
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": headers,
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})


def _extract_queue_names(
    operation: Operation,
    path_params: Mapping[str, str],
    body: bytes,
) -> list[str | None]:
    if "queue_name" in path_params:
        return [path_params["queue_name"]]
    if operation is Operation.CLAIM_TASKS:
        if not body:
            return [None]
        try:
            parsed = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return [None]
        if not isinstance(parsed, dict):
            return [None]
        queues = parsed.get("queues")
        if not isinstance(queues, list) or not queues:
            return [None]
        names: list[str | None] = []
        for item in queues:
            names.append(item if isinstance(item, str) else None)
        return names or [None]
    return [None]


_CLAIM_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


def _claim_queues_ready_for_scope_check(queue_names: list[str | None]) -> bool:
    """Return True when claim ``queues`` are structurally ready for exact scope checks.

    Malformed lists (empty, non-strings, duplicates, overlong names, invalid pattern)
    are deferred to the claim handler so workers receive ``validation_failed`` instead
    of a premature ``permission_denied``.
    """

    if not queue_names or any(name is None for name in queue_names):
        return False
    names = [name for name in queue_names if isinstance(name, str)]
    if len(names) != len(queue_names):
        return False
    if len(set(names)) != len(names):
        return False
    for name in names:
        if not (1 <= len(name) <= 128) or _CLAIM_QUEUE_NAME_RE.fullmatch(name) is None:
            return False
    return True


def _role_only_authorization(
    principal: Principal,
    operation: Operation,
) -> AuthorizationContext | AuthorizationDenied:
    granted = ROLE_OPERATION_GRANTS.get(principal.role, frozenset())
    if operation not in granted:
        return AuthorizationDenied()
    return AuthorizationContext(
        principal=principal,
        operation=operation,
        queue_name=None,
        producer_id=None,
    )


def _authorize(
    authorizer: Authorizer,
    principal: Principal,
    operation: Operation,
    path_params: Mapping[str, str],
    body: bytes,
) -> AuthorizationContext | AuthorizationDenied:
    if operation not in QUEUE_SCOPED_OPERATIONS:
        return authorizer.authorize(principal, operation, queue_name=None)

    queue_names = _extract_queue_names(operation, path_params, body)
    if operation is Operation.CLAIM_TASKS:
        if not _claim_queues_ready_for_scope_check(queue_names):
            return _role_only_authorization(principal, operation)
        last: AuthorizationContext | AuthorizationDenied = AuthorizationDenied()
        for queue_name in queue_names:
            last = authorizer.authorize(principal, operation, queue_name=queue_name)
            if isinstance(last, AuthorizationDenied):
                return last
        return last

    queue_name = queue_names[0]
    if queue_name is not None:
        return authorizer.authorize(principal, operation, queue_name=queue_name)

    # Path/body has no queue yet (task_id / claim_id routes). Deny wrong roles
    # uniformly before lookup; handlers re-check exact queue after metadata read.
    return _role_only_authorization(principal, operation)


def _log_security_event(
    *,
    request_id: str,
    code: str,
    operation: str | None,
    principal_id: str | None,
) -> None:
    diagnostic = sanitize_for_diagnostics(
        {
            "request_id": request_id,
            "code": code,
            "operation": operation,
            "principal_id": principal_id,
            "status": "denied" if code in {"unauthenticated", "permission_denied"} else "ok",
        }
    )
    logger.info("security_decision %s", diagnostic)


Handler = Callable[
    [
        MutableMapping[str, Any],
        Callable[[], Awaitable[dict[str, Any]]],
        Callable[[dict[str, Any]], Awaitable[None]],
        RequestContext,
        bytes,
    ],
    Awaitable[None],
]


def create_plane_app(
    *,
    plane: PlaneName,
    authenticator: IdentityAuthenticator,
    authorizer: Authorizer,
    bind: ListenerBind,
    handler: Handler,
    lookup_probe: Any | None = None,
    openapi_path: Path | None = None,
) -> Any:
    """Compose one plane ASGI app with authenticate → authorize → handler."""

    routes = build_routes(plane, openapi_path=openapi_path)

    async def app(
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http":
            return

        request_id = str(uuid.uuid4())
        headers = _header_map(scope)
        method = str(scope.get("method", "GET"))
        path = str(scope.get("path", "/"))
        body = await _read_body(receive)

        matched = match_route(routes, method, path)
        if matched is None:
            _log_security_event(
                request_id=request_id,
                code="not_found",
                operation=None,
                principal_id=None,
            )
            await send_json(
                send,
                status=404,
                payload=error_envelope(
                    code="task_not_found",
                    message="resource not found",
                    retryable=False,
                    request_id=request_id,
                ),
            )
            return

        route, path_params = matched
        try:
            operation = Operation(route.operation_id)
        except ValueError:
            await send_json(
                send,
                status=404,
                payload=error_envelope(
                    code="task_not_found",
                    message="resource not found",
                    retryable=False,
                    request_id=request_id,
                ),
            )
            return

        auth_result = authenticator.authenticate(headers.get("authorization"))
        if isinstance(auth_result, Unauthenticated):
            _log_security_event(
                request_id=request_id,
                code="unauthenticated",
                operation=operation.value,
                principal_id=None,
            )
            await send_json(
                send,
                status=401,
                payload=error_envelope(
                    code="unauthenticated",
                    message="authentication required",
                    retryable=False,
                    request_id=request_id,
                ),
            )
            return

        principal = auth_result
        decision = _authorize(authorizer, principal, operation, path_params, body)
        if isinstance(decision, AuthorizationDenied):
            _log_security_event(
                request_id=request_id,
                code="permission_denied",
                operation=operation.value,
                principal_id=principal.principal_id,
            )
            await send_json(
                send,
                status=403,
                payload=error_envelope(
                    code="permission_denied",
                    message="permission denied",
                    retryable=False,
                    request_id=request_id,
                ),
            )
            return

        context = RequestContext(
            request_id=request_id,
            principal=principal,
            authorization=decision,
            path_params=path_params,
            operation=operation,
        )
        scope["queue_request_context"] = context
        scope["queue_lookup_probe"] = lookup_probe
        # Body already consumed; handlers receive a replay closure via argument.
        await handler(scope, receive, send, context, body)

    app.bind = bind  # type: ignore[attr-defined]
    app.plane = plane  # type: ignore[attr-defined]
    app.routes = routes  # type: ignore[attr-defined]
    return app
