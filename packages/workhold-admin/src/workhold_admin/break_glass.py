"""Emergency break-glass SDK adapter (OpenAPI BREAK_GLASS operations only)."""

from __future__ import annotations

from typing import Any, Final

from _workhold_client_core.errors import MalformedResponseError
from _workhold_client_core.transport import HttpJsonTransport, encode_path_segment

from workhold_admin.models import (
    BreakGlassCounterResult,
    BreakGlassMutationResult,
    BreakGlassReplayLimitResult,
    validate_acknowledge_duplicate_window,
    validate_break_glass_reason,
    validate_event_id,
    validate_extend_seconds,
    validate_failure_code,
    validate_incident_reference,
    validate_partition_name,
    validate_registry_entry_id,
    validate_replay_factor,
    validate_replay_ttl_seconds,
    validate_risk_acknowledged,
    validate_queue_name,
    validate_task_id,
)

_DEFAULT_FAILURE_CODE: Final[str] = "break_glass_force_dead_letter"
_DEFAULT_EXTEND_SECONDS: Final[int] = 86400
_DEFAULT_REGISTRY: Final[str] = "enqueue_dedup"
_DEFAULT_REPLAY_FACTOR: Final[float] = 2.0
_DEFAULT_REPLAY_TTL_SECONDS: Final[int] = 60


class BreakGlassClient:
    """Break-glass emergency mutations on the admin control plane.

    Requires deployment-issued short-lived JIT credentials with role
    ``BREAK_GLASS`` (Bearer token). The SDK does not parse, validate expiry,
    or mint JIT credentials; the server remains authoritative for credential
    lifetime and ``allowed_operations`` audience.

    Every mutation requires a non-empty ``reason``, ``incident_reference`` and
    literal ``risk_acknowledged=True``. Operations never return worker claim
    tokens. The SDK does not silently retry requests.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError(f"{cls.__name__} cannot be subclassed")

    def __init__(self, transport: HttpJsonTransport, *, bearer_token: str) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        self._transport = transport
        self._bearer_token = bearer_token

    def __repr__(self) -> str:
        return "BreakGlassClient(transport=..., bearer_token=<redacted>)"

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    def force_lease_expiry(
        self,
        queue_name: str,
        task_id: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassMutationResult:
        """POST ``.../tasks/{task_id}:force-lease-expiry`` (``forceLeaseExpiry``)."""

        wire_name = validate_queue_name(queue_name)
        wire_task_id = validate_task_id(task_id)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
            extra={"task_id": wire_task_id},
        )
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_name)}/tasks/"
            f"{encode_path_segment(wire_task_id)}:force-lease-expiry"
        )
        return self._post(path, body, BreakGlassMutationResult.parse)

    def force_delivery_reclaim(
        self,
        queue_name: str,
        event_id: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassMutationResult:
        """POST ``.../delivery-events/{event_id}:force-reclaim``."""

        wire_name = validate_queue_name(queue_name)
        wire_event_id = validate_event_id(event_id)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
        )
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_name)}/delivery-events/"
            f"{encode_path_segment(wire_event_id)}:force-reclaim"
        )
        return self._post(path, body, BreakGlassMutationResult.parse)

    def force_delivery_dead_letter(
        self,
        queue_name: str,
        event_id: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        failure_code: str = _DEFAULT_FAILURE_CODE,
    ) -> BreakGlassMutationResult:
        """POST ``.../delivery-events/{event_id}:force-dead-letter``."""

        wire_name = validate_queue_name(queue_name)
        wire_event_id = validate_event_id(event_id)
        wire_failure_code = validate_failure_code(failure_code)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
            extra={"failure_code": wire_failure_code},
        )
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_name)}/delivery-events/"
            f"{encode_path_segment(wire_event_id)}:force-dead-letter"
        )
        return self._post(path, body, BreakGlassMutationResult.parse)

    def reconcile_counters(
        self,
        queue_name: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassCounterResult:
        """POST ``/admin/v1/queues/{queue_name}:reconcile-counters``."""

        wire_name = validate_queue_name(queue_name)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
        )
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}:reconcile-counters"
        return self._post(path, body, BreakGlassCounterResult.parse)

    def raise_replay_limit(
        self,
        queue_name: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        factor: float = _DEFAULT_REPLAY_FACTOR,
        ttl_seconds: int = _DEFAULT_REPLAY_TTL_SECONDS,
    ) -> BreakGlassReplayLimitResult:
        """POST ``/admin/v1/queues/{queue_name}:raise-replay-limit``."""

        wire_name = validate_queue_name(queue_name)
        wire_factor = validate_replay_factor(factor)
        wire_ttl = validate_replay_ttl_seconds(ttl_seconds)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
            extra={"factor": wire_factor, "ttl_seconds": wire_ttl},
        )
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}:raise-replay-limit"
        return self._post(path, body, BreakGlassReplayLimitResult.parse)

    def drop_expired_partition(
        self,
        partition_name: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassMutationResult:
        """POST ``/admin/v1/partitions/{partition_name}:force-drop``."""

        wire_partition = validate_partition_name(partition_name)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
        )
        path = (
            f"/admin/v1/partitions/{encode_path_segment(wire_partition)}:force-drop"
        )
        return self._post(path, body, BreakGlassMutationResult.parse)

    def repair_registry_entry(
        self,
        queue_name: str,
        *,
        entry_id: int,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        acknowledge_duplicate_window: bool,
        extend_seconds: int = _DEFAULT_EXTEND_SECONDS,
        registry: str = _DEFAULT_REGISTRY,
    ) -> BreakGlassMutationResult:
        """POST ``/admin/v1/queues/{queue_name}/registry:repair``."""

        wire_name = validate_queue_name(queue_name)
        wire_entry_id = validate_registry_entry_id(entry_id)
        wire_extend = validate_extend_seconds(extend_seconds)
        wire_registry = _wire_registry(registry)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
            extra={
                "entry_id": wire_entry_id,
                "acknowledge_duplicate_window": validate_acknowledge_duplicate_window(
                    acknowledge_duplicate_window
                ),
                "extend_seconds": wire_extend,
                "registry": wire_registry,
            },
        )
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}/registry:repair"
        return self._post(path, body, BreakGlassMutationResult.parse)

    def _post(
        self,
        path: str,
        body: dict[str, Any],
        parser: Any,
    ) -> Any:
        response = self._transport.request(
            "POST",
            path,
            headers=self._auth_headers(),
            json_body=body,
        )
        return self._parse(parser, response.status_code, response.body)

    @staticmethod
    def _parse(parser: Any, status_code: int, body: object | None) -> Any:
        if body is None:
            raise MalformedResponseError(
                status_code=status_code,
                reason="empty success response body",
            )
        try:
            return parser(body)
        except ValueError as exc:
            raise MalformedResponseError(
                status_code=status_code,
                reason=str(exc),
            ) from exc


def _ack_body(
    *,
    reason: str,
    incident_reference: str,
    risk_acknowledged: bool,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "reason": validate_break_glass_reason(reason),
        "incident_reference": validate_incident_reference(incident_reference),
        "risk_acknowledged": validate_risk_acknowledged(risk_acknowledged),
    }
    if extra:
        body.update(extra)
    return body


def _wire_registry(registry: str) -> str:
    if registry != _DEFAULT_REGISTRY:
        raise ValueError("registry must be enqueue_dedup")
    return registry
