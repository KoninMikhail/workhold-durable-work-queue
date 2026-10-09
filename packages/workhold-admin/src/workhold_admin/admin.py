"""Admin SDK adapter: queue/policy/state, audit/maintenance, and recovery."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.errors import MalformedResponseError
from _workhold_client_core.transport import HttpJsonTransport, encode_path_segment

from workhold_admin.models import (
    AdminMutationResult,
    AttemptPage,
    AuditPage,
    BulkExecuteResult,
    BulkPreviewResult,
    ConfigVersion,
    DeadLetterPage,
    DeadLetterReplayResult,
    MaintenanceRunResult,
    MaintenanceStatus,
    PAGE_LIMIT_DEFAULT,
    PolicyVersion,
    Queue,
    QueuePage,
    QueueState,
    RetryPolicyDraft,
    StatsSnapshot,
    TaskPage,
    format_datetime,
    validate_batch_limit,
    validate_bulk_filters,
    validate_confirmation_token,
    validate_cursor,
    validate_idempotency_key,
    validate_known_queue_state,
    validate_page_limit,
    validate_queue_name,
    validate_reason,
    validate_start_index,
    validate_task_id,
    validate_time_range,
)
from workhold_admin.observer import ObserverClient, _page_query


class AdminClient:
    """Admin mutations: queue/policy/state, audit/maintenance, and recovery.

    Single-transport form ``AdminClient(transport, bearer_token=...)`` uses the
    same transport for admin-plane calls. Dual-transport form
    ``AdminClient(public, bearer_token=..., admin_transport=admin)`` routes
    admin-plane ops through ``admin_transport`` and composes ``ObserverClient``
    for maintenance status reads. Break-glass operations are intentionally
    absent. The SDK does not silently retry or auto-read stale optimistic
    mutations.

    Recovery: preview and execute are separate calls. Execute methods require an
    explicit ``BulkPreviewResult`` from a prior preview; the SDK never
    auto-previews or silently widens filters. Replay is at-least-once and may
    repeat external effects; models preserve immutable ``source_task_id`` lineage.
    """

    def __init__(
        self,
        transport: HttpJsonTransport,
        *,
        bearer_token: str,
        admin_transport: HttpJsonTransport | None = None,
    ) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        resolved_admin = transport if admin_transport is None else admin_transport
        self._observer = ObserverClient(
            transport,
            bearer_token=bearer_token,
            admin_transport=resolved_admin,
        )
        self._transport = transport
        self._admin_transport = resolved_admin
        self._bearer_token = bearer_token

    def __repr__(self) -> str:
        return "AdminClient(transport=..., admin_transport=..., bearer_token=<redacted>)"

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    def list_queues(
        self,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> QueuePage:
        """GET ``/admin/v1/queues`` (OpenAPI ``listQueues``)."""

        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        query = _page_query(limit=wire_limit, cursor=wire_cursor)
        response = self._admin_transport.request(
            "GET",
            f"/admin/v1/queues{query}",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(QueuePage.parse, response.status_code, response.body)

    def create_queue(
        self,
        name: str,
        *,
        initial_policy: RetryPolicyDraft,
        idempotency_key: str,
    ) -> AdminMutationResult:
        """POST ``/admin/v1/queues`` (OpenAPI ``createQueue``)."""

        wire_name = validate_queue_name(name)
        wire_key = validate_idempotency_key(idempotency_key)
        body = {
            "name": wire_name,
            "initial_policy": initial_policy.to_wire(),
        }
        response = self._admin_transport.request(
            "POST",
            "/admin/v1/queues",
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=body,
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    def get_queue(self, queue_name: str) -> Queue:
        """GET ``/admin/v1/queues/{queue_name}`` (OpenAPI ``getQueue``)."""

        wire_name = validate_queue_name(queue_name)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}"
        response = self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Queue.parse, response.status_code, response.body)

    def create_queue_policy(
        self,
        queue_name: str,
        policy: RetryPolicyDraft,
        *,
        idempotency_key: str,
    ) -> AdminMutationResult:
        """POST ``/admin/v1/queues/{queue_name}/policies`` (OpenAPI ``createQueuePolicy``)."""

        wire_name = validate_queue_name(queue_name)
        wire_key = validate_idempotency_key(idempotency_key)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}/policies"
        response = self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=policy.to_wire(),
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    def activate_queue_policy(
        self,
        queue_name: str,
        policy_version: PolicyVersion | int,
        *,
        expected_config_version: ConfigVersion | int,
        idempotency_key: str,
    ) -> AdminMutationResult:
        """POST ``/admin/v1/queues/{queue_name}/policies/{policy_version}:activate``."""

        wire_name = validate_queue_name(queue_name)
        wire_key = validate_idempotency_key(idempotency_key)
        wire_policy_version = _wire_policy_version(policy_version)
        wire_config_version = _wire_config_version(expected_config_version)
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_name)}/policies/"
            f"{wire_policy_version}:activate"
        )
        body = {"expected_config_version": wire_config_version}
        response = self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=body,
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    def set_queue_state(
        self,
        queue_name: str,
        state: QueueState | str,
        *,
        expected_config_version: ConfigVersion | int,
        idempotency_key: str,
    ) -> AdminMutationResult:
        """POST ``/admin/v1/queues/{queue_name}:set-state`` (OpenAPI ``setQueueState``)."""

        wire_name = validate_queue_name(queue_name)
        wire_key = validate_idempotency_key(idempotency_key)
        wire_config_version = _wire_config_version(expected_config_version)
        wire_state = _wire_queue_state(state)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}:set-state"
        body = {
            "expected_config_version": wire_config_version,
            "state": wire_state,
        }
        response = self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=body,
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    def get_maintenance_status(self) -> MaintenanceStatus:
        """GET ``/admin/v1/maintenance`` (OpenAPI ``getMaintenanceStatus``)."""

        return self._observer.get_maintenance_status()

    def get_capabilities(self) -> Capabilities:
        """GET ``/v1/capabilities`` (OpenAPI ``getCapabilities``; admin-authorized)."""

        return self._observer.get_capabilities()

    def get_stats(self) -> StatsSnapshot:
        """GET ``/admin/v1/stats`` (OpenAPI ``getStats``; parameterless)."""

        return self._observer.get_stats()

    def list_inspection_tasks(
        self,
        queue_name: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> TaskPage:
        """GET ``/admin/v1/tasks`` (OpenAPI ``listInspectionTasks``)."""

        return self._observer.list_inspection_tasks(
            queue_name, cursor=cursor, limit=limit
        )

    def list_inspection_attempts(
        self,
        task_id: str,
        *,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AttemptPage:
        """GET ``/admin/v1/attempts`` (OpenAPI ``listInspectionAttempts``)."""

        return self._observer.list_inspection_attempts(
            task_id,
            time_from=time_from,
            time_to=time_to,
            cursor=cursor,
            limit=limit,
        )

    def list_dead_letters(
        self,
        queue_name: str,
        *,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> DeadLetterPage:
        """GET ``/admin/v1/dead-letters`` (OpenAPI ``listDeadLetters``)."""

        return self._observer.list_dead_letters(
            queue_name,
            time_from=time_from,
            time_to=time_to,
            cursor=cursor,
            limit=limit,
        )

    def list_admin_audit(
        self,
        *,
        time_from: datetime,
        time_to: datetime,
        queue_name: str | None = None,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AuditPage:
        """GET ``/admin/v1/audit`` (OpenAPI ``listAdminAudit``)."""

        validate_time_range(time_from, time_to)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        params: dict[str, str] = {
            "from": format_datetime(time_from),
            "to": format_datetime(time_to),
        }
        if queue_name is not None:
            params["queue_name"] = validate_queue_name(queue_name)
        params.update(_page_params(limit=wire_limit, cursor=wire_cursor))
        path = f"/admin/v1/audit?{urlencode(params)}"
        response = self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(AuditPage.parse, response.status_code, response.body)

    def run_maintenance(self, *, idempotency_key: str) -> MaintenanceRunResult:
        """POST ``/admin/v1/maintenance:run`` (OpenAPI ``runMaintenance``)."""

        wire_key = validate_idempotency_key(idempotency_key)
        response = self._admin_transport.request(
            "POST",
            "/admin/v1/maintenance:run",
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=None,
        )
        return self._parse(MaintenanceRunResult.parse, response.status_code, response.body)

    def replay_dead_letter(
        self,
        queue_name: str,
        task_id: str,
        *,
        idempotency_key: str,
        reason: str,
    ) -> DeadLetterReplayResult:
        """POST ``.../dead-letters/{task_id}:replay`` (OpenAPI ``replayDeadLetter``).

        Creates a new ready task linked to the immutable dead-letter source.
        Never mutates terminal history. Replay is at-least-once; ``replayed`` may
        be true when the same admin idempotency key is retried within ADR017 TTL.
        """

        wire_queue = validate_queue_name(queue_name)
        wire_task_id = validate_task_id(task_id)
        wire_key = validate_idempotency_key(idempotency_key)
        wire_reason = validate_reason(reason)
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_queue)}"
            f"/dead-letters/{encode_path_segment(wire_task_id)}:replay"
        )
        response = self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body={"reason": wire_reason},
        )
        return self._parse(DeadLetterReplayResult.parse, response.status_code, response.body)

    def preview_bulk_replay(
        self,
        queue_name: str,
        *,
        filters: Mapping[str, str] | None = None,
    ) -> BulkPreviewResult:
        """POST ``.../bulk:preview-replay`` (OpenAPI ``previewBulkReplay``).

        Dry-run only: returns a principal-bound expiring confirmation token and
        bounded candidate summary without mutating tasks.
        """

        return self._preview_bulk(
            queue_name,
            operation_path="bulk:preview-replay",
            filters=filters,
        )

    def execute_bulk_replay(
        self,
        queue_name: str,
        *,
        preview: BulkPreviewResult,
        idempotency_key: str,
        reason: str,
        filters: Mapping[str, str],
        start_index: int = 0,
        batch_limit: int | None = None,
    ) -> BulkExecuteResult:
        """POST ``.../bulk:execute-replay`` (OpenAPI ``executeBulkReplay``).

        Requires ``preview`` from ``preview_bulk_replay`` with matching queue,
        operation and caller-supplied ``filters``. Partial batches are explicit
        via ``partial`` and ``next_start_index``; replay remains at-least-once.
        """

        return self._execute_bulk(
            queue_name,
            operation_path="bulk:execute-replay",
            expected_operation="bulk_replay",
            preview=preview,
            idempotency_key=idempotency_key,
            reason=reason,
            filters=filters,
            start_index=start_index,
            batch_limit=batch_limit,
        )

    def preview_bulk_cancel(
        self,
        queue_name: str,
        *,
        filters: Mapping[str, str] | None = None,
    ) -> BulkPreviewResult:
        """POST ``.../bulk:preview-cancel`` (OpenAPI ``previewBulkCancel``).

        Dry-run only: returns a confirmation token for bounded bulk cancellation.
        """

        return self._preview_bulk(
            queue_name,
            operation_path="bulk:preview-cancel",
            filters=filters,
        )

    def execute_bulk_cancel(
        self,
        queue_name: str,
        *,
        preview: BulkPreviewResult,
        reason: str,
        filters: Mapping[str, str],
        start_index: int = 0,
        batch_limit: int | None = None,
    ) -> BulkExecuteResult:
        """POST ``.../bulk:execute-cancel`` (OpenAPI ``executeBulkCancel``).

        Requires ``preview`` from ``preview_bulk_cancel`` with matching queue,
        operation and ``filters``. Never creates spawn or delivery events.
        """

        return self._execute_bulk(
            queue_name,
            operation_path="bulk:execute-cancel",
            expected_operation="bulk_cancel",
            preview=preview,
            idempotency_key=None,
            reason=reason,
            filters=filters,
            start_index=start_index,
            batch_limit=batch_limit,
        )

    def _preview_bulk(
        self,
        queue_name: str,
        *,
        operation_path: str,
        filters: Mapping[str, str] | None,
    ) -> BulkPreviewResult:
        wire_queue = validate_queue_name(queue_name)
        body: dict[str, Any] = {}
        if filters is not None:
            body["filters"] = validate_bulk_filters(filters)
        path = f"/admin/v1/queues/{encode_path_segment(wire_queue)}/{operation_path}"
        response = self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers(),
            json_body=body,
        )
        result = self._parse(BulkPreviewResult.parse, response.status_code, response.body)
        if result.queue != wire_queue:
            raise ValueError("preview.queue does not match queue_name")
        return result

    def _execute_bulk(
        self,
        queue_name: str,
        *,
        operation_path: str,
        expected_operation: str,
        preview: BulkPreviewResult,
        idempotency_key: str | None,
        reason: str,
        filters: Mapping[str, str],
        start_index: int,
        batch_limit: int | None,
    ) -> BulkExecuteResult:
        wire_queue = validate_queue_name(queue_name)
        self._assert_execute_preview_binding(preview, wire_queue, expected_operation)
        wire_token = validate_confirmation_token(preview.confirmation_token)
        wire_reason = validate_reason(reason)
        wire_filters = validate_bulk_filters(filters)
        wire_start = validate_start_index(start_index)
        wire_batch_limit = validate_batch_limit(batch_limit)
        body: dict[str, Any] = {
            "confirmation_token": wire_token,
            "filters": wire_filters,
            "reason": wire_reason,
            "start_index": wire_start,
        }
        if wire_batch_limit is not None:
            body["batch_limit"] = wire_batch_limit
        extra_headers: dict[str, str] = {}
        if idempotency_key is not None:
            extra_headers["Idempotency-Key"] = validate_idempotency_key(idempotency_key)
        path = f"/admin/v1/queues/{encode_path_segment(wire_queue)}/{operation_path}"
        response = self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers(extra_headers or None),
            json_body=body,
        )
        return self._parse(BulkExecuteResult.parse, response.status_code, response.body)

    @staticmethod
    def _assert_execute_preview_binding(
        preview: BulkPreviewResult,
        queue_name: str,
        expected_operation: str,
    ) -> None:
        if preview.queue != queue_name:
            raise ValueError("preview.queue does not match queue_name")
        if preview.operation.value != expected_operation:
            raise ValueError(
                f"preview.operation must be {expected_operation!r}, "
                f"got {preview.operation.value!r}"
            )

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


def _wire_config_version(value: ConfigVersion | int) -> int:
    if isinstance(value, ConfigVersion):
        return value.to_wire()
    return ConfigVersion.parse(value).to_wire()


def _wire_policy_version(value: PolicyVersion | int) -> int:
    if isinstance(value, PolicyVersion):
        return value.to_wire()
    return PolicyVersion.parse(value).to_wire()


def _wire_queue_state(state: QueueState | str) -> str:
    if isinstance(state, QueueState):
        return validate_known_queue_state(state).value
    if not isinstance(state, str) or not state:
        raise ValueError("state must be a non-empty string")
    return validate_known_queue_state(QueueState.parse(state)).value


def _page_params(*, limit: int, cursor: str | None) -> dict[str, str]:
    params: dict[str, str] = {"limit": str(limit)}
    if cursor is not None:
        params["cursor"] = cursor
    return params
