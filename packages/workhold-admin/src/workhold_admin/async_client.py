"""Async admin/observer/break-glass client surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from _workhold_client_core.async_transport import AsyncTransport
from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.errors import MalformedResponseError
from _workhold_client_core.models import Task
from _workhold_client_core.transport import encode_path_segment

from workhold_admin.admin import (
    _wire_config_version,
    _wire_policy_version,
    _wire_queue_state,
)
from workhold_admin.break_glass import (
    _DEFAULT_EXTEND_SECONDS,
    _DEFAULT_FAILURE_CODE,
    _DEFAULT_REGISTRY,
    _DEFAULT_REPLAY_FACTOR,
    _DEFAULT_REPLAY_TTL_SECONDS,
    _ack_body,
    _wire_registry,
)
from workhold_admin.models import (
    AdminMutationResult,
    AttemptPage,
    AuditPage,
    BreakGlassCounterResult,
    BreakGlassMutationResult,
    BreakGlassReplayLimitResult,
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
    validate_acknowledge_duplicate_window,
    validate_batch_limit,
    validate_bulk_filters,
    validate_confirmation_token,
    validate_cursor,
    validate_event_id,
    validate_extend_seconds,
    validate_failure_code,
    validate_idempotency_key,
    validate_page_limit,
    validate_partition_name,
    validate_queue_name,
    validate_reason,
    validate_registry_entry_id,
    validate_replay_factor,
    validate_replay_ttl_seconds,
    validate_start_index,
    validate_task_id,
    validate_time_range,
)
from workhold_admin.observer import _page_params, _page_query


class AsyncObserverClient:
    """Async observer reads mirroring :class:`ObserverClient`."""

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        bearer_token: str,
        admin_transport: AsyncTransport,
        owns_transport: bool = False,
        owns_admin_transport: bool = False,
    ) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        self._transport = transport
        self._admin_transport = admin_transport
        self._bearer_token = bearer_token
        self._owns_transport = owns_transport
        self._owns_admin_transport = owns_admin_transport

    @classmethod
    def from_urls(
        cls,
        public_base_url: str,
        admin_base_url: str,
        *,
        bearer_token: str,
        timeout_s: float = 30.0,
    ) -> AsyncObserverClient:
        from _workhold_client_core.async_transport import HttpxAsyncTransport

        public = HttpxAsyncTransport(public_base_url, timeout_s=timeout_s)
        admin = HttpxAsyncTransport(admin_base_url, timeout_s=timeout_s)
        return cls(
            public,
            bearer_token=bearer_token,
            admin_transport=admin,
            owns_transport=True,
            owns_admin_transport=True,
        )

    async def __aenter__(self) -> AsyncObserverClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._owns_transport:
            await self._transport.aclose()
        if self._owns_admin_transport:
            await self._admin_transport.aclose()

    def __repr__(self) -> str:
        return (
            "AsyncObserverClient(transport=..., admin_transport=..., "
            "bearer_token=<redacted>)"
        )

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    async def get_capabilities(self) -> Capabilities:
        response = await self._transport.request(
            "GET",
            "/v1/capabilities",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Capabilities.parse, response.status_code, response.body)

    async def get_task(self, task_id: str) -> Task:
        wire_task_id = validate_task_id(task_id)
        path = f"/v1/tasks/{encode_path_segment(wire_task_id)}"
        response = await self._transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Task.parse, response.status_code, response.body)

    async def list_task_attempts(
        self,
        task_id: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AttemptPage:
        wire_task_id = validate_task_id(task_id)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        query = _page_query(limit=wire_limit, cursor=wire_cursor)
        path = f"/v1/tasks/{encode_path_segment(wire_task_id)}/attempts{query}"
        response = await self._transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(AttemptPage.parse, response.status_code, response.body)

    async def get_queue(self, queue_name: str) -> Queue:
        wire_name = validate_queue_name(queue_name)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}"
        response = await self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Queue.parse, response.status_code, response.body)

    async def get_stats(self) -> StatsSnapshot:
        """GET ``/admin/v1/stats`` (OpenAPI ``getStats``; parameterless)."""

        response = await self._admin_transport.request(
            "GET",
            "/admin/v1/stats",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(StatsSnapshot.parse, response.status_code, response.body)

    async def get_maintenance_status(self) -> MaintenanceStatus:
        response = await self._admin_transport.request(
            "GET",
            "/admin/v1/maintenance",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(MaintenanceStatus.parse, response.status_code, response.body)

    async def list_inspection_tasks(
        self,
        queue_name: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> TaskPage:
        wire_name = validate_queue_name(queue_name)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        params: dict[str, str] = {"queue_name": wire_name}
        params.update(_page_params(limit=wire_limit, cursor=wire_cursor))
        path = f"/admin/v1/tasks?{urlencode(params)}"
        response = await self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(TaskPage.parse, response.status_code, response.body)

    async def list_inspection_attempts(
        self,
        task_id: str,
        *,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AttemptPage:
        wire_task_id = validate_task_id(task_id)
        validate_time_range(time_from, time_to)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        params: dict[str, str] = {
            "task_id": wire_task_id,
            "from": format_datetime(time_from),
            "to": format_datetime(time_to),
        }
        params.update(_page_params(limit=wire_limit, cursor=wire_cursor))
        path = f"/admin/v1/attempts?{urlencode(params)}"
        response = await self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(AttemptPage.parse, response.status_code, response.body)

    async def list_dead_letters(
        self,
        queue_name: str,
        *,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> DeadLetterPage:
        wire_name = validate_queue_name(queue_name)
        validate_time_range(time_from, time_to)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        params: dict[str, str] = {
            "queue_name": wire_name,
            "from": format_datetime(time_from),
            "to": format_datetime(time_to),
        }
        params.update(_page_params(limit=wire_limit, cursor=wire_cursor))
        path = f"/admin/v1/dead-letters?{urlencode(params)}"
        response = await self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(DeadLetterPage.parse, response.status_code, response.body)

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


class AsyncAdminClient:
    """Async admin mutations mirroring :class:`AdminClient`.

    Single-transport form uses the same transport for admin-plane calls.
    Dual-transport form routes admin ops through ``admin_transport`` and
    composes :class:`AsyncObserverClient` for maintenance status reads.
    """

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        bearer_token: str,
        admin_transport: AsyncTransport | None = None,
        owns_transport: bool = False,
        owns_admin_transport: bool = False,
    ) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        resolved_admin = transport if admin_transport is None else admin_transport
        self._observer = AsyncObserverClient(
            transport,
            bearer_token=bearer_token,
            admin_transport=resolved_admin,
        )
        self._transport = transport
        self._admin_transport = resolved_admin
        self._bearer_token = bearer_token
        self._owns_transport = owns_transport
        self._owns_admin_transport = owns_admin_transport and admin_transport is not None

    @classmethod
    def from_url(
        cls,
        admin_base_url: str,
        *,
        bearer_token: str,
        timeout_s: float = 30.0,
    ) -> AsyncAdminClient:
        """Build a client for collocated public+admin deployments.

        Uses one owned transport for both planes. This does **not** support
        separate public and admin base URLs; use :meth:`from_urls` when the
        planes are split.
        """

        from _workhold_client_core.async_transport import HttpxAsyncTransport

        transport = HttpxAsyncTransport(admin_base_url, timeout_s=timeout_s)
        return cls(transport, bearer_token=bearer_token, owns_transport=True)

    @classmethod
    def from_urls(
        cls,
        public_base_url: str,
        admin_base_url: str,
        *,
        bearer_token: str,
        timeout_s: float = 30.0,
    ) -> AsyncAdminClient:
        """Build a dual-plane client with distinct owned public and admin transports.

        Public-plane ops (for example ``get_capabilities``) use
        ``public_base_url``; admin-plane ops use ``admin_base_url``. Even when
        the URLs are equal, two transport instances are created and each is
        closed once on :meth:`aclose`.
        """

        from _workhold_client_core.async_transport import HttpxAsyncTransport

        public = HttpxAsyncTransport(public_base_url, timeout_s=timeout_s)
        admin = HttpxAsyncTransport(admin_base_url, timeout_s=timeout_s)
        return cls(
            public,
            bearer_token=bearer_token,
            admin_transport=admin,
            owns_transport=True,
            owns_admin_transport=True,
        )

    async def __aenter__(self) -> AsyncAdminClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close each client-owned transport at most once; leave caller-owned alone.

        Idempotent: a second call or ``aclose`` followed by ``__aexit__`` does
        not close again. If public and admin refer to the same owned object,
        that object is closed once.
        """

        to_close: list[AsyncTransport] = []
        if self._owns_transport:
            to_close.append(self._transport)
        if self._owns_admin_transport and not any(
            transport is self._admin_transport for transport in to_close
        ):
            to_close.append(self._admin_transport)
        self._owns_transport = False
        self._owns_admin_transport = False
        for transport in to_close:
            await transport.aclose()

    def __repr__(self) -> str:
        return (
            "AsyncAdminClient(transport=..., admin_transport=..., "
            "bearer_token=<redacted>)"
        )

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    async def list_queues(
        self,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> QueuePage:
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        query = _page_query(limit=wire_limit, cursor=wire_cursor)
        response = await self._admin_transport.request(
            "GET",
            f"/admin/v1/queues{query}",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(QueuePage.parse, response.status_code, response.body)

    async def create_queue(
        self,
        name: str,
        *,
        initial_policy: RetryPolicyDraft,
        idempotency_key: str,
    ) -> AdminMutationResult:
        wire_name = validate_queue_name(name)
        wire_key = validate_idempotency_key(idempotency_key)
        body = {
            "name": wire_name,
            "initial_policy": initial_policy.to_wire(),
        }
        response = await self._admin_transport.request(
            "POST",
            "/admin/v1/queues",
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=body,
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    async def get_queue(self, queue_name: str) -> Queue:
        wire_name = validate_queue_name(queue_name)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}"
        response = await self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Queue.parse, response.status_code, response.body)

    async def create_queue_policy(
        self,
        queue_name: str,
        policy: RetryPolicyDraft,
        *,
        idempotency_key: str,
    ) -> AdminMutationResult:
        wire_name = validate_queue_name(queue_name)
        wire_key = validate_idempotency_key(idempotency_key)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}/policies"
        response = await self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=policy.to_wire(),
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    async def activate_queue_policy(
        self,
        queue_name: str,
        policy_version: PolicyVersion | int,
        *,
        expected_config_version: ConfigVersion | int,
        idempotency_key: str,
    ) -> AdminMutationResult:
        wire_name = validate_queue_name(queue_name)
        wire_key = validate_idempotency_key(idempotency_key)
        wire_policy_version = _wire_policy_version(policy_version)
        wire_config_version = _wire_config_version(expected_config_version)
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_name)}/policies/"
            f"{wire_policy_version}:activate"
        )
        body = {"expected_config_version": wire_config_version}
        response = await self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=body,
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    async def set_queue_state(
        self,
        queue_name: str,
        state: QueueState | str,
        *,
        expected_config_version: ConfigVersion | int,
        idempotency_key: str,
    ) -> AdminMutationResult:
        wire_name = validate_queue_name(queue_name)
        wire_key = validate_idempotency_key(idempotency_key)
        wire_config_version = _wire_config_version(expected_config_version)
        wire_state = _wire_queue_state(state)
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}:set-state"
        body = {
            "expected_config_version": wire_config_version,
            "state": wire_state,
        }
        response = await self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=body,
        )
        return self._parse(AdminMutationResult.parse, response.status_code, response.body)

    async def get_maintenance_status(self) -> MaintenanceStatus:
        """GET ``/admin/v1/maintenance`` (OpenAPI ``getMaintenanceStatus``)."""

        return await self._observer.get_maintenance_status()

    async def get_capabilities(self) -> Capabilities:
        """GET ``/v1/capabilities`` (OpenAPI ``getCapabilities``; admin-authorized)."""

        return await self._observer.get_capabilities()

    async def get_stats(self) -> StatsSnapshot:
        """GET ``/admin/v1/stats`` (OpenAPI ``getStats``; parameterless)."""

        return await self._observer.get_stats()

    async def list_inspection_tasks(
        self,
        queue_name: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> TaskPage:
        """GET ``/admin/v1/tasks`` (OpenAPI ``listInspectionTasks``)."""

        return await self._observer.list_inspection_tasks(
            queue_name, cursor=cursor, limit=limit
        )

    async def list_inspection_attempts(
        self,
        task_id: str,
        *,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AttemptPage:
        """GET ``/admin/v1/attempts`` (OpenAPI ``listInspectionAttempts``)."""

        return await self._observer.list_inspection_attempts(
            task_id,
            time_from=time_from,
            time_to=time_to,
            cursor=cursor,
            limit=limit,
        )

    async def list_dead_letters(
        self,
        queue_name: str,
        *,
        time_from: datetime,
        time_to: datetime,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> DeadLetterPage:
        """GET ``/admin/v1/dead-letters`` (OpenAPI ``listDeadLetters``)."""

        return await self._observer.list_dead_letters(
            queue_name,
            time_from=time_from,
            time_to=time_to,
            cursor=cursor,
            limit=limit,
        )

    async def list_admin_audit(
        self,
        *,
        time_from: datetime,
        time_to: datetime,
        queue_name: str | None = None,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AuditPage:
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
        response = await self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(AuditPage.parse, response.status_code, response.body)

    async def run_maintenance(self, *, idempotency_key: str) -> MaintenanceRunResult:
        wire_key = validate_idempotency_key(idempotency_key)
        response = await self._admin_transport.request(
            "POST",
            "/admin/v1/maintenance:run",
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body=None,
        )
        return self._parse(MaintenanceRunResult.parse, response.status_code, response.body)

    async def replay_dead_letter(
        self,
        queue_name: str,
        task_id: str,
        *,
        idempotency_key: str,
        reason: str,
    ) -> DeadLetterReplayResult:
        wire_queue = validate_queue_name(queue_name)
        wire_task_id = validate_task_id(task_id)
        wire_key = validate_idempotency_key(idempotency_key)
        wire_reason = validate_reason(reason)
        path = (
            f"/admin/v1/queues/{encode_path_segment(wire_queue)}"
            f"/dead-letters/{encode_path_segment(wire_task_id)}:replay"
        )
        response = await self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": wire_key}),
            json_body={"reason": wire_reason},
        )
        return self._parse(
            DeadLetterReplayResult.parse, response.status_code, response.body
        )

    async def preview_bulk_replay(
        self,
        queue_name: str,
        *,
        filters: Mapping[str, str] | None = None,
    ) -> BulkPreviewResult:
        return await self._preview_bulk(
            queue_name,
            operation_path="bulk:preview-replay",
            filters=filters,
        )

    async def execute_bulk_replay(
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
        return await self._execute_bulk(
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

    async def preview_bulk_cancel(
        self,
        queue_name: str,
        *,
        filters: Mapping[str, str] | None = None,
    ) -> BulkPreviewResult:
        return await self._preview_bulk(
            queue_name,
            operation_path="bulk:preview-cancel",
            filters=filters,
        )

    async def execute_bulk_cancel(
        self,
        queue_name: str,
        *,
        preview: BulkPreviewResult,
        reason: str,
        filters: Mapping[str, str],
        start_index: int = 0,
        batch_limit: int | None = None,
    ) -> BulkExecuteResult:
        return await self._execute_bulk(
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

    async def _preview_bulk(
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
        response = await self._admin_transport.request(
            "POST",
            path,
            headers=self._auth_headers(),
            json_body=body,
        )
        result = self._parse(BulkPreviewResult.parse, response.status_code, response.body)
        if result.queue != wire_queue:
            raise ValueError("preview.queue does not match queue_name")
        return result

    async def _execute_bulk(
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
        wire_reason = validate_reason(reason)
        wire_filters = validate_bulk_filters(filters)
        wire_start = validate_start_index(start_index)
        wire_batch_limit = validate_batch_limit(batch_limit)
        wire_token = validate_confirmation_token(preview.confirmation_token)
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
        response = await self._admin_transport.request(
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


class AsyncBreakGlassClient:
    """Async break-glass emergency mutations mirroring :class:`BreakGlassClient`."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError(f"{cls.__name__} cannot be subclassed")

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        bearer_token: str,
        owns_transport: bool = False,
    ) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        self._transport = transport
        self._bearer_token = bearer_token
        self._owns_transport = owns_transport

    @classmethod
    def from_url(
        cls,
        admin_base_url: str,
        *,
        bearer_token: str,
        timeout_s: float = 30.0,
    ) -> AsyncBreakGlassClient:
        from _workhold_client_core.async_transport import HttpxAsyncTransport

        transport = HttpxAsyncTransport(admin_base_url, timeout_s=timeout_s)
        return cls(transport, bearer_token=bearer_token, owns_transport=True)

    async def __aenter__(self) -> AsyncBreakGlassClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._owns_transport:
            await self._transport.aclose()

    def __repr__(self) -> str:
        return "AsyncBreakGlassClient(transport=..., bearer_token=<redacted>)"

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    async def force_lease_expiry(
        self,
        queue_name: str,
        task_id: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassMutationResult:
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
        return await self._post(path, body, BreakGlassMutationResult.parse)

    async def force_delivery_reclaim(
        self,
        queue_name: str,
        event_id: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassMutationResult:
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
        return await self._post(path, body, BreakGlassMutationResult.parse)

    async def force_delivery_dead_letter(
        self,
        queue_name: str,
        event_id: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        failure_code: str = _DEFAULT_FAILURE_CODE,
    ) -> BreakGlassMutationResult:
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
        return await self._post(path, body, BreakGlassMutationResult.parse)

    async def reconcile_counters(
        self,
        queue_name: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassCounterResult:
        wire_name = validate_queue_name(queue_name)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
        )
        path = f"/admin/v1/queues/{encode_path_segment(wire_name)}:reconcile-counters"
        return await self._post(path, body, BreakGlassCounterResult.parse)

    async def raise_replay_limit(
        self,
        queue_name: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
        factor: float = _DEFAULT_REPLAY_FACTOR,
        ttl_seconds: int = _DEFAULT_REPLAY_TTL_SECONDS,
    ) -> BreakGlassReplayLimitResult:
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
        return await self._post(path, body, BreakGlassReplayLimitResult.parse)

    async def drop_expired_partition(
        self,
        partition_name: str,
        *,
        reason: str,
        incident_reference: str,
        risk_acknowledged: bool,
    ) -> BreakGlassMutationResult:
        wire_partition = validate_partition_name(partition_name)
        body = _ack_body(
            reason=reason,
            incident_reference=incident_reference,
            risk_acknowledged=risk_acknowledged,
        )
        path = (
            f"/admin/v1/partitions/{encode_path_segment(wire_partition)}:force-drop"
        )
        return await self._post(path, body, BreakGlassMutationResult.parse)

    async def repair_registry_entry(
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
        return await self._post(path, body, BreakGlassMutationResult.parse)

    async def _post(
        self,
        path: str,
        body: dict[str, Any],
        parser: Any,
    ) -> Any:
        response = await self._transport.request(
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


__all__ = [
    "AsyncAdminClient",
    "AsyncBreakGlassClient",
    "AsyncObserverClient",
]
