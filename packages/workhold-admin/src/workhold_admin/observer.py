"""Read-only observer SDK adapter over OpenAPI observer-authorized surfaces."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.errors import MalformedResponseError
from _workhold_client_core.models import Task
from _workhold_client_core.transport import HttpJsonTransport, encode_path_segment

from workhold_admin.models import (
    AttemptPage,
    DeadLetterPage,
    MaintenanceStatus,
    PAGE_LIMIT_DEFAULT,
    Queue,
    StatsSnapshot,
    TaskPage,
    format_datetime,
    validate_cursor,
    validate_page_limit,
    validate_queue_name,
    validate_task_id,
    validate_time_range,
)


class ObserverClient:
    """Observer reads: capabilities, task/attempt inspection, queue, stats, maintenance.

    Public-plane operations use ``transport`` (application API base URL).
    Admin-plane operations use ``admin_transport`` (admin control-plane base URL).
    The SDK does not silently retry requests.
    """

    def __init__(
        self,
        transport: HttpJsonTransport,
        *,
        bearer_token: str,
        admin_transport: HttpJsonTransport,
    ) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        self._transport = transport
        self._admin_transport = admin_transport
        self._bearer_token = bearer_token

    def __repr__(self) -> str:
        return "ObserverClient(transport=..., admin_transport=..., bearer_token=<redacted>)"

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    def get_capabilities(self) -> Capabilities:
        """GET ``/v1/capabilities`` (OpenAPI ``getCapabilities``)."""

        response = self._transport.request(
            "GET",
            "/v1/capabilities",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Capabilities.parse, response.status_code, response.body)

    def get_task(self, task_id: str) -> Task:
        """GET ``/v1/tasks/{task_id}`` (OpenAPI ``getTask``, observer-authorized)."""

        wire_task_id = validate_task_id(task_id)
        path = f"/v1/tasks/{encode_path_segment(wire_task_id)}"
        response = self._transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Task.parse, response.status_code, response.body)

    def list_task_attempts(
        self,
        task_id: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> AttemptPage:
        """GET ``/v1/tasks/{task_id}/attempts`` (OpenAPI ``listTaskAttempts``)."""

        wire_task_id = validate_task_id(task_id)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        query = _page_query(limit=wire_limit, cursor=wire_cursor)
        path = f"/v1/tasks/{encode_path_segment(wire_task_id)}/attempts{query}"
        response = self._transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(AttemptPage.parse, response.status_code, response.body)

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

    def get_stats(self) -> StatsSnapshot:
        """GET ``/admin/v1/stats`` (OpenAPI ``getStats``; parameterless)."""

        response = self._admin_transport.request(
            "GET",
            "/admin/v1/stats",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(StatsSnapshot.parse, response.status_code, response.body)

    def get_maintenance_status(self) -> MaintenanceStatus:
        """GET ``/admin/v1/maintenance`` (OpenAPI ``getMaintenanceStatus``)."""

        response = self._admin_transport.request(
            "GET",
            "/admin/v1/maintenance",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(MaintenanceStatus.parse, response.status_code, response.body)

    def list_inspection_tasks(
        self,
        queue_name: str,
        *,
        cursor: str | None = None,
        limit: int = PAGE_LIMIT_DEFAULT,
    ) -> TaskPage:
        """GET ``/admin/v1/tasks`` (OpenAPI ``listInspectionTasks``)."""

        wire_name = validate_queue_name(queue_name)
        wire_limit = validate_page_limit(limit)
        wire_cursor = validate_cursor(cursor)
        params: dict[str, str] = {"queue_name": wire_name}
        params.update(_page_params(limit=wire_limit, cursor=wire_cursor))
        path = f"/admin/v1/tasks?{urlencode(params)}"
        response = self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(TaskPage.parse, response.status_code, response.body)

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
        response = self._admin_transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(AttemptPage.parse, response.status_code, response.body)

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
        response = self._admin_transport.request(
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


def _page_params(*, limit: int, cursor: str | None) -> dict[str, str]:
    params: dict[str, str] = {"limit": str(limit)}
    if cursor is not None:
        params["cursor"] = cursor
    return params


def _page_query(*, limit: int, cursor: str | None) -> str:
    params = _page_params(limit=limit, cursor=cursor)
    return f"?{urlencode(params)}"
