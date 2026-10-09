"""Bounded lazy async pagination for Observer/Admin cursor-list operations.

Mirrors :mod:`workhold_admin.pagination` with async fetch callables.
Checks task cancellation before each page request so a cancelled consumer
issues zero subsequent requests.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, TypeVar

from workhold_admin.async_client import AsyncAdminClient, AsyncObserverClient
from workhold_admin.models import (
    PAGE_LIMIT_DEFAULT,
    AttemptPage,
    AuditPage,
    DeadLetterPage,
    QueuePage,
    TaskPage,
)
from workhold_admin.pagination import CursorPage, _effective_max_pages, _validate_bounds

P = TypeVar("P", bound=CursorPage)
T = TypeVar("T")


async def _raise_if_cancelled() -> None:
    """Honor cancellation before issuing another page request."""

    await asyncio.sleep(0)
    task = asyncio.current_task()
    if task is not None and task.cancelled():
        raise asyncio.CancelledError


@dataclass(slots=True)
class AsyncBoundedPageIterator(Generic[P]):
    """Async lazy page iterator with explicit page/item ceilings."""

    _fetch: Callable[[str | None], Awaitable[P]]
    _max_pages: int | None
    _max_items: int | None
    _cursor: str | None = None
    _pages_fetched: int = 0
    _items_seen: int = 0
    _last_cursor: str | None = None
    _stopped: bool = False

    @property
    def last_cursor(self) -> str | None:
        return self._last_cursor

    @property
    def page_count(self) -> int:
        return self._pages_fetched

    @property
    def item_count(self) -> int:
        return self._items_seen

    def __aiter__(self) -> AsyncIterator[P]:
        return self

    async def __anext__(self) -> P:
        if self._stopped:
            raise StopAsyncIteration
        if self._max_pages is not None and self._pages_fetched >= self._max_pages:
            self._stopped = True
            raise StopAsyncIteration
        if self._max_items is not None and self._items_seen >= self._max_items:
            self._stopped = True
            raise StopAsyncIteration

        await _raise_if_cancelled()
        page = await self._fetch(self._cursor)
        self._pages_fetched += 1
        self._last_cursor = page.next_cursor
        self._items_seen += len(page.items)

        if page.next_cursor is None:
            self._stopped = True
        else:
            self._cursor = page.next_cursor
        return page


@dataclass(slots=True)
class AsyncBoundedItemIterator(Generic[T]):
    """Async lazy item iterator with the same ceilings and cancel checks."""

    _fetch: Callable[[str | None], Awaitable[CursorPage]]
    _max_pages: int | None
    _max_items: int | None
    _cursor: str | None = None
    _pages_fetched: int = 0
    _items_yielded: int = 0
    _last_cursor: str | None = None
    _pending: list[T] = field(default_factory=list)
    _pages_exhausted: bool = False
    _stopped: bool = False

    @property
    def last_cursor(self) -> str | None:
        return self._last_cursor

    @property
    def page_count(self) -> int:
        return self._pages_fetched

    @property
    def item_count(self) -> int:
        return self._items_yielded

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        while True:
            if self._pending:
                item = self._pending.pop(0)
                self._items_yielded += 1
                if self._max_items is not None and self._items_yielded >= self._max_items:
                    self._pending.clear()
                    self._stopped = True
                return item

            if self._stopped or self._pages_exhausted:
                self._stopped = True
                raise StopAsyncIteration
            if self._max_pages is not None and self._pages_fetched >= self._max_pages:
                self._stopped = True
                raise StopAsyncIteration
            if self._max_items is not None and self._items_yielded >= self._max_items:
                self._stopped = True
                raise StopAsyncIteration

            await _raise_if_cancelled()
            page = await self._fetch(self._cursor)
            self._pages_fetched += 1
            self._last_cursor = page.next_cursor
            self._pending = list(page.items)  # type: ignore[arg-type]
            if page.next_cursor is None:
                self._pages_exhausted = True
            else:
                self._cursor = page.next_cursor
            if not self._pending:
                continue


def async_bounded_page_iterator(
    fetch: Callable[[str | None], Awaitable[P]],
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[P]:
    _validate_bounds(max_pages=max_pages, max_items=max_items)
    return AsyncBoundedPageIterator(
        _fetch=fetch,
        _max_pages=_effective_max_pages(max_pages, max_items),
        _max_items=max_items,
        _cursor=cursor,
    )


def async_bounded_item_iterator(
    fetch: Callable[[str | None], Awaitable[CursorPage]],
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[T]:
    _validate_bounds(max_pages=max_pages, max_items=max_items)
    return AsyncBoundedItemIterator(
        _fetch=fetch,
        _max_pages=_effective_max_pages(max_pages, max_items),
        _max_items=max_items,
        _cursor=cursor,
    )


def iter_task_attempt_pages(
    client: AsyncObserverClient,
    task_id: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[AttemptPage]:
    return async_bounded_page_iterator(
        lambda cur: client.list_task_attempts(task_id, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_task_attempts(
    client: AsyncObserverClient,
    task_id: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[object]:
    return async_bounded_item_iterator(
        lambda cur: client.list_task_attempts(task_id, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_task_pages(
    client: AsyncObserverClient,
    queue_name: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[TaskPage]:
    return async_bounded_page_iterator(
        lambda cur: client.list_inspection_tasks(queue_name, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_tasks(
    client: AsyncObserverClient,
    queue_name: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[object]:
    return async_bounded_item_iterator(
        lambda cur: client.list_inspection_tasks(queue_name, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_attempt_pages(
    client: AsyncObserverClient,
    task_id: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[AttemptPage]:
    return async_bounded_page_iterator(
        lambda cur: client.list_inspection_attempts(
            task_id,
            time_from=time_from,
            time_to=time_to,
            cursor=cur,
            limit=limit,
        ),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_attempts(
    client: AsyncObserverClient,
    task_id: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[object]:
    return async_bounded_item_iterator(
        lambda cur: client.list_inspection_attempts(
            task_id,
            time_from=time_from,
            time_to=time_to,
            cursor=cur,
            limit=limit,
        ),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_dead_letter_pages(
    client: AsyncObserverClient,
    queue_name: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[DeadLetterPage]:
    return async_bounded_page_iterator(
        lambda cur: client.list_dead_letters(
            queue_name,
            time_from=time_from,
            time_to=time_to,
            cursor=cur,
            limit=limit,
        ),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_dead_letters(
    client: AsyncObserverClient,
    queue_name: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[object]:
    return async_bounded_item_iterator(
        lambda cur: client.list_dead_letters(
            queue_name,
            time_from=time_from,
            time_to=time_to,
            cursor=cur,
            limit=limit,
        ),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_queue_pages(
    client: AsyncAdminClient,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[QueuePage]:
    return async_bounded_page_iterator(
        lambda cur: client.list_queues(cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_queues(
    client: AsyncAdminClient,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[object]:
    return async_bounded_item_iterator(
        lambda cur: client.list_queues(cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_admin_audit_pages(
    client: AsyncAdminClient,
    *,
    time_from: datetime,
    time_to: datetime,
    queue_name: str | None = None,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedPageIterator[AuditPage]:
    return async_bounded_page_iterator(
        lambda cur: client.list_admin_audit(
            time_from=time_from,
            time_to=time_to,
            queue_name=queue_name,
            cursor=cur,
            limit=limit,
        ),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_admin_audit(
    client: AsyncAdminClient,
    *,
    time_from: datetime,
    time_to: datetime,
    queue_name: str | None = None,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> AsyncBoundedItemIterator[object]:
    return async_bounded_item_iterator(
        lambda cur: client.list_admin_audit(
            time_from=time_from,
            time_to=time_to,
            queue_name=queue_name,
            cursor=cur,
            limit=limit,
        ),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


__all__ = [
    "AsyncBoundedItemIterator",
    "AsyncBoundedPageIterator",
    "async_bounded_item_iterator",
    "async_bounded_page_iterator",
    "iter_admin_audit",
    "iter_admin_audit_pages",
    "iter_dead_letter_pages",
    "iter_dead_letters",
    "iter_inspection_attempt_pages",
    "iter_inspection_attempts",
    "iter_inspection_task_pages",
    "iter_inspection_tasks",
    "iter_queue_pages",
    "iter_queues",
    "iter_task_attempt_pages",
    "iter_task_attempts",
]
