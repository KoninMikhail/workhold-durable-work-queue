"""Bounded lazy sync pagination for Observer/Admin cursor-list operations.

Requires an explicit positive ``max_pages`` and/or ``max_items`` ceiling.
Preserves caller filters and page size on every request, forwards server
cursors only (never synthesizes), and surfaces the last server cursor plus
page/item counts. Does not prefetch or retry cursor protocol errors.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, Protocol, TypeVar

from queue_service_admin.admin import AdminClient
from queue_service_admin.models import (
    PAGE_LIMIT_DEFAULT,
    AttemptPage,
    AuditPage,
    DeadLetterPage,
    QueuePage,
    TaskPage,
)
from queue_service_admin.observer import ObserverClient

P = TypeVar("P", bound="CursorPage")
T = TypeVar("T")


class CursorPage(Protocol):
    """Minimal page shape required by bounded iterators."""

    @property
    def items(self) -> tuple[object, ...]: ...

    @property
    def next_cursor(self) -> str | None: ...


def _effective_max_pages(max_pages: int | None, max_items: int | None) -> int | None:
    """When only ``max_items`` is set, bound page fetches to avoid empty-cursor spin."""

    if max_pages is not None:
        return max_pages
    if max_items is not None:
        return max_items
    return None


def _validate_bounds(*, max_pages: int | None, max_items: int | None) -> None:
    if max_pages is None and max_items is None:
        raise ValueError("at least one of max_pages or max_items is required")
    if max_pages is not None and (
        not isinstance(max_pages, int) or isinstance(max_pages, bool) or max_pages < 1
    ):
        raise ValueError("max_pages must be a positive integer")
    if max_items is not None and (
        not isinstance(max_items, int) or isinstance(max_items, bool) or max_items < 1
    ):
        raise ValueError("max_items must be a positive integer")


@dataclass(slots=True)
class BoundedPageIterator(Generic[P]):
    """Lazy page iterator that stops at ``max_pages`` and/or ``max_items``."""

    _fetch: Callable[[str | None], P]
    _max_pages: int | None
    _max_items: int | None
    _cursor: str | None = None
    _pages_fetched: int = 0
    _items_seen: int = 0
    _last_cursor: str | None = None
    _stopped: bool = False

    @property
    def last_cursor(self) -> str | None:
        """Most recent ``next_cursor`` returned by the server (may be ``None``)."""

        return self._last_cursor

    @property
    def page_count(self) -> int:
        return self._pages_fetched

    @property
    def item_count(self) -> int:
        return self._items_seen

    def __iter__(self) -> Iterator[P]:
        return self

    def __next__(self) -> P:
        if self._stopped:
            raise StopIteration
        if self._max_pages is not None and self._pages_fetched >= self._max_pages:
            self._stopped = True
            raise StopIteration
        if self._max_items is not None and self._items_seen >= self._max_items:
            self._stopped = True
            raise StopIteration

        page = self._fetch(self._cursor)
        self._pages_fetched += 1
        self._last_cursor = page.next_cursor
        self._items_seen += len(page.items)

        if page.next_cursor is None:
            self._stopped = True
        else:
            self._cursor = page.next_cursor
        return page


@dataclass(slots=True)
class BoundedItemIterator(Generic[T]):
    """Lazy item iterator over cursor pages with the same ceilings."""

    _fetch: Callable[[str | None], CursorPage]
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

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
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
                raise StopIteration
            if self._max_pages is not None and self._pages_fetched >= self._max_pages:
                self._stopped = True
                raise StopIteration
            if self._max_items is not None and self._items_yielded >= self._max_items:
                self._stopped = True
                raise StopIteration

            page = self._fetch(self._cursor)
            self._pages_fetched += 1
            self._last_cursor = page.next_cursor
            self._pending = list(page.items)  # type: ignore[arg-type]
            if page.next_cursor is None:
                self._pages_exhausted = True
            else:
                self._cursor = page.next_cursor
            if not self._pending:
                continue


def bounded_page_iterator(
    fetch: Callable[[str | None], P],
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    cursor: str | None = None,
) -> BoundedPageIterator[P]:
    _validate_bounds(max_pages=max_pages, max_items=max_items)
    return BoundedPageIterator(
        _fetch=fetch,
        _max_pages=_effective_max_pages(max_pages, max_items),
        _max_items=max_items,
        _cursor=cursor,
    )


def bounded_item_iterator(
    fetch: Callable[[str | None], CursorPage],
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    cursor: str | None = None,
) -> BoundedItemIterator[T]:
    _validate_bounds(max_pages=max_pages, max_items=max_items)
    return BoundedItemIterator(
        _fetch=fetch,
        _max_pages=_effective_max_pages(max_pages, max_items),
        _max_items=max_items,
        _cursor=cursor,
    )


def iter_task_attempt_pages(
    client: ObserverClient,
    task_id: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedPageIterator[AttemptPage]:
    return bounded_page_iterator(
        lambda cur: client.list_task_attempts(task_id, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_task_attempts(
    client: ObserverClient,
    task_id: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedItemIterator[object]:
    return bounded_item_iterator(
        lambda cur: client.list_task_attempts(task_id, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_task_pages(
    client: ObserverClient,
    queue_name: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedPageIterator[TaskPage]:
    return bounded_page_iterator(
        lambda cur: client.list_inspection_tasks(queue_name, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_tasks(
    client: ObserverClient,
    queue_name: str,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedItemIterator[object]:
    return bounded_item_iterator(
        lambda cur: client.list_inspection_tasks(queue_name, cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_inspection_attempt_pages(
    client: ObserverClient,
    task_id: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedPageIterator[AttemptPage]:
    return bounded_page_iterator(
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
    client: ObserverClient,
    task_id: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedItemIterator[object]:
    return bounded_item_iterator(
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
    client: ObserverClient,
    queue_name: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedPageIterator[DeadLetterPage]:
    return bounded_page_iterator(
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
    client: ObserverClient,
    queue_name: str,
    *,
    time_from: datetime,
    time_to: datetime,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedItemIterator[object]:
    return bounded_item_iterator(
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
    client: AdminClient,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedPageIterator[QueuePage]:
    return bounded_page_iterator(
        lambda cur: client.list_queues(cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_queues(
    client: AdminClient,
    *,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedItemIterator[object]:
    return bounded_item_iterator(
        lambda cur: client.list_queues(cursor=cur, limit=limit),
        max_pages=max_pages,
        max_items=max_items,
        cursor=cursor,
    )


def iter_admin_audit_pages(
    client: AdminClient,
    *,
    time_from: datetime,
    time_to: datetime,
    queue_name: str | None = None,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedPageIterator[AuditPage]:
    return bounded_page_iterator(
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
    client: AdminClient,
    *,
    time_from: datetime,
    time_to: datetime,
    queue_name: str | None = None,
    max_pages: int | None = None,
    max_items: int | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
    cursor: str | None = None,
) -> BoundedItemIterator[object]:
    return bounded_item_iterator(
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
    "BoundedItemIterator",
    "BoundedPageIterator",
    "CursorPage",
    "bounded_item_iterator",
    "bounded_page_iterator",
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
