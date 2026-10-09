"""Bounded async pagination helpers (SDK-10), including cancellation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from workhold_admin.async_pagination import (
    async_bounded_item_iterator,
    async_bounded_page_iterator,
    iter_task_attempt_pages,
    iter_task_attempts,
)


@dataclass(frozen=True, slots=True)
class _FakePage:
    items: tuple[str, ...]
    next_cursor: str | None


@pytest.mark.asyncio
async def test_async_page_iterator_respects_max_pages() -> None:
    calls: list[str | None] = []

    async def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        if cursor is None:
            return _FakePage(("a",), "c1")
        if cursor == "c1":
            return _FakePage(("b",), "c2")
        return _FakePage(("c",), None)

    it = async_bounded_page_iterator(fetch, max_pages=2)
    pages = [page async for page in it]
    assert [p.items for p in pages] == [("a",), ("b",)]
    assert calls == [None, "c1"]
    assert it.page_count == 2
    assert it.last_cursor == "c2"


@pytest.mark.asyncio
async def test_async_item_iterator_respects_max_items() -> None:
    calls: list[str | None] = []

    async def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        if cursor is None:
            return _FakePage(("a", "b"), "c1")
        return _FakePage(("c", "d"), None)

    it = async_bounded_item_iterator(fetch, max_items=3)
    items = [item async for item in it]
    assert items == ["a", "b", "c"]
    assert calls == [None, "c1"]
    assert it.item_count == 3


@pytest.mark.asyncio
async def test_async_cancellation_before_next_page_skips_request() -> None:
    gate = asyncio.Event()
    calls: list[str | None] = []

    async def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        if cursor is None:
            return _FakePage(("a",), "c1")
        await gate.wait()
        return _FakePage(("b",), None)

    it = async_bounded_page_iterator(fetch, max_pages=5)

    async def consume() -> list[_FakePage]:
        out: list[_FakePage] = []
        async for page in it:
            out.append(page)
            if len(out) == 1:
                await asyncio.sleep(0)
        return out

    task = asyncio.create_task(consume())
    while len(calls) < 1:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Allow a stray second fetch to complete if it had already started.
    gate.set()
    await asyncio.sleep(0)
    assert calls == [None]
    assert it.page_count == 1


@pytest.mark.asyncio
async def test_async_item_cancellation_before_second_page() -> None:
    gate = asyncio.Event()
    calls: list[str | None] = []

    async def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        if cursor is None:
            return _FakePage(("a",), "c1")
        await gate.wait()
        return _FakePage(("b",), None)

    it = async_bounded_item_iterator(fetch, max_pages=5)

    async def consume() -> list[str]:
        out: list[str] = []
        async for item in it:
            out.append(item)
            if len(out) == 1:
                await asyncio.sleep(0)
        return out

    task = asyncio.create_task(consume())
    while len(calls) < 1:
        await asyncio.sleep(0)
    # Finish draining first page then cancel before second fetch checkpoint.
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    gate.set()
    await asyncio.sleep(0)
    assert calls == [None]


@pytest.mark.asyncio
async def test_async_item_iterator_max_items_only_stops_on_empty_cursor_pages() -> None:
    calls: list[str | None] = []

    async def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        return _FakePage((), "still-going")

    it = async_bounded_item_iterator(fetch, max_items=10)
    items = [item async for item in it]
    assert items == []
    assert len(calls) == 10
    assert it.page_count == 10
    assert it.last_cursor == "still-going"


@pytest.mark.asyncio
async def test_async_wrappers_are_importable_and_bound() -> None:
    # Smoke: wrappers construct without contacting the network.
    class _Stub:
        async def list_task_attempts(self, *args: Any, **kwargs: Any) -> _FakePage:
            raise AssertionError("must not fetch until iterated")

    stub = _Stub()
    pages = iter_task_attempt_pages(stub, "t", max_pages=1)  # type: ignore[arg-type]
    items = iter_task_attempts(stub, "t", max_items=1)  # type: ignore[arg-type]
    assert pages.page_count == 0
    assert items.item_count == 0
