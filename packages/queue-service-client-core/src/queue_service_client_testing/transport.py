"""Scripted sync/async transports with safe request recording."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeAlias

from _queue_service_client_core.errors import RequestCancelledError
from _queue_service_client_core.transport import TransportResponse

from queue_service_client_testing._secrets import (
    RedactedAssertionError,
    redact_failure_text,
    safe_headers,
    safe_json,
)

OutcomeFactory: TypeAlias = Callable[["RecordedRequest"], "StepOutcome"]
StepOutcome: TypeAlias = (
    TransportResponse | BaseException | type[BaseException] | OutcomeFactory
)


@dataclass(frozen=True, slots=True)
class RequestMatcher:
    method: str | None = None
    path: str | None = None
    path_prefix: str | None = None
    query_keys: frozenset[str] | None = None

    def matches(
        self,
        *,
        method: str,
        path: str,
        query: Mapping[str, str] | None,
    ) -> bool:
        if self.method is not None and method.upper() != self.method.upper():
            return False
        if self.path is not None and path != self.path:
            return False
        if self.path_prefix is not None and not path.startswith(self.path_prefix):
            return False
        if self.query_keys is not None:
            present = frozenset((query or {}).keys())
            if present != self.query_keys:
                return False
        return True


@dataclass(frozen=True, slots=True)
class ScriptStep:
    outcome: StepOutcome
    match: RequestMatcher | None = None


@dataclass(slots=True)
class RecordedRequest:
    method: str
    path: str
    query_keys: tuple[str, ...]
    header_names: tuple[str, ...]
    has_json_body: bool
    _headers: dict[str, str] = field(repr=False)
    _json_body: object | None = field(repr=False)
    _query: dict[str, str] | None = field(repr=False)

    def headers(self, *, include_secrets: bool = False) -> Mapping[str, str]:
        if include_secrets:
            return dict(self._headers)
        return safe_headers(self._headers)

    def json_body(self, *, include_secrets: bool = False) -> object | None:
        if include_secrets:
            return self._json_body
        if self._json_body is None:
            return None
        return safe_json(self._json_body)

    def query(self, *, include_secrets: bool = False) -> Mapping[str, str] | None:
        if self._query is None:
            return None
        if include_secrets:
            return dict(self._query)
        return {key: "[REDACTED]" for key in self._query}

    def __repr__(self) -> str:
        return (
            "RecordedRequest("
            f"method={self.method!r}, path={self.path!r}, "
            f"query_keys={self.query_keys!r}, header_names={self.header_names!r}, "
            f"has_json_body={self.has_json_body!r})"
        )


def _normalize_steps(steps: Sequence[ScriptStep | StepOutcome]) -> list[ScriptStep]:
    out: list[ScriptStep] = []
    for step in steps:
        if isinstance(step, ScriptStep):
            out.append(step)
        else:
            out.append(ScriptStep(outcome=step))
    return out


def _resolve_outcome(
    outcome: StepOutcome, recorded: RecordedRequest
) -> TransportResponse | BaseException:
    current: object = outcome
    if callable(current) and not isinstance(current, type):
        current = current(recorded)
    if isinstance(current, type) and issubclass(current, BaseException):
        return current()
    if isinstance(current, BaseException):
        return current
    if isinstance(current, TransportResponse):
        return current
    raise RedactedAssertionError(
        f"script step produced unsupported outcome type {type(current)!r}"
    )


class _ScriptEngine:
    def __init__(self, steps: Sequence[ScriptStep | StepOutcome]) -> None:
        self._remaining = _normalize_steps(steps)
        self.calls: list[RecordedRequest] = []

    def record(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None,
        json_body: object | None,
        query: Mapping[str, str] | None,
    ) -> RecordedRequest:
        hdrs = {str(k): str(v) for k, v in dict(headers or {}).items()}
        q = None if query is None else {str(k): str(v) for k, v in query.items()}
        recorded = RecordedRequest(
            method=method.upper(),
            path=path,
            query_keys=tuple(sorted((q or {}).keys())),
            header_names=tuple(sorted(hdrs.keys())),
            has_json_body=json_body is not None,
            _headers=hdrs,
            _json_body=json_body,
            _query=q,
        )
        self.calls.append(recorded)
        return recorded

    def next_outcome(self, recorded: RecordedRequest) -> TransportResponse | BaseException:
        if not self._remaining:
            raise RedactedAssertionError(
                f"script exhausted before request {recorded.method} {recorded.path}"
            )
        step = self._remaining.pop(0)
        if step.match is not None and not step.match.matches(
            method=recorded.method,
            path=recorded.path,
            query=recorded._query,
        ):
            raise RedactedAssertionError(
                "request did not match scripted matcher: "
                f"got {recorded.method} {recorded.path} "
                f"query_keys={recorded.query_keys}"
            )
        return _resolve_outcome(step.outcome, recorded)

    def assert_request_count(self, expected: int) -> None:
        actual = len(self.calls)
        if actual != expected:
            raise RedactedAssertionError(
                f"expected {expected} requests, observed {actual}: "
                f"{[f'{c.method} {c.path}' for c in self.calls]}"
            )

    def assert_request_order(self, expected: Sequence[tuple[str, str]]) -> None:
        actual = [(c.method, c.path) for c in self.calls]
        normalized = [(m.upper(), p) for m, p in expected]
        if actual != normalized:
            raise RedactedAssertionError(
                f"request order mismatch: expected {normalized!r}, got {actual!r}"
            )

    def remaining_steps(self) -> int:
        return len(self._remaining)


class ScriptedSyncTransport:
    def __init__(self, steps: Sequence[ScriptStep | StepOutcome]) -> None:
        self._engine = _ScriptEngine(steps)

    @property
    def calls(self) -> list[RecordedRequest]:
        return self._engine.calls

    def assert_request_count(self, expected: int) -> None:
        self._engine.assert_request_count(expected)

    def assert_request_order(self, expected: Sequence[tuple[str, str]]) -> None:
        self._engine.assert_request_order(expected)

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: object | None = None,
        query: Mapping[str, str] | None = None,
        expect_body: bool = True,
        read_timeout_s: float | None = None,
        total_timeout_s: float | None = None,
        cancellation: object | None = None,
    ) -> TransportResponse:
        del expect_body, read_timeout_s, total_timeout_s
        if cancellation is not None and getattr(cancellation, "is_set", lambda: False)():
            raise RequestCancelledError(reason="cancelled")
        recorded = self._engine.record(
            method, path, headers=headers, json_body=json_body, query=query
        )
        outcome = self._engine.next_outcome(recorded)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def __repr__(self) -> str:
        return (
            f"ScriptedSyncTransport(calls={len(self.calls)}, "
            f"remaining={self._engine.remaining_steps()})"
        )


class ScriptedAsyncTransport:
    def __init__(self, steps: Sequence[ScriptStep | StepOutcome]) -> None:
        self._engine = _ScriptEngine(steps)

    @property
    def calls(self) -> list[RecordedRequest]:
        return self._engine.calls

    def assert_request_count(self, expected: int) -> None:
        self._engine.assert_request_count(expected)

    def assert_request_order(self, expected: Sequence[tuple[str, str]]) -> None:
        self._engine.assert_request_order(expected)

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: object | None = None,
        query: Mapping[str, str] | None = None,
        expect_body: bool = True,
        read_timeout_s: float | None = None,
        total_timeout_s: float | None = None,
        cancellation: object | None = None,
    ) -> TransportResponse:
        del expect_body, read_timeout_s, total_timeout_s
        if isinstance(cancellation, asyncio.Event) and cancellation.is_set():
            raise asyncio.CancelledError()
        recorded = self._engine.record(
            method, path, headers=headers, json_body=json_body, query=query
        )
        outcome = self._engine.next_outcome(recorded)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def __repr__(self) -> str:
        return (
            f"ScriptedAsyncTransport(calls={len(self.calls)}, "
            f"remaining={self._engine.remaining_steps()})"
        )


@dataclass(frozen=True, slots=True)
class ScriptedScenario:
    name: str
    steps: tuple[ScriptStep | StepOutcome, ...]

    def sync_transport(self) -> ScriptedSyncTransport:
        return ScriptedSyncTransport(self.steps)

    def async_transport(self) -> ScriptedAsyncTransport:
        return ScriptedAsyncTransport(self.steps)


def explain_calls(calls: Sequence[RecordedRequest]) -> str:
    lines = [
        f"{idx}: {call.method} {call.path} query_keys={list(call.query_keys)}"
        for idx, call in enumerate(calls)
    ]
    return redact_failure_text("\n".join(lines))
