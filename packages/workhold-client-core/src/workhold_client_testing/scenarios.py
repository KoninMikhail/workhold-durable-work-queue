"""Reusable scripted scenarios for common client behaviors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from _workhold_client_core.instrumentation import SyncInstrumentation
from _workhold_client_core.transport import TransportResponse

from workhold_client_testing.fixtures import (
    capabilities_body,
    claim_response_body,
    enqueue_response_body,
    page_body,
    task_body,
)
from workhold_client_testing.responses import (
    cancelled_error,
    lease_lost_error,
    ok,
    protocol_error,
    uncertain_transport_error,
)
from workhold_client_testing.transport import (
    RequestMatcher,
    ScriptStep,
    ScriptedScenario,
    StepOutcome,
)


def capabilities_ok() -> TransportResponse:
    return ok(capabilities_body())


def scenario_uncertain_enqueue() -> ScriptedScenario:
    return ScriptedScenario(
        name="uncertain_enqueue",
        steps=(
            ScriptStep(
                outcome=uncertain_transport_error(reason="connection_reset"),
                match=RequestMatcher(method="POST", path_prefix="/v1/queues/"),
            ),
        ),
    )


def scenario_lease_lost_on_complete() -> ScriptedScenario:
    return ScriptedScenario(
        name="lease_lost_on_complete",
        steps=(
            ScriptStep(
                outcome=ok(claim_response_body()),
                match=RequestMatcher(method="POST", path="/v1/claims"),
            ),
            ScriptStep(
                outcome=lease_lost_error(),
                match=RequestMatcher(method="POST", path_prefix="/v1/claims/"),
            ),
        ),
    )


def scenario_cancellation() -> ScriptedScenario:
    return ScriptedScenario(name="cancellation", steps=(cancelled_error(),))


def scenario_pagination(
    *,
    pages: int = 2,
    item_factory: Callable[..., dict[str, Any]] = task_body,
) -> ScriptedScenario:
    steps: list[StepOutcome] = []
    for index in range(pages):
        next_cursor = None if index == pages - 1 else f"cursor-{index + 1}"
        steps.append(
            ok(
                page_body(
                    [item_factory(task_id=f"aaaaaaaa-aaaa-4aaa-8aaa-{index:012d}")],
                    next_cursor=next_cursor,
                )
            )
        )
    return ScriptedScenario(name="pagination", steps=tuple(steps))


def scenario_retry_budget(*, failures_before_success: int = 2) -> ScriptedScenario:
    steps: list[StepOutcome] = [
        protocol_error(
            429,
            code="resource_exhausted",
            message="slow down",
            retryable=True,
            retry_after_ms=25,
        )
        for _ in range(failures_before_success)
    ]
    steps.append(ok(enqueue_response_body()))
    return ScriptedScenario(name="retry_budget", steps=tuple(steps))


def scenario_instrumentation_enqueue() -> ScriptedScenario:
    return ScriptedScenario(
        name="instrumentation_enqueue",
        steps=(
            ScriptStep(
                outcome=capabilities_ok(),
                match=RequestMatcher(method="GET", path="/v1/capabilities"),
            ),
            ScriptStep(
                outcome=ok(enqueue_response_body()),
                match=RequestMatcher(method="POST", path_prefix="/v1/queues/"),
            ),
        ),
    )


class RecordingHooks:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def on_start(self, event: object) -> None:
        self.events.append(("start", event))

    def on_attempt(self, event: object) -> None:
        self.events.append(("attempt", event))

    def on_success(self, event: object) -> None:
        self.events.append(("success", event))

    def on_failure(self, event: object) -> None:
        self.events.append(("failure", event))

    def on_cancelled(self, event: object) -> None:
        self.events.append(("cancelled", event))


def recording_instrumentation() -> tuple[SyncInstrumentation, RecordingHooks]:
    hooks = RecordingHooks()
    return SyncInstrumentation(hooks=hooks), hooks


def combine_scenarios(*scenarios: ScriptedScenario) -> ScriptedScenario:
    steps: list[ScriptStep | StepOutcome] = []
    names: list[str] = []
    for scenario in scenarios:
        names.append(scenario.name)
        steps.extend(scenario.steps)
    return ScriptedScenario(name="+".join(names), steps=tuple(steps))


def as_steps(responses: Sequence[StepOutcome]) -> tuple[StepOutcome, ...]:
    return tuple(responses)
