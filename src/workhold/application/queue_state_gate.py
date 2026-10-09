"""Shared queue-state operation gate for producer/worker adapters."""

from __future__ import annotations

from workhold.domain.queue_control import (
    OperationGateOutcome,
    QueueOperation,
    QueueState,
    evaluate_operation_gate,
)

__all__ = [
    "OperationGateOutcome",
    "QueueOperation",
    "QueueState",
    "evaluate_queue_state_gate",
]


def evaluate_queue_state_gate(
    state: QueueState,
    operation: QueueOperation,
) -> OperationGateOutcome:
    """Return the runtime-semantics gate outcome for ``state`` × ``operation``.

    Downstream enqueue, spawn, claim, lease, cancel, and delivery adapters share
    this function so paused-empty claim and drain-rejected external enqueue stay
    consistent with ``runtime-semantics.md``.
    """

    return evaluate_operation_gate(state, operation)
