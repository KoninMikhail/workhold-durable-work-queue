"""Regression: repairRegistryEntry success must increment break-glass metrics.

The JSON body field ``registry`` must not shadow the metrics handle used after
commit (``kernel_metrics`` rename in ea2ff89).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import Mock

from queue_service.api.admin_break_glass import build_admin_break_glass_handler
from queue_service.api.security import RequestContext
from queue_service.observability.metrics import KernelMetrics
from queue_service.operations.break_glass import BreakGlassResult
from queue_service.security.authorization import AuthorizationContext, Operation
from queue_service.security.principals import Principal, ServiceRole


def test_repair_registry_entry_increments_break_glass_total_with_registry_body(
    monkeypatch: Any,
) -> None:
    """Success path records queue_break_glass_total even when body has registry."""

    def fake_repair(
        _session: object,
        *,
        queue_name: str,
        actor_id: str,
        request_id: str,
        ack: object,
        registry: str,
        entry_id: int,
        extend_seconds: int,
        acknowledge_duplicate_window: bool,
    ) -> BreakGlassResult:
        _ = (queue_name, actor_id, request_id, ack, entry_id, extend_seconds)
        _ = acknowledge_duplicate_window
        assert registry == "enqueue_dedup"
        return BreakGlassResult(
            operation="repairRegistryEntry",
            queue="orders",
            target_id="7",
            outcome="repaired",
            generation=1,
        )

    monkeypatch.setattr(
        "queue_service.api.admin_break_glass.repair_registry_entry",
        fake_repair,
    )

    session = Mock()
    session_factory = Mock(return_value=session)
    metrics = KernelMetrics(process_role="admin")
    handler = build_admin_break_glass_handler(
        session_factory=session_factory,
        engine=Mock(),
        rate_gate=Mock(),
        payload_retention_policy=Mock(),
        metrics=metrics,
    )

    principal = Principal(principal_id="admin-1", role=ServiceRole.ADMIN)
    context = RequestContext(
        request_id="req-1",
        principal=principal,
        authorization=AuthorizationContext(
            principal=principal,
            operation=Operation.REPAIR_REGISTRY_ENTRY,
            queue_name="orders",
        ),
        path_params={"queue_name": "orders"},
        operation=Operation.REPAIR_REGISTRY_ENTRY,
    )
    body = json.dumps(
        {
            "reason": "r",
            "incident_reference": "i",
            "risk_acknowledged": True,
            "entry_id": 7,
            "registry": "enqueue_dedup",
        }
    ).encode("utf-8")

    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    asyncio.run(
        handler(
            {"type": "http"},
            receive,
            send,
            context,
            body,
        )
    )

    start = next(m for m in messages if m["type"] == "http.response.start")
    assert start["status"] == 200

    hits = [
        s
        for s in metrics.snapshot()
        if s.name == "queue_break_glass_total"
    ]
    assert len(hits) == 1
    assert hits[0].labels["operation"] == "repairRegistryEntry"
    assert hits[0].labels["result"] == "repaired"
    assert hits[0].labels["queue"] == "orders"
    assert "reason" not in hits[0].labels
    assert "incident_reference" not in hits[0].labels
    session.commit.assert_called_once()
    session.close.assert_called_once()
