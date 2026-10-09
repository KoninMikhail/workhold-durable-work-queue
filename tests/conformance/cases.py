"""Phase 3.1 skeleton conformance case catalog (OpenAPI-linked fixtures)."""

from __future__ import annotations

from tests.conformance.harness import RequestCase

COMPLETE_OPERATION_ID = "completeClaim"
RESERVED_COMPLETE_FIELD = "events"

_CLAIM_ID = "00000000-0000-4000-8000-000000000001"
_QUEUE_NAME = "skeleton-demo"
_BEARER = "Bearer secret-token"
_CLAIM_TOKEN = "claim-secret"


def build_phase_31_skeleton_cases() -> list[RequestCase]:
    """Return the fixed Phase 3.1 black-box skeleton case set.

    Capabilities, enqueue, claim, heartbeat, complete (without reserved
    ``events``), and one admin list operation. Every case is expected to
    surface as ``UNSUPPORTED_OPERATION`` against the contract stub.
    """

    auth = {"Authorization": _BEARER}
    claim_headers = {
        "Authorization": _BEARER,
        "X-Queue-Claim-Token": _CLAIM_TOKEN,
    }

    return [
        RequestCase(
            operation_id="getCapabilities",
            method="GET",
            path="/v1/capabilities",
            headers={},
            body=None,
        ),
        RequestCase(
            operation_id="enqueueTask",
            method="POST",
            path=f"/v1/queues/{_QUEUE_NAME}/tasks",
            headers={
                **auth,
                "Idempotency-Key": "skeleton-enqueue-1",
            },
            body={"payload": "skeleton", "priority": 0},
        ),
        RequestCase(
            operation_id="claimTasks",
            method="POST",
            path="/v1/claims",
            headers=auth,
            body={
                "queues": [_QUEUE_NAME],
                "max_tasks": 1,
                "lease_seconds": 30,
                "wait_seconds": 0,
                "worker_id": "skeleton-worker",
            },
        ),
        RequestCase(
            operation_id="heartbeatClaim",
            method="POST",
            path=f"/v1/claims/{_CLAIM_ID}:heartbeat",
            headers=claim_headers,
            body={"generation": 1, "lease_seconds": 30},
        ),
        RequestCase(
            operation_id=COMPLETE_OPERATION_ID,
            method="POST",
            path=f"/v1/claims/{_CLAIM_ID}:complete",
            headers=claim_headers,
            # Reserved additive field ``events`` is intentionally omitted.
            body={"generation": 1, "spawn": []},
        ),
        RequestCase(
            operation_id="listQueues",
            method="GET",
            path="/admin/v1/queues",
            headers=auth,
            body=None,
        ),
    ]
