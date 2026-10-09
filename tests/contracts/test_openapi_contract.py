"""Structural contract tests locking the frozen 03.1-01 plan OpenAPI catalog."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"

PUBLIC_OPERATIONS: dict[str, tuple[str, str]] = {
    "getCapabilities": ("get", "/v1/capabilities"),
    "enqueueTask": ("post", "/v1/queues/{queue_name}/tasks"),
    "resolveSubmission": ("post", "/v1/queues/{queue_name}/submissions:resolve"),
    "getTask": ("get", "/v1/tasks/{task_id}"),
    "cancelTask": ("post", "/v1/tasks/{task_id}:cancel"),
    "listTaskAttempts": ("get", "/v1/tasks/{task_id}/attempts"),
    "claimTasks": ("post", "/v1/claims"),
    "heartbeatClaim": ("post", "/v1/claims/{claim_id}:heartbeat"),
    "completeClaim": ("post", "/v1/claims/{claim_id}:complete"),
    "failClaim": ("post", "/v1/claims/{claim_id}:fail"),
    "acknowledgeClaimCancellation": ("post", "/v1/claims/{claim_id}:ack-cancel"),
}

ADMIN_OPERATIONS: dict[str, tuple[str, str]] = {
    "listQueues": ("get", "/admin/v1/queues"),
    "createQueue": ("post", "/admin/v1/queues"),
    "getQueue": ("get", "/admin/v1/queues/{queue_name}"),
    "createQueuePolicy": ("post", "/admin/v1/queues/{queue_name}/policies"),
    "activateQueuePolicy": (
        "post",
        "/admin/v1/queues/{queue_name}/policies/{policy_version}:activate",
    ),
    "setQueueState": ("post", "/admin/v1/queues/{queue_name}:set-state"),
    "getStats": ("get", "/admin/v1/stats"),
    "listInspectionTasks": ("get", "/admin/v1/tasks"),
    "listInspectionAttempts": ("get", "/admin/v1/attempts"),
    "listDeadLetters": ("get", "/admin/v1/dead-letters"),
    "listAdminAudit": ("get", "/admin/v1/audit"),
    "getMaintenanceStatus": ("get", "/admin/v1/maintenance"),
    "runMaintenance": ("post", "/admin/v1/maintenance:run"),
    "replayDeadLetter": (
        "post",
        "/admin/v1/queues/{queue_name}/dead-letters/{task_id}:replay",
    ),
    "previewBulkReplay": (
        "post",
        "/admin/v1/queues/{queue_name}/bulk:preview-replay",
    ),
    "executeBulkReplay": (
        "post",
        "/admin/v1/queues/{queue_name}/bulk:execute-replay",
    ),
    "previewBulkCancel": (
        "post",
        "/admin/v1/queues/{queue_name}/bulk:preview-cancel",
    ),
    "executeBulkCancel": (
        "post",
        "/admin/v1/queues/{queue_name}/bulk:execute-cancel",
    ),
    "forceLeaseExpiry": (
        "post",
        "/admin/v1/queues/{queue_name}/tasks/{task_id}:force-lease-expiry",
    ),
    "forceDeliveryReclaim": (
        "post",
        "/admin/v1/queues/{queue_name}/delivery-events/{event_id}:force-reclaim",
    ),
    "forceDeliveryDeadLetter": (
        "post",
        "/admin/v1/queues/{queue_name}/delivery-events/{event_id}:force-dead-letter",
    ),
    "reconcileCounters": (
        "post",
        "/admin/v1/queues/{queue_name}:reconcile-counters",
    ),
    "raiseReplayLimit": (
        "post",
        "/admin/v1/queues/{queue_name}:raise-replay-limit",
    ),
    "dropExpiredPartition": (
        "post",
        "/admin/v1/partitions/{partition_name}:force-drop",
    ),
    "repairRegistryEntry": (
        "post",
        "/admin/v1/queues/{queue_name}/registry:repair",
    ),
}

ALL_OPERATIONS = {**PUBLIC_OPERATIONS, **ADMIN_OPERATIONS}

LEASE_MUTATION_IDS = {
    "heartbeatClaim",
    "completeClaim",
    "failClaim",
    "acknowledgeClaimCancellation",
}

FORBIDDEN_OPERATION_IDS = {
    "ackCancelClaim",
    "transitionQueueState",
    "listAuditEvents",
    "triggerMaintenance",
}

ERROR_CODES = {
    "validation_failed",
    "payload_too_large",
    "idempotency_key_required",
    "idempotency_conflict",
    "queue_not_found",
    "queue_draining",
    "task_not_found",
    "claim_not_found",
    "lease_lost",
    "task_already_terminal",
    "cancel_race_lost",
    "config_version_conflict",
    "permission_denied",
    "unauthenticated",
    "resource_exhausted",
    "dependency_unavailable",
    "internal_error",
}

SECURITY_SCHEMES = {
    "ProducerBearer",
    "WorkerBearer",
    "ObserverBearer",
    "AdminBearer",
    "ClaimTokenHeader",
}

STATUS_MATRICES: dict[str, set[str]] = {
    "getCapabilities": {"200", "401", "403", "500"},
    "enqueueTask": {
        "200",
        "201",
        "400",
        "401",
        "403",
        "404",
        "409",
        "413",
        "429",
        "503",
        "500",
    },
    "resolveSubmission": {"200", "400", "401", "403", "404", "500"},
    "getTask": {"200", "400", "401", "403", "404", "500"},
    "cancelTask": {"200", "400", "401", "403", "404", "409", "500"},
    "listTaskAttempts": {"200", "400", "401", "403", "404", "500"},
    "claimTasks": {"200", "400", "401", "403", "429", "503", "500"},
    "heartbeatClaim": {"200", "400", "401", "403", "404", "409", "503", "500"},
    "acknowledgeClaimCancellation": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "503",
        "500",
    },
    "completeClaim": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "413",
        "429",
        "503",
        "500",
    },
    "failClaim": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "413",
        "429",
        "503",
        "500",
    },
    "listQueues": {"200", "400", "401", "403", "404", "500"},
    "getQueue": {"200", "400", "401", "403", "404", "500"},
    "getStats": {"200", "400", "401", "403", "500"},
    "listInspectionTasks": {"200", "400", "401", "403", "404", "500"},
    "listInspectionAttempts": {"200", "400", "401", "403", "404", "500"},
    "listDeadLetters": {"200", "400", "401", "403", "404", "500"},
    "listAdminAudit": {"200", "400", "401", "403", "404", "500"},
    "getMaintenanceStatus": {"200", "400", "401", "403", "404", "500"},
    "createQueue": {
        "200",
        "201",
        "400",
        "401",
        "403",
        "409",
        "413",
        "429",
        "503",
        "500",
    },
    "createQueuePolicy": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "412",
        "429",
        "503",
        "500",
    },
    "activateQueuePolicy": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "412",
        "429",
        "503",
        "500",
    },
    "setQueueState": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "412",
        "429",
        "503",
        "500",
    },
    "runMaintenance": {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "412",
        "429",
        "503",
        "500",
    },
}

SUCCESS_BODY_REFS: dict[str, str] = {
    "getCapabilities": "Capabilities",
    "enqueueTask": "EnqueueTaskResponse",
    "resolveSubmission": "ResolveSubmissionResponse",
    "getTask": "Task",
    "cancelTask": "CancelTaskResponse",
    "listTaskAttempts": "AttemptPage",
    "claimTasks": "ClaimResponse",
    "heartbeatClaim": "HeartbeatResponse",
    "completeClaim": "CompleteResult",
    "failClaim": "FailResult",
    "acknowledgeClaimCancellation": "AckCancelResult",
    "listQueues": "QueuePage",
    "createQueue": "AdminMutationResult",
    "getQueue": "Queue",
    "createQueuePolicy": "AdminMutationResult",
    "activateQueuePolicy": "AdminMutationResult",
    "setQueueState": "AdminMutationResult",
    "getStats": "StatsSnapshot",
    "listInspectionTasks": "TaskPage",
    "listInspectionAttempts": "AttemptPage",
    "listDeadLetters": "DeadLetterPage",
    "listAdminAudit": "AuditPage",
    "getMaintenanceStatus": "MaintenanceStatus",
    "runMaintenance": "MaintenanceRunResult",
}

IDEMPOTENCY_HEADER_OPS = {
    "enqueueTask",
    "createQueue",
    "createQueuePolicy",
    "activateQueuePolicy",
    "setQueueState",
    "runMaintenance",
}

CAPABILITIES_CONSTS: dict[str, Any] = {
    "protocol_major": 1,
    "protocol_version": "1.0",
    "schema_revision": "0001",
    "scheduling": True,
    "priority": True,
    "delivery_events": False,
    "batch_claim": False,
    "long_polling": True,
    "max_claim_tasks": 1,
    "max_wait_seconds": 20,
    "payload_runtime_max_bytes": 262144,
    "payload_hard_max_bytes": 1048576,
    "enqueue_dedup_ttl_seconds": 7776000,
    "enqueue_dedup_ttl_min_seconds": 2592000,
    "enqueue_dedup_ttl_max_seconds": 31536000,
    "terminal_replay_ttl_seconds": 604800,
    "terminal_replay_ttl_min_seconds": 86400,
    "terminal_replay_ttl_max_seconds": 2592000,
    "admin_replay_ttl_seconds": 2592000,
    "admin_replay_ttl_min_seconds": 604800,
    "admin_replay_ttl_max_seconds": 7776000,
}

FORBIDDEN_BUSINESS_KEYS = {"result", "parse_result", "business_result", "parser_v1"}

# Plan catalog notation: `?` alone marks an optional nullable field.
CATALOG_NULLABLE_FIELDS: dict[str, tuple[str, ...]] = {
    "Error": ("retry_after_ms",),
    "Task": (
        "payload",
        "current_claim",
        "terminal_at",
        "failure_code",
        "failure_detail",
        "source_task_id",
        "spawn_ordinal",
    ),
    "Attempt": ("ended_at", "failure_code", "failure_detail"),
    "EnqueueTaskRequest": ("available_at",),
    "SpawnRequest": ("available_at",),
    "FailRequest": ("failure_detail",),
    "CancelTaskRequest": ("reason",),
    "AttemptPage": ("next_cursor",),
    "QueuePage": ("next_cursor",),
    "AuditPage": ("next_cursor",),
    "AuditRecord": ("queue_id", "previous_config_version", "new_config_version"),
    "MaintenanceStatus": (
        "last_started_at",
        "last_succeeded_at",
        "premade_through",
        "retained_from",
        "last_error_code",
    ),
}

QUEUE_NAME_PATTERN = "^[a-z0-9][a-z0-9._-]*$"

PRIORITY_MIN = -32_768
PRIORITY_MAX = 32_767
PRIORITY_DESCRIPTION_MARKERS = (
    "Higher numeric value",
    "due candidates",
    "not named bands",
)


def _load() -> dict[str, Any]:
    assert OPENAPI_PATH.is_file(), f"missing {OPENAPI_PATH}"
    with OPENAPI_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _ops(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for path, item in spec["paths"].items():
        for method, operation in item.items():
            if method.startswith("x-") or not isinstance(operation, dict):
                continue
            if "operationId" not in operation:
                continue
            oid = operation["operationId"]
            assert oid not in found, oid
            found[oid] = {"path": path, "method": method, "operation": operation}
    return found


def _schema(spec: dict[str, Any], name: str) -> dict[str, Any]:
    return spec["components"]["schemas"][name]


def _resolve(spec: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    if "$ref" in node:
        return _schema(spec, node["$ref"].rsplit("/", 1)[-1])
    return node


def _body_name(response: dict[str, Any]) -> str | None:
    schema = response.get("content", {}).get("application/json", {}).get("schema", {})
    ref = schema.get("$ref")
    return ref.rsplit("/", 1)[-1] if ref else None


def _is_additive(schema: dict[str, Any]) -> bool:
    return schema.get("additionalProperties") is not False


def _is_closed(schema: dict[str, Any]) -> bool:
    return schema.get("additionalProperties") is False


def _has_idempotency(operation: dict[str, Any]) -> bool:
    for param in operation.get("parameters", []):
        if (
            param.get("in") == "header"
            and param.get("name") == "Idempotency-Key"
            and param.get("required") is True
        ):
            schema = param.get("schema", {})
            return (
                schema.get("type") == "string"
                and schema.get("minLength") == 1
                and schema.get("maxLength") == 256
            )
    return False


def _forbidden_keys(node: object, found: set[str] | None = None) -> set[str]:
    if found is None:
        found = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key in FORBIDDEN_BUSINESS_KEYS:
                found.add(key)
            _forbidden_keys(value, found)
    elif isinstance(node, list):
        for item in node:
            _forbidden_keys(item, found)
    return found


def test_openapi_31_and_schema_revision_0001() -> None:
    spec = _load()
    assert spec["openapi"] == "3.1.0"
    assert spec["x-queue-request-hard-max-bytes"] == 1_048_576
    caps = _schema(spec, "Capabilities")
    assert caps["properties"]["schema_revision"]["const"] == "0001"
    assert caps["properties"]["protocol_version"]["const"] == "1.0"


def test_exact_operation_catalog_only() -> None:
    ops = _ops(_load())
    assert set(ops) == set(ALL_OPERATIONS)
    assert not (set(ops) & FORBIDDEN_OPERATION_IDS)
    for oid, (method, path) in ALL_OPERATIONS.items():
        assert ops[oid]["method"] == method
        assert ops[oid]["path"] == path


def test_security_schemes_and_matrix() -> None:
    spec = _load()
    schemes = spec["components"]["securitySchemes"]
    assert set(schemes) == SECURITY_SCHEMES
    for name in ("ProducerBearer", "WorkerBearer", "ObserverBearer", "AdminBearer"):
        assert schemes[name]["type"] == "http"
        assert schemes[name]["scheme"] == "bearer"
    claim = schemes["ClaimTokenHeader"]
    assert claim["type"] == "apiKey"
    assert claim["in"] == "header"
    assert claim["name"] == "X-Queue-Claim-Token"

    ops = _ops(spec)
    caps = ops["getCapabilities"]["operation"]["security"]
    assert sorted(caps, key=lambda item: next(iter(item))) == sorted(
        [
            {"ProducerBearer": []},
            {"WorkerBearer": []},
            {"ObserverBearer": []},
            {"AdminBearer": []},
        ],
        key=lambda item: next(iter(item)),
    )
    for oid in ("enqueueTask", "resolveSubmission", "cancelTask"):
        assert ops[oid]["operation"]["security"] == [{"ProducerBearer": []}]
    assert ops["getTask"]["operation"]["security"] == [
        {"ProducerBearer": []},
        {"ObserverBearer": []},
    ]
    assert ops["listTaskAttempts"]["operation"]["security"] == [{"ObserverBearer": []}]
    assert ops["claimTasks"]["operation"]["security"] == [{"WorkerBearer": []}]
    for oid in LEASE_MUTATION_IDS:
        assert ops[oid]["operation"]["security"] == [
            {"WorkerBearer": [], "ClaimTokenHeader": []}
        ]
    assert ops["getStats"]["operation"]["security"] == [
        {"ObserverBearer": []},
        {"AdminBearer": []},
    ]
    for oid in (
        "listInspectionTasks",
        "listInspectionAttempts",
        "listDeadLetters",
    ):
        assert ops[oid]["operation"]["security"] == [
            {"ObserverBearer": []},
            {"AdminBearer": []},
        ]
    assert ops["getQueue"]["operation"]["security"] == [
        {"ObserverBearer": []},
        {"AdminBearer": []},
    ]
    assert ops["getMaintenanceStatus"]["operation"]["security"] == [
        {"ObserverBearer": []},
        {"AdminBearer": []},
    ]
    for oid in ADMIN_OPERATIONS:
        if oid in {
            "getStats",
            "listInspectionTasks",
            "listInspectionAttempts",
            "listDeadLetters",
            "getQueue",
            "getMaintenanceStatus",
        }:
            continue
        assert ops[oid]["operation"]["security"] == [{"AdminBearer": []}]


# Deployment-bound fields: advertised from claim_max_wait_seconds (0..20), not OpenAPI consts.
_CAPABILITIES_DEPLOYMENT_FIELDS = frozenset({"long_polling", "max_wait_seconds"})


def test_capabilities_full_const_catalog() -> None:
    caps = _schema(_load(), "Capabilities")
    assert set(caps["required"]) == set(CAPABILITIES_CONSTS)
    assert _is_additive(caps)
    for field, value in CAPABILITIES_CONSTS.items():
        if field in _CAPABILITIES_DEPLOYMENT_FIELDS:
            continue
        assert caps["properties"][field]["const"] == value, field
    long_polling = caps["properties"]["long_polling"]
    assert long_polling["type"] == "boolean"
    assert "const" not in long_polling
    max_wait = caps["properties"]["max_wait_seconds"]
    assert max_wait["type"] == "integer"
    assert max_wait["minimum"] == 0
    assert max_wait["maximum"] == 20
    assert "const" not in max_wait


def test_error_envelope_excludes_skeleton_code() -> None:
    error = _schema(_load(), "Error")
    assert set(error["required"]) == {
        "code",
        "message",
        "retryable",
        "request_id",
        "details",
    }
    assert "retry_after_ms" not in error["required"]
    assert _is_additive(error)
    props = error["properties"]
    assert set(props["code"]["enum"]) == ERROR_CODES
    assert "skeleton_operation_unsupported" not in props["code"]["enum"]
    assert props["message"]["minLength"] == 1
    assert props["message"]["maxLength"] == 1024
    assert props["retry_after_ms"]["minimum"] == 0
    assert props["retry_after_ms"]["format"] == "int64"
    assert props["retry_after_ms"].get("nullable") is True
    assert props["request_id"]["format"] == "uuid"
    assert props["details"]["maxProperties"] == 32
    assert props["details"].get("additionalProperties") is not False


def test_json_value_and_dual_payload_ceilings() -> None:
    spec = _load()
    json_value = _schema(spec, "JsonValue")
    types = {opt.get("type") for opt in json_value["oneOf"]}
    assert {"null", "boolean", "number", "string", "array", "object"} <= types
    array_opt = next(opt for opt in json_value["oneOf"] if opt.get("type") == "array")
    object_opt = next(opt for opt in json_value["oneOf"] if opt.get("type") == "object")
    assert array_opt["items"] == {"$ref": "#/components/schemas/JsonValue"}
    assert object_opt["additionalProperties"] == {
        "$ref": "#/components/schemas/JsonValue"
    }
    assert spec["x-queue-request-hard-max-bytes"] == 1_048_576
    payload = _schema(spec, "EnqueueTaskRequest")["properties"]["payload"]
    assert payload["$ref"] == "#/components/schemas/JsonValue"
    assert payload["x-queue-runtime-default-max-bytes"] == 262144
    assert payload["x-queue-hard-max-bytes"] == 1_048_576


def test_request_closed_response_additive() -> None:
    spec = _load()
    for name in (
        "EnqueueTaskRequest",
        "ResolveSubmissionRequest",
        "ClaimRequest",
        "HeartbeatRequest",
        "CompleteRequest",
        "FailRequest",
        "AckCancelRequest",
        "CancelTaskRequest",
        "SpawnRequest",
        "CreateQueueRequest",
        "CreatePolicyRequest",
        "ActivatePolicyRequest",
        "SetQueueStateRequest",
    ):
        assert _is_closed(_schema(spec, name)), name
    for name in (
        "Error",
        "Capabilities",
        "Task",
        "Attempt",
        "ClaimSummary",
        "ClaimGrant",
        "ClaimResponse",
        "EnqueueTaskResponse",
        "ResolveSubmissionResponse",
        "CancelTaskResponse",
        "AttemptPage",
        "HeartbeatResponse",
        "CompleteResult",
        "AckCancelResult",
        "Queue",
        "RetryPolicy",
        "QueuePage",
        "AdminMutationResult",
        "AuditRecord",
        "AuditPage",
        "MaintenanceStatus",
        "MaintenanceRunResult",
    ):
        assert _is_additive(_schema(spec, name)), name
    fail = _schema(spec, "FailResult")
    assert "oneOf" in fail and len(fail["oneOf"]) == 2
    for branch in fail["oneOf"]:
        assert _is_additive(_resolve(spec, branch))


def test_core_domain_shapes() -> None:
    spec = _load()
    task = _schema(spec, "Task")
    assert set(task["properties"]["state"]["enum"]) == {
        "delayed",
        "ready",
        "leased",
        "retry_scheduled",
        "succeeded",
        "dead_lettered",
        "cancelled",
    }
    assert "retry_policy_version" in task["properties"]
    _assert_bounded_priority_schema(task["properties"]["priority"])
    attempt = _schema(spec, "Attempt")
    assert attempt["properties"]["attempt_id"]["type"] == "integer"
    assert attempt["properties"]["attempt_id"]["format"] == "int64"
    assert attempt["properties"]["attempt_id"]["minimum"] == 1
    assert set(attempt["properties"]["outcome"]["enum"]) == {
        "active",
        "succeeded",
        "retry_scheduled",
        "dead_lettered",
        "expired",
        "cancelled",
    }
    enqueue = _schema(spec, "EnqueueTaskRequest")
    assert set(enqueue["required"]) == {"payload", "priority"}
    assert "idempotency_key" not in enqueue["properties"]
    complete = _schema(spec, "CompleteRequest")
    assert set(complete["required"]) == {"generation", "spawn"}
    assert "events" not in complete["properties"]
    assert complete["x-queue-reserved-additive-fields"] == ["events"]
    assert _schema(spec, "CompleteResult")["x-queue-reserved-additive-fields"] == [
        "events"
    ]
    fail_req = _schema(spec, "FailRequest")
    assert set(fail_req["required"]) == {"generation", "retryable", "failure_code"}
    claim_req = _schema(spec, "ClaimRequest")
    assert claim_req["properties"]["max_tasks"]["const"] == 1
    wait_seconds = claim_req["properties"]["wait_seconds"]
    assert wait_seconds["type"] == "integer"
    assert wait_seconds["minimum"] == 0
    assert wait_seconds["maximum"] == 20
    assert "const" not in wait_seconds
    claim_resp = _schema(spec, "ClaimResponse")
    for field in (
        "tasks",
        "server_time",
        "recommended_heartbeat_seconds",
        "queue_states",
    ):
        assert field in claim_resp["required"]
    assert claim_resp["properties"]["tasks"]["maxItems"] == 1
    assert "claim_token" in _schema(spec, "ClaimGrant")["required"]
    assert "claim_token" not in _schema(spec, "ClaimSummary").get("properties", {})


def test_live_capabilities_equal_openapi_and_runtime_bound() -> None:
    """Default LIVE_CAPABILITIES matches default deployment ceiling; OpenAPI bounds 0..20."""

    from workhold.api.v1.capabilities import LIVE_CAPABILITIES
    from workhold.settings import (
        CLAIM_MAX_WAIT_SECONDS_DEFAULT,
        CLAIM_MAX_WAIT_SECONDS_MAX,
        CLAIM_MAX_WAIT_SECONDS_MIN,
    )

    assert LIVE_CAPABILITIES["long_polling"] is True
    assert LIVE_CAPABILITIES["batch_claim"] is False
    assert LIVE_CAPABILITIES["max_claim_tasks"] == 1
    assert LIVE_CAPABILITIES["max_wait_seconds"] == CLAIM_MAX_WAIT_SECONDS_DEFAULT
    assert CAPABILITIES_CONSTS["long_polling"] is True
    assert CAPABILITIES_CONSTS["batch_claim"] is False
    assert CAPABILITIES_CONSTS["max_claim_tasks"] == 1
    assert CAPABILITIES_CONSTS["max_wait_seconds"] == CLAIM_MAX_WAIT_SECONDS_DEFAULT
    for key in (
        "long_polling",
        "batch_claim",
        "max_claim_tasks",
        "max_wait_seconds",
    ):
        assert LIVE_CAPABILITIES[key] == CAPABILITIES_CONSTS[key], key
    caps = _schema(_load(), "Capabilities")
    assert caps["properties"]["batch_claim"]["const"] is False
    assert caps["properties"]["max_claim_tasks"]["const"] == 1
    assert caps["properties"]["long_polling"]["type"] == "boolean"
    assert "const" not in caps["properties"]["long_polling"]
    max_wait = caps["properties"]["max_wait_seconds"]
    assert max_wait["type"] == "integer"
    assert max_wait["minimum"] == CLAIM_MAX_WAIT_SECONDS_MIN
    assert max_wait["maximum"] == CLAIM_MAX_WAIT_SECONDS_MAX
    assert "const" not in max_wait


def test_idempotency_key_header_not_body() -> None:
    ops = _ops(_load())
    for oid in IDEMPOTENCY_HEADER_OPS:
        assert _has_idempotency(ops[oid]["operation"]), oid
    resolve = _schema(_load(), "ResolveSubmissionRequest")
    assert "idempotency_key" in resolve["required"]


def test_status_matrices_success_refs_and_headers() -> None:
    ops = _ops(_load())
    error_ref = {"$ref": "#/components/schemas/Error"}
    for oid, expected in STATUS_MATRICES.items():
        responses = ops[oid]["operation"]["responses"]
        assert set(responses) == expected, oid
        for status, response in responses.items():
            headers = response.get("headers", {})
            assert "X-Request-ID" in headers, f"{oid} {status}"
            assert headers["X-Request-ID"]["schema"]["format"] == "uuid"
            if status == "201":
                assert "Location" in headers, oid
            if status in {"409", "429", "503"}:
                assert "Retry-After" in headers, f"{oid} {status}"
                assert headers["Retry-After"]["schema"]["type"] == "integer"
            if status.startswith("2"):
                assert _body_name(response) == SUCCESS_BODY_REFS[oid], oid
            else:
                assert _body_name(response) == "Error", f"{oid} {status}"
                assert response["content"]["application/json"]["schema"] == error_ref


def test_adr017_expiry_docs_and_ttl_consts() -> None:
    spec = _load()
    dumped = json.dumps(spec)
    assert "90 days" in dumped or "90-day" in dumped
    assert "7 days" in dumped or "7-day" in dumped
    assert "30 days" in dumped or "30-day" in dumped
    assert "claim_not_found" in dumped
    assert "resource_kind" in dumped
    assert "admin_replay" in dumped
    assert "7776000" in dumped
    assert "604800" in dumped
    assert "2592000" in dumped
    caps = _schema(spec, "Capabilities")
    assert caps["properties"]["enqueue_dedup_ttl_seconds"]["const"] == 7_776_000
    assert caps["properties"]["terminal_replay_ttl_seconds"]["const"] == 604_800
    assert caps["properties"]["admin_replay_ttl_seconds"]["const"] == 2_592_000


def test_skeleton_contract_covers_every_operation() -> None:
    spec = _load()
    skeleton = spec["x-queue-conformance-skeleton"]
    assert skeleton["http_status"] == 501
    assert skeleton["headers"]["X-Queue-Skeleton"] == "true"
    assert skeleton["body"]["code"]["const"] == "skeleton_operation_unsupported"
    assert skeleton["body"]["retryable"]["const"] is False
    assert skeleton["applies_to_operation_ids"] == sorted(ALL_OPERATIONS)
    for oid, entry in _ops(spec).items():
        encoded = json.dumps(entry["operation"]["responses"])
        assert "skeleton_operation_unsupported" not in encoded, oid
        assert "501" not in entry["operation"]["responses"], oid


def test_no_delivery_or_events_acceptance() -> None:
    spec = _load()
    assert not _forbidden_keys(spec)
    assert (
        _schema(spec, "Capabilities")["properties"]["delivery_events"]["const"]
        is False
    )
    assert "events" not in _schema(spec, "CompleteRequest")["properties"]
    dumped = json.dumps(spec).lower()
    assert "cloudevents" not in dumped
    assert "webhook" not in dumped
    assert "parser_v1" not in dumped
    assert "broker://" not in dumped
    assert '"delivery_events": true' not in dumped.replace(" ", "")


def test_compatibility_extension_present() -> None:
    spec = _load()
    compat = spec["x-queue-compatibility"]
    assert compat["tolerant_response_fields"] is True
    assert compat["unknown_enum_fallback"] is True
    description = spec["info"]["description"].lower()
    assert "tolerant" in description or "tolerate" in description or "additive" in description
    assert "unknown" in description


def test_catalog_optional_fields_are_nullable() -> None:
    """Every plan `?` field must allow JSON null via nullable:true."""
    spec = _load()
    for schema_name, fields in CATALOG_NULLABLE_FIELDS.items():
        schema = _schema(spec, schema_name)
        required = set(schema.get("required") or [])
        props = schema["properties"]
        for field in fields:
            assert field in props, f"{schema_name}.{field}"
            assert field not in required, f"{schema_name}.{field} must be optional"
            assert props[field].get("nullable") is True, (
                f"{schema_name}.{field} must be nullable"
            )


def test_claimed_task_requires_payload() -> None:
    spec = _load()
    claimed = _schema(spec, "ClaimedTask")
    assert set(claimed["required"]) == {"task", "claim"}
    task_node = claimed["properties"]["task"]
    assert "allOf" in task_node
    refs = [part.get("$ref") for part in task_node["allOf"] if "$ref" in part]
    assert "#/components/schemas/Task" in refs
    required_sets = [
        set(part.get("required") or [])
        for part in task_node["allOf"]
        if "required" in part
    ]
    assert any("payload" in req for req in required_sets)
    # ClaimedTask.payload requirement does not remove Task payload nullability
    # on the base Task schema; nullability remains locked separately.
    assert _schema(spec, "Task")["properties"]["payload"].get("nullable") is True


def test_path_parameter_schemas() -> None:
    ops = _ops(_load())

    def path_param(oid: str, name: str) -> dict[str, Any]:
        for param in ops[oid]["operation"].get("parameters", []):
            if param.get("in") == "path" and param.get("name") == name:
                return param
        raise AssertionError(f"{oid} missing path param {name}")

    queue = path_param("enqueueTask", "queue_name")
    assert queue["required"] is True
    queue_schema = queue["schema"]
    assert queue_schema["type"] == "string"
    assert queue_schema["minLength"] == 1
    assert queue_schema["maxLength"] == 128
    assert queue_schema["pattern"] == QUEUE_NAME_PATTERN

    task = path_param("getTask", "task_id")
    assert task["required"] is True
    assert task["schema"]["type"] == "string"
    assert task["schema"]["format"] == "uuid"

    claim = path_param("heartbeatClaim", "claim_id")
    assert claim["required"] is True
    assert claim["schema"]["type"] == "string"
    assert claim["schema"]["format"] == "uuid"

    policy = path_param("activateQueuePolicy", "policy_version")
    assert policy["required"] is True
    policy_schema = policy["schema"]
    assert policy_schema["type"] == "integer"
    assert policy_schema["format"] == "int32"
    assert policy_schema["minimum"] == 1


def test_unicode_code_point_string_bounds_documented() -> None:
    description = _load()["info"]["description"]
    assert "Unicode code points" in description
    assert "byte" in description.lower()
    assert "minLength" in description or "maxLength" in description


def test_enqueue_and_spawn_available_at_semantics_documented() -> None:
    spec = _load()
    for schema_name in ("EnqueueTaskRequest", "SpawnRequest"):
        field = _schema(spec, schema_name)["properties"]["available_at"]
        description = field.get("description", "")
        assert field.get("nullable") is True
        assert field["format"] == "date-time"
        assert "RFC3339" in description
        assert "Queue-store" in description
        assert "86400" in description
        assert "validation_failed" in description

    caps = _schema(spec, "Capabilities")
    assert caps["properties"]["scheduling"]["const"] is True
    assert caps["properties"]["priority"]["const"] is True


def _assert_bounded_priority_schema(schema: dict[str, Any]) -> None:
    assert schema["type"] == "integer"
    assert schema["format"] == "int32"
    assert schema["minimum"] == PRIORITY_MIN
    assert schema["maximum"] == PRIORITY_MAX
    assert "const" not in schema
    description = schema.get("description", "")
    for marker in PRIORITY_DESCRIPTION_MARKERS:
        assert marker in description


def test_bounded_priority_schemas_match_server_constants() -> None:
    spec = _load()
    enqueue = _schema(spec, "EnqueueTaskRequest")["properties"]["priority"]
    spawn = _schema(spec, "SpawnRequest")["properties"]["priority"]
    task = _schema(spec, "Task")["properties"]["priority"]
    for schema in (enqueue, spawn, task):
        _assert_bounded_priority_schema(schema)
    assert enqueue.get("default") == 0
    assert "default" not in spawn
    assert "default" not in task
    assert _schema(spec, "Capabilities")["properties"]["priority"]["const"] is True
