"""Process-level tests for the Phase 3.1 OpenAPI contract stub."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"
COMPOSE_PATH = ROOT / "docker-compose.conformance.yml"
HARD_MAX_BYTES = 1_048_576

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

SAMPLE_VALUES = {
    "queue_name": "demo-queue",
    "task_id": "11111111-1111-4111-8111-111111111111",
    "event_id": "33333333-3333-4333-8333-333333333333",
    "claim_id": "22222222-2222-4222-8222-222222222222",
    "policy_version": "1",
    "partition_name": "tasks_terminal_20260101",
}


def _load_openapi() -> dict[str, Any]:
    return json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))


def _concrete_path(template: str) -> str:
    path = template
    for name, value in SAMPLE_VALUES.items():
        path = path.replace("{" + name + "}", value)
    assert "{" not in path, path
    return path


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_ready(host: str, port: int, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"stub did not listen on {host}:{port}")


def _http_json(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], Any]:
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    request = urllib.request.Request(url, data=body, method=method.upper(), headers=req_headers)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read()
            payload = json.loads(raw.decode("utf-8")) if raw else None
            return int(response.status), dict(response.headers.items()), payload
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        payload = json.loads(raw.decode("utf-8")) if raw else None
        return int(exc.code), dict(exc.headers.items()), payload


@pytest.fixture()
def stub_process() -> subprocess.Popen[str]:
    port = _free_port()
    env = os.environ.copy()
    env["QUEUE_CONTRACT_HOST"] = "127.0.0.1"
    env["QUEUE_CONTRACT_PORT"] = str(port)
    # Ensure the local package is importable without installing.
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")

    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

    proc = subprocess.Popen(
        [sys.executable, "-m", "workhold.contract_stub"],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=creationflags,
    )
    try:
        _wait_ready("127.0.0.1", port)
    except Exception:
        proc.kill()
        out, err = proc.communicate(timeout=5)
        raise RuntimeError(f"stub failed to start\nstdout:\n{out}\nstderr:\n{err}") from None

    proc._test_base_url = f"http://127.0.0.1:{port}"  # type: ignore[attr-defined]
    try:
        yield proc
    finally:
        if proc.poll() is None:
            _signal_shutdown(proc)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


def _signal_shutdown(proc: subprocess.Popen[str]) -> None:
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
    else:
        proc.send_signal(signal.SIGTERM)


def test_every_openapi_operation_returns_skeleton_only(stub_process: subprocess.Popen[str]) -> None:
    spec = _load_openapi()
    skeleton = spec["x-queue-conformance-skeleton"]
    assert skeleton["http_status"] == 501
    assert skeleton["headers"]["X-Queue-Skeleton"] == "true"
    assert skeleton["body"]["code"]["const"] == "skeleton_operation_unsupported"
    assert skeleton["body"]["retryable"]["const"] is False
    assert set(skeleton["applies_to_operation_ids"]) == set(ALL_OPERATIONS)

    base = stub_process._test_base_url  # type: ignore[attr-defined]
    secret_marker = "super-secret-token-should-never-echo"
    body_marker = '{"payload":{"secret":"never-echo-me"}}'

    for operation_id, (method, template) in sorted(ALL_OPERATIONS.items()):
        path = _concrete_path(template)
        status, headers, payload = _http_json(
            method,
            f"{base}{path}",
            body=body_marker.encode("utf-8") if method == "post" else None,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {secret_marker}",
                "X-Queue-Claim-Token": secret_marker,
            },
        )
        assert status == 501, operation_id
        assert headers.get("X-Queue-Skeleton") == "true", operation_id
        assert "application/json" in headers.get("Content-Type", ""), operation_id
        assert isinstance(payload, dict), operation_id
        assert payload["code"] == "skeleton_operation_unsupported", operation_id
        assert payload["retryable"] is False, operation_id
        assert payload["retry_after_ms"] is None, operation_id
        assert payload["message"] == "operation is not implemented", operation_id
        assert payload["details"] == {}, operation_id
        assert payload["code"] != "internal_error", operation_id
        uuid.UUID(payload["request_id"])
        dumped = json.dumps(payload)
        assert secret_marker not in dumped, operation_id
        assert "never-echo-me" not in dumped, operation_id


def test_unknown_route_returns_production_404(stub_process: subprocess.Popen[str]) -> None:
    base = stub_process._test_base_url  # type: ignore[attr-defined]
    status, headers, payload = _http_json("get", f"{base}/v1/does-not-exist")
    assert status == 404
    assert headers.get("X-Queue-Skeleton") is None
    assert "application/json" in headers.get("Content-Type", "")
    assert isinstance(payload, dict)
    assert payload["code"] == "task_not_found"
    assert payload["retryable"] is False
    assert payload["retry_after_ms"] is None
    assert payload["details"] == {}
    assert payload["code"] != "skeleton_operation_unsupported"
    assert payload["code"] != "internal_error"
    uuid.UUID(payload["request_id"])


def test_oversized_body_rejected_before_parse(stub_process: subprocess.Popen[str]) -> None:
    base = stub_process._test_base_url  # type: ignore[attr-defined]
    oversized = b"x" * (HARD_MAX_BYTES + 1)
    status, headers, payload = _http_json(
        "post",
        f"{base}/v1/claims",
        body=oversized,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert status == 413
    assert "application/json" in headers.get("Content-Type", "")
    assert payload["code"] == "payload_too_large"
    assert payload["retryable"] is False
    assert payload["retry_after_ms"] is None
    assert payload["details"] == {}
    uuid.UUID(payload["request_id"])
    # Oversized body must not be treated as a recognized skeleton operation.
    assert headers.get("X-Queue-Skeleton") is None
    assert payload["code"] != "skeleton_operation_unsupported"


def test_clean_shutdown_on_sigterm(stub_process: subprocess.Popen[str]) -> None:
    _signal_shutdown(stub_process)
    try:
        code = stub_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        stub_process.kill()
        stub_process.wait(timeout=5)
        pytest.fail("contract stub did not exit cleanly after SIGTERM/CTRL_BREAK")
    assert code == 0


def test_conformance_compose_topology() -> None:
    assert COMPOSE_PATH.is_file()
    text = COMPOSE_PATH.read_text(encoding="utf-8")

    assert "conformance-postgres:" in text
    assert "conformance-service:" in text
    assert "postgres:18.6-alpine" in text
    assert "postgres:18.6-alpine@sha256:" in text
    assert "pg_isready" in text
    # Ephemeral postgres: no persistent host/named data volume binding.
    assert "queue-pgdata" not in text
    assert "/var/lib/postgresql/data" not in text

    assert "target: dev" in text
    assert "8080:8080" in text
    assert "workhold.contract_stub" in text
    assert "DATABASE_URL" in text
    assert "@conformance-postgres:" in text or "@conformance-postgres/" in text
    assert "condition: service_healthy" in text

    assert ".:/app" in text or "./:/app" in text
    assert "/app/.venv" in text

    service_block = text[text.index("conformance-service:") :]
    assert "socket.create_connection" in service_block
    assert "8080" in service_block
    health_lower = service_block.lower()
    for forbidden in ("curl ", "wget ", "urllib", "/v1/", "capabilities", "http.get"):
        assert forbidden not in health_lower, forbidden
