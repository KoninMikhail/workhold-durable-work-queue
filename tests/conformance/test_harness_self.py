"""Self-tests for the black-box conformance harness semantics."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable

import pytest

from tests.conformance.harness import (
    CaseOutcome,
    ConformanceHarness,
    OpenApiCatalogError,
    RequestCase,
    SECRET_HEADER_NAMES,
    load_operation_catalog,
)

ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"

CAPABILITIES_BODY: dict[str, Any] = {
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


def _skeleton_body(**overrides: Any) -> dict[str, Any]:
    body = {
        "code": "skeleton_operation_unsupported",
        "message": "operation not implemented in Phase 3.1 skeleton",
        "retryable": False,
        "request_id": "00000000-0000-4000-8000-000000000001",
        "details": {},
    }
    body.update(overrides)
    return body


class _Router(BaseHTTPRequestHandler):
    routes: dict[tuple[str, str], Callable[["_Router"], None]] = {}

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _dispatch(self) -> None:
        handler = self.routes.get((self.command, self.path.split("?", 1)[0]))
        if handler is None:
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"code":"validation_failed","message":"missing route","retryable":false,"request_id":"00000000-0000-4000-8000-000000000099","details":{}}')
            return
        handler(self)

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()


@pytest.fixture
def http_server():
    server = HTTPServer(("127.0.0.1", 0), _Router)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://{host}:{port}"
    try:
        yield base_url, _Router
    finally:
        server.shutdown()
        thread.join(timeout=2)
        _Router.routes = {}


def _json_response(
    handler: _Router,
    status: int,
    body: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
) -> None:
    payload = json.dumps(body).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        handler.send_header(key, value)
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def test_catalog_loads_from_openapi_and_rejects_duplicates(tmp_path: Path) -> None:
    ops = load_operation_catalog(OPENAPI_PATH)
    assert "getCapabilities" in ops
    assert ops["getCapabilities"].method == "GET"
    assert ops["getCapabilities"].path == "/v1/capabilities"
    assert len(ops) >= 20

    broken = tmp_path / "broken.openapi.json"
    broken.write_text(
        json.dumps(
            {
                "openapi": "3.1.0",
                "info": {"title": "x", "version": "1"},
                "paths": {
                    "/a": {"get": {"operationId": "dup", "responses": {"200": {}}}},
                    "/b": {"get": {"operationId": "dup", "responses": {"200": {}}}},
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(OpenApiCatalogError, match="duplicate"):
        load_operation_catalog(broken)

    missing = tmp_path / "missing.openapi.json"
    missing.write_text(
        json.dumps(
            {
                "openapi": "3.1.0",
                "info": {"title": "x", "version": "1"},
                "paths": {"/a": {"get": {"responses": {"200": {}}}}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(OpenApiCatalogError, match="missing operationId"):
        load_operation_catalog(missing)


def test_pass_with_unknown_additive_response_fields(http_server) -> None:
    base_url, router = http_server

    def ok(handler: _Router) -> None:
        body = dict(CAPABILITIES_BODY)
        body["future_top"] = "ok"
        body["nested_unknown"] = {"x": 1}
        _json_response(handler, 200, body)

    router.routes = {("GET", "/v1/capabilities"): ok}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.PASS
    assert report.exit_code == 0


def test_contract_failure_when_required_field_missing(http_server) -> None:
    base_url, router = http_server

    def bad(handler: _Router) -> None:
        body = dict(CAPABILITIES_BODY)
        del body["schema_revision"]
        _json_response(handler, 200, body)

    router.routes = {("GET", "/v1/capabilities"): bad}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.CONTRACT_FAILURE
    assert report.exit_code != 0
    assert any(f.code == "missing_required_property" for f in report.cases[0].findings)


def test_unsupported_when_full_skeleton_contract_matches(http_server) -> None:
    base_url, router = http_server

    def skel(handler: _Router) -> None:
        _json_response(
            handler,
            501,
            _skeleton_body(),
            headers={"X-Queue-Skeleton": "true"},
        )

    router.routes = {("GET", "/v1/capabilities"): skel}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.UNSUPPORTED_OPERATION
    assert report.exit_code != 0


def test_bare_501_is_contract_failure(http_server) -> None:
    base_url, router = http_server

    def bare(handler: _Router) -> None:
        _json_response(
            handler,
            501,
            {
                "code": "internal_error",
                "message": "not implemented",
                "retryable": False,
                "request_id": "00000000-0000-4000-8000-000000000002",
                "details": {},
            },
        )

    router.routes = {("GET", "/v1/capabilities"): bare}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.CONTRACT_FAILURE
    assert report.exit_code != 0


def test_wrong_skeleton_header_is_contract_failure(http_server) -> None:
    base_url, router = http_server

    def wrong(handler: _Router) -> None:
        _json_response(
            handler,
            501,
            _skeleton_body(),
            headers={"X-Queue-Skeleton": "false"},
        )

    router.routes = {("GET", "/v1/capabilities"): wrong}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.CONTRACT_FAILURE


def test_skeleton_code_with_retry_hint_is_contract_failure(http_server) -> None:
    base_url, router = http_server

    def hinted(handler: _Router) -> None:
        _json_response(
            handler,
            501,
            _skeleton_body(retry_after_ms=1000),
            headers={"X-Queue-Skeleton": "true"},
        )

    router.routes = {("GET", "/v1/capabilities"): hinted}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.CONTRACT_FAILURE


def test_connection_refusal_is_harness_error_not_unsupported() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:1",
        timeout_s=0.5,
    )
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.HARNESS_ERROR
    assert report.exit_code != 0


def test_empty_case_set_is_not_success() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
        timeout_s=0.5,
    )
    report = harness.run_cases([])
    assert report.exit_code != 0
    assert report.suite_outcome == CaseOutcome.HARNESS_ERROR
    assert any(f.code == "empty_suite" for f in report.findings)


def test_absent_adapter_is_unsupported() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
        timeout_s=0.5,
    )
    report = harness.run_operation("getCapabilities")
    assert report.cases[0].outcome == CaseOutcome.UNSUPPORTED_OPERATION
    assert report.exit_code != 0
    assert any(f.code == "missing_adapter" for f in report.cases[0].findings)


def test_closed_request_rejects_unknown_fields() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
        timeout_s=0.5,
    )
    with pytest.raises(OpenApiCatalogError, match="unknown"):
        harness.validate_request_fixture(
            "claimTasks",
            {
                "queues": ["default"],
                "max_tasks": 1,
                "lease_seconds": 30,
                "wait_seconds": 0,
                "worker_id": "w1",
                "unexpected": True,
            },
        )


def test_report_redacts_authorization_and_claim_token(http_server) -> None:
    base_url, router = http_server

    def ok(handler: _Router) -> None:
        _json_response(handler, 200, dict(CAPABILITIES_BODY))

    router.routes = {("GET", "/v1/capabilities"): ok}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={
                    "Authorization": "Bearer secret-token-value",
                    "X-Queue-Claim-Token": "claim-secret-value",
                },
                body=None,
            )
        ]
    )
    dumped = json.dumps(report.to_json_dict(), sort_keys=True)
    assert "secret-token-value" not in dumped
    assert "claim-secret-value" not in dumped
    assert "Bearer " not in dumped
    for name in SECRET_HEADER_NAMES:
        assert name.lower() not in dumped.lower() or "[REDACTED]" in dumped


def test_capability_const_mismatch_is_contract_failure(http_server) -> None:
    base_url, router = http_server

    def bad_caps(handler: _Router) -> None:
        body = dict(CAPABILITIES_BODY)
        body["schema_revision"] = "9999"
        _json_response(handler, 200, body)

    router.routes = {("GET", "/v1/capabilities"): bad_caps}
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url, timeout_s=2.0)
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="getCapabilities",
                method="GET",
                path="/v1/capabilities",
                headers={},
                body=None,
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.CONTRACT_FAILURE
    assert any(f.code == "const_mismatch" for f in report.cases[0].findings)


def test_no_skip_or_xfail_helpers_imported() -> None:
    import tests.conformance.harness as harness_mod

    source = Path(harness_mod.__file__).read_text(encoding="utf-8")
    assert "pytest.skip" not in source
    assert "pytest.xfail" not in source
    assert "skip(" not in source
    assert "xfail(" not in source
