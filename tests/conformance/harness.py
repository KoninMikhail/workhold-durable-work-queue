"""Stdlib-only black-box conformance harness for Queue OpenAPI."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urljoin

DEFAULT_OPENAPI_PATH = (
    Path(__file__).resolve().parents[2] / "openapi" / "queue.openapi.json"
)
MAX_RESPONSE_BYTES = 1_048_576
MAX_FINDING_MESSAGE = 512
SECRET_HEADER_NAMES = frozenset({"authorization", "x-queue-claim-token"})
SKELETON_EXTENSION = "x-queue-conformance-skeleton"


class CaseOutcome(str, Enum):
    PASS = "PASS"
    CONTRACT_FAILURE = "CONTRACT_FAILURE"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    HARNESS_ERROR = "HARNESS_ERROR"
    # Explicit expected loss: never suite success by itself (needs a later PASS).
    EXPECTED_TRANSPORT_LOSS = "EXPECTED_TRANSPORT_LOSS"


class OpenApiCatalogError(ValueError):
    """OpenAPI catalog or closed request-fixture validation failure."""


@dataclass(frozen=True)
class OperationSpec:
    operation_id: str
    method: str
    path: str
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class RequestCase:
    operation_id: str
    method: str
    path: str
    headers: Mapping[str, str] = field(default_factory=dict)
    body: Any | None = None
    content_type: str = "application/json"
    # When True, a transport failure is EXPECTED_TRANSPORT_LOSS (not HARNESS_ERROR).
    # Suite PASS still requires a subsequent ordinary PASS proving durable state.
    expect_transport_loss: bool = False


@dataclass(frozen=True)
class AssertionFinding:
    code: str
    message: str

    def to_json_dict(self) -> dict[str, str]:
        msg = self.message
        if len(msg) > MAX_FINDING_MESSAGE:
            msg = msg[: MAX_FINDING_MESSAGE - 3] + "..."
        return {"code": self.code, "message": msg}


@dataclass(frozen=True)
class ObservedResponse:
    status: int
    headers: dict[str, str]
    body_text: str
    body_json: Any | None
    content_type: str | None


@dataclass
class CaseResult:
    operation_id: str
    outcome: CaseOutcome
    findings: list[AssertionFinding] = field(default_factory=list)
    request_meta: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "outcome": self.outcome.value,
            "findings": [f.to_json_dict() for f in self.findings],
            "request": self.request_meta,
        }


@dataclass
class RunReport:
    protocol_version: str | None
    schema_revision: str | None
    openapi_version: str | None
    server_capabilities: dict[str, Any] | None
    suite_outcome: CaseOutcome
    exit_code: int
    findings: list[AssertionFinding]
    cases: list[CaseResult]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "schema_revision": self.schema_revision,
            "openapi_version": self.openapi_version,
            "server_capabilities": self.server_capabilities,
            "suite_outcome": self.suite_outcome.value,
            "exit_code": self.exit_code,
            "findings": [f.to_json_dict() for f in self.findings],
            "cases": [c.to_json_dict() for c in self.cases],
        }


def _redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in SECRET_HEADER_NAMES:
            out[key] = "[REDACTED]"
        else:
            out[key] = value
    return out


def _resolve_ref(doc: Mapping[str, Any], node: Any) -> Any:
    if not isinstance(node, Mapping) or "$ref" not in node:
        return node
    ref = node["$ref"]
    if not isinstance(ref, str) or not ref.startswith("#/"):
        raise OpenApiCatalogError(f"unsupported $ref: {ref!r}")
    cur: Any = doc
    for part in ref[2:].split("/"):
        if not isinstance(cur, Mapping) or part not in cur:
            raise OpenApiCatalogError(f"unresolvable $ref: {ref}")
        cur = cur[part]
    return cur


def load_operation_catalog(openapi_path: Path | str) -> dict[str, OperationSpec]:
    path = Path(openapi_path)
    doc = json.loads(path.read_text(encoding="utf-8"))
    paths = doc.get("paths")
    if not isinstance(paths, Mapping):
        raise OpenApiCatalogError("OpenAPI document missing paths object")

    http_methods = {
        "get",
        "post",
        "put",
        "patch",
        "delete",
        "head",
        "options",
        "trace",
    }
    ops: dict[str, OperationSpec] = {}
    for raw_path, methods in paths.items():
        if not isinstance(methods, Mapping):
            continue
        for method, operation in methods.items():
            if method.startswith("x-") or method.lower() not in http_methods:
                continue
            if not isinstance(operation, Mapping):
                continue
            operation_id = operation.get("operationId")
            if not isinstance(operation_id, str) or not operation_id:
                raise OpenApiCatalogError(
                    f"missing operationId for {method.upper()} {raw_path}"
                )
            if operation_id in ops:
                raise OpenApiCatalogError(f"duplicate operationId: {operation_id}")
            ops[operation_id] = OperationSpec(
                operation_id=operation_id,
                method=method.upper(),
                path=str(raw_path),
                raw=operation,
            )
    if not ops:
        raise OpenApiCatalogError("OpenAPI document contains no operations")
    return ops


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _matches_type(value: Any, expected: str) -> bool:
    actual = _type_name(value)
    if expected == "number":
        return actual in {"number", "integer"}
    return actual == expected


class _SchemaValidator:
    def __init__(
        self,
        doc: Mapping[str, Any],
        *,
        closed_objects: bool,
        allow_unknown_enum: bool,
    ) -> None:
        self.doc = doc
        self.closed_objects = closed_objects
        self.allow_unknown_enum = allow_unknown_enum
        self.findings: list[AssertionFinding] = []

    def validate(self, schema: Any, value: Any, path: str) -> None:
        schema = _resolve_ref(self.doc, schema)
        if not isinstance(schema, Mapping):
            self.findings.append(
                AssertionFinding("invalid_schema", f"{path}: schema is not an object")
            )
            return

        if "oneOf" in schema:
            matches = 0
            for option in schema["oneOf"]:
                probe = _SchemaValidator(
                    self.doc,
                    closed_objects=self.closed_objects,
                    allow_unknown_enum=self.allow_unknown_enum,
                )
                probe.validate(option, value, path)
                if not probe.findings:
                    matches += 1
            if matches != 1:
                self.findings.append(
                    AssertionFinding(
                        "oneof_mismatch",
                        f"{path}: expected one oneOf match, got {matches}",
                    )
                )
            return

        if "anyOf" in schema:
            ok = False
            for option in schema["anyOf"]:
                probe = _SchemaValidator(
                    self.doc,
                    closed_objects=self.closed_objects,
                    allow_unknown_enum=self.allow_unknown_enum,
                )
                probe.validate(option, value, path)
                if not probe.findings:
                    ok = True
                    break
            if not ok:
                self.findings.append(
                    AssertionFinding("anyof_mismatch", f"{path}: matched no anyOf option")
                )
            return

        if "allOf" in schema:
            for option in schema["allOf"]:
                self.validate(option, value, path)

        nullable = bool(schema.get("nullable"))
        raw_type = schema.get("type")
        if isinstance(raw_type, list):
            types = [str(t) for t in raw_type]
        elif isinstance(raw_type, str):
            types = [raw_type]
        else:
            types = []
        if nullable and "null" not in types:
            types = [*types, "null"]

        if types and not any(_matches_type(value, t) for t in types):
            self.findings.append(
                AssertionFinding(
                    "type_mismatch",
                    f"{path}: expected {'|'.join(types)}, got {_type_name(value)}",
                )
            )
            return

        if "const" in schema and value != schema["const"]:
            self.findings.append(
                AssertionFinding(
                    "const_mismatch",
                    f"{path}: expected const {schema['const']!r}, got {value!r}",
                )
            )

        if "enum" in schema and isinstance(schema["enum"], list):
            if value not in schema["enum"]:
                if not (self.allow_unknown_enum and isinstance(value, str)):
                    self.findings.append(
                        AssertionFinding(
                            "enum_mismatch",
                            f"{path}: value {value!r} not in enum",
                        )
                    )

        if isinstance(value, str):
            length = len(value)
            if "minLength" in schema and length < int(schema["minLength"]):
                self.findings.append(
                    AssertionFinding(
                        "min_length",
                        f"{path}: shorter than minLength {schema['minLength']}",
                    )
                )
            if "maxLength" in schema and length > int(schema["maxLength"]):
                self.findings.append(
                    AssertionFinding(
                        "max_length",
                        f"{path}: longer than maxLength {schema['maxLength']}",
                    )
                )
            pattern = schema.get("pattern")
            if isinstance(pattern, str) and re.fullmatch(pattern, value) is None:
                self.findings.append(
                    AssertionFinding(
                        "pattern_mismatch",
                        f"{path}: failed pattern {pattern}",
                    )
                )

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if "minimum" in schema and value < schema["minimum"]:
                self.findings.append(
                    AssertionFinding(
                        "minimum",
                        f"{path}: {value} below minimum {schema['minimum']}",
                    )
                )
            if "maximum" in schema and value > schema["maximum"]:
                self.findings.append(
                    AssertionFinding(
                        "maximum",
                        f"{path}: {value} above maximum {schema['maximum']}",
                    )
                )

        if isinstance(value, list):
            if "minItems" in schema and len(value) < int(schema["minItems"]):
                self.findings.append(
                    AssertionFinding(
                        "min_items",
                        f"{path}: shorter than minItems {schema['minItems']}",
                    )
                )
            if "maxItems" in schema and len(value) > int(schema["maxItems"]):
                self.findings.append(
                    AssertionFinding(
                        "max_items",
                        f"{path}: longer than maxItems {schema['maxItems']}",
                    )
                )
            item_schema = schema.get("items")
            if isinstance(item_schema, Mapping):
                for idx, item in enumerate(value):
                    self.validate(item_schema, item, f"{path}/{idx}")

        if isinstance(value, dict):
            props = schema.get("properties")
            if not isinstance(props, Mapping):
                props = {}
            required = schema.get("required") or []
            if isinstance(required, list):
                for key in required:
                    if key not in value:
                        self.findings.append(
                            AssertionFinding(
                                "missing_required_property",
                                f"{path}: missing required property '{key}'",
                            )
                        )
            additional = schema.get("additionalProperties", True)
            reject_unknown = additional is False or self.closed_objects
            for key, child in value.items():
                if key in props and isinstance(props[key], Mapping):
                    child_path = f"{path}/{key}"
                    self.validate(props[key], child, child_path)
                    continue
                if reject_unknown:
                    self.findings.append(
                        AssertionFinding(
                            "unknown_property",
                            f"{path}: unknown property '{key}'",
                        )
                    )
                elif isinstance(additional, Mapping):
                    self.validate(additional, child, f"{path}/{key}")


class ConformanceHarness:
    def __init__(
        self,
        *,
        openapi_path: Path | str = DEFAULT_OPENAPI_PATH,
        base_url: str,
        timeout_s: float = 5.0,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
    ) -> None:
        self.openapi_path = Path(openapi_path)
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout_s = timeout_s
        self.max_response_bytes = max_response_bytes
        self.doc: dict[str, Any] = json.loads(self.openapi_path.read_text(encoding="utf-8"))
        self.operations = load_operation_catalog(self.openapi_path)
        skeleton = self.doc.get(SKELETON_EXTENSION)
        if not isinstance(skeleton, Mapping):
            raise OpenApiCatalogError(f"missing {SKELETON_EXTENSION} extension")
        self.skeleton = skeleton
        self.compatibility = self.doc.get("x-queue-compatibility") or {}
        self.adapters: dict[str, RequestCase] = {}
        caps = (
            self.doc.get("components", {})
            .get("schemas", {})
            .get("Capabilities", {})
            .get("properties", {})
        )
        self._capability_consts: dict[str, Any] = {
            key: prop["const"]
            for key, prop in caps.items()
            if isinstance(prop, Mapping) and "const" in prop
        }

    def register_adapter(self, operation_id: str, case: RequestCase) -> None:
        if operation_id not in self.operations:
            raise OpenApiCatalogError(f"unknown operationId: {operation_id}")
        self.adapters[operation_id] = case

    def validate_request_fixture(self, operation_id: str, body: Any) -> None:
        operation = self.operations.get(operation_id)
        if operation is None:
            raise OpenApiCatalogError(f"unknown operationId: {operation_id}")
        request_body = operation.raw.get("requestBody")
        if not isinstance(request_body, Mapping):
            raise OpenApiCatalogError(f"operation {operation_id} has no requestBody")
        content = request_body.get("content") or {}
        app_json = content.get("application/json") if isinstance(content, Mapping) else None
        if not isinstance(app_json, Mapping) or "schema" not in app_json:
            raise OpenApiCatalogError(
                f"operation {operation_id} lacks application/json request schema"
            )
        schema = _resolve_ref(self.doc, app_json["schema"])
        validator = _SchemaValidator(
            self.doc,
            closed_objects=True,
            allow_unknown_enum=False,
        )
        validator.validate(schema, body, "$")
        if validator.findings:
            raise OpenApiCatalogError(
                "; ".join(f"{f.code}: {f.message}" for f in validator.findings)
            )

    def run_operation(self, operation_id: str) -> RunReport:
        if operation_id not in self.adapters:
            case = CaseResult(
                operation_id=operation_id,
                outcome=CaseOutcome.UNSUPPORTED_OPERATION,
                findings=[
                    AssertionFinding(
                        "missing_adapter",
                        f"no adapter registered for operationId '{operation_id}'",
                    )
                ],
                request_meta={"headers": {}, "method": None, "path": None},
            )
            return self._finalize([case], suite_findings=[])
        return self.run_cases([self.adapters[operation_id]])

    def run_cases(self, cases: list[RequestCase]) -> RunReport:
        if not cases:
            return self._finalize(
                [],
                suite_findings=[
                    AssertionFinding(
                        "empty_suite",
                        "conformance suite contained zero cases",
                    )
                ],
                forced_suite_outcome=CaseOutcome.HARNESS_ERROR,
            )
        results = [self._run_one(case) for case in cases]
        return self._finalize(results, suite_findings=[])

    def _run_one(self, case: RequestCase) -> CaseResult:
        request_meta = {
            "method": case.method,
            "path": case.path,
            "headers": _redact_headers(case.headers),
            "has_body": case.body is not None,
        }
        if case.operation_id not in self.operations:
            return CaseResult(
                operation_id=case.operation_id,
                outcome=CaseOutcome.HARNESS_ERROR,
                findings=[
                    AssertionFinding(
                        "unknown_operation",
                        f"operationId '{case.operation_id}' not in OpenAPI catalog",
                    )
                ],
                request_meta=request_meta,
            )

        try:
            observed = self._http_exchange(case)
        except Exception as exc:  # noqa: BLE001 - harness boundary
            if case.expect_transport_loss:
                return CaseResult(
                    operation_id=case.operation_id,
                    outcome=CaseOutcome.EXPECTED_TRANSPORT_LOSS,
                    findings=[
                        AssertionFinding(
                            "expected_transport_loss",
                            f"{type(exc).__name__}: {exc}",
                        )
                    ],
                    request_meta=request_meta,
                )
            return CaseResult(
                operation_id=case.operation_id,
                outcome=CaseOutcome.HARNESS_ERROR,
                findings=[
                    AssertionFinding(
                        "transport_error",
                        f"{type(exc).__name__}: {exc}",
                    )
                ],
                request_meta=request_meta,
            )

        if self._is_full_skeleton(observed):
            return CaseResult(
                operation_id=case.operation_id,
                outcome=CaseOutcome.UNSUPPORTED_OPERATION,
                findings=[
                    AssertionFinding(
                        "skeleton_unsupported",
                        f"server matched full {SKELETON_EXTENSION} contract",
                    )
                ],
                request_meta=request_meta,
            )

        findings = self._validate_response(case.operation_id, observed)
        if findings:
            return CaseResult(
                operation_id=case.operation_id,
                outcome=CaseOutcome.CONTRACT_FAILURE,
                findings=findings,
                request_meta=request_meta,
            )
        return CaseResult(
            operation_id=case.operation_id,
            outcome=CaseOutcome.PASS,
            findings=[],
            request_meta=request_meta,
        )

    def _http_exchange(self, case: RequestCase) -> ObservedResponse:
        url = urljoin(self.base_url, case.path.lstrip("/"))
        data: bytes | None = None
        headers = {str(k): str(v) for k, v in case.headers.items()}
        if case.body is not None:
            data = json.dumps(case.body).encode("utf-8")
            headers.setdefault("Content-Type", case.content_type)
        request = urllib.request.Request(
            url,
            data=data,
            headers=headers,
            method=case.method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                raw = response.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes:
                    raise OSError(
                        f"response exceeded max_response_bytes={self.max_response_bytes}"
                    )
                header_map = {k: v for k, v in response.headers.items()}
                content_type = response.headers.get("Content-Type")
                status = int(response.status)
        except urllib.error.HTTPError as err:
            raw = err.read(self.max_response_bytes + 1)
            if len(raw) > self.max_response_bytes:
                raise OSError(
                    f"response exceeded max_response_bytes={self.max_response_bytes}"
                ) from err
            header_map = {k: v for k, v in err.headers.items()} if err.headers else {}
            content_type = header_map.get("Content-Type")
            status = int(err.code)

        text = raw.decode("utf-8", errors="replace")
        try:
            body_json: Any | None = json.loads(text) if text else None
        except json.JSONDecodeError:
            body_json = None
        return ObservedResponse(
            status=status,
            headers=header_map,
            body_text=text,
            body_json=body_json,
            content_type=content_type,
        )

    def _is_full_skeleton(self, observed: ObservedResponse) -> bool:
        expected_status = int(self.skeleton.get("http_status", 501))
        if observed.status != expected_status:
            return False

        expected_headers = self.skeleton.get("headers") or {}
        if not isinstance(expected_headers, Mapping):
            return False
        observed_lower = {k.lower(): v for k, v in observed.headers.items()}
        for key, expected in expected_headers.items():
            if observed_lower.get(str(key).lower()) != str(expected):
                return False

        body = observed.body_json
        if not isinstance(body, Mapping):
            return False
        body_contract = self.skeleton.get("body") or {}
        if not isinstance(body_contract, Mapping):
            return False

        code_const = (body_contract.get("code") or {}).get("const")
        retryable_const = (body_contract.get("retryable") or {}).get("const")
        if body.get("code") != code_const:
            return False
        if body.get("retryable") != retryable_const:
            return False
        if body.get("retry_after_ms") is not None:
            return False

        production_codes = (
            self.doc.get("components", {})
            .get("schemas", {})
            .get("Error", {})
            .get("properties", {})
            .get("code", {})
            .get("enum", [])
        )
        if code_const in production_codes:
            return False
        return True

    def _validate_response(
        self,
        operation_id: str,
        observed: ObservedResponse,
    ) -> list[AssertionFinding]:
        findings: list[AssertionFinding] = []
        operation = self.operations[operation_id]
        responses = operation.raw.get("responses") or {}
        status_key = str(observed.status)
        response_obj = responses.get(status_key)
        if response_obj is None:
            findings.append(
                AssertionFinding(
                    "unexpected_status",
                    f"status {observed.status} not declared for {operation_id}",
                )
            )
            return findings

        response_obj = _resolve_ref(self.doc, response_obj)
        content = response_obj.get("content") if isinstance(response_obj, Mapping) else None
        if not isinstance(content, Mapping) or not content:
            return findings

        if not observed.content_type or "application/json" not in observed.content_type:
            findings.append(
                AssertionFinding(
                    "content_type_mismatch",
                    f"expected application/json, got {observed.content_type!r}",
                )
            )
            return findings

        app_json = content.get("application/json")
        if not isinstance(app_json, Mapping) or "schema" not in app_json:
            findings.append(
                AssertionFinding(
                    "missing_response_schema",
                    f"no application/json schema for status {observed.status}",
                )
            )
            return findings

        if observed.body_json is None:
            findings.append(
                AssertionFinding("invalid_json", "response body is not valid JSON")
            )
            return findings

        schema = _resolve_ref(self.doc, app_json["schema"])
        allow_unknown_enum = bool(
            isinstance(self.compatibility, Mapping)
            and self.compatibility.get("unknown_enum_fallback")
        )
        validator = _SchemaValidator(
            self.doc,
            closed_objects=False,
            allow_unknown_enum=allow_unknown_enum,
        )
        validator.validate(schema, observed.body_json, "$")
        findings.extend(validator.findings)

        if operation_id == "getCapabilities" and isinstance(observed.body_json, Mapping):
            for key, expected in self._capability_consts.items():
                if key in observed.body_json and observed.body_json[key] != expected:
                    findings.append(
                        AssertionFinding(
                            "const_mismatch",
                            f"$.{key}: expected capability const {expected!r}, "
                            f"got {observed.body_json[key]!r}",
                        )
                    )

        if isinstance(observed.body_json, Mapping):
            skeleton_code = (
                (self.skeleton.get("body") or {}).get("code") or {}
            ).get("const")
            if (
                skeleton_code
                and observed.body_json.get("code") == skeleton_code
                and not self._is_full_skeleton(observed)
            ):
                findings.append(
                    AssertionFinding(
                        "skeleton_code_outside_contract",
                        f"{skeleton_code} used without full skeleton contract",
                    )
                )
        return findings

    def _finalize(
        self,
        cases: list[CaseResult],
        *,
        suite_findings: list[AssertionFinding],
        forced_suite_outcome: CaseOutcome | None = None,
    ) -> RunReport:
        findings = list(suite_findings)
        if forced_suite_outcome is not None:
            suite_outcome = forced_suite_outcome
        elif any(c.outcome == CaseOutcome.HARNESS_ERROR for c in cases) or any(
            f.code == "empty_suite" for f in findings
        ):
            suite_outcome = CaseOutcome.HARNESS_ERROR
        elif any(c.outcome == CaseOutcome.UNSUPPORTED_OPERATION for c in cases):
            suite_outcome = CaseOutcome.UNSUPPORTED_OPERATION
        elif any(c.outcome == CaseOutcome.CONTRACT_FAILURE for c in cases):
            suite_outcome = CaseOutcome.CONTRACT_FAILURE
        elif not cases:
            suite_outcome = CaseOutcome.HARNESS_ERROR
        else:
            has_expected_loss = any(
                c.outcome == CaseOutcome.EXPECTED_TRANSPORT_LOSS for c in cases
            )
            has_pass = any(c.outcome == CaseOutcome.PASS for c in cases)
            if has_expected_loss and not has_pass:
                # Transport loss alone is never conformance success.
                findings.append(
                    AssertionFinding(
                        "expected_transport_loss_without_durable_proof",
                        "EXPECTED_TRANSPORT_LOSS requires a subsequent PASS "
                        "proving durable state",
                    )
                )
                suite_outcome = CaseOutcome.HARNESS_ERROR
            elif has_expected_loss and has_pass:
                suite_outcome = CaseOutcome.PASS
            elif all(
                c.outcome
                in {CaseOutcome.PASS, CaseOutcome.EXPECTED_TRANSPORT_LOSS}
                for c in cases
            ):
                suite_outcome = CaseOutcome.PASS
            else:
                suite_outcome = CaseOutcome.HARNESS_ERROR

        return RunReport(
            protocol_version=str(self._capability_consts.get("protocol_version")),
            schema_revision=str(self._capability_consts.get("schema_revision")),
            openapi_version=str(self.doc.get("openapi")),
            server_capabilities=dict(self._capability_consts),
            suite_outcome=suite_outcome,
            exit_code=0 if suite_outcome == CaseOutcome.PASS else 1,
            findings=findings,
            cases=cases,
        )
