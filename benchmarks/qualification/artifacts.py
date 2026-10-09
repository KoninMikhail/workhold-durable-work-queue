"""Immutable qualification artifact validation, derivation and checksums."""

from __future__ import annotations

import gzip
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable, Mapping

from benchmarks.qualification.statistics import (
    LatencySample,
    Outcome,
    aggregate_operation_stats,
    success_ratio,
)

RAW_CLASSES: tuple[str, ...] = (
    "manifest.json",
    "environment.json",
    "workload.json",
    "conformance.xml",
    "latencies.jsonl.gz",
    "postgres-before.json",
    "postgres-after.json",
    "plans/index.json",
)
DERIVED_CLASSES: tuple[str, ...] = (
    "summary.json",
    "qualification.json",
    "report.md",
)
CHECKSUM_CLASS = "SHA256SUMS"
ARTIFACT_CLASSES = {
    "raw": RAW_CLASSES,
    "derived": DERIVED_CLASSES,
    "checksums": (CHECKSUM_CLASS,),
}

# Back-compat alias used by package exports / later plans.
ArtifactClasses = ARTIFACT_CLASSES
FINAL_TOP_LEVEL_NAMES = frozenset(
    {
        "manifest.json",
        "environment.json",
        "workload.json",
        "conformance.xml",
        "latencies.jsonl.gz",
        "postgres-before.json",
        "postgres-after.json",
        "plans",
        "summary.json",
        "qualification.json",
        "report.md",
        CHECKSUM_CLASS,
    }
)
VALIDATION_STAMP = ".raw-validation.json"
SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "run-manifest.schema.json"

OUTCOME_MAP = {
    "success": Outcome.SUCCESS,
    "empty": Outcome.EMPTY,
    "error": Outcome.ERROR,
    "planned_pause": Outcome.PLANNED_PAUSE,
    "planned_drain": Outcome.PLANNED_DRAIN,
    "invalid_request": Outcome.INVALID_REQUEST,
}

P99_HOT_NS = 100_000_000
P99_BASELINE_COMPLETE_NS = 200_000_000
MIN_SUCCESS_RATIO = 0.999
MIN_CLAIMS_PER_SECOND = 500.0
EVIDENCE_MODES = frozenset({"live", "synthetic", "compressed"})
PHASE12_QUALIFICATION_PROFILE = "priority-claim"
PHASE12_EVIDENCE_PACKAGE = "phase-12-priority-claim"
PHASE12_PRIORITY_WORKLOADS = ("due-heavy", "future-high-heavy", "reclaim-heavy")


class ArtifactError(Exception):
    """Fail-closed qualification artifact error."""


def assert_evidence_mode_for_qualification(
    mode: str | None,
    *,
    allow_synthetic: bool = False,
) -> None:
    """Refuse production qualification on non-live evidence unless explicitly allowed.

    Plan 12 / derive / validate-final must call this before emitting a production
    PASS. CI-only synthetic derive uses ``allow_synthetic=True`` (``--allow-synthetic``).
    """
    if mode not in EVIDENCE_MODES:
        raise ArtifactError(
            "evidence_mode must be one of live|synthetic|compressed, "
            f"got {mode!r}"
        )
    if mode != "live" and not allow_synthetic:
        raise ArtifactError(
            f"production qualification refuses evidence_mode={mode!r} "
            "without --allow-synthetic"
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def require_file(bundle: Path, relative: str) -> Path:
    path = bundle / relative
    if not path.is_file():
        raise ArtifactError(f"missing required artifact: {relative}")
    return path


def list_top_level(bundle: Path) -> set[str]:
    return {path.name for path in bundle.iterdir()}


def raw_set_digest(bundle: Path) -> str:
    """Digest the eight raw classes + indexed EXPLAIN files.

    Manifest hashing is stage-stable: ``bundle_stage`` is forced to ``raw`` and
    ``validated_raw_set_digest`` is omitted so promoting the bundle to final does
    not invalidate the digest recorded during validate-raw.
    """
    index = load_json(require_file(bundle, "plans/index.json"))
    explain_paths = [str(entry["path"]) for entry in index.get("files", [])]
    parts: list[str] = []
    for relative in RAW_CLASSES:
        if relative == "manifest.json":
            manifest = load_json(bundle / relative)
            canonical = {
                key: value
                for key, value in manifest.items()
                if key not in {"bundle_stage", "validated_raw_set_digest"}
            }
            canonical["bundle_stage"] = "raw"
            payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
            parts.append(f"{relative}:{hashlib.sha256(payload).hexdigest()}")
        else:
            parts.append(f"{relative}:{sha256_file(bundle / relative)}")
    parts.extend(f"{rel}:{sha256_file(bundle / rel)}" for rel in sorted(explain_paths))
    return sha256_text("\n".join(parts))


def validate_manifest_shape(manifest: Mapping[str, Any], *, expect_stage: str) -> None:
    schema = load_json(SCHEMA_PATH)
    for key in schema["required"]:
        if key not in manifest:
            raise ArtifactError(f"manifest missing required field: {key}")
    if manifest.get("schema_version") != 1:
        raise ArtifactError("manifest.schema_version must be 1")
    if manifest.get("bundle_stage") != expect_stage:
        raise ArtifactError(
            f"manifest.bundle_stage must be {expect_stage!r}, "
            f"got {manifest.get('bundle_stage')!r}"
        )
    mode = manifest.get("evidence_mode")
    if mode not in EVIDENCE_MODES:
        raise ArtifactError(
            "manifest.evidence_mode must be one of live|synthetic|compressed, "
            f"got {mode!r}"
        )
    sample_count = manifest.get("latency_sample_count")
    if not isinstance(sample_count, int) or isinstance(sample_count, bool) or sample_count < 1:
        raise ArtifactError(
            "manifest.latency_sample_count must be a positive integer, "
            f"got {sample_count!r}"
        )
    classes = manifest.get("artifact_classes") or {}
    if tuple(classes.get("raw") or ()) != RAW_CLASSES:
        raise ArtifactError("manifest.artifact_classes.raw mismatch")
    if tuple(classes.get("derived") or ()) != DERIVED_CLASSES:
        raise ArtifactError("manifest.artifact_classes.derived mismatch")
    if tuple(classes.get("checksums") or ()) != (CHECKSUM_CLASS,):
        raise ArtifactError("manifest.artifact_classes.checksums must be [SHA256SUMS]")
    variants = sorted((manifest.get("conformance") or {}).get("variants") or [])
    if variants != ["raw_http", "sdk"]:
        raise ArtifactError("manifest.conformance.variants must be raw_http and sdk")
    if expect_stage == "final" and not manifest.get("validated_raw_set_digest"):
        raise ArtifactError("final manifest missing validated_raw_set_digest")


def validate_evidence_disclosure(
    manifest: Mapping[str, Any],
    workload: Mapping[str, Any],
    *,
    sample_count: int,
) -> None:
    """Require matching evidence_mode disclosure and separate latency sample counts."""
    mode = manifest.get("evidence_mode")
    if mode not in EVIDENCE_MODES:
        raise ArtifactError(
            "manifest.evidence_mode must be one of live|synthetic|compressed, "
            f"got {mode!r}"
        )
    wl_mode = workload.get("evidence_mode")
    if wl_mode != mode:
        raise ArtifactError(
            "workload.evidence_mode must match manifest.evidence_mode "
            f"({mode!r} vs {wl_mode!r})"
        )
    if mode != "live" and not workload.get("evidence_note"):
        raise ArtifactError(
            "non-live evidence_mode requires workload.evidence_note disclosure"
        )
    manifest_count = manifest.get("latency_sample_count")
    workload_count = workload.get("latency_sample_count")
    if manifest_count != sample_count:
        raise ArtifactError(
            "manifest.latency_sample_count does not match latencies.jsonl.gz "
            f"({manifest_count!r} vs {sample_count})"
        )
    if workload_count != sample_count:
        raise ArtifactError(
            "workload.latency_sample_count does not match latencies.jsonl.gz "
            f"({workload_count!r} vs {sample_count})"
        )
    # terminal_lifecycles remains the declared envelope; sample count is separate.


def validate_phase12_priority_bindings(
    bundle: Path,
    manifest: Mapping[str, Any],
    *,
    environment: Mapping[str, Any] | None = None,
    workload: Mapping[str, Any] | None = None,
) -> None:
    """Fail closed when Phase 12 priority qualification bindings drift."""
    from benchmarks.qualification.storage_candidates import (
        PHASE12_PHYSICAL_SIGNATURE,
        PHASE12_SCHEMA_REVISION,
        qualified_physical_signature_digest,
    )

    if manifest.get("qualification_profile") != PHASE12_QUALIFICATION_PROFILE:
        raise ArtifactError(
            "manifest.qualification_profile must be "
            f"{PHASE12_QUALIFICATION_PROFILE!r}"
        )
    if manifest.get("evidence_package") != PHASE12_EVIDENCE_PACKAGE:
        raise ArtifactError(
            f"manifest.evidence_package must be {PHASE12_EVIDENCE_PACKAGE!r}"
        )
    if manifest.get("schema_revision") != PHASE12_SCHEMA_REVISION:
        raise ArtifactError(
            f"manifest.schema_revision must be {PHASE12_SCHEMA_REVISION!r}"
        )
    signature = manifest.get("physical_signature")
    if signature != PHASE12_PHYSICAL_SIGNATURE:
        raise ArtifactError("manifest.physical_signature mismatch")
    expected_digest = qualified_physical_signature_digest()
    if manifest.get("physical_signature_digest") != expected_digest:
        raise ArtifactError("manifest.physical_signature_digest mismatch")
    workload_name = manifest.get("priority_workload")
    if workload_name not in PHASE12_PRIORITY_WORKLOADS:
        raise ArtifactError(
            f"manifest.priority_workload must be one of {PHASE12_PRIORITY_WORKLOADS}"
        )
    env = environment if environment is not None else load_json(
        require_file(bundle, "environment.json")
    )
    wl = workload if workload is not None else load_json(require_file(bundle, "workload.json"))
    if env.get("schema_revision") != PHASE12_SCHEMA_REVISION:
        raise ArtifactError("environment.schema_revision mismatch")
    if wl.get("qualification_profile") != PHASE12_QUALIFICATION_PROFILE:
        raise ArtifactError("workload.qualification_profile mismatch")
    if wl.get("priority_workload") != workload_name:
        raise ArtifactError("workload.priority_workload mismatch")
    if wl.get("physical_signature_digest") != expected_digest:
        raise ArtifactError("workload.physical_signature_digest mismatch")
    plan_path = require_file(bundle, "plans/claim-priority.json")
    plan = load_json(plan_path)
    plan_body = plan.get("Plan") if isinstance(plan.get("Plan"), dict) else plan
    if not isinstance(plan_body, dict):
        raise ArtifactError("plans/claim-priority.json missing Plan object")
    from benchmarks.qualification.postgres_probe import validate_claim_explain_plan

    validate_claim_explain_plan(plan_body)


def validate_bindings(bundle: Path, manifest: Mapping[str, Any]) -> None:
    environment = load_json(require_file(bundle, "environment.json"))
    workload = load_json(require_file(bundle, "workload.json"))
    before = load_json(require_file(bundle, "postgres-before.json"))
    after = load_json(require_file(bundle, "postgres-after.json"))
    if environment.get("environment_hash") != manifest["environment_hash"]:
        raise ArtifactError("environment.json hash does not match manifest")
    if workload.get("workload_hash") != manifest["workload_hash"]:
        raise ArtifactError("workload.json hash does not match manifest")
    if before.get("environment_hash") != manifest["environment_hash"]:
        raise ArtifactError("postgres-before.json environment_hash mismatch")
    if after.get("environment_hash") != manifest["environment_hash"]:
        raise ArtifactError("postgres-after.json environment_hash mismatch")
    if environment.get("git_sha") != manifest.get("git_sha"):
        raise ArtifactError("environment git_sha does not match manifest")
    if environment.get("image_digests") != manifest.get("image_digests"):
        raise ArtifactError("environment image_digests do not match manifest")
    if manifest.get("qualification_profile") == PHASE12_QUALIFICATION_PROFILE:
        validate_phase12_priority_bindings(
            bundle,
            manifest,
            environment=environment,
            workload=workload,
        )


def parse_conformance(bundle: Path, manifest: Mapping[str, Any]) -> None:
    path = require_file(bundle, "conformance.xml")
    try:
        root = ET.fromstring(path.read_text(encoding="utf-8"))
    except ET.ParseError as exc:
        raise ArtifactError(f"conformance.xml is not well-formed: {exc}") from exc

    suites = [root] if root.tag.endswith("testsuite") else list(root.iter("testsuite"))
    if not suites:
        raise ArtifactError("conformance.xml contains no testsuite elements")

    variants_seen: set[str] = set()
    total_failures = total_errors = total_skipped = total_tests = 0
    for suite in suites:
        total_failures += int(suite.attrib.get("failures", "0"))
        total_errors += int(suite.attrib.get("errors", "0"))
        total_skipped += int(suite.attrib.get("skipped", "0"))
        total_tests += int(suite.attrib.get("tests", "0"))
        for prop in suite.findall("./properties/property"):
            name = prop.attrib.get("name")
            value = prop.attrib.get("value", "")
            if name == "client_variant":
                variants_seen.add(value)
            elif name == "environment_hash" and value != manifest["environment_hash"]:
                raise ArtifactError("conformance environment_hash mismatch")
            elif name == "workload_hash" and value != manifest["workload_hash"]:
                raise ArtifactError("conformance workload_hash mismatch")
            elif name == "run_id" and value != manifest["run_id"]:
                raise ArtifactError("conformance run_id mismatch")

    if total_tests <= 0:
        raise ArtifactError("conformance.xml reports zero tests")
    if total_failures or total_errors or total_skipped:
        raise ArtifactError(
            "conformance.xml must have zero failures/errors/skips "
            f"(failures={total_failures}, errors={total_errors}, skipped={total_skipped})"
        )
    if variants_seen != {"raw_http", "sdk"}:
        raise ArtifactError(
            f"conformance.xml must cover raw_http and sdk, got {sorted(variants_seen)}"
        )


def validate_plans(bundle: Path) -> list[str]:
    index = load_json(require_file(bundle, "plans/index.json"))
    files = index.get("files")
    if not isinstance(files, list) or not files:
        raise ArtifactError("plans/index.json must list at least one EXPLAIN file")
    seen: set[str] = set()
    paths: list[str] = []
    for entry in files:
        if not isinstance(entry, Mapping) or "path" not in entry:
            raise ArtifactError("plans/index.json entries must include path")
        relative = str(entry["path"])
        if relative in seen:
            raise ArtifactError(f"duplicate EXPLAIN path: {relative}")
        seen.add(relative)
        if not relative.startswith("plans/") or relative == "plans/index.json":
            raise ArtifactError(f"invalid EXPLAIN path: {relative}")
        require_file(bundle, relative)
        paths.append(relative)
    for path in (bundle / "plans").rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(bundle).as_posix()
        if relative == "plans/index.json":
            continue
        if relative not in seen:
            raise ArtifactError(f"unindexed EXPLAIN file: {relative}")
    return paths


def load_latency_samples(bundle: Path) -> list[LatencySample]:
    path = require_file(bundle, "latencies.jsonl.gz")
    samples: list[LatencySample] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ArtifactError(f"latencies.jsonl.gz line {line_no}: {exc}") from exc
            try:
                outcome = OUTCOME_MAP[str(row["outcome"])]
            except KeyError as exc:
                raise ArtifactError(
                    f"latencies.jsonl.gz line {line_no}: unknown outcome {row.get('outcome')!r}"
                ) from exc
            samples.append(
                LatencySample(
                    operation=str(row["operation"]),
                    scenario=str(row["scenario"]),
                    latency_ns=int(row["latency_ns"]),
                    outcome=outcome,
                    fan_out=int(row.get("fan_out", 0)),
                )
            )
    if not samples:
        raise ArtifactError("latencies.jsonl.gz contains no samples")
    return samples


def validate_raw(bundle: Path) -> str:
    """Validate the eight raw classes. Writes a stamp; writes no derived artifacts."""
    bundle = bundle.resolve()
    if not bundle.is_dir():
        raise ArtifactError(f"bundle directory not found: {bundle}")
    for relative in RAW_CLASSES:
        require_file(bundle, relative)

    manifest = load_json(bundle / "manifest.json")
    stage = manifest.get("bundle_stage")
    if stage not in {"raw", "final"}:
        raise ArtifactError("manifest.bundle_stage must be raw or final")
    validate_manifest_shape(manifest, expect_stage=str(stage))
    validate_bindings(bundle, manifest)
    parse_conformance(bundle, manifest)
    validate_plans(bundle)
    samples = load_latency_samples(bundle)
    workload = load_json(bundle / "workload.json")
    validate_evidence_disclosure(manifest, workload, sample_count=len(samples))

    digest = raw_set_digest(bundle)
    write_json(
        bundle / VALIDATION_STAMP,
        {"validated_raw_set_digest": digest, "raw_classes": list(RAW_CLASSES)},
    )
    return digest


def require_raw_stamp(bundle: Path) -> str:
    stamp_path = bundle / VALIDATION_STAMP
    if not stamp_path.is_file():
        raise ArtifactError("derive requires validate-raw success for this bundle")
    stamp = load_json(stamp_path)
    current = raw_set_digest(bundle)
    if stamp.get("validated_raw_set_digest") != current:
        raise ArtifactError("validate-raw stamp does not match current raw hashes")
    return current


def operation_rows(samples: Iterable[LatencySample]) -> list[dict[str, Any]]:
    stats_map = aggregate_operation_stats(samples)
    rows: list[dict[str, Any]] = []
    for (operation, scenario), stats in sorted(stats_map.items()):
        row: dict[str, Any] = {
            "operation": operation,
            "scenario": scenario,
            "count": stats.count,
            "valid_attempts": stats.valid_attempts,
            "successes": stats.successes,
            "errors": stats.errors,
            "planned_pause": stats.planned_pause,
            "planned_drain": stats.planned_drain,
            "invalid_requests": stats.invalid_requests,
            "p50_ns": stats.p50_ns,
            "p99_ns": stats.p99_ns,
        }
        row["success_ratio"] = (
            success_ratio(stats.successes, stats.valid_attempts)
            if stats.valid_attempts > 0
            else None
        )
        rows.append(row)
    return rows


def compute_qualification(
    samples: Iterable[LatencySample],
    workload: Mapping[str, Any],
    *,
    evidence_mode: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], float, float, int, float, str]:
    """Recompute operations, checks, aggregates and verdict from raw samples + workload.

    Shared by ``derive`` (write path) and ``validate_final`` (re-agreement).

    Threshold success on non-live evidence yields ``CI_SYNTHETIC_PASS`` so CI
    fixtures cannot be mistaken for a live production qualification PASS.
    """
    operations = operation_rows(samples)
    measured_seconds = float(workload.get("measured_seconds") or 0)
    successful_claims = int(workload.get("successful_claims") or 0)
    claims_per_second = (
        successful_claims / measured_seconds if measured_seconds > 0 else 0.0
    )
    total_valid = sum(int(row["valid_attempts"]) for row in operations)
    total_success = sum(int(row["successes"]) for row in operations)
    overall_ratio = (
        success_ratio(total_success, total_valid) if total_valid > 0 else 0.0
    )

    def p99(operation: str, scenario: str) -> int | None:
        for row in operations:
            if row["operation"] == operation and row["scenario"] == scenario:
                return None if row["p99_ns"] is None else int(row["p99_ns"])
        return None

    hot_path_scope = str(workload.get("hot_path_scope") or "full")
    hot = {
        "enqueue": p99("enqueue", "baseline"),
        "claim": p99("claim", "baseline"),
        "heartbeat": p99("heartbeat", "baseline"),
    }
    baseline_complete = p99("complete", "baseline")
    max_fanout_complete = p99("complete", "max_fanout")

    claim_only_note = (
        "claim-only qualification scope; enqueue/heartbeat/complete not measured"
    )
    checks: dict[str, Any] = {
        "claims_per_second": {
            "actual": claims_per_second,
            "limit": MIN_CLAIMS_PER_SECOND,
            "pass": claims_per_second >= MIN_CLAIMS_PER_SECOND,
        },
        "valid_success_ratio": {
            "actual": overall_ratio,
            "limit": MIN_SUCCESS_RATIO,
            "pass": overall_ratio >= MIN_SUCCESS_RATIO,
        },
        "p99_claim_ns": {
            "actual": hot["claim"],
            "limit": P99_HOT_NS,
            "pass": hot["claim"] is not None and hot["claim"] <= P99_HOT_NS,
        },
        "max_fanout_complete_p99_ns": {
            "actual": max_fanout_complete,
            "limit": None,
            "pass": True,
            "note": "reported separately; never merged into baseline complete p99",
        },
    }
    if hot_path_scope == "claim-only":
        checks["p99_enqueue_ns"] = {
            "actual": hot["enqueue"],
            "limit": P99_HOT_NS,
            "pass": True,
            "note": claim_only_note,
        }
        checks["p99_heartbeat_ns"] = {
            "actual": hot["heartbeat"],
            "limit": P99_HOT_NS,
            "pass": True,
            "note": claim_only_note,
        }
        checks["p99_baseline_complete_ns"] = {
            "actual": baseline_complete,
            "limit": P99_BASELINE_COMPLETE_NS,
            "pass": True,
            "note": claim_only_note,
        }
    else:
        checks["p99_enqueue_ns"] = {
            "actual": hot["enqueue"],
            "limit": P99_HOT_NS,
            "pass": hot["enqueue"] is not None and hot["enqueue"] <= P99_HOT_NS,
        }
        checks["p99_heartbeat_ns"] = {
            "actual": hot["heartbeat"],
            "limit": P99_HOT_NS,
            "pass": hot["heartbeat"] is not None and hot["heartbeat"] <= P99_HOT_NS,
        }
        checks["p99_baseline_complete_ns"] = {
            "actual": baseline_complete,
            "limit": P99_BASELINE_COMPLETE_NS,
            "pass": baseline_complete is not None
            and baseline_complete <= P99_BASELINE_COMPLETE_NS,
        }
    thresholds_ok = all(bool(item["pass"]) for item in checks.values())
    if not thresholds_ok:
        verdict = "FAIL"
    elif evidence_mode in (None, "live"):
        verdict = "PASS"
    else:
        # synthetic | compressed — never silent production PASS
        verdict = "CI_SYNTHETIC_PASS"
    return (
        operations,
        checks,
        overall_ratio,
        claims_per_second,
        successful_claims,
        measured_seconds,
        verdict,
    )


def derive(bundle: Path, *, allow_synthetic: bool = False) -> None:
    """Write summary.json, qualification.json and report.md from validated raw evidence."""
    bundle = bundle.resolve()
    digest = require_raw_stamp(bundle)
    manifest = load_json(bundle / "manifest.json")
    if manifest.get("bundle_stage") != "raw":
        raise ArtifactError("derive requires bundle_stage=raw")

    samples = load_latency_samples(bundle)
    workload = load_json(bundle / "workload.json")
    assert_evidence_mode_for_qualification(
        str(manifest.get("evidence_mode") or workload.get("evidence_mode")),
        allow_synthetic=allow_synthetic,
    )
    before = load_json(bundle / "postgres-before.json")
    after = load_json(bundle / "postgres-after.json")

    evidence_mode = str(
        manifest.get("evidence_mode") or workload.get("evidence_mode") or ""
    )
    (
        operations,
        checks,
        overall_ratio,
        claims_per_second,
        successful_claims,
        measured_seconds,
        verdict,
    ) = compute_qualification(samples, workload, evidence_mode=evidence_mode)

    max_fanout_complete = checks["max_fanout_complete_p99_ns"]["actual"]
    hot = {
        "enqueue": checks["p99_enqueue_ns"]["actual"],
        "claim": checks["p99_claim_ns"]["actual"],
        "heartbeat": checks["p99_heartbeat_ns"]["actual"],
    }
    baseline_complete = checks["p99_baseline_complete_ns"]["actual"]

    summary = {
        "run_id": manifest["run_id"],
        "validated_raw_set_digest": digest,
        "operations": operations,
        "throughput": {
            "successful_claims": successful_claims,
            "measured_seconds": measured_seconds,
            "claims_per_second": claims_per_second,
        },
        "postgres": {
            "wal_bytes": int(after.get("wal_bytes", 0)) - int(before.get("wal_bytes", 0)),
            "buffer_hits": int(after.get("buffer_hits", 0)),
            "dead_tuples": int(after.get("dead_tuples", 0)),
            "autovacuum_lag_seconds": int(after.get("autovacuum_lag_seconds", 0)),
            "touched_partitions": list(after.get("touched_partitions") or []),
            "planning_time_ms": after.get("planning_time_ms"),
            "execution_time_ms": after.get("execution_time_ms"),
        },
        "overall_valid_success_ratio": overall_ratio,
        "max_fanout_complete_p99_ns": max_fanout_complete,
    }
    production_qualified = evidence_mode == "live" and verdict == "PASS"
    if verdict == "CI_SYNTHETIC_PASS":
        verdict_note = (
            "Synthetic CI evidence met numeric thresholds; "
            "NOT a live production qualification PASS."
        )
    elif verdict == "PASS":
        verdict_note = "Live reference-host evidence met ADR 019 thresholds."
    else:
        verdict_note = "One or more ADR 019 thresholds failed."

    qualification = {
        "run_id": manifest["run_id"],
        "validated_raw_set_digest": digest,
        "evidence_mode": evidence_mode,
        "production_qualified": production_qualified,
        "thresholds": {
            "claims_per_second_min": MIN_CLAIMS_PER_SECOND,
            "valid_success_ratio_min": MIN_SUCCESS_RATIO,
            "p99_enqueue_claim_heartbeat_ns_max": P99_HOT_NS,
            "p99_baseline_complete_ns_max": P99_BASELINE_COMPLETE_NS,
        },
        "checks": checks,
        "verdict": verdict,
        "verdict_note": verdict_note,
        "sources": {
            "adr_019": "docs/04-architecture/adr/019-initial-production-gate.md",
            "adr_023": "docs/04-architecture/adr/023-qualified-kernel-storage-profile.md",
            "observability": "docs/05-operations/03-observability.md",
        },
    }
    lines = [
        f"# Qualification report for `{manifest['run_id']}`",
        "",
        f"Verdict: **{verdict}**",
        "",
        f"Validated raw digest: `{digest}`",
        "",
        f"Claims/s: {claims_per_second:.3f} (min {MIN_CLAIMS_PER_SECOND})",
        f"Valid success ratio: {overall_ratio:.6f} (min {MIN_SUCCESS_RATIO})",
        f"p99 enqueue/claim/heartbeat ns: {hot}",
        f"p99 baseline complete ns: {baseline_complete}",
        f"p99 max-fanout complete ns (separate): {max_fanout_complete}",
        "",
        "## Operations",
        "",
    ]
    for row in operations:
        lines.append(
            f"- {row['operation']}/{row['scenario']}: "
            f"valid={row['valid_attempts']} success={row['successes']} "
            f"p50={row['p50_ns']} p99={row['p99_ns']}"
        )
    write_json(bundle / "summary.json", summary)
    write_json(bundle / "qualification.json", qualification)
    (bundle / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def checksum_targets(bundle: Path) -> list[str]:
    index = load_json(bundle / "plans" / "index.json")
    explain = [str(entry["path"]) for entry in index["files"]]
    return [
        "manifest.json",
        "environment.json",
        "workload.json",
        "conformance.xml",
        "latencies.jsonl.gz",
        "postgres-before.json",
        "postgres-after.json",
        "plans/index.json",
        "summary.json",
        "qualification.json",
        "report.md",
        *explain,
    ]


def write_checksums(bundle: Path) -> None:
    """Promote bundle_stage to final and write SHA256SUMS for artifacts 1-11 + EXPLAINs."""
    bundle = bundle.resolve()
    digest = require_raw_stamp(bundle)
    for relative in DERIVED_CLASSES:
        require_file(bundle, relative)

    manifest = load_json(bundle / "manifest.json")
    if manifest.get("bundle_stage") != "raw":
        raise ArtifactError("checksums requires bundle_stage=raw")
    manifest["bundle_stage"] = "final"
    manifest["validated_raw_set_digest"] = digest
    write_json(bundle / "manifest.json", manifest)

    lines = [
        f"{sha256_file(bundle / relative)}  {relative}"
        for relative in checksum_targets(bundle)
    ]
    (bundle / CHECKSUM_CLASS).write_text("\n".join(lines) + "\n", encoding="utf-8")


def verify_checksums(bundle: Path) -> None:
    sums_path = require_file(bundle, CHECKSUM_CLASS)
    expected: dict[str, str] = {}
    for line in sums_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        digest_hex, relative = text.split(maxsplit=1)
        relative = relative.lstrip("*").strip()
        if relative == CHECKSUM_CLASS:
            raise ArtifactError("SHA256SUMS must not checksum itself")
        expected[relative] = digest_hex
    targets = checksum_targets(bundle)
    if set(expected) != set(targets):
        raise ArtifactError(
            "SHA256SUMS entries do not match required artifact set: "
            f"missing={sorted(set(targets) - set(expected))} "
            f"extra={sorted(set(expected) - set(targets))}"
        )
    for relative, digest_hex in expected.items():
        if sha256_file(bundle / relative) != digest_hex:
            raise ArtifactError(f"checksum mismatch for {relative}")


def validate_final(bundle: Path, *, allow_synthetic: bool = False) -> None:
    """Read-only final validation: checksums, stage, bindings and threshold agreement."""
    bundle = bundle.resolve()
    if not bundle.is_dir():
        raise ArtifactError(f"bundle directory not found: {bundle}")

    top = list_top_level(bundle)
    top.discard(VALIDATION_STAMP)
    if top != set(FINAL_TOP_LEVEL_NAMES):
        raise ArtifactError(
            "final bundle must contain exactly the 12 authoritative top-level classes: "
            f"got {sorted(top)}"
        )

    verify_checksums(bundle)
    manifest = load_json(bundle / "manifest.json")
    validate_manifest_shape(manifest, expect_stage="final")
    validate_bindings(bundle, manifest)
    parse_conformance(bundle, manifest)
    validate_plans(bundle)
    samples = load_latency_samples(bundle)
    workload = load_json(bundle / "workload.json")
    validate_evidence_disclosure(manifest, workload, sample_count=len(samples))
    assert_evidence_mode_for_qualification(
        str(manifest.get("evidence_mode") or workload.get("evidence_mode")),
        allow_synthetic=allow_synthetic,
    )

    current_digest = raw_set_digest(bundle)
    if current_digest != manifest["validated_raw_set_digest"]:
        raise ArtifactError("validated_raw_set_digest does not match current raw bytes")

    summary = load_json(bundle / "summary.json")
    qualification = load_json(bundle / "qualification.json")
    require_file(bundle, "report.md")

    evidence_mode = str(
        manifest.get("evidence_mode") or workload.get("evidence_mode") or ""
    )
    (
        recomputed_ops,
        recomputed_checks,
        overall_ratio,
        claims_per_second,
        successful_claims,
        measured_seconds,
        verdict,
    ) = compute_qualification(samples, workload, evidence_mode=evidence_mode)

    if summary.get("operations") != recomputed_ops:
        raise ArtifactError("summary.json operations do not match recomputed statistics")
    if summary.get("validated_raw_set_digest") != current_digest:
        raise ArtifactError("summary.json digest mismatch")
    if qualification.get("validated_raw_set_digest") != current_digest:
        raise ArtifactError("qualification.json digest mismatch")

    if summary.get("overall_valid_success_ratio") != overall_ratio:
        raise ArtifactError(
            "summary.json overall_valid_success_ratio does not match recomputed ratio"
        )
    throughput = summary.get("throughput") or {}
    if throughput.get("claims_per_second") != claims_per_second:
        raise ArtifactError(
            "summary.json throughput.claims_per_second does not match recomputed value"
        )
    if throughput.get("successful_claims") != successful_claims:
        raise ArtifactError(
            "summary.json throughput.successful_claims does not match workload"
        )
    if float(throughput.get("measured_seconds") or 0) != measured_seconds:
        raise ArtifactError(
            "summary.json throughput.measured_seconds does not match workload"
        )

    if qualification.get("checks") != recomputed_checks:
        raise ArtifactError(
            "qualification.json checks do not match recomputed threshold calculations"
        )
    if qualification.get("verdict") != verdict:
        raise ArtifactError(
            f"qualification.json verdict {qualification.get('verdict')!r} "
            f"does not match recomputed verdict {verdict!r}"
        )
    if qualification.get("evidence_mode") != evidence_mode:
        raise ArtifactError(
            "qualification.json evidence_mode does not match manifest/workload"
        )
    expected_prod = evidence_mode == "live" and verdict == "PASS"
    if qualification.get("production_qualified") != expected_prod:
        raise ArtifactError(
            "qualification.json production_qualified does not match evidence/verdict"
        )
    if not qualification.get("verdict_note"):
        raise ArtifactError("qualification.json missing verdict_note")
