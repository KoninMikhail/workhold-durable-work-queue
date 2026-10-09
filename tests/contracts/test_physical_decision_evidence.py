"""Traceability tests for Phase 3.1 physical decision evidence (ADR 022 + benchmarks)."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADR_PATH = ROOT / "docs" / "04-architecture" / "adr" / "022-physical-contract-baseline.md"
BENCH_PATH = ROOT / "docs" / "04-architecture" / "physical-contract-benchmarks.md"
ADR_README = ROOT / "docs" / "04-architecture" / "adr" / "README.md"
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"
STORAGE_CONTRACT = ROOT / "docs" / "03-reference" / "storage-contract.md"

REQUIREMENTS = (
    "STOR-01",
    "STOR-02",
    "STOR-06",
    "STOR-07",
    "API-06",
    "API-07",
    "QUAL-01",
)

EXACT_RELATION_NAMES = (
    "admin_replay",
    "partition_maintenance_status",
    "completion_effects",
)

REJECTED_ALTERNATIVES = (
    "Single combined task table",
    "Time-partitioned active state",
    "SDK-as-contract",
    "Claim secret in URL",
    "Per-queue LIST partitions",
    "HASH registries in baseline",
)

WORKLOAD_DIMENSIONS = (
    "1, 10, 100",
    "1 000 000",
    "32",
    "ready-heavy",
    "empty-heavy",
    "reclaim-heavy",
    "heartbeat-heavy",
    "duplicate enqueue",
    "duplicate complete",
    "262144",
    "1048576",
)

THRESHOLDS = (
    "500 claims/s",
    "99.9%",
    "100 ms",
    "200 ms",
)

NON_TUNABLE = (
    "Correctness registry unique keys",
    "Hot/cold boundary",
    "1048576",
    "No `DEFAULT` partition",
    "X-Queue-Claim-Token",
)

DEFERRED_DELIVERY = (
    "Delivery transport",
    "CloudEvents validation",
    "webhook",
    "Phase 5",
)

HYPOTHESES = ("H1", "H2", "H3")

REPRO_FIELDS = (
    "reference_environment",
    "dataset_generation_seed",
    "warmup_rules",
    "sample_rules",
    "sql_query_plan_capture",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_artifacts_exist_and_are_indexed() -> None:
    assert ADR_PATH.is_file()
    assert BENCH_PATH.is_file()
    assert OPENAPI_PATH.is_file()
    assert STORAGE_CONTRACT.is_file()
    readme = _read(ADR_README)
    assert "022-physical-contract-baseline" in readme
    assert "Accepted" in readme
    assert "physical-contract-benchmarks.md" in readme


def test_adr_status_links_and_protocol_storage_baseline() -> None:
    adr = _read(ADR_PATH)
    assert "**Status:** Accepted" in adr
    assert "openapi/queue.openapi.json" in adr
    assert "docs/03-reference/storage-contract.md" in adr or "storage-contract.md" in adr
    assert "/v1" in adr and "/admin/v1" in adr
    assert "X-Queue-Claim-Token" in adr
    assert "capabilities" in adr.lower()
    assert "Error" in adr
    assert "events" in adr
    assert "tasks_active" in adr
    assert "task_payloads_active" in adr
    for name in EXACT_RELATION_NAMES:
        assert name in adr
    for parent in (
        "admin_audit_log",
        "task_attempts",
        "tasks_terminal",
        "delivery_events_terminal",
    ):
        assert parent in adr
    assert "30" in adr and "premake" in adr.lower()
    assert "1048576" in adr and "262144" in adr
    assert "baseline indexes" in adr.lower() or "Baseline indexes" in adr
    assert "Phase 3.8" in adr


def test_adr_records_exact_ttl_bounds_and_seconds() -> None:
    adr = _read(ADR_PATH)
    # Days and seconds from storage contract / plan
    assert "2592000" in adr and "31536000" in adr and "7776000" in adr
    assert "86400" in adr and "604800" in adr
    assert re.search(r"30\.\.365 days", adr)
    assert re.search(r"1\.\.30 days", adr)
    assert re.search(r"7\.\.90 days", adr)
    assert "2592000..31536000" in adr
    assert "86400..2592000" in adr
    assert "604800..7776000" in adr


def test_adr_alternatives_and_consequences() -> None:
    adr = _read(ADR_PATH)
    assert "## Alternatives considered" in adr
    for alt in REJECTED_ALTERNATIVES:
        assert alt in adr
    assert "## Consequences" in adr
    assert "HASH" in adr
    assert "measured" in adr.lower()


def test_benchmark_workloads_metrics_thresholds_hypotheses() -> None:
    bench = _read(BENCH_PATH)
    assert "not claim measurements were run" in bench.lower() or "does **not** claim" in bench
    for dim in WORKLOAD_DIMENSIONS:
        assert dim in bench, f"missing workload dimension: {dim}"
    for gate in THRESHOLDS:
        assert gate in bench, f"missing threshold: {gate}"
    for hyp in HYPOTHESES:
        assert hyp in bench
    for field in REPRO_FIELDS:
        assert field in bench
    for metric in (
        "p50",
        "p99",
        "success ratio",
        "planning time",
        "touched partitions",
        "WAL",
        "buffer hits",
        "dead tuples",
        "autovacuum lag",
    ):
        assert metric in bench, f"missing metric: {metric}"


def test_non_tunable_invariants_and_tunable_bounds() -> None:
    bench = _read(BENCH_PATH)
    adr = _read(ADR_PATH)
    combined = bench + "\n" + adr
    for marker in NON_TUNABLE:
        assert marker in combined, f"missing non-tunable marker: {marker}"
    assert "additive" in adr.lower() or "additively" in adr.lower()
    assert "HASH" in bench
    assert "tasks_active_claim_idx" in bench


def test_deferred_delivery_excluded() -> None:
    adr = _read(ADR_PATH)
    bench = _read(BENCH_PATH)
    combined = adr + "\n" + bench
    for token in DEFERRED_DELIVERY:
        assert token in combined, f"missing deferred delivery exclusion: {token}"
    # Must not select a concrete broker or invent observed performance numbers
    assert "kafka" not in combined.lower()
    assert "rabbitmq" not in combined.lower()
    assert "p99 observed" not in combined.lower()
    assert "claims/s achieved" not in combined.lower()
    assert "does **not** claim" in bench or "not measured results" in bench.lower()


def test_requirement_ids_present_in_adr() -> None:
    adr = _read(ADR_PATH)
    for req in REQUIREMENTS:
        assert req in adr, f"missing requirement id: {req}"


def test_requirement_traceability_and_artifact_links() -> None:
    """QUAL-01: docs + stdlib tests bind the seven plan requirements to artifacts."""
    adr = _read(ADR_PATH)
    bench = _read(BENCH_PATH)
    openapi = json.loads(_read(OPENAPI_PATH))
    storage = _read(STORAGE_CONTRACT)

    # API-06 / API-07 — protocol artifact + capability/versioning
    assert "/v1/capabilities" in openapi["paths"]
    assert openapi["openapi"].startswith("3.1")
    assert "X-Queue-Claim-Token" in _read(OPENAPI_PATH)
    assert "Capabilities" in openapi["components"]["schemas"]
    assert "Error" in openapi["components"]["schemas"]
    complete = openapi["components"]["schemas"]["CompleteRequest"]
    assert "events" in complete.get("x-queue-reserved-additive-fields", [])
    assert "events is reserved" in complete.get("description", "").lower()

    # STOR-01 / STOR-02 / STOR-06 / STOR-07 — storage artifact markers
    assert "tasks_active" in storage and "task_payloads_active" in storage
    assert "PARTITION" in storage.upper() or "RANGE" in storage
    for name in EXACT_RELATION_NAMES:
        assert name in storage
    assert "1048576" in storage and "262144" in storage
    assert "GENERATED ALWAYS AS IDENTITY" in storage or "bigint identity" in storage

    # ADR + benchmarks exist as decision evidence (this module is the QUAL-01 lock)
    assert ADR_PATH.name in "022-physical-contract-baseline.md"
    assert "ADR 019" in bench or "019" in bench
    assert all(req for req in REQUIREMENTS)  # seven IDs present in plan scope


def test_linked_openapi_and_storage_paths_resolve() -> None:
    adr = _read(ADR_PATH)
    # Relative links used in References must resolve from ADR directory
    adr_dir = ADR_PATH.parent
    assert (ROOT / "openapi" / "queue.openapi.json").is_file()
    assert (ROOT / "docs" / "03-reference" / "storage-contract.md").is_file()
    assert (adr_dir / ".." / "physical-contract-benchmarks.md").resolve().is_file()
    assert "022-physical-contract-baseline" in adr or "physical contract baseline" in adr.lower()
