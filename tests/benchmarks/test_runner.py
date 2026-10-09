"""Rate-controlled capacity runner and PostgreSQL evidence paths (QUAL-03)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKLOAD_PATH = ROOT / "benchmarks" / "qualification" / "workloads" / "kernel-capacity.yaml"
PROFILE_PATH = ROOT / "benchmarks" / "qualification" / "reference-environment.yaml"

FINALIZATION_ARTIFACTS = (
    "summary.json",
    "qualification.json",
    "report.md",
    "SHA256SUMS",
    ".immutable",
    "PASS",
)

RUNNER_RAW_CLASSES = (
    "manifest.json",
    "environment.json",
    "workload.json",
    "latencies.jsonl.gz",
    "postgres-before.json",
    "postgres-after.json",
    "plans/index.json",
)


def test_full_mode_cannot_complete_before_million_terminals_and_300s() -> None:
    from benchmarks.qualification.runner import FullModeCompletionGate

    gate = FullModeCompletionGate(
        terminal_lifecycles=1_000_000,
        min_measured_seconds=300,
    )
    assert not gate.is_complete(terminals=999_999, measured_seconds=10_000)
    assert not gate.is_complete(terminals=1_000_000, measured_seconds=299.999)
    assert not gate.is_complete(terminals=1_000_001, measured_seconds=300)
    assert gate.is_complete(terminals=1_000_000, measured_seconds=300)
    assert gate.is_complete(terminals=1_000_000, measured_seconds=301)


def test_monotonic_rate_control_never_exceeds_target() -> None:
    from benchmarks.qualification.load import MonotonicRateController

    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    ctrl = MonotonicRateController(target_per_second=500.0, now=now)
    permits = 0
    # Simulate 2 seconds of wall time in 1ms steps.
    for _ in range(2_000):
        if ctrl.try_acquire():
            permits += 1
        clock["t"] += 0.001
    assert permits <= 1_000
    assert permits >= 990  # allow tiny undershoot from discrete steps
    # Rate never goes negative / backwards: acquired count is monotone.
    assert ctrl.acquired == permits
    assert ctrl.acquired >= 0


def test_round_robin_queues_and_claimer_cap() -> None:
    from benchmarks.qualification.load import assign_queues_round_robin, clamp_claimers

    assert clamp_claimers(64, max_claimers=32) == 32
    assert clamp_claimers(8, max_claimers=32) == 8
    queues = assign_queues_round_robin(queue_count=10, claimer_count=8)
    assert len(queues) == 8
    assert queues == [0, 1, 2, 3, 4, 5, 6, 7]
    queues100 = assign_queues_round_robin(queue_count=100, claimer_count=32)
    assert len(queues100) == 32
    assert queues100[0] == 0
    assert queues100[31] == 31


def test_smoke_runner_writes_raw_seven_classes_without_finalization(
    tmp_path: Path,
) -> None:
    from benchmarks.qualification.runner import run_capacity

    output = tmp_path / "phase-3.9-smoke"
    result = run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="smoke",
        output_dir=output,
        backend="simulated",
        cell_limit=1,
    )
    assert result.bundle_stage == "raw"
    assert result.aborted is False
    for relative in RUNNER_RAW_CLASSES:
        assert (output / relative).is_file(), relative
    assert not (output / "conformance.xml").exists()
    for forbidden in FINALIZATION_ARTIFACTS:
        assert not (output / forbidden).exists(), forbidden

    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["bundle_stage"] == "raw"
    # Schema lists derived/checksum class names for later promotion; smoke must
    # not materialize those files (Plan 06 contract + Plan 08 smoke raw subset).
    assert manifest["artifact_classes"]["derived"] == [
        "summary.json",
        "qualification.json",
        "report.md",
    ]
    assert not (output / "summary.json").exists()
    # Runner records the eight raw class names but does not write conformance yet.
    assert manifest["artifact_classes"]["raw"][3] == "conformance.xml"

    with gzip.open(output / "latencies.jsonl.gz", "rt", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    assert lines
    for row in lines:
        assert "payload" not in row
        assert "idempotency_key" not in row
        assert "claim_token" not in row


def test_hot_operations_emit_latency_postgres_and_explain(tmp_path: Path) -> None:
    from benchmarks.qualification.postgres_probe import HOT_OPERATIONS
    from benchmarks.qualification.runner import run_capacity

    output = tmp_path / "probe-run"
    run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="smoke",
        output_dir=output,
        backend="simulated",
        cell_limit=1,
    )
    before = json.loads((output / "postgres-before.json").read_text(encoding="utf-8"))
    after = json.loads((output / "postgres-after.json").read_text(encoding="utf-8"))
    for key in (
        "buffers",
        "wal",
        "dead_tuples",
        "autovacuum",
        "captured_at_utc",
    ):
        assert key in before
        assert key in after

    index = json.loads((output / "plans" / "index.json").read_text(encoding="utf-8"))
    paths = {entry["path"] for entry in index["files"]}
    for operation in HOT_OPERATIONS:
        matched = [p for p in paths if f"/{operation}." in p or p.endswith(f"{operation}.json")]
        assert matched, f"missing EXPLAIN for {operation}"
        explain = json.loads((output / matched[0]).read_text(encoding="utf-8"))
        assert "Plan" in explain or "QUERY PLAN" in explain or "plan" in explain

    text_blob = (output / "postgres-before.json").read_text(encoding="utf-8")
    text_blob += (output / "postgres-after.json").read_text(encoding="utf-8")
    assert "idempotency" not in text_blob.lower() or "idempotency_key" not in text_blob
    assert "claim_token" not in text_blob
    assert "payload" not in text_blob or '"payload"' not in text_blob


def test_clean_abort_marker(tmp_path: Path) -> None:
    from benchmarks.qualification.runner import run_capacity

    output = tmp_path / "aborted"
    result = run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="smoke",
        output_dir=output,
        backend="simulated",
        cell_limit=1,
        force_abort_after_cells=0,
    )
    assert result.aborted is True
    marker = output / "abort.json"
    assert marker.is_file()
    payload: dict[str, Any] = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["aborted"] is True
    assert "reason" in payload
    assert "payload" not in payload
    assert "claim_token" not in payload


def test_full_mode_simulated_completion_requires_exact_terminals(tmp_path: Path) -> None:
    """Compressed full-mode sustain: observed gate targets via injectable clock."""
    from benchmarks.qualification.runner import SimulatedCapacityBackend, run_capacity

    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(dt: float) -> None:
        clock["t"] += float(dt)

    output = tmp_path / "full-sim"
    result = run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="full",
        output_dir=output,
        backend="simulated",
        cell_limit=1,
        now=now,
        sleep=sleep,
        workload_backend=SimulatedCapacityBackend(),
        # Unit path uses reduced gate targets; production YAML remains 1M / 300s.
        gate_terminal_lifecycles=1_000,
        gate_min_measured_seconds=3.0,
        full_target_claims_per_second=500.0,
        full_warmup_seconds=0.0,
    )
    assert result.terminal_lifecycles == 1_000
    assert result.measured_seconds >= 3.0
    workload = json.loads((output / "workload.json").read_text(encoding="utf-8"))
    assert workload["terminal_lifecycles"] == 1_000
    assert workload["measured_seconds"] >= 3.0


def test_cli_module_entrypoint_help() -> None:
    import subprocess
    import sys

    proc = subprocess.run(
        [sys.executable, "-m", "benchmarks.qualification.runner", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "--mode" in proc.stdout
    assert "--workload" in proc.stdout
    assert "smoke" in proc.stdout


def test_run_capacity_full_fails_when_observed_under_target(tmp_path: Path) -> None:
    """Full mode must gate on observed work, not YAML defaults (review fix)."""
    from benchmarks.qualification.runner import SimulatedCapacityBackend, run_capacity

    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(dt: float) -> None:
        clock["t"] += float(dt)

    # Backend exhausts after 5 terminals; gate wants 20 over ≥2s → must fail.
    backend = SimulatedCapacityBackend(max_terminals=5)
    output = tmp_path / "full-under"
    with pytest.raises(RuntimeError, match="full mode cannot complete"):
        run_capacity(
            profile_path=PROFILE_PATH,
            workload_path=WORKLOAD_PATH,
            mode="full",
            output_dir=output,
            backend="simulated",
            cell_limit=1,
            now=now,
            sleep=sleep,
            workload_backend=backend,
            gate_terminal_lifecycles=20,
            gate_min_measured_seconds=2.0,
            full_target_claims_per_second=100.0,
            full_warmup_seconds=0.0,
        )


def test_run_capacity_full_passes_only_when_observed_targets_met(tmp_path: Path) -> None:
    from benchmarks.qualification.runner import SimulatedCapacityBackend, run_capacity

    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(dt: float) -> None:
        clock["t"] += float(dt)

    backend = SimulatedCapacityBackend()
    output = tmp_path / "full-ok"
    result = run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="full",
        output_dir=output,
        backend="simulated",
        cell_limit=1,
        now=now,
        sleep=sleep,
        workload_backend=backend,
        gate_terminal_lifecycles=50,
        gate_min_measured_seconds=1.0,
        full_target_claims_per_second=100.0,
        full_warmup_seconds=0.0,
    )
    assert result.terminal_lifecycles == 50
    assert result.measured_seconds >= 1.0
    assert backend.terminals == 50
    workload = json.loads((output / "workload.json").read_text(encoding="utf-8"))
    assert workload["terminal_lifecycles"] == 50
    assert workload["rate_acquired"] == 50
    assert workload["rate_acquired"] <= int(100.0 * workload["measured_seconds"]) + 1


def test_run_capacity_wires_monotonic_rate_into_smoke_sample(tmp_path: Path) -> None:
    from benchmarks.qualification.runner import SimulatedCapacityBackend, run_capacity

    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(dt: float) -> None:
        clock["t"] += float(dt)

    backend = SimulatedCapacityBackend()
    output = tmp_path / "smoke-rate"
    result = run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="smoke",
        output_dir=output,
        backend="simulated",
        cell_limit=1,
        now=now,
        sleep=sleep,
        workload_backend=backend,
        smoke_warmup_seconds=0.5,
        smoke_sample_seconds=2.0,
        smoke_target_claims_per_second=50.0,
        smoke_preseed_tasks=100,
    )
    assert result.aborted is False
    workload = json.loads((output / "workload.json").read_text(encoding="utf-8"))
    assert workload["warmup_elapsed_seconds"] == pytest.approx(0.5, abs=0.05)
    assert workload["sample_elapsed_seconds"] == pytest.approx(2.0, abs=0.05)
    assert workload["preseed_applied"] == 100
    # Acquired work cannot exceed target * sample elapsed.
    assert workload["rate_acquired"] <= int(50.0 * workload["sample_elapsed_seconds"]) + 1
    assert workload["rate_acquired"] > 0


def test_run_capacity_executes_scenario_semantics_against_backend(tmp_path: Path) -> None:
    from benchmarks.qualification.load import expand_matrix, load_workload
    from benchmarks.qualification.runner import SimulatedCapacityBackend, run_capacity

    workload_doc = load_workload(WORKLOAD_PATH)
    cells = expand_matrix(workload_doc)
    scenario_cells = {
        "reclaim-heavy": next(c for c in cells if c.scenario == "reclaim-heavy"),
        "heartbeat-heavy": next(c for c in cells if c.scenario == "heartbeat-heavy"),
        "duplicate-enqueue-storm": next(
            c for c in cells if c.scenario == "duplicate-enqueue-storm"
        ),
        "complete-replay-storm": next(
            c for c in cells if c.scenario == "complete-replay-storm"
        ),
    }

    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(dt: float) -> None:
        clock["t"] += float(dt)

    for scenario, cell in scenario_cells.items():
        backend = SimulatedCapacityBackend()
        output = tmp_path / f"scen-{scenario}"
        run_capacity(
            profile_path=PROFILE_PATH,
            workload_path=WORKLOAD_PATH,
            mode="smoke",
            output_dir=output,
            backend="simulated",
            cell_limit=1,
            now=now,
            sleep=sleep,
            workload_backend=backend,
            smoke_warmup_seconds=0.0,
            smoke_sample_seconds=0.1,
            smoke_target_claims_per_second=10.0,
            smoke_preseed_tasks=10,
            force_cells=[cell],
        )
        drives = backend.scenario_drives
        assert drives, f"expected scenario drive for {scenario}"
        drive = drives[0]
        assert drive["scenario"] == scenario
        if scenario == "reclaim-heavy":
            assert drive["expire_once_ratio"] == pytest.approx(0.20)
            assert drive["expire_once_count"] == 200  # lease_count=1000 default
        elif scenario == "heartbeat-heavy":
            assert drive["heartbeat_interval_ms"] == 250
            assert drive["heartbeats_sent"] > 0
        elif scenario == "duplicate-enqueue-storm":
            assert drive["duplicate_requests"] == 100_000
            assert drive["unique_keys_used"] == 10_000
        elif scenario == "complete-replay-storm":
            assert drive["replay_times"] == 10
            assert drive["replay_requests"] == drive["accepted_terminals"] * 10


def test_live_backend_fails_closed_without_diagnostics_dsn(tmp_path: Path) -> None:
    from benchmarks.qualification.postgres_probe import ProbeError
    from benchmarks.qualification.runner import run_capacity

    output = tmp_path / "live-no-dsn"
    with pytest.raises((ProbeError, RuntimeError), match="DIAGNOSTICS_DSN|DATABASE_URL|live"):
        run_capacity(
            profile_path=PROFILE_PATH,
            workload_path=WORKLOAD_PATH,
            mode="smoke",
            output_dir=output,
            backend="live",
            cell_limit=1,
            environ={},  # no DSN
            smoke_warmup_seconds=0.0,
            smoke_sample_seconds=0.0,
            smoke_preseed_tasks=0,
        )


def test_live_backend_uses_injected_diagnostics_connection(tmp_path: Path) -> None:
    from benchmarks.qualification.runner import SimulatedCapacityBackend, run_capacity

    class RecordingConn:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
            self.calls.append(sql.strip().split()[0].upper())
            if "pg_extension" in sql:
                return []
            if "pg_stat_database" in sql:
                return [
                    {
                        "shared_blks_hit": 1,
                        "shared_blks_read": 0,
                        "blk_read_time": 0.0,
                    }
                ]
            if "pg_current_wal_lsn" in sql:
                return [{"wal_lsn": "0/1"}]
            if "n_dead_tup" in sql:
                return [{"n_dead_tup": 0, "n_live_tup": 1}]
            if "autovacuum" in sql:
                return [{"last_autovacuum": None, "autovacuum_count": 0}]
            if sql.strip().upper().startswith("EXPLAIN"):
                return [
                    {
                        "QUERY PLAN": [
                            {"Plan": {"Node Type": "Result", "Operation": "live"}}
                        ]
                    }
                ]
            return [{}]

    conn = RecordingConn()
    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(dt: float) -> None:
        clock["t"] += float(dt)

    output = tmp_path / "live-injected"
    run_capacity(
        profile_path=PROFILE_PATH,
        workload_path=WORKLOAD_PATH,
        mode="smoke",
        output_dir=output,
        backend="live",
        cell_limit=1,
        diagnostics_conn=conn,  # type: ignore[arg-type]
        now=now,
        sleep=sleep,
        workload_backend=SimulatedCapacityBackend(),
        smoke_warmup_seconds=0.0,
        smoke_sample_seconds=0.1,
        smoke_target_claims_per_second=10.0,
        smoke_preseed_tasks=5,
    )
    assert conn.calls, "live path must query PostgreSQL via DiagnosticsConnection"
    before = json.loads((output / "postgres-before.json").read_text(encoding="utf-8"))
    assert before["buffers"]["shared_blks_hit"] == 1
    index = json.loads((output / "plans" / "index.json").read_text(encoding="utf-8"))
    assert index["files"]
    plan_path = output / index["files"][0]["path"]
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan.get("Plan", {}).get("Node Type") != "Simulated"
