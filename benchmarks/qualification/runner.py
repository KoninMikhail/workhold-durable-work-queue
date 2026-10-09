"""Rate-controlled kernel capacity benchmark orchestrator (QUAL-03).

Smoke writes seven measurement classes with ``bundle_stage: raw``; the shared
conformance command supplies the eighth class ``conformance.xml``. Full mode
sustains a monotonic claim rate until observed terminal lifecycles and measured
wall time meet the gate. Never logs payloads, idempotency keys or claim tokens
(T-039-21 / T-039-22).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from benchmarks.qualification.artifacts import ARTIFACT_CLASSES, RAW_CLASSES
from benchmarks.qualification.load import (
    FullModeParams,
    MatrixCell,
    MonotonicRateController,
    ScenarioFixture,
    SmokeModeParams,
    assign_queues_round_robin,
    clamp_claimers,
    expand_matrix,
    load_workload,
    mode_parameters,
    new_run_id,
    redact_log_fields,
    scenario_fixture,
    workload_hash,
)
from benchmarks.qualification.postgres_probe import (
    HOT_OPERATIONS,
    ProbeError,
    capture_snapshot,
    resolve_probe_connection,
    write_explain_files,
)
from benchmarks.qualification.profile_loader import load_profile

FINALIZATION_NAMES = frozenset(
    {
        "summary.json",
        "qualification.json",
        "report.md",
        "SHA256SUMS",
        ".immutable",
        "PASS",
    }
)

# Default lease pool size for reclaim-heavy scenario drives (plan: 20% of leases).
_RECLAIM_LEASE_COUNT = 1_000
_DEFAULT_HEARTBEAT_INTERVAL_MS = 250


@dataclass(slots=True)
class FullModeCompletionGate:
    terminal_lifecycles: int
    min_measured_seconds: float

    def is_complete(self, *, terminals: int, measured_seconds: float) -> bool:
        return (
            terminals == self.terminal_lifecycles
            and measured_seconds >= self.min_measured_seconds
        )


@dataclass(slots=True)
class RunResult:
    bundle_stage: str
    output_dir: Path
    aborted: bool = False
    terminal_lifecycles: int = 0
    measured_seconds: float = 0.0
    cells_run: int = 0
    abort_reason: str | None = None


@dataclass(slots=True)
class SimulatedCapacityBackend:
    """Injectable workload backend for unit tests and offline simulation.

    Executes scenario fixture semantics (reclaim expire-once, heartbeat
    interval pressure, duplicate enqueue stream, complete-replay sequence)
    and produces terminal lifecycles under rate control. Does not contact
    a live API or PostgreSQL.
    """

    max_terminals: int | None = None
    terminals: int = 0
    successful_claims: int = 0
    preseeded: int = 0
    rate_acquired: int = 0
    scenario_drives: list[dict[str, Any]] = field(default_factory=list)
    latencies: list[dict[str, Any]] = field(default_factory=list)

    def preseed(self, count: int, *, cell: MatrixCell, seed: int) -> int:
        applied = max(0, int(count))
        self.preseeded += applied
        return applied

    def drive_scenario(
        self,
        fixture: ScenarioFixture,
        *,
        cell: MatrixCell,
        seed: int,
    ) -> dict[str, Any]:
        drive: dict[str, Any] = {"scenario": fixture.name, "cell_id": cell.cell_id}
        if fixture.name == "reclaim-heavy":
            # Expire selected leases once, then reclaim (ratio from fixture).
            for _idx in fixture.expire_once_indices:
                self._record_latency(cell, "claim", seed=seed, outcome="reclaim")
            drive["expire_once_ratio"] = fixture.expire_once_ratio
            drive["expire_once_count"] = fixture.expire_once_count
        elif fixture.name == "heartbeat-heavy":
            interval = int(fixture.heartbeat_interval_ms or _DEFAULT_HEARTBEAT_INTERVAL_MS)
            # Bounded heartbeat burst at the server-recommended interval semantics.
            bursts = 8
            for _ in range(bursts):
                self._record_latency(cell, "heartbeat", seed=seed, outcome="success")
            drive["heartbeat_interval_ms"] = interval
            drive["heartbeats_sent"] = bursts
            drive["interval_source"] = fixture.interval_source
        elif fixture.name == "duplicate-enqueue-storm":
            keys = fixture.idempotency_keys()
            # Drive the full key stream against the backend (no key values logged).
            for i, _key in enumerate(keys):
                outcome = "duplicate" if i >= int(round(len(keys) * 0.10)) else "success"
                self._record_latency(cell, "enqueue", seed=seed, outcome=outcome)
            drive["duplicate_requests"] = fixture.request_count
            drive["unique_keys_used"] = len(set(keys))
            drive["duplicate_ratio"] = fixture.duplicate_ratio
        elif fixture.name == "complete-replay-storm":
            accepted = fixture.accepted_terminal_ids or ("term-a", "term-b")
            # Rebuild fixture with accepted ids when caller passed empty.
            if not fixture.accepted_terminal_ids:
                rebuilt = scenario_fixture(
                    "complete-replay-storm",
                    accepted_terminal_ids=accepted,
                    seed=seed,
                )
                sequence = rebuilt.replay_sequence()
                replay_times = rebuilt.replay_times
            else:
                sequence = fixture.replay_sequence()
                replay_times = fixture.replay_times
            for _terminal_id in sequence:
                self._record_latency(cell, "complete", seed=seed, outcome="replay")
            drive["replay_times"] = replay_times
            drive["replay_requests"] = len(sequence)
            drive["accepted_terminals"] = len(accepted)
        elif fixture.name in {"ready", "empty"}:
            pass
        self.scenario_drives.append(drive)
        return drive

    def claim_and_complete_one(self, *, cell: MatrixCell, seed: int) -> bool:
        if self.max_terminals is not None and self.terminals >= self.max_terminals:
            return False
        self.successful_claims += 1
        self.terminals += 1
        self._record_latency(cell, "claim", seed=seed, outcome="success")
        self._record_latency(cell, "complete", seed=seed, outcome="success")
        return True

    def note_rate_acquire(self) -> None:
        self.rate_acquired += 1

    def _record_latency(
        self,
        cell: MatrixCell,
        operation: str,
        *,
        seed: int,
        outcome: str,
    ) -> None:
        self.latencies.append(
            {
                "operation": operation,
                "scenario": cell.scenario,
                "latency_ns": 1_000_000 + (seed % 1000) * 1000 + len(self.latencies),
                "outcome": outcome,
                "fan_out": 0,
                "cell_id": cell.cell_id,
            }
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256_mapping(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _environment_stub(profile: Mapping[str, Any], *, git_sha: str) -> dict[str, Any]:
    images = profile.get("images") if isinstance(profile.get("images"), dict) else {}
    postgres_img = images.get("postgres") if isinstance(images.get("postgres"), dict) else {}
    queue_img = images.get("queue") if isinstance(images.get("queue"), dict) else {}
    queue_digest = str(
        queue_img.get("digest")
        or "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    )
    postgres_digest = str(
        postgres_img.get("digest")
        or "sha256:3c5c8892d184f738f4fe282d14ddaa613a38f00f4189d2d94725ebe6f2909ddb"
    )
    env = {
        "profile_id": profile.get("profile_id") or "phase-3.9-linux-x86_64-v1",
        "host": {
            "architecture": (profile.get("host") or {}).get("architecture", "x86_64"),
            "os": (profile.get("host") or {}).get("os", "linux"),
        },
        "runtime": {"role": "qualification-load"},
        "image_digests": {"queue": queue_digest, "postgres": postgres_digest},
        "postgres": {"version": "simulated", "settings": {}},
        "schema_revision": str(
            (profile.get("schema") or {}).get("expected_head") or "simulated-schema"
        ),
        "captured_at_utc": _utc_now(),
        "git_sha": git_sha,
    }
    env["environment_hash"] = _sha256_mapping(
        {k: v for k, v in env.items() if k != "environment_hash"}
    )
    return env


def _build_scenario_fixture(cell: MatrixCell, *, seed: int) -> ScenarioFixture:
    if cell.scenario == "reclaim-heavy":
        return scenario_fixture(
            "reclaim-heavy", lease_count=_RECLAIM_LEASE_COUNT, seed=seed
        )
    if cell.scenario == "heartbeat-heavy":
        return scenario_fixture(
            "heartbeat-heavy",
            server_recommended_min_safe_interval_ms=_DEFAULT_HEARTBEAT_INTERVAL_MS,
            seed=seed,
        )
    if cell.scenario == "duplicate-enqueue-storm":
        return scenario_fixture("duplicate-enqueue-storm", seed=seed)
    if cell.scenario == "complete-replay-storm":
        return scenario_fixture(
            "complete-replay-storm",
            accepted_terminal_ids=("term-a", "term-b"),
            seed=seed,
        )
    return scenario_fixture(cell.scenario, seed=seed)


def _run_rate_limited_work(
    *,
    backend: SimulatedCapacityBackend,
    cell: MatrixCell,
    seed: int,
    target_per_second: float,
    duration_seconds: float,
    now: Callable[[], float],
    sleep: Callable[[float], None],
    terminal_cap: int | None = None,
) -> tuple[int, float]:
    """Acquire work under ``MonotonicRateController`` for ``duration_seconds``.

    Returns ``(acquired, elapsed)``. When ``terminal_cap`` is set, stops once
    that many claim+complete terminals have been produced (full-mode sustain).
    """
    rate = MonotonicRateController(target_per_second=target_per_second, now=now)
    started = now()
    acquired = 0
    idle_steps = 0
    while True:
        elapsed = max(0.0, now() - started)
        if terminal_cap is None and elapsed >= duration_seconds:
            break
        if terminal_cap is not None and backend.terminals >= terminal_cap:
            # Keep sampling wall time until min measured window is also met by caller.
            break
        if rate.try_acquire():
            backend.note_rate_acquire()
            acquired += 1
            produced = backend.claim_and_complete_one(cell=cell, seed=seed)
            idle_steps = 0
            if not produced and terminal_cap is not None:
                # Backend exhausted; stop acquiring further work.
                break
        else:
            idle_steps += 1
            # Advance time just enough for the next permit (injectable sleep).
            sleep(max(1.0 / target_per_second, 0.001))
            if terminal_cap is not None and idle_steps > target_per_second * 10:
                # Safety: avoid infinite spin if clock is frozen.
                break
        if terminal_cap is None and max(0.0, now() - started) >= duration_seconds:
            break
    return acquired, max(0.0, now() - started)


def _sustain_full_mode(
    *,
    backend: SimulatedCapacityBackend,
    cell: MatrixCell,
    seed: int,
    gate: FullModeCompletionGate,
    target_claims_per_second: float,
    warmup_seconds: float,
    now: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[int, float]:
    """Warm up, then sustain until observed terminals and measured time meet gate."""
    if warmup_seconds > 0:
        sleep(warmup_seconds)

    measured_start = now()
    # First loop: produce exactly gate.terminal_lifecycles under rate control.
    while backend.terminals < gate.terminal_lifecycles:
        before = backend.terminals
        _run_rate_limited_work(
            backend=backend,
            cell=cell,
            seed=seed,
            target_per_second=target_claims_per_second,
            duration_seconds=1.0,
            now=now,
            sleep=sleep,
            terminal_cap=gate.terminal_lifecycles,
        )
        if backend.terminals == before:
            # Backend cannot produce more terminals.
            break

    # Ensure measured window reaches the minimum even if terminals arrived early.
    while True:
        measured = max(0.0, now() - measured_start)
        if gate.is_complete(terminals=backend.terminals, measured_seconds=measured):
            return backend.terminals, measured
        if backend.terminals != gate.terminal_lifecycles:
            # Under-target terminals after sustain attempt — fail after min window.
            if measured >= gate.min_measured_seconds:
                return backend.terminals, measured
            sleep(0.05)
            continue
        # Terminals met; wait out remaining measured window.
        remaining = gate.min_measured_seconds - measured
        if remaining > 0:
            sleep(remaining)
        measured = max(0.0, now() - measured_start)
        return backend.terminals, measured


def run_capacity(
    *,
    profile_path: Path | str,
    workload_path: Path | str,
    mode: str,
    output_dir: Path | str,
    backend: str = "live",
    cell_limit: int | None = None,
    force_abort_after_cells: int | None = None,
    force_cells: Sequence[MatrixCell] | None = None,
    git_sha: str = "0" * 40,
    now: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
    workload_backend: SimulatedCapacityBackend | None = None,
    diagnostics_conn: Any | None = None,
    environ: Mapping[str, str] | None = None,
    diagnostics_dsn: str | None = None,
    # Test overrides (production path reads YAML via mode_parameters).
    gate_terminal_lifecycles: int | None = None,
    gate_min_measured_seconds: float | None = None,
    full_target_claims_per_second: float | None = None,
    full_warmup_seconds: float | None = None,
    smoke_warmup_seconds: float | None = None,
    smoke_sample_seconds: float | None = None,
    smoke_target_claims_per_second: float | None = None,
    smoke_preseed_tasks: int | None = None,
    # Legacy aliases kept for callers that still pass compressed observations;
    # they override gate targets only (observed work still comes from the loop).
    simulate_terminals: int | None = None,
    simulate_measured_seconds: float | None = None,
) -> RunResult:
    """Execute the capacity matrix and write a raw measurement bundle.

    ``backend="simulated"`` is the unit-test / offline path: no network, no
    PostgreSQL. Live execution requires a diagnostics DSN (or injected
    connection) and fails closed when unavailable.
    """
    if mode not in {"smoke", "full"}:
        raise ValueError(f"mode must be smoke|full, got {mode!r}")
    if backend not in {"simulated", "live"}:
        raise ValueError(f"backend must be simulated|live, got {backend!r}")

    # Simulated + no injected clock → compressed unit path (artifact shape).
    # Injectable now/sleep proves real warm-up/sample/rate windows in tests.
    compressed = backend == "simulated" and now is None and sleep is None
    clock_now = now or time.monotonic
    clock_sleep = sleep or time.sleep

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    profile = load_profile(profile_path)
    workload = load_workload(workload_path)
    params = mode_parameters(workload, mode)
    cells = list(force_cells) if force_cells is not None else expand_matrix(workload)
    if cell_limit is not None:
        cells = cells[: max(0, int(cell_limit))]

    run_id = new_run_id()
    started = _utc_now()
    env = _environment_stub(profile, git_sha=git_sha)
    wl_hash = workload_hash(workload)
    seed = int(workload.get("seed", 0))

    if force_abort_after_cells is not None and force_abort_after_cells <= 0:
        abort_payload = redact_log_fields(
            {
                "aborted": True,
                "reason": "forced_abort_before_cells",
                "run_id": run_id,
                "mode": mode,
            }
        )
        _write_json(output / "abort.json", abort_payload)
        return RunResult(
            bundle_stage="raw",
            output_dir=output,
            aborted=True,
            abort_reason="forced_abort_before_cells",
        )

    try:
        probe_conn = resolve_probe_connection(
            backend,
            injected=diagnostics_conn,
            dsn=diagnostics_dsn,
            environ=environ,
        )
    except ProbeError:
        raise

    before = capture_snapshot(probe_conn, environment_hash=env["environment_hash"])
    _write_json(output / "postgres-before.json", before.to_dict())

    driver = workload_backend or SimulatedCapacityBackend()
    explain_entries: list[dict[str, str]] = []
    cells_run = 0
    warmup_elapsed = 0.0
    sample_elapsed = 0.0
    measured_seconds = 0.0
    preseed_applied = 0
    total_rate_acquired = 0

    # Resolve mode timings (YAML defaults with optional test overrides).
    if isinstance(params, SmokeModeParams):
        if smoke_warmup_seconds is not None:
            smoke_warmup = float(smoke_warmup_seconds)
        elif compressed:
            smoke_warmup = 0.0
        else:
            smoke_warmup = float(params.warmup_seconds)
        if smoke_sample_seconds is not None:
            smoke_sample = float(smoke_sample_seconds)
        elif compressed:
            smoke_sample = 0.05
        else:
            smoke_sample = float(params.sample_seconds)
        if smoke_preseed_tasks is not None:
            smoke_preseed = int(smoke_preseed_tasks)
        elif compressed:
            smoke_preseed = min(int(params.preseed_tasks), 10)
        else:
            smoke_preseed = int(params.preseed_tasks)
        smoke_rate = (
            float(smoke_target_claims_per_second)
            if smoke_target_claims_per_second is not None
            else (200.0 if compressed else 100.0)
        )
    else:
        smoke_warmup = smoke_sample = smoke_rate = 0.0
        smoke_preseed = 0

    if isinstance(params, FullModeParams):
        if gate_terminal_lifecycles is not None:
            gate_terminals = int(gate_terminal_lifecycles)
        elif simulate_terminals is not None:
            gate_terminals = int(simulate_terminals)
        elif compressed:
            raise RuntimeError(
                "full mode on simulated backend requires gate_terminal_lifecycles "
                "(or simulate_terminals) override; refusing uncompressed 1M loop"
            )
        else:
            gate_terminals = int(params.terminal_lifecycles)
        if gate_min_measured_seconds is not None:
            gate_min_s = float(gate_min_measured_seconds)
        elif simulate_measured_seconds is not None:
            gate_min_s = float(simulate_measured_seconds)
        elif compressed:
            raise RuntimeError(
                "full mode on simulated backend requires gate_min_measured_seconds "
                "(or simulate_measured_seconds) override"
            )
        else:
            gate_min_s = float(params.min_measured_seconds)
        full_rate = (
            float(full_target_claims_per_second)
            if full_target_claims_per_second is not None
            else float(params.target_claims_per_second)
        )
        if full_warmup_seconds is not None:
            full_warmup = float(full_warmup_seconds)
        elif compressed:
            full_warmup = 0.0
        else:
            full_warmup = float(params.warmup_seconds)
        max_claimers = int(params.max_claimers)
    else:
        gate_terminals = 0
        gate_min_s = 0.0
        full_rate = 0.0
        full_warmup = 0.0
        max_claimers = 32

    for cell in cells:
        if force_abort_after_cells is not None and cells_run >= force_abort_after_cells:
            break

        claimers = clamp_claimers(cell.claimer_count, max_claimers=max_claimers)
        _ = assign_queues_round_robin(queue_count=cell.queue_count, claimer_count=claimers)

        fixture = _build_scenario_fixture(cell, seed=seed)
        driver.drive_scenario(fixture, cell=cell, seed=seed)

        if mode == "smoke" and isinstance(params, SmokeModeParams):
            warm_start = clock_now()
            if smoke_warmup > 0:
                clock_sleep(smoke_warmup)
            warmup_elapsed = max(warmup_elapsed, max(0.0, clock_now() - warm_start))

            if cell.scenario != "empty" and smoke_preseed > 0:
                preseed_applied += driver.preseed(
                    smoke_preseed, cell=cell, seed=seed
                )

            _acquired, elapsed = _run_rate_limited_work(
                backend=driver,
                cell=cell,
                seed=seed,
                target_per_second=smoke_rate,
                duration_seconds=smoke_sample,
                now=clock_now,
                sleep=clock_sleep,
            )
            sample_elapsed = max(sample_elapsed, elapsed)
            total_rate_acquired += _acquired
            measured_seconds = sample_elapsed
        else:
            gate = FullModeCompletionGate(
                terminal_lifecycles=gate_terminals,
                min_measured_seconds=gate_min_s,
            )
            observed_terminals, observed_measured = _sustain_full_mode(
                backend=driver,
                cell=cell,
                seed=seed,
                gate=gate,
                target_claims_per_second=full_rate,
                warmup_seconds=full_warmup,
                now=clock_now,
                sleep=clock_sleep,
            )
            measured_seconds = observed_measured
            total_rate_acquired = driver.rate_acquired
            if not gate.is_complete(
                terminals=observed_terminals, measured_seconds=observed_measured
            ):
                raise RuntimeError(
                    "full mode cannot complete before exactly "
                    f"{gate.terminal_lifecycles} terminal lifecycles and "
                    f"{gate.min_measured_seconds}s measured "
                    f"(observed terminals={observed_terminals}, "
                    f"measured_seconds={observed_measured})"
                )
            explain_entries.extend(
                write_explain_files(
                    output / "plans", conn=probe_conn, cell_id=cell.cell_id
                )
            )
            cells_run += 1
            break

        explain_entries.extend(
            write_explain_files(
                output / "plans", conn=probe_conn, cell_id=cell.cell_id
            )
        )
        cells_run += 1

    after = capture_snapshot(probe_conn, environment_hash=env["environment_hash"])
    after_dict = after.to_dict()
    if backend == "simulated":
        after_dict["buffers"] = {
            **after_dict["buffers"],
            "shared_blks_hit": int(after_dict["buffers"].get("shared_blks_hit", 0))
            + driver.successful_claims,
        }
    _write_json(output / "postgres-after.json", after_dict)

    index = {"files": explain_entries, "hot_operations": list(HOT_OPERATIONS)}
    _write_json(output / "plans" / "index.json", index)

    with gzip.open(output / "latencies.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in driver.latencies:
            handle.write(json.dumps(redact_log_fields(row), sort_keys=True) + "\n")

    terminals = driver.terminals
    workload_artifact = {
        "name": workload.get("name", "kernel-capacity"),
        "version": workload.get("version", 1),
        "mode": mode,
        "workload_hash": wl_hash,
        "seed": workload.get("seed"),
        "queue_counts": list(workload["queue_counts"]),
        "claimer_counts": list(workload["claimer_counts"]),
        "payload_bytes": list(workload["payload_bytes"]),
        "scenarios": list(workload["scenarios"]),
        "cells_run": cells_run,
        "cell_limit": cell_limit,
        "successful_claims": driver.successful_claims,
        "terminal_lifecycles": terminals,
        "measured_seconds": measured_seconds,
        "warmup_seconds": (
            smoke_warmup if mode == "smoke" else full_warmup
        ),
        "warmup_elapsed_seconds": warmup_elapsed if mode == "smoke" else full_warmup,
        "sample_elapsed_seconds": sample_elapsed if mode == "smoke" else None,
        "target_claims_per_second": (
            full_rate if mode == "full" else smoke_rate if mode == "smoke" else None
        ),
        "preseed_tasks": smoke_preseed if mode == "smoke" else None,
        "preseed_applied": preseed_applied if mode == "smoke" else None,
        "rate_acquired": total_rate_acquired,
        "scenario_drives": list(driver.scenario_drives),
        "backend": backend,
        "timing_mode": "compressed" if compressed else "wall",
    }
    _write_json(output / "workload.json", workload_artifact)

    ended = _utc_now()
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "bundle_stage": "raw",
        "environment_hash": env["environment_hash"],
        "workload_hash": wl_hash,
        "schema_revision": env["schema_revision"],
        "catalog_signature": "simulated-catalog" if backend == "simulated" else "live-catalog",
        "git_sha": git_sha,
        "image_digests": env["image_digests"],
        "started_at_utc": started,
        "ended_at_utc": ended,
        "conformance": {
            "path": "conformance.xml",
            "variants": ["raw_http", "sdk"],
        },
        "artifact_classes": {
            "raw": list(RAW_CLASSES),
            "derived": list(ARTIFACT_CLASSES["derived"]),
            "checksums": list(ARTIFACT_CLASSES["checksums"]),
        },
    }
    _write_json(output / "manifest.json", manifest)
    _write_json(output / "environment.json", env)

    for name in FINALIZATION_NAMES:
        path = output / name
        if path.exists():
            path.unlink()

    # Close live connections we opened (not injected ones).
    if (
        probe_conn is not None
        and diagnostics_conn is None
        and hasattr(probe_conn, "close")
    ):
        try:
            probe_conn.close()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    return RunResult(
        bundle_stage="raw",
        output_dir=output,
        aborted=False,
        terminal_lifecycles=terminals,
        measured_seconds=measured_seconds,
        cells_run=cells_run,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.qualification.runner",
        description="Kernel capacity workload runner (QUAL-03).",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Named profile (priority-claim) or path to reference-environment.yaml",
    )
    parser.add_argument(
        "--workload",
        type=Path,
        required=False,
        help="Path to kernel-capacity.yaml",
    )
    parser.add_argument(
        "--mode",
        choices=("smoke", "full"),
        required=False,
        help="smoke: 30s warm-up + 60s sample; full: 1M terminals @ 500 claims/s",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=False,
        help="Raw evidence output directory",
    )
    parser.add_argument(
        "--claimers",
        type=int,
        default=32,
        help="Concurrent claimers for the priority-claim profile (default: 32)",
    )
    parser.add_argument(
        "--backend",
        choices=("simulated", "live"),
        default="live",
        help="simulated for offline/unit paths; live against the reference stack",
    )
    parser.add_argument(
        "--cell-limit",
        type=int,
        default=None,
        help="Optional cap on matrix cells (smoke debugging)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    profile_text = str(args.profile)
    try:
        if profile_text == "priority-claim":
            from benchmarks.qualification.priority_claim import run_priority_claim_profile

            results = run_priority_claim_profile(claimers=int(args.claimers))
            for item in results:
                print(
                    f"priority-claim OK workload={item.workload} "
                    f"verdict={item.verdict} claims/s={item.claims_per_second:.3f} "
                    f"output={item.output_dir}"
                )
            return 0
        if args.workload is None or args.mode is None or args.output is None:
            parser.error(
                "--workload, --mode and --output are required unless "
                "--profile priority-claim"
            )
        result = run_capacity(
            profile_path=Path(profile_text),
            workload_path=args.workload,
            mode=args.mode,
            output_dir=args.output,
            backend=args.backend,
            cell_limit=args.cell_limit,
        )
    except Exception as exc:  # noqa: BLE001 — CLI fail-closed
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"runner OK stage={result.bundle_stage} cells={result.cells_run} "
        f"terminals={result.terminal_lifecycles} measured_s={result.measured_seconds}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
