"""Kernel capacity workload matrix expansion and scenario fixtures (QUAL-03)."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKLOAD_PATH = ROOT / "benchmarks" / "qualification" / "workloads" / "kernel-capacity.yaml"

EXPECTED_QUEUE_COUNTS = (1, 10, 100)
EXPECTED_CLAIMER_COUNTS = (1, 8, 32)
EXPECTED_PAYLOAD_BYTES = (1024, 262_144, 1_048_576)
EXPECTED_SCENARIOS = (
    "ready",
    "empty",
    "reclaim-heavy",
    "heartbeat-heavy",
    "duplicate-enqueue-storm",
    "complete-replay-storm",
)


def test_workload_yaml_declares_exact_matrix_axes() -> None:
    from benchmarks.qualification.load import load_workload

    workload = load_workload(WORKLOAD_PATH)
    assert tuple(workload["queue_counts"]) == EXPECTED_QUEUE_COUNTS
    assert tuple(workload["claimer_counts"]) == EXPECTED_CLAIMER_COUNTS
    assert tuple(workload["payload_bytes"]) == EXPECTED_PAYLOAD_BYTES
    assert tuple(workload["scenarios"]) == EXPECTED_SCENARIOS


def test_matrix_expansion_is_full_cross_product() -> None:
    from benchmarks.qualification.load import expand_matrix, load_workload

    workload = load_workload(WORKLOAD_PATH)
    cells = expand_matrix(workload)
    expected_n = (
        len(EXPECTED_QUEUE_COUNTS)
        * len(EXPECTED_CLAIMER_COUNTS)
        * len(EXPECTED_SCENARIOS)
        * len(EXPECTED_PAYLOAD_BYTES)
    )
    assert len(cells) == expected_n
    keys = {
        (cell.queue_count, cell.claimer_count, cell.scenario, cell.payload_bytes)
        for cell in cells
    }
    assert len(keys) == expected_n
    for queue_count in EXPECTED_QUEUE_COUNTS:
        for claimer_count in EXPECTED_CLAIMER_COUNTS:
            for scenario in EXPECTED_SCENARIOS:
                for payload_bytes in EXPECTED_PAYLOAD_BYTES:
                    assert (
                        queue_count,
                        claimer_count,
                        scenario,
                        payload_bytes,
                    ) in keys


def test_smoke_and_full_mode_parameters() -> None:
    from benchmarks.qualification.load import load_workload, mode_parameters

    workload = load_workload(WORKLOAD_PATH)
    smoke = mode_parameters(workload, "smoke")
    assert smoke.warmup_seconds == 30
    assert smoke.sample_seconds == 60
    assert smoke.preseed_tasks == 10_000

    full = mode_parameters(workload, "full")
    assert full.warmup_seconds == 120
    assert full.target_claims_per_second == 500
    assert full.terminal_lifecycles == 1_000_000
    assert full.min_measured_seconds == 300
    assert full.max_claimers == 32


def test_reclaim_heavy_expires_twenty_percent_once() -> None:
    from benchmarks.qualification.load import scenario_fixture

    fixture = scenario_fixture("reclaim-heavy", lease_count=1_000, seed=42)
    assert fixture.expire_once_ratio == pytest.approx(0.20)
    assert fixture.expire_once_count == 200
    assert fixture.expire_once_indices == tuple(sorted(fixture.expire_once_indices))
    assert len(fixture.expire_once_indices) == 200
    assert all(0 <= idx < 1_000 for idx in fixture.expire_once_indices)


def test_heartbeat_heavy_uses_server_recommended_min_safe_interval() -> None:
    from benchmarks.qualification.load import scenario_fixture

    fixture = scenario_fixture(
        "heartbeat-heavy",
        server_recommended_min_safe_interval_ms=250,
        seed=7,
    )
    assert fixture.heartbeat_interval_ms == 250
    assert fixture.interval_source == "server_recommended_min_safe"


def test_duplicate_enqueue_storm_ninety_percent_over_100k() -> None:
    from benchmarks.qualification.load import scenario_fixture

    fixture = scenario_fixture("duplicate-enqueue-storm", seed=99)
    assert fixture.request_count == 100_000
    assert fixture.duplicate_ratio == pytest.approx(0.90)
    keys = fixture.idempotency_keys()
    assert len(keys) == 100_000
    unique = len(set(keys))
    # 90% duplicates ⇒ 10% unique keys over 100k requests.
    assert unique == 10_000


def test_complete_replay_storm_repeats_each_terminal_ten_times() -> None:
    from benchmarks.qualification.load import scenario_fixture

    fixture = scenario_fixture(
        "complete-replay-storm",
        accepted_terminal_ids=("t1", "t2", "t3"),
        seed=1,
    )
    assert fixture.replay_times == 10
    sequence = fixture.replay_sequence()
    assert len(sequence) == 30
    assert sequence.count("t1") == 10
    assert sequence.count("t2") == 10
    assert sequence.count("t3") == 10


def test_deterministic_seeded_payloads_and_keys_never_logged() -> None:
    from benchmarks.qualification.load import (
        deterministic_idempotency_key,
        deterministic_payload,
        redact_log_fields,
    )

    p1 = deterministic_payload(seed=11, index=3, size_bytes=1024)
    p2 = deterministic_payload(seed=11, index=3, size_bytes=1024)
    p3 = deterministic_payload(seed=11, index=4, size_bytes=1024)
    assert len(p1) == 1024
    assert p1 == p2
    assert p1 != p3

    k1 = deterministic_idempotency_key(seed=11, index=3)
    k2 = deterministic_idempotency_key(seed=11, index=3)
    assert k1 == k2

    redacted = redact_log_fields(
        {
            "operation": "enqueue",
            "payload": p1,
            "idempotency_key": k1,
            "claim_token": "secret-token",
            "queue": "q-0",
        }
    )
    assert "payload" not in redacted
    assert "idempotency_key" not in redacted
    assert "claim_token" not in redacted
    assert redacted["operation"] == "enqueue"
    assert redacted["queue"] == "q-0"
