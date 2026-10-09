"""Runtime and concurrency fault scenarios (QUAL-04 kernel slice)."""

from __future__ import annotations

from tests.chaos.kernel.harness import (
    EXCLUDED_PHASE5,
    SCENARIO_IDS,
    assert_matrix_covers_qual04_kernel,
    scenario_matrix,
)


def test_scenario_matrix_covers_runtime_categories() -> None:
    """Matrix enumerates API/worker/PG/race scenarios; Phase 5 relay excluded."""
    assert_matrix_covers_qual04_kernel()
    specs = scenario_matrix()
    assert "RT-API-BEFORE-COMMIT" in SCENARIO_IDS
    assert "RT-PG-ENQUEUE" in SCENARIO_IDS
    assert "RT-RACE-PAUSE-CLAIM" in SCENARIO_IDS
    assert "RT-RELAY-DUP-PUBLISH" in EXCLUDED_PHASE5
    assert "RT-RELAY-DUP-PUBLISH" not in SCENARIO_IDS
    assert len(specs) == len(SCENARIO_IDS)


def test_api_death_before_enqueue_commit(chaos_harness) -> None:
    chaos_harness.run("RT-API-BEFORE-COMMIT")


def test_api_death_after_enqueue_commit(chaos_harness) -> None:
    chaos_harness.run("RT-API-AFTER-COMMIT")


def test_worker_death_during_lease_fences_stale(chaos_harness) -> None:
    chaos_harness.run("RT-WORKER-LEASE")


def test_worker_death_during_terminal_idempotent_replay(chaos_harness) -> None:
    chaos_harness.run("RT-WORKER-TERMINAL")


def test_postgres_restart_during_enqueue(chaos_harness) -> None:
    chaos_harness.run("RT-PG-ENQUEUE")


def test_postgres_restart_during_claim_heartbeat_complete(chaos_harness) -> None:
    chaos_harness.run("RT-PG-CLAIM-HB-COMPLETE")


def test_postgres_restart_during_maint_admin(chaos_harness) -> None:
    chaos_harness.run("RT-PG-MAINT-ADMIN")


def test_race_pause_with_claim(chaos_harness) -> None:
    chaos_harness.run("RT-RACE-PAUSE-CLAIM")


def test_race_drain_with_enqueue_and_spawn(chaos_harness) -> None:
    chaos_harness.run("RT-RACE-DRAIN-ENQUEUE-SPAWN")


def test_race_cancel_with_expiry_and_complete(chaos_harness) -> None:
    chaos_harness.run("RT-RACE-CANCEL-EXPIRY-COMPLETE")


def test_race_retry_with_lease_expiry(chaos_harness) -> None:
    chaos_harness.run("RT-RACE-RETRY-LEASE-EXPIRY")
