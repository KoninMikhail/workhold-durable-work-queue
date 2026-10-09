"""Generate synthetic Phase 7 PostgreSQL 18.6 qualification raw evidence (CI).

Produces a fresh bundle under benchmarks/results/phase-7-postgresql-18.6 with the
digest-pinned 18.6 engine identity. Does not rewrite phase-3.9-reference.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from benchmarks.qualification.artifacts import (
    CHECKSUM_CLASS,
    DERIVED_CLASSES,
    RAW_CLASSES,
    validate_raw,
)
from benchmarks.qualification.load import load_release_gate

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "benchmarks" / "results" / "phase-7-postgresql-18.6"
RELEASE_GATE = (
    ROOT / "benchmarks" / "qualification" / "workloads" / "release-gate.yaml"
)

CATALOG_SIGNATURE = (
    "admin_audit_log_queue_audit_idx,admin_replay_expires_at_idx,"
    "complete_replay_expires_at_idx,delivery_events_terminal_event_idx,"
    "task_attempts_task_claimed_idx,tasks_active_claim_idx,"
    "tasks_terminal_spawn_lineage_idx,tasks_terminal_task_terminal_idx"
)
SCHEMA_REVISION = "039_apply_qualified_storage_layout"
# Live OCI index digest for postgres:18.6-alpine (re-resolved at Phase 7 execution).
IMAGE_DIGESTS = {
    "queue": "sha256:" + "f" * 64,
    "postgres": (
        "sha256:6c538e7206ea40ff740ef27883529390a690b6ead6ba96b44c67a9f7c638e8fd"
    ),
}
RUN_ID = "run-phase7-postgresql-18.6-001"
STARTED = "2026-09-19T12:00:00Z"
ENDED = "2026-09-19T12:05:20Z"
POSTGRES_VERSION = "18.6"
SERVER_VERSION_NUM = 180_006


def _sha(obj: object) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> None:
    gate = load_release_gate(RELEASE_GATE)
    git_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    environment_body = {
        "ended_at_utc": ENDED,
        "git_sha": git_sha,
        "image_digests": IMAGE_DIGESTS,
        "postgres_version": POSTGRES_VERSION,
        "server_version_num": SERVER_VERSION_NUM,
        "profile_id": gate["profile"],
        "schema_revision": SCHEMA_REVISION,
        "started_at_utc": STARTED,
    }
    env_hash = _sha(environment_body)
    environment = {**environment_body, "environment_hash": env_hash}

    _write_json(
        OUT / "plans" / "claim-baseline.json",
        {
            "Plan": {
                "Index Name": "tasks_active_claim_idx",
                "Node Type": "Index Scan",
                "Relation Name": "tasks_active",
                "Total Cost": 12.5,
            }
        },
    )
    _write_json(
        OUT / "plans" / "index.json",
        {
            "files": [
                {
                    "operation": "claim",
                    "path": "plans/claim-baseline.json",
                    "scenario": "baseline",
                }
            ]
        },
    )
    _write_json(
        OUT / "postgres-before.json",
        {
            "autovacuum_lag_seconds": 0,
            "buffer_hits": 100,
            "dead_tuples": 0,
            "environment_hash": env_hash,
            "touched_partitions": [],
            "wal_bytes": 1000,
        },
    )
    _write_json(
        OUT / "postgres-after.json",
        {
            "autovacuum_lag_seconds": 2,
            "buffer_hits": 900_000,
            "dead_tuples": 1200,
            "environment_hash": env_hash,
            "execution_time_ms": 40.0,
            "planning_time_ms": 2.5,
            "touched_partitions": ["history_2026_09_19"],
            "wal_bytes": 5_000_000,
        },
    )

    rows: list[dict[str, object]] = []
    for i in range(100):
        rows.append(
            {
                "fan_out": 0,
                "latency_ns": 1_000_000 + i * 10_000,
                "operation": "claim",
                "outcome": "success" if i % 10 else "empty",
                "scenario": "baseline",
            }
        )
    for i in range(50):
        rows.append(
            {
                "fan_out": 0,
                "latency_ns": 2_000_000 + i * 10_000,
                "operation": "enqueue",
                "outcome": "success",
                "scenario": "baseline",
            }
        )
    for i in range(50):
        rows.append(
            {
                "fan_out": 0,
                "latency_ns": 800_000 + i * 5_000,
                "operation": "heartbeat",
                "outcome": "success",
                "scenario": "baseline",
            }
        )
    for i in range(50):
        rows.append(
            {
                "fan_out": 0 if i % 2 == 0 else 8,
                "latency_ns": 5_000_000 + i * 20_000,
                "operation": "complete",
                "outcome": "success",
                "scenario": "baseline",
            }
        )
    for i in range(64):
        rows.append(
            {
                "fan_out": 64,
                "latency_ns": 80_000_000 + i * 100_000,
                "operation": "complete",
                "outcome": "success",
                "scenario": "max_fanout",
            }
        )
    for _ in range(10):
        for op in ("fail", "cancel", "inspect"):
            rows.append(
                {
                    "fan_out": 0,
                    "latency_ns": 2_000_000,
                    "operation": op,
                    "outcome": "success",
                    "scenario": "baseline",
                }
            )

    sample_count = len(rows)
    workload_body = {
        "claimers": gate["claimers"],
        "evidence_mode": "synthetic",
        "evidence_note": (
            "Synthetic Phase 7 PostgreSQL 18.6 qualification raw evidence for CI: "
            "digest-pinned postgres:18.6-alpine "
            f"@sha256:6c538e7206ea40ff740ef27883529390a690b6ead6ba96b44c67a9f7c638e8fd "
            f"(server_version_num={SERVER_VERSION_NUM}). Declares the normative "
            "1_000_000 terminal / >=300s envelope and dual-client conformance "
            "bindings without a multi-hour live 1M host run. "
            f"latency_sample_count={sample_count} is the compressed sample size, "
            "not a measured 1M-row capture. Re-measure on the reference host before "
            "treating as production qualification input. Fresh bundle — not a "
            "rewrite of benchmarks/results/phase-3.9-reference."
        ),
        "fanout_64_sample": dict(gate["fanout_64_sample"]),
        "latency_sample_count": sample_count,
        "measured_seconds": 320.0,
        "named_queues": gate["named_queues"],
        "operation_mix": dict(gate["operation_mix"]),
        "payload_bytes_baseline": gate["payload_bytes_baseline"],
        "profile": gate["profile"],
        "successful_claims": 160_000,
        "target_claims_per_second": gate["target_claims_per_second"],
        "terminal_lifecycles": gate["terminal_lifecycles"],
        "warmup_seconds": gate["warmup_seconds"],
    }
    wl_hash = _sha(workload_body)
    workload = {**workload_body, "workload_hash": wl_hash}

    with gzip.open(OUT / "latencies.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    def suite(variant: str) -> str:
        props = [
            ("client_variant", variant),
            ("run_id", RUN_ID),
            ("environment_hash", env_hash),
            ("workload_hash", wl_hash),
            ("git_sha", git_sha),
            ("schema_revision", SCHEMA_REVISION),
            ("catalog_signature", CATALOG_SIGNATURE),
        ]
        prop_xml = "\n".join(
            f'      <property name="{key}" value="{value}"/>' for key, value in props
        )
        return (
            f'  <testsuite name="{variant}" tests="2" failures="0" '
            f'errors="0" skipped="0">\n'
            f"    <properties>\n{prop_xml}\n    </properties>\n"
            f'    <testcase classname="kernel" name="enqueue_claim_complete" '
            f'time="0.01"/>\n'
            f'    <testcase classname="kernel" name="heartbeat" time="0.01"/>\n'
            f"  </testsuite>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<testsuites name="queue-qualification" tests="4" failures="0" '
        'errors="0" skipped="0">\n'
        f"{suite('raw_http')}\n"
        f"{suite('sdk')}\n"
        "</testsuites>\n"
    )
    (OUT / "conformance.xml").write_text(xml, encoding="utf-8")
    _write_json(OUT / "environment.json", environment)
    _write_json(OUT / "workload.json", workload)
    _write_json(
        OUT / "manifest.json",
        {
            "artifact_classes": {
                "checksums": [CHECKSUM_CLASS],
                "derived": list(DERIVED_CLASSES),
                "raw": list(RAW_CLASSES),
            },
            "bundle_stage": "raw",
            "catalog_signature": CATALOG_SIGNATURE,
            "conformance": {
                "path": "conformance.xml",
                "variants": ["raw_http", "sdk"],
            },
            "ended_at_utc": ENDED,
            "environment_hash": env_hash,
            "evidence_mode": "synthetic",
            "git_sha": git_sha,
            "image_digests": IMAGE_DIGESTS,
            "latency_sample_count": sample_count,
            "run_id": RUN_ID,
            "schema_revision": SCHEMA_REVISION,
            "schema_version": 1,
            "started_at_utc": STARTED,
            "workload_hash": wl_hash,
        },
    )

    digest = validate_raw(OUT)
    stamp = OUT / ".raw-validation.json"
    if stamp.exists():
        stamp.unlink()
    print(f"wrote {OUT} validate-raw digest={digest}")


if __name__ == "__main__":
    main()
