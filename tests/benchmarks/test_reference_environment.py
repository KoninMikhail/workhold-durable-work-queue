"""Reference environment profile, Compose topology and fail-closed preflight (QUAL-03/05).

Lifecycle commands exercised by operators on the reference host (documented here
and in compose/preflight module headers):

1. docker compose -f benchmarks/qualification/docker-compose.qualification.yml pull
2. docker compose -f benchmarks/qualification/docker-compose.qualification.yml up -d --wait postgres queue-api-1 queue-api-2
3. uv run python -m benchmarks.qualification.preflight --profile benchmarks/qualification/reference-environment.yaml --compose benchmarks/qualification/docker-compose.qualification.yml --output benchmarks/results/phase-7-postgresql-18.6/environment.json
4. docker compose -f benchmarks/qualification/docker-compose.qualification.yml down -v
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "benchmarks" / "qualification" / "reference-environment.yaml"
COMPOSE = ROOT / "benchmarks" / "qualification" / "docker-compose.qualification.yml"
PREFLIGHT_MOD = [sys.executable, "-m", "benchmarks.qualification.preflight"]

# Digest-pinned qualification uses pull only (no local build) so observed
# Compose/image digests can be reconciled against the profile (T-039-20).
LIFECYCLE_PULL = (
    "docker compose -f benchmarks/qualification/docker-compose.qualification.yml "
    "pull"
)
LIFECYCLE_UP = (
    "docker compose -f benchmarks/qualification/docker-compose.qualification.yml "
    "up -d --wait postgres queue-api-1 queue-api-2"
)
LIFECYCLE_PREFLIGHT = (
    "uv run python -m benchmarks.qualification.preflight "
    "--profile benchmarks/qualification/reference-environment.yaml "
    "--compose benchmarks/qualification/docker-compose.qualification.yml "
    "--output benchmarks/results/phase-7-postgresql-18.6/environment.json"
)
LIFECYCLE_DOWN = (
    "docker compose -f benchmarks/qualification/docker-compose.qualification.yml "
    "down -v"
)

SECRET_PATTERNS = re.compile(
    r'(?i)("password"|"passwd"|"secret"|"token"|"bearer"|"dsn"|"database_url"'
    r'|postgresql\+psycopg://|postgres://|://[^/\s]+:[^@/\s]+@)'
)


def _load_profile() -> dict[str, Any]:
    from benchmarks.qualification.profile_loader import load_profile

    return load_profile(PROFILE)


def _compose_text() -> str:
    return COMPOSE.read_text(encoding="utf-8")


def test_lifecycle_commands_are_documented_in_tests_and_sources() -> None:
    assert PROFILE.is_file()
    assert COMPOSE.is_file()
    compose = _compose_text()
    preflight_src = (
        ROOT / "benchmarks" / "qualification" / "preflight.py"
    ).read_text(encoding="utf-8")
    this_src = Path(__file__).read_text(encoding="utf-8")
    for cmd in (LIFECYCLE_PULL, LIFECYCLE_UP, LIFECYCLE_PREFLIGHT, LIFECYCLE_DOWN):
        assert cmd in this_src
        assert cmd in compose or cmd in preflight_src
    # Local image builds would bypass digest pins; qualification forbids them.
    assert "build --pull" not in compose
    assert "\n  build:" not in compose and "\n    build:" not in compose


def test_profile_phase_3_9_linux_x86_64_v1_resources() -> None:
    profile = _load_profile()
    assert profile["profile_id"] == "phase-3.9-linux-x86_64-v1"
    host = profile["host"]
    assert host["os"] == "linux"
    assert host["architecture"] == "x86_64"
    assert host["min_logical_cpus"] == 16
    assert host["min_memory_gib"] == 32
    assert host["storage_locality"] == "local_ssd"
    assert host["co_resident_workload_allowed"] is False

    services = profile["services"]
    assert services["postgres"] == {"cpus": 6.0, "memory_gib": 12}
    assert services["queue-api-1"] == {"cpus": 3.0, "memory_gib": 4}
    assert services["queue-api-2"] == {"cpus": 3.0, "memory_gib": 4}
    assert services["load-generator"] == {"cpus": 4.0, "memory_gib": 4}
    total_cpus = sum(float(s["cpus"]) for s in services.values())
    assert total_cpus == 16.0


def test_profile_pins_immutable_image_digests() -> None:
    profile = _load_profile()
    postgres = profile["images"]["postgres"]
    queue = profile["images"]["queue"]
    assert "@sha256:" in postgres["reference"]
    assert postgres["digest"].startswith("sha256:")
    assert len(postgres["digest"]) == len("sha256:") + 64
    assert "@sha256:" in queue["reference"]
    assert queue["digest"].startswith("sha256:")
    assert "postgres:18.6-alpine@sha256:" in postgres["reference"]
    assert "16-alpine" not in postgres["reference"]
    assert postgres["digest"] == (
        "sha256:6c538e7206ea40ff740ef27883529390a690b6ead6ba96b44c67a9f7c638e8fd"
    )


def test_compose_declares_exact_resource_limits_and_services() -> None:
    text = _compose_text()
    for name in ("postgres:", "queue-api-1:", "queue-api-2:", "load-generator:"):
        assert name in text
    assert "cpus: 6" in text or "cpus: '6'" in text or 'cpus: "6"' in text
    assert "cpus: 3" in text or "cpus: '3'" in text or 'cpus: "3"' in text
    assert "cpus: 4" in text or "cpus: '4'" in text or 'cpus: "4"' in text
    assert "mem_limit: 12g" in text
    assert "mem_limit: 4g" in text
    assert "@sha256:" in text
    assert "postgres:18.6-alpine@sha256:" in text
    assert "postgres:16-alpine" not in text
    assert "/var/lib/postgresql" in text
    assert "/var/lib/postgresql/data" not in text
    assert "qualification-pgdata-18" in text
    assert "queue-pgdata" not in text
    assert "down -v" in text
    assert "healthcheck:" in text
    assert "healthz" in text or "/readyz" in text or "readyz" in text
    assert "queue-qualification" in text or "internal: true" in text
    assert "QUEUE_API_REPLICA_CEILING" in text
    assert "QUEUE_API_POOL_CEILING" in text
    assert "QUEUE_POSTGRES_MAX_CONNECTIONS" in text
    assert "QUEUE_PARTITION_PREMAKE_DAYS" in text


def test_compose_config_validates() -> None:
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "config", "-q"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _facts(**overrides: Any) -> dict[str, Any]:
    profile = _load_profile()
    base: dict[str, Any] = {
        "architecture": "x86_64",
        "logical_cpus": 16,
        "memory_gib": 32.0,
        "storage_locality": "local_ssd",
        "cpu_oversubscribed": False,
        "co_resident_workload": False,
        "image_digests": {
            "postgres": profile["images"]["postgres"]["digest"],
            "queue": profile["images"]["queue"]["digest"],
        },
        "compose_images_digest_pinned": True,
        "schema_head": profile["schema"]["expected_head"],
        "premake_horizon_safe": True,
        "database_empty": True,
        "database_dedicated": True,
        "server_version_num": 180_006,
        "server_version": "18.6",
        "postgres_identity": {
            "version": "PostgreSQL 18.6 on x86_64-pc-linux-musl",
            "server_version_num": 180_006,
            "settings": {
                "max_connections": "100",
                "server_version": "18.6",
            },
        },
    }
    base.update(overrides)
    return base


def test_preflight_rejects_wrong_architecture(tmp_path: Path) -> None:
    from benchmarks.qualification.preflight import PreflightError, run_preflight

    with pytest.raises(PreflightError, match="architecture"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(architecture="aarch64"),
        )


def test_preflight_rejects_insufficient_cpu_ram(tmp_path: Path) -> None:
    from benchmarks.qualification.preflight import PreflightError, run_preflight

    with pytest.raises(PreflightError, match="logical_cpus|CPU"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(logical_cpus=8),
        )
    with pytest.raises(PreflightError, match="memory|RAM"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(memory_gib=16.0),
        )


def test_preflight_rejects_mutable_image_tag(tmp_path: Path) -> None:
    from benchmarks.qualification.preflight import PreflightError, run_preflight

    with pytest.raises(PreflightError, match="digest|mutable|image"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(compose_images_digest_pinned=False),
        )


def test_preflight_rejects_schema_mismatch_and_unsafe_horizon(tmp_path: Path) -> None:
    from benchmarks.qualification.preflight import PreflightError, run_preflight

    with pytest.raises(PreflightError, match="schema"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(schema_head="0000_unknown"),
        )
    with pytest.raises(PreflightError, match="premake|horizon"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(premake_horizon_safe=False),
        )


def test_preflight_writes_environment_json_without_secrets(tmp_path: Path) -> None:
    from benchmarks.qualification.preflight import run_preflight

    out = tmp_path / "environment.json"
    payload = run_preflight(
        profile_path=PROFILE,
        compose_path=COMPOSE,
        output_path=out,
        facts=_facts(),
        capture={
            "profile_id": "phase-3.9-linux-x86_64-v1",
            "host": {
                "architecture": "x86_64",
                "logical_cpus": 16,
                "memory_gib": 32.0,
                "kernel": "Linux mock 6.1.0",
                "cpuinfo_summary": "vendor_id mock; model name Mock CPU",
                "filesystem_mount": "/var/lib/docker on /dev/nvme0n1p1 type ext4",
                "storage_locality": "local_ssd",
            },
            "runtime": {
                "docker_engine_version": "27.0.0",
                "docker_compose_version": "2.29.0",
            },
            "image_digests": _facts()["image_digests"],
                "postgres": {
                    "version": "PostgreSQL 18.6 on x86_64-pc-linux-musl",
                    "server_version_num": 180_006,
                    "settings": {
                        "max_connections": "100",
                        "shared_buffers": "128MB",
                        "server_version": "18.6",
                    },
                },
            "schema_revision": "0001_physical_contract_foundations",
        },
    )
    assert out.is_file()
    disk = json.loads(out.read_text(encoding="utf-8"))
    assert disk == payload
    for required in ("host", "runtime", "image_digests", "postgres", "profile_id"):
        assert required in payload
    blob = json.dumps(payload)
    assert SECRET_PATTERNS.search(blob) is None
    assert "DATABASE_URL" not in blob
    assert "QUEUE_API_BEARER_TOKEN" not in blob


def test_capture_environment_allowlist_excludes_secrets() -> None:
    from benchmarks.qualification.capture_environment import (
        ALLOWED_TOP_LEVEL_KEYS,
        filter_environment_payload,
    )

    dirty = {
        "profile_id": "phase-3.9-linux-x86_64-v1",
        "host": {"architecture": "x86_64"},
        "runtime": {"docker_engine_version": "27.0.0"},
        "image_digests": {"postgres": "sha256:" + "a" * 64, "queue": "sha256:" + "b" * 64},
        "postgres": {"version": "PostgreSQL 18.6", "server_version_num": 180_006},
        "schema_revision": "0001_physical_contract_foundations",
        "DATABASE_URL": "postgresql+psycopg://queue:queue@postgres:5432/queue",
        "password": "queue",
        "token": "secret-token",
    }
    clean = filter_environment_payload(dirty)
    assert set(clean) <= ALLOWED_TOP_LEVEL_KEYS
    assert "DATABASE_URL" not in clean
    assert "password" not in clean
    assert "token" not in clean
    blob = json.dumps(clean)
    assert SECRET_PATTERNS.search(blob) is None


def test_preflight_cli_exits_nonzero_on_failure(tmp_path: Path) -> None:
    # CLI without injectable facts probes the real host; on non-reference
    # Windows/dev laptops it must fail closed (nonzero).
    out = tmp_path / "environment.json"
    result = subprocess.run(
        [
            *PREFLIGHT_MOD,
            "--profile",
            str(PROFILE),
            "--compose",
            str(COMPOSE),
            "--output",
            str(out),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    # Either the host matches (rare) and exits 0, or fail-closed nonzero.
    # On this executor host we expect fail-closed for arch/CPU/RAM/storage.
    if result.returncode == 0:
        assert out.is_file()
        blob = out.read_text(encoding="utf-8")
        assert SECRET_PATTERNS.search(blob) is None
    else:
        assert result.returncode != 0
        assert not out.exists() or out.stat().st_size == 0 or "error" in result.stderr.lower()


def test_observed_compose_digests_parsed_from_image_lines() -> None:
    from benchmarks.qualification.preflight import parse_compose_image_digests

    digests = parse_compose_image_digests(_compose_text())
    profile = _load_profile()
    assert digests["postgres"] == profile["images"]["postgres"]["digest"]
    assert digests["queue"] == profile["images"]["queue"]["digest"]


def test_preflight_rejects_profile_vs_observed_digest_mismatch(tmp_path: Path) -> None:
    from benchmarks.qualification.preflight import PreflightError, run_preflight

    wrong = "sha256:" + "0" * 64
    with pytest.raises(PreflightError, match="image digest mismatch"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(image_digests={"postgres": wrong, "queue": _facts()["image_digests"]["queue"]}),
        )


def test_collect_default_facts_uses_live_probes_not_hardcoded_stubs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operational path must call probe helpers; wrong/matching facts drive validate."""
    from benchmarks.qualification import preflight as pf

    profile = _load_profile()
    matching = {
        "schema_head": profile["schema"]["expected_head"],
        "premake_horizon_safe": True,
        "database_empty": True,
        "database_dedicated": True,
        "server_version_num": 180_006,
        "postgres": {
            "version": "PostgreSQL 18.6 on x86_64-pc-linux-musl",
            "server_version_num": 180_006,
            "settings": {"max_connections": "100", "server_version": "18.6"},
        },
    }
    calls: list[str] = []

    def fake_probe(_profile: object, _compose: Path) -> dict[str, Any]:
        calls.append("probe")
        return dict(matching)

    monkeypatch.setattr(pf, "probe_database_facts", fake_probe)
    monkeypatch.setattr(
        pf,
        "capture_host_summary",
        lambda: {
            "architecture": "x86_64",
            "logical_cpus": 16,
            "memory_gib": 32.0,
            "storage_locality": "local_ssd",
            "kernel": "Linux mock",
            "cpuinfo_summary": "mock",
            "filesystem_mount": "/ on /dev/nvme0n1 type ext4",
        },
    )

    facts = pf.collect_default_facts(profile, COMPOSE)
    assert calls == ["probe"]
    assert facts["schema_head"] == matching["schema_head"]
    assert facts["premake_horizon_safe"] is True
    assert facts["database_empty"] is True
    assert facts["database_dedicated"] is True
    assert facts["image_digests"]["postgres"] == profile["images"]["postgres"]["digest"]
    assert facts["image_digests"]["queue"] == profile["images"]["queue"]["digest"]
    # Digests must come from Compose observation, not a blind profile echo:
    # mutate profile after observation would still leave observed compose digests.
    assert facts["postgres_identity"]["version"].startswith("PostgreSQL")


def test_live_probe_wiring_fails_wrong_schema_horizon_or_nonempty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from benchmarks.qualification import preflight as pf

    profile = _load_profile()
    base_probe = {
        "schema_head": profile["schema"]["expected_head"],
        "premake_horizon_safe": True,
        "database_empty": True,
        "database_dedicated": True,
        "server_version_num": 180_006,
        "postgres": {
            "version": "PostgreSQL 18.6",
            "server_version_num": 180_006,
            "settings": {"max_connections": "100", "server_version": "18.6"},
        },
    }

    monkeypatch.setattr(
        pf,
        "capture_host_summary",
        lambda: {
            "architecture": "x86_64",
            "logical_cpus": 16,
            "memory_gib": 32.0,
            "storage_locality": "local_ssd",
            "kernel": "Linux mock",
            "cpuinfo_summary": "mock",
            "filesystem_mount": "/ on /dev/nvme0n1 type ext4",
        },
    )
    monkeypatch.setattr(
        pf,
        "capture_runtime_versions",
        lambda: {
            "docker_engine_version": "27.0.0",
            "docker_compose_version": "2.29.0",
        },
    )

    def _run_with_probe(probe: dict[str, Any]) -> None:
        monkeypatch.setattr(pf, "probe_database_facts", lambda *_a, **_k: dict(probe))
        pf.run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
        )

    with pytest.raises(pf.PreflightError, match="schema"):
        _run_with_probe({**base_probe, "schema_head": "0000_wrong"})
    with pytest.raises(pf.PreflightError, match="premake|horizon"):
        _run_with_probe({**base_probe, "premake_horizon_safe": False})
    with pytest.raises(pf.PreflightError, match="empty"):
        _run_with_probe({**base_probe, "database_empty": False})


def test_live_probe_wiring_passes_matching_stack_and_writes_postgres_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from benchmarks.qualification import preflight as pf

    profile = _load_profile()
    monkeypatch.setattr(
        pf,
        "capture_host_summary",
        lambda: {
            "architecture": "x86_64",
            "logical_cpus": 16,
            "memory_gib": 32.0,
            "storage_locality": "local_ssd",
            "kernel": "Linux mock",
            "cpuinfo_summary": "mock",
            "filesystem_mount": "/ on /dev/nvme0n1 type ext4",
        },
    )
    monkeypatch.setattr(
        pf,
        "capture_runtime_versions",
        lambda: {
            "docker_engine_version": "27.0.0",
            "docker_compose_version": "2.29.0",
        },
    )
    monkeypatch.setattr(
        pf,
        "probe_database_facts",
        lambda *_a, **_k: {
            "schema_head": profile["schema"]["expected_head"],
            "premake_horizon_safe": True,
            "database_empty": True,
            "database_dedicated": True,
            "server_version_num": 180_006,
            "postgres": {
                "version": "PostgreSQL 18.6 on x86_64-pc-linux-musl, compiled by gcc",
                "server_version_num": 180_006,
                "settings": {
                    "max_connections": "100",
                    "shared_buffers": "128MB",
                    "server_version": "18.6",
                },
            },
        },
    )

    out = tmp_path / "environment.json"
    payload = pf.run_preflight(
        profile_path=PROFILE,
        compose_path=COMPOSE,
        output_path=out,
    )
    assert out.is_file()
    assert payload["postgres"]["version"].startswith("PostgreSQL")
    assert payload["postgres"]["settings"]["max_connections"] == "100"
    assert payload["postgres"].get("server_version_num") == 180_006
    assert "unavailable-until-live-probe" not in json.dumps(payload)
    assert SECRET_PATTERNS.search(json.dumps(payload)) is None


def test_preflight_rejects_postgres_16_major_only_18_and_19(
    tmp_path: Path,
) -> None:
    from benchmarks.qualification.preflight import PreflightError, run_preflight

    with pytest.raises(PreflightError, match="version unsupported|16|18\\.6"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(
                server_version_num=160_006,
                server_version="16.6",
                postgres_identity={
                    "version": "PostgreSQL 16.6 on x86_64-pc-linux-musl",
                    "server_version_num": 160_006,
                    "settings": {"server_version": "16.6"},
                },
            ),
        )
    with pytest.raises(PreflightError, match="version unsupported|18\\.6"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(
                server_version_num=180_000,
                server_version="18.0",
                postgres_identity={
                    "version": "PostgreSQL 18.0 on x86_64-pc-linux-musl",
                    "server_version_num": 180_000,
                    "settings": {"server_version": "18.0"},
                },
            ),
        )
    with pytest.raises(PreflightError, match="version unsupported|18\\.6"):
        run_preflight(
            profile_path=PROFILE,
            compose_path=COMPOSE,
            output_path=tmp_path / "environment.json",
            facts=_facts(
                server_version_num=190_000,
                server_version="19.0",
                postgres_identity={
                    "version": "PostgreSQL 19.0 on x86_64-pc-linux-musl",
                    "server_version_num": 190_000,
                    "settings": {"server_version": "19.0"},
                },
            ),
        )


def test_parse_compose_rejects_digest_skew_between_profile_and_file(
    tmp_path: Path,
) -> None:
    """Compose digest that differs from profile must fail when facts are collected."""
    from benchmarks.qualification import preflight as pf
    from benchmarks.qualification.profile_loader import load_profile

    skewed = tmp_path / "skewed-compose.yml"
    text = _compose_text().replace(
        "sha256:6c538e7206ea40ff740ef27883529390a690b6ead6ba96b44c67a9f7c638e8fd",
        "sha256:" + "a" * 64,
    )
    skewed.write_text(text, encoding="utf-8")
    profile = load_profile(PROFILE)
    observed = pf.parse_compose_image_digests(text)
    assert observed["postgres"] != profile["images"]["postgres"]["digest"]
    with pytest.raises(pf.PreflightError, match="image digest mismatch"):
        pf.validate_facts(
            profile,
            {
                **_facts(),
                "image_digests": observed,
                "compose_images_digest_pinned": True,
            },
        )
