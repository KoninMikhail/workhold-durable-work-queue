"""Fail-closed preflight for the Phase 3.9 reference environment (QUAL-03/05).

Lifecycle (exact):
  docker compose -f benchmarks/qualification/docker-compose.qualification.yml pull
  docker compose -f benchmarks/qualification/docker-compose.qualification.yml up -d --wait postgres queue-api-1 queue-api-2
  uv run python -m benchmarks.qualification.preflight --profile benchmarks/qualification/reference-environment.yaml --compose benchmarks/qualification/docker-compose.qualification.yml --output benchmarks/results/phase-7-postgresql-18.6/environment.json
  docker compose -f benchmarks/qualification/docker-compose.qualification.yml down -v

Validates host architecture/CPU/RAM/storage locality, no CPU oversubscription
beyond the declared profile, immutable image digests (observed from Compose),
exact PostgreSQL 18.6 (server_version_num / version banner), schema head,
premake horizon and empty dedicated database before benchmark traffic is
allowed. Local image builds are forbidden so digests stay reconcileable.
Rejects mutable tags, major-only 18, PostgreSQL 16, and PostgreSQL 19.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

from benchmarks.qualification.capture_environment import (
    ALLOWED_POSTGRES_SETTING_KEYS,
    capture_host_summary,
    capture_runtime_versions,
    filter_postgres_settings,
    write_environment_json,
)
from benchmarks.qualification.profile_loader import load_profile

_DIGEST_IMAGE_RE = re.compile(
    r"^[^\s@]+@sha256:[0-9a-fA-F]{64}$|.*@sha256:[0-9a-fA-F]{64}"
)
_SHA256_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
_SHA256_EXTRACT_RE = re.compile(r"sha256:([0-9a-fA-F]{64})")

# Exact minor required for Phase 7 qualification (STOR-09 / QUAL-06).
_REQUIRED_PG_MAJOR = 18
_REQUIRED_PG_MINOR = 6
_REQUIRED_SERVER_VERSION_NUM = 180_006
_REQUIRED_PG_IMAGE_PREFIX = "postgres:18.6-alpine@sha256:"

_HISTORY_PARENTS: tuple[str, ...] = (
    "admin_audit_log",
    "task_attempts",
    "tasks_terminal",
    "delivery_events_terminal",
)


class PreflightError(RuntimeError):
    """Reference environment does not match the versioned profile."""


def compose_images_are_digest_pinned(compose_text: str) -> bool:
    """Return True when every ``image:`` line uses an immutable ``@sha256:`` pin."""
    images: list[str] = []
    for line in compose_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("image:"):
            continue
        value = stripped.split(":", 1)[1].strip().strip("\"'")
        images.append(value)
    if not images:
        return False
    return all("@sha256:" in image and _DIGEST_IMAGE_RE.search(image) for image in images)


def parse_compose_image_digests(compose_text: str) -> dict[str, str]:
    """Observe ``sha256:…`` digests from Compose ``image:`` lines.

    Classifies by image reference prefix (``postgres`` → profile key ``postgres``,
    ``queue`` → ``queue``). Works with YAML anchors/merges that hoist ``image:``
    under ``x-*`` extensions. Conflicting digests for the same key raise
    :class:`PreflightError`.
    """
    observed: dict[str, str] = {}
    for raw in compose_text.splitlines():
        stripped = raw.strip()
        if not stripped.startswith("image:"):
            continue
        value = stripped.split(":", 1)[1].strip().strip("\"'")
        match = _SHA256_EXTRACT_RE.search(value)
        if match is None:
            raise PreflightError(
                f"compose image is not digest-pinned: {value!r}"
            )
        digest = f"sha256:{match.group(1).lower()}"
        lower = value.lower()
        if lower.startswith("postgres"):
            key = "postgres"
        elif lower.startswith("queue"):
            key = "queue"
        else:
            # Ignore unrelated images (none expected in the qualification stack).
            continue
        prior = observed.get(key)
        if prior is not None and prior != digest:
            raise PreflightError(
                f"compose image digest conflict for {key}: {prior!r} vs {digest!r}"
            )
        observed[key] = digest
    if "postgres" not in observed or "queue" not in observed:
        raise PreflightError(
            "compose image digests incomplete: need postgres and queue image pins"
        )
    return observed


def _assert_postgres_image_reference(reference: str) -> None:
    """Reject mutable tags, 16, major-only 18, and 19; require 18.6-alpine@sha256:."""
    ref = reference.strip()
    lower = ref.lower()
    if "@sha256:" not in ref:
        raise PreflightError(f"mutable image tag in profile for postgres: {ref!r}")
    if "16-alpine" in lower or re.search(r"postgres:16([\s@-]|$)", lower):
        raise PreflightError(
            f"PostgreSQL 16 image is forbidden for qualification: {ref!r}"
        )
    if re.search(r"postgres:19([\s@.-]|$)", lower):
        raise PreflightError(
            f"PostgreSQL 19 image is forbidden for qualification: {ref!r}"
        )
    # Major-only 18 (postgres:18@… / postgres:18-alpine@…) without 18.6.
    if re.search(r"postgres:18(@|-alpine)", lower) and "postgres:18.6" not in lower:
        raise PreflightError(
            f"major-only PostgreSQL 18 image is forbidden; require 18.6: {ref!r}"
        )
    if _REQUIRED_PG_IMAGE_PREFIX not in lower:
        raise PreflightError(
            "postgres image must be digest-pinned postgres:18.6-alpine@sha256:…; "
            f"got {ref!r}"
        )


def _assert_postgres_exact_minor_18_6(
    *,
    server_version_num: int | None,
    server_version: str | None,
    version_banner: str | None,
) -> None:
    """Require divmod(server_version_num, 10000)==(18, 6) and 18.6 banner prefix."""
    if server_version_num is None:
        raise PreflightError(
            "postgres server_version_num missing; require exact 18.6 "
            f"(expected {_REQUIRED_SERVER_VERSION_NUM})"
        )
    major, minor = divmod(int(server_version_num), 10_000)
    if (major, minor) != (_REQUIRED_PG_MAJOR, _REQUIRED_PG_MINOR):
        raise PreflightError(
            "postgres version unsupported: "
            f"server_version_num={server_version_num} "
            f"(divmod→{(major, minor)}); require "
            f"({_REQUIRED_PG_MAJOR}, {_REQUIRED_PG_MINOR}) / "
            f"{_REQUIRED_SERVER_VERSION_NUM}"
        )
    # Prefer pg_settings server_version; fall back to version() banner.
    banner = (server_version or "").strip()
    if not banner and version_banner:
        # version() looks like "PostgreSQL 18.6 on …"
        match = re.search(r"(\d+\.\d+)", version_banner)
        banner = match.group(1) if match else version_banner
    if not banner.startswith("18.6"):
        raise PreflightError(
            "postgres server_version must start with 18.6; "
            f"got server_version={server_version!r} version()={version_banner!r}"
        )


def validate_facts(profile: Mapping[str, Any], facts: Mapping[str, Any]) -> None:
    """Raise PreflightError when injected or probed facts fail the profile."""
    host = profile["host"]
    arch = str(facts.get("architecture", "")).lower().replace("amd64", "x86_64")
    expected_arch = str(host["architecture"]).lower().replace("amd64", "x86_64")
    if arch != expected_arch:
        raise PreflightError(
            f"architecture mismatch: host={arch!r} required={expected_arch!r}"
        )

    logical_cpus = int(facts.get("logical_cpus") or 0)
    min_cpus = int(host["min_logical_cpus"])
    if logical_cpus < min_cpus:
        raise PreflightError(
            f"insufficient logical_cpus/CPU: host={logical_cpus} required>={min_cpus}"
        )

    memory_gib = float(facts.get("memory_gib") or 0.0)
    min_memory = float(host["min_memory_gib"])
    if memory_gib < min_memory:
        raise PreflightError(
            f"insufficient memory/RAM: host={memory_gib} GiB required>={min_memory} GiB"
        )

    locality = str(facts.get("storage_locality", ""))
    if locality != host["storage_locality"]:
        raise PreflightError(
            f"storage locality mismatch: host={locality!r} "
            f"required={host['storage_locality']!r}"
        )

    if host.get("co_resident_workload_allowed") is False and facts.get(
        "co_resident_workload"
    ):
        raise PreflightError("co-resident workload is not allowed on the reference host")

    services = profile["services"]
    declared_cpus = sum(float(spec["cpus"]) for spec in services.values())
    if facts.get("cpu_oversubscribed") or declared_cpus > logical_cpus:
        raise PreflightError(
            "CPU oversubscription beyond the declared profile: "
            f"declared={declared_cpus} host={logical_cpus}"
        )

    if not facts.get("compose_images_digest_pinned", False):
        raise PreflightError("mutable image tag: compose images must be digest-pinned")

    expected_images = profile["images"]
    actual_digests = facts.get("image_digests") or {}
    for name, spec in expected_images.items():
        expected = str(spec["digest"])
        if not _SHA256_RE.match(expected):
            raise PreflightError(f"profile image digest malformed for {name}: {expected}")
        actual = str(actual_digests.get(name, ""))
        if actual != expected:
            raise PreflightError(
                f"image digest mismatch for {name}: actual={actual!r} expected={expected!r}"
            )
        reference = str(spec.get("reference", ""))
        if reference and "@sha256:" not in reference:
            raise PreflightError(f"mutable image tag in profile for {name}: {reference!r}")
        if name == "postgres":
            _assert_postgres_image_reference(reference)

    # Exact PostgreSQL 18.6 identity (fail-closed on 16 / major-only 18 / 19).
    postgres_identity = facts.get("postgres_identity") or {}
    settings: Mapping[str, Any] = {}
    if isinstance(postgres_identity, Mapping):
        raw_settings = postgres_identity.get("settings") or {}
        if isinstance(raw_settings, Mapping):
            settings = raw_settings
    server_version_num = facts.get("server_version_num")
    if server_version_num is None and isinstance(postgres_identity, Mapping):
        server_version_num = postgres_identity.get("server_version_num")
    server_version = facts.get("server_version")
    if server_version is None:
        server_version = settings.get("server_version")
    version_banner = None
    if isinstance(postgres_identity, Mapping):
        version_banner = postgres_identity.get("version")
    _assert_postgres_exact_minor_18_6(
        server_version_num=(
            int(server_version_num) if server_version_num is not None else None
        ),
        server_version=str(server_version) if server_version is not None else None,
        version_banner=str(version_banner) if version_banner is not None else None,
    )

    expected_head = str(profile["schema"]["expected_head"])
    schema_head = str(facts.get("schema_head", ""))
    if schema_head != expected_head:
        raise PreflightError(
            f"schema head mismatch: actual={schema_head!r} expected={expected_head!r}"
        )

    if not facts.get("premake_horizon_safe", False):
        raise PreflightError("unsafe partition premake horizon")

    if profile.get("database", {}).get("must_be_empty", True) and not facts.get(
        "database_empty", False
    ):
        raise PreflightError("dedicated database is not empty")

    if profile.get("database", {}).get("dedicated", True) and not facts.get(
        "database_dedicated", False
    ):
        raise PreflightError("database is not dedicated to qualification")


def _compose_psql(
    compose_path: Path,
    sql: str,
    *,
    service: str = "postgres",
) -> str:
    """Run read-only SQL via ``docker compose exec`` against the stack postgres."""
    argv = [
        "docker",
        "compose",
        "-f",
        str(compose_path),
        "exec",
        "-T",
        service,
        "psql",
        "-U",
        "queue",
        "-d",
        "queue",
        "-v",
        "ON_ERROR_STOP=1",
        "-tAc",
        sql,
    ]
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        raise PreflightError(f"postgres live probe unavailable: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise PreflightError(
            f"postgres live probe failed (exit {result.returncode}): {detail[:200]}"
        )
    return (result.stdout or "").strip()


def _horizon_safe_sql(premake_days: int) -> str:
    parents_array = ",".join(f"'{p}'" for p in _HISTORY_PARENTS)
    return f"""
WITH bounds AS (
  SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date AS today
),
required AS (
  SELECT p.parent_name,
         to_char(b.today + s.offset, 'YYYYMMDD') AS suffix
  FROM (SELECT unnest(ARRAY[{parents_array}]) AS parent_name) p
  CROSS JOIN bounds b
  CROSS JOIN generate_series(0, {int(premake_days)}) AS s(offset)
),
missing AS (
  SELECT r.parent_name, r.suffix
  FROM required r
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
      AND c.relkind = 'r'
      AND c.relname = r.parent_name || '_' || r.suffix
  )
)
SELECT CASE WHEN EXISTS (SELECT 1 FROM missing) THEN 'f' ELSE 't' END
""".strip()


def probe_database_facts(
    profile: Mapping[str, Any],
    compose_path: Path,
) -> dict[str, Any]:
    """Live-probe schema head, premake horizon, emptiness, dedication and PG identity.

    Uses ``docker compose exec`` against the qualification postgres service. Raises
    :class:`PreflightError` when the stack is unreachable so the operational path
    fails closed instead of silently stubbing PASS/FAIL facts.
    """
    schema_head = _compose_psql(
        compose_path, "SELECT version_num FROM alembic_version LIMIT 1"
    )
    premake_days = int(profile["schema"]["premake_days"])
    horizon_flag = _compose_psql(compose_path, _horizon_safe_sql(premake_days))
    premake_horizon_safe = horizon_flag.lower() in {"t", "true", "1"}

    empty_flag = _compose_psql(
        compose_path,
        """
SELECT CASE WHEN
  (SELECT count(*) FROM tasks_active) = 0
  AND (SELECT count(*) FROM claim_registry) = 0
  AND (SELECT count(*) FROM delivery_events_active) = 0
  AND (SELECT count(*) FROM enqueue_dedup) = 0
THEN 't' ELSE 'f' END
""".strip(),
    )
    database_empty = empty_flag.lower() in {"t", "true", "1"}

    dedicated_flag = _compose_psql(
        compose_path,
        "SELECT CASE WHEN current_database() = 'queue' AND current_user = 'queue' "
        "THEN 't' ELSE 'f' END",
    )
    database_dedicated = dedicated_flag.lower() in {"t", "true", "1"}

    version = _compose_psql(compose_path, "SELECT version()")
    version_num_raw = _compose_psql(compose_path, "SHOW server_version_num")
    try:
        server_version_num = int(version_num_raw)
    except ValueError as exc:
        raise PreflightError(
            f"postgres server_version_num unparseable: {version_num_raw!r}"
        ) from exc
    settings_raw = _compose_psql(
        compose_path,
        "SELECT name || '=' || setting FROM pg_settings WHERE name = ANY("
        "ARRAY['max_connections','shared_buffers','work_mem','maintenance_work_mem',"
        "'effective_cache_size','wal_level','max_wal_senders','listen_addresses',"
        "'server_version','server_encoding','TimeZone']) ORDER BY name",
    )
    settings: dict[str, str] = {}
    for line in settings_raw.splitlines():
        if "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name in ALLOWED_POSTGRES_SETTING_KEYS:
            settings[name] = value

    return {
        "schema_head": schema_head,
        "premake_horizon_safe": premake_horizon_safe,
        "database_empty": database_empty,
        "database_dedicated": database_dedicated,
        "server_version_num": server_version_num,
        "postgres": {
            "version": version,
            "server_version_num": server_version_num,
            "settings": filter_postgres_settings(settings),
        },
    }


def collect_default_facts(profile: Mapping[str, Any], compose_path: Path) -> dict[str, Any]:
    """Probe the local host/compose/stack for operational fail-closed facts."""
    host = capture_host_summary()
    compose_text = compose_path.read_text(encoding="utf-8")
    pinned = compose_images_are_digest_pinned(compose_text)
    observed_digests = parse_compose_image_digests(compose_text)
    services = profile["services"]
    declared_cpus = sum(float(spec["cpus"]) for spec in services.values())
    logical_cpus = int(host["logical_cpus"])
    db_facts = probe_database_facts(profile, compose_path)
    return {
        "architecture": host["architecture"],
        "logical_cpus": logical_cpus,
        "memory_gib": float(host["memory_gib"]),
        "storage_locality": host["storage_locality"],
        "cpu_oversubscribed": declared_cpus > logical_cpus,
        "co_resident_workload": False,
        "image_digests": observed_digests,
        "compose_images_digest_pinned": pinned,
        "schema_head": db_facts["schema_head"],
        "premake_horizon_safe": db_facts["premake_horizon_safe"],
        "database_empty": db_facts["database_empty"],
        "database_dedicated": db_facts["database_dedicated"],
        "server_version_num": db_facts["server_version_num"],
        "server_version": (db_facts["postgres"].get("settings") or {}).get(
            "server_version"
        ),
        "postgres_identity": db_facts["postgres"],
    }


def run_preflight(
    *,
    profile_path: Path,
    compose_path: Path,
    output_path: Path,
    facts: Mapping[str, Any] | None = None,
    capture: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate the reference profile and write allowlisted environment.json."""
    profile = load_profile(profile_path)
    resolved_facts = dict(facts) if facts is not None else collect_default_facts(
        profile, compose_path
    )
    if "compose_images_digest_pinned" not in resolved_facts:
        resolved_facts["compose_images_digest_pinned"] = compose_images_are_digest_pinned(
            compose_path.read_text(encoding="utf-8")
        )
    if "image_digests" not in resolved_facts and facts is None:
        resolved_facts["image_digests"] = parse_compose_image_digests(
            compose_path.read_text(encoding="utf-8")
        )
    validate_facts(profile, resolved_facts)

    if capture is not None:
        payload = dict(capture)
    else:
        postgres_identity = resolved_facts.get("postgres_identity") or {
            "version": "",
            "settings": {},
        }
        image_digests = dict(resolved_facts.get("image_digests") or {})
        payload = {
            "profile_id": profile["profile_id"],
            "captured_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "host": capture_host_summary(),
            "runtime": capture_runtime_versions(),
            "image_digests": image_digests,
            "postgres": {
                "version": str(postgres_identity.get("version") or ""),
                "server_version_num": (
                    postgres_identity.get("server_version_num")
                    if postgres_identity.get("server_version_num") is not None
                    else resolved_facts.get("server_version_num")
                ),
                "settings": filter_postgres_settings(
                    postgres_identity.get("settings") or {}
                ),
            },
            "schema_revision": str(resolved_facts.get("schema_head") or ""),
        }
    payload.setdefault("profile_id", profile["profile_id"])
    return write_environment_json(output_path, payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.qualification.preflight",
        description="Fail-closed reference-environment preflight for Phase 3 qualification.",
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--compose", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        run_preflight(
            profile_path=args.profile,
            compose_path=args.compose,
            output_path=args.output,
        )
    except PreflightError as exc:
        print(f"preflight error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI fail-closed
        print(f"preflight error: {exc}", file=sys.stderr)
        return 1
    print(f"preflight OK wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
