"""Capture host/runtime/image/PostgreSQL identity for qualification evidence.

Never records DSNs, passwords, bearer tokens or other secrets (T-039-19).
"""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Any, Mapping

ALLOWED_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "profile_id",
        "host",
        "runtime",
        "image_digests",
        "postgres",
        "schema_revision",
        "captured_at_utc",
    }
)

# Non-secret PostgreSQL settings allowed in environment.json (T-039-19).
ALLOWED_POSTGRES_SETTING_KEYS: frozenset[str] = frozenset(
    {
        "max_connections",
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "effective_cache_size",
        "wal_level",
        "max_wal_senders",
        "listen_addresses",
        "server_version",
        "server_encoding",
        "TimeZone",
    }
)

_SECRET_KEY_RE = re.compile(
    r"(?i)^(password|passwd|secret|token|bearer|dsn|database_url|"
    r"queue_api_bearer|postgres_password).*$"
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)(postgresql\+psycopg://|postgres://|://[^/\s]+:[^@/\s]+@|"
    r"bearer\s+[a-z0-9._\-]+)"
)


class CaptureError(RuntimeError):
    """Environment capture failed closed."""


def filter_postgres_settings(settings: Mapping[str, Any]) -> dict[str, str]:
    """Keep only allowlisted non-secret PostgreSQL setting names."""
    out: dict[str, str] = {}
    for key, value in settings.items():
        name = str(key)
        if name not in ALLOWED_POSTGRES_SETTING_KEYS:
            continue
        if _SECRET_KEY_RE.match(name):
            continue
        text = str(value)
        if _SECRET_VALUE_RE.search(text):
            continue
        out[name] = text
    return out


def filter_environment_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return an allowlisted copy with secret-looking keys/values removed."""
    clean: dict[str, Any] = {}
    for key, value in payload.items():
        if key not in ALLOWED_TOP_LEVEL_KEYS:
            continue
        if _SECRET_KEY_RE.match(str(key)):
            continue
        if key == "postgres" and isinstance(value, Mapping):
            version = value.get("version", "")
            settings = value.get("settings") or {}
            scrubbed_version = _scrub(version)
            cleaned_pg: dict[str, Any] = {
                "version": scrubbed_version if isinstance(scrubbed_version, str) else "",
                "settings": filter_postgres_settings(
                    settings if isinstance(settings, Mapping) else {}
                ),
            }
            # Bind exact engine identity when present (Phase 7 / QUAL-06).
            if "server_version_num" in value and value.get("server_version_num") is not None:
                try:
                    cleaned_pg["server_version_num"] = int(value["server_version_num"])
                except (TypeError, ValueError):
                    pass
            clean[key] = cleaned_pg
            continue
        clean[key] = _scrub(value)
    return clean


def _scrub(value: Any) -> Any:
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, child in value.items():
            if _SECRET_KEY_RE.match(str(key)):
                continue
            scrubbed = _scrub(child)
            if isinstance(scrubbed, str) and _SECRET_VALUE_RE.search(scrubbed):
                continue
            out[str(key)] = scrubbed
        return out
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        return "<redacted>"
    return value


def capture_host_summary() -> dict[str, Any]:
    """Collect non-secret host identity suitable for environment.json."""
    arch = platform.machine().lower().replace("amd64", "x86_64")
    logical_cpus = os.cpu_count() or 0
    memory_gib = _read_memory_gib()
    return {
        "architecture": arch,
        "logical_cpus": logical_cpus,
        "memory_gib": memory_gib,
        "kernel": _safe_text(["uname", "-a"]) or platform.platform(),
        "cpuinfo_summary": _cpuinfo_summary(),
        "filesystem_mount": _filesystem_mount_summary(),
        "storage_locality": _guess_storage_locality(),
    }


def capture_runtime_versions() -> dict[str, Any]:
    engine = _safe_text(["docker", "version", "--format", "{{.Server.Version}}"])
    compose = _safe_text(["docker", "compose", "version", "--short"])
    if not compose:
        compose = _safe_text(["docker", "compose", "version"])
    return {
        "docker_engine_version": engine or "unknown",
        "docker_compose_version": compose or "unknown",
    }


def write_environment_json(path: Path | str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Filter, write and return the allowlisted environment capture."""
    clean = filter_environment_payload(payload)
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(clean, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return clean


def _read_memory_gib() -> float:
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        for line in meminfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("MemTotal:"):
                kb = int(line.split()[1])
                return round(kb / (1024 * 1024), 3)
    # Windows / non-Linux probe: report 0 so preflight fails closed off-profile.
    return 0.0


def _cpuinfo_summary() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if not cpuinfo.is_file():
        return platform.processor() or "unavailable"
    vendor = ""
    model = ""
    for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("vendor_id") and not vendor:
            vendor = line.split(":", 1)[1].strip()
        elif line.startswith("model name") and not model:
            model = line.split(":", 1)[1].strip()
        if vendor and model:
            break
    return f"vendor_id {vendor}; model name {model}".strip()


def _filesystem_mount_summary() -> str:
    mounts = Path("/proc/mounts")
    if not mounts.is_file():
        return "unavailable"
    for line in mounts.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[1] in ("/", "/var/lib/docker", "/var"):
            return f"{parts[1]} on {parts[0]} type {parts[2]}"
    first = mounts.read_text(encoding="utf-8", errors="replace").splitlines()[:1]
    return first[0] if first else "unavailable"


def _guess_storage_locality() -> str:
    """Best-effort SSD locality label; preflight still requires profile match."""
    mounts = Path("/proc/mounts")
    if not mounts.is_file():
        return "unknown"
    text = mounts.read_text(encoding="utf-8", errors="replace")
    if any(token in text for token in ("nvme", "ssd", "virtio")):
        return "local_ssd"
    if any(token in text for token in ("nfs", "cifs", "fuse")):
        return "network"
    return "local_other"


def _safe_text(argv: list[str]) -> str | None:
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if result.returncode != 0:
        return None
    return (result.stdout or "").strip() or None
