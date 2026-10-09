"""Compose lifecycle runner for PostgreSQL 18.6 storage integration tests.

Starts an isolated `docker-compose.conformance.yml` project, asserts exact
PostgreSQL 18.6 via ``server_version_num``, runs focused pytest files inside a
one-off `conformance-service` container, and always tears down with
leftover-resource assertions.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.conformance.yml"
HEALTH_DEADLINE_S = 60
POLL_INTERVAL_S = 2

# Internal DSN used only inside the compose network — never printed.
_INTERNAL_DSN = (
    "postgresql+psycopg://queue:queue@conformance-postgres:5432/queue"
)

_SECRET_IN_DSN = re.compile(
    r"(postgresql(?:\+psycopg)?://[^:]+:)([^@]+)(@)",
    flags=re.IGNORECASE,
)


def redact(text: str) -> str:
    """Strip credentials from any accidental DSN echo."""
    return _SECRET_IN_DSN.sub(r"\1***\3", text)


def _run(
    argv: list[str],
    *,
    check: bool = False,
    capture: bool = False,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=str(ROOT),
        check=check,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )


def preflight() -> None:
    docker = _run(["docker", "version", "--format", "{{.Server.Version}}"], capture=True)
    if docker.returncode != 0:
        raise RuntimeError(
            "Docker is required for storage integration "
            f"(docker version failed: {redact(docker.stderr or docker.stdout)})"
        )
    compose = _run(["docker", "compose", "version"], capture=True)
    if compose.returncode != 0:
        raise RuntimeError(
            "Docker Compose is required for storage integration "
            f"(docker compose version failed: {redact(compose.stderr or compose.stdout)})"
        )
    if not COMPOSE_FILE.is_file():
        raise RuntimeError(f"missing compose file: {COMPOSE_FILE}")


def compose_argv(project: str) -> list[str]:
    return [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(COMPOSE_FILE),
    ]


def wait_postgres_healthy(project: str, deadline_s: float = HEALTH_DEADLINE_S) -> None:
    service = f"{project}-conformance-postgres-1"
    deadline = time.monotonic() + deadline_s
    last = ""
    while time.monotonic() < deadline:
        probe = _run(
            [
                "docker",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                service,
            ],
            capture=True,
        )
        last = (probe.stdout or probe.stderr or "").strip()
        if probe.returncode == 0 and last == "healthy":
            return
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(
        f"conformance-postgres not healthy within {deadline_s}s "
        f"(last status={last!r}, service={service})"
    )


def assert_postgres_exact_minor_18_6(project: str) -> tuple[str, int, int, int]:
    """Fail closed unless engine is PostgreSQL 18.6.x (server_version_num=180006)."""

    def _psql_show(guc: str) -> str:
        probe = _run(
            [
                *compose_argv(project),
                "exec",
                "-T",
                "conformance-postgres",
                "psql",
                "-U",
                "queue",
                "-d",
                "queue",
                "-tAc",
                f"SHOW {guc}",
            ],
            capture=True,
        )
        if probe.returncode != 0:
            raise RuntimeError(
                f"failed reading PostgreSQL {guc}: "
                + redact((probe.stderr or probe.stdout or "").strip())
            )
        return (probe.stdout or "").strip()

    version = _psql_show("server_version")
    version_num_text = _psql_show("server_version_num")
    if not version_num_text.isdigit():
        raise RuntimeError(
            f"unexpected PostgreSQL server_version_num: {version_num_text!r}"
        )
    version_num = int(version_num_text)
    major, minor = divmod(version_num, 10_000)
    if (major, minor) != (18, 6) or not version.startswith("18.6"):
        raise RuntimeError(
            "expected PostgreSQL 18.6.x "
            f"(server_version_num=180006), got version={version!r} "
            f"server_version_num={version_num}"
        )
    return version, version_num, major, minor


def project_resources(project: str) -> tuple[list[str], list[str], list[str]]:
    label = f"com.docker.compose.project={project}"
    containers = _run(
        ["docker", "ps", "-aq", "--filter", f"label={label}"],
        capture=True,
    )
    volumes = _run(
        ["docker", "volume", "ls", "-q", "--filter", f"label={label}"],
        capture=True,
    )
    networks = _run(
        ["docker", "network", "ls", "-q", "--filter", f"label={label}"],
        capture=True,
    )
    def lines(proc: subprocess.CompletedProcess[str]) -> list[str]:
        if proc.returncode != 0:
            raise RuntimeError(
                "failed listing docker resources: "
                + redact((proc.stderr or proc.stdout or "").strip())
            )
        return [line for line in (proc.stdout or "").splitlines() if line.strip()]

    return lines(containers), lines(volumes), lines(networks)


def assert_project_clean(project: str) -> None:
    containers, volumes, networks = project_resources(project)
    if containers or volumes or networks:
        raise RuntimeError(
            "compose teardown left project-labeled resources: "
            f"containers={containers!r} volumes={volumes!r} networks={networks!r}"
        )


def run_pytest(project: str) -> int:
    argv = [
        *compose_argv(project),
        "run",
        "--rm",
        "--no-deps",
        "-e",
        f"TEST_DATABASE_URL={_INTERNAL_DSN}",
        "conformance-service",
        "uv",
        "run",
        "--group",
        "dev",
        "pytest",
        "tests/integration/test_postgresql_schema.py",
        "tests/integration/test_partition_horizon.py",
        "tests/integration/test_range_unique_partition_constraint.py",
        "-q",
    ]
    print("running focused storage integration pytest via compose run", flush=True)
    result = _run(argv)
    return int(result.returncode)


def teardown(project: str) -> None:
    down = _run(
        [
            *compose_argv(project),
            "down",
            "--volumes",
            "--remove-orphans",
        ],
        capture=True,
    )
    if down.returncode != 0:
        raise RuntimeError(
            "compose down failed: "
            + redact((down.stderr or down.stdout or "").strip())
        )
    assert_project_clean(project)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)

    project = f"queue-storage-{uuid.uuid4()}"
    test_exit = 1
    cleanup_failed = False

    try:
        preflight()
        print(f"starting isolated compose project {project}", flush=True)
        _run(
            [*compose_argv(project), "up", "-d", "conformance-postgres"],
            check=True,
        )
        wait_postgres_healthy(project)
        pg_version, pg_version_num, _, _ = assert_postgres_exact_minor_18_6(project)
        print(
            f"postgres exact-minor gate ok: "
            f"version={pg_version!r} server_version_num={pg_version_num}",
            flush=True,
        )
        test_exit = run_pytest(project)
    except Exception as exc:  # noqa: BLE001 — runner must always tear down
        print(f"storage integration failed: {redact(str(exc))}", file=sys.stderr)
        test_exit = 1
    finally:
        try:
            teardown(project)
        except Exception as exc:  # noqa: BLE001
            cleanup_failed = True
            print(
                f"storage integration cleanup failed: {redact(str(exc))}",
                file=sys.stderr,
            )

    if cleanup_failed:
        return 1
    return test_exit


if __name__ == "__main__":
    raise SystemExit(main())
