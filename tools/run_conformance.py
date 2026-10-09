"""Compose lifecycle runner for the Phase 3.1 conformance skeleton.

Boots an isolated docker-compose.conformance.yml project, waits for PostgreSQL 18.6
and service health, runs Alembic inside the service container, executes the
black-box harness case set, writes a machine-readable report, and always tears
down with volumes and orphans removed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.conformance.cases import build_phase_31_skeleton_cases  # noqa: E402
from tests.conformance.harness import CaseOutcome, ConformanceHarness  # noqa: E402

COMPOSE_FILE = ROOT / "docker-compose.conformance.yml"
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"
HEALTH_DEADLINE_S = 90.0
POLL_INTERVAL_S = 2.0
DIAG_LIMIT = 8192
EXPECTED_ALEMBIC_REVISION = "039_apply_qualified_storage_layout"
SERVICE_NAME = "conformance-service"
POSTGRES_NAME = "conformance-postgres"

_SECRET_DSN = re.compile(
    r"(postgresql(?:\+[\w]+)?://[^:\s/]+:)([^@\s]+)(@)",
    flags=re.IGNORECASE,
)


def redact(text: str) -> str:
    """Strip credentials and sensitive literals from diagnostics."""
    redacted = _SECRET_DSN.sub(r"\1***\3", text)
    for src, dst in (
        ("DATABASE_URL", "[REDACTED]"),
        ("Authorization", "[REDACTED]"),
        ("X-Queue-Claim-Token", "[REDACTED]"),
        ("claim-secret", "[REDACTED]"),
        ("Bearer secret-token", "[REDACTED]"),
        ("queue:queue@", "[REDACTED]@"),
        ("postgresql+psycopg://queue:queue@", "postgresql+psycopg://[REDACTED]@"),
    ):
        redacted = redacted.replace(src, dst)
    return redacted


def clip(text: str, limit: int = DIAG_LIMIT) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def run_cmd(
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
    docker = run_cmd(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        capture=True,
    )
    if docker.returncode != 0:
        raise RuntimeError(
            "Docker is required for conformance "
            f"(docker version failed: {redact(docker.stderr or docker.stdout)})"
        )
    compose = run_cmd(["docker", "compose", "version"], capture=True)
    if compose.returncode != 0:
        raise RuntimeError(
            "Docker Compose is required for conformance "
            f"(docker compose version failed: {redact(compose.stderr or compose.stdout)})"
        )
    if not COMPOSE_FILE.is_file():
        raise RuntimeError(f"missing compose file: {COMPOSE_FILE}")


def compose_argv(project: str) -> list[str]:
    return ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE)]


def wait_service_healthy(
    project: str,
    service: str,
    deadline_s: float = HEALTH_DEADLINE_S,
) -> None:
    container = f"{project}-{service}-1"
    deadline = time.monotonic() + deadline_s
    last = ""
    while time.monotonic() < deadline:
        probe = run_cmd(
            [
                "docker",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                container,
            ],
            capture=True,
        )
        last = (probe.stdout or probe.stderr or "").strip()
        if probe.returncode == 0 and last == "healthy":
            return
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(
        f"{service} not healthy within {deadline_s}s "
        f"(last status={last!r}, container={container})"
    )


def project_resources(project: str) -> tuple[list[str], list[str], list[str]]:
    label = f"com.docker.compose.project={project}"

    def lines(argv: list[str]) -> list[str]:
        proc = run_cmd(argv, capture=True)
        if proc.returncode != 0:
            raise RuntimeError(
                "failed listing docker resources: "
                + redact((proc.stderr or proc.stdout or "").strip())
            )
        return [line for line in (proc.stdout or "").splitlines() if line.strip()]

    containers = lines(["docker", "ps", "-aq", "--filter", f"label={label}"])
    volumes = lines(["docker", "volume", "ls", "-q", "--filter", f"label={label}"])
    networks = lines(["docker", "network", "ls", "-q", "--filter", f"label={label}"])
    return containers, volumes, networks


def assert_project_clean(project: str) -> None:
    containers, volumes, networks = project_resources(project)
    if containers or volumes or networks:
        raise RuntimeError(
            "compose teardown left project-labeled resources: "
            f"containers={containers!r} volumes={volumes!r} networks={networks!r}"
        )


def teardown(project: str) -> None:
    down = run_cmd(
        [*compose_argv(project), "down", "--volumes", "--remove-orphans"],
        capture=True,
    )
    if down.returncode != 0:
        raise RuntimeError(
            "compose down failed: "
            + redact((down.stderr or down.stdout or "").strip())
        )
    assert_project_clean(project)


def published_base_url(project: str) -> str:
    probe = run_cmd(
        [*compose_argv(project), "port", SERVICE_NAME, "8080"],
        capture=True,
    )
    if probe.returncode != 0:
        raise RuntimeError(
            "failed to resolve published service port: "
            + redact((probe.stderr or probe.stdout or "").strip())
        )
    binding = (probe.stdout or "").strip().splitlines()[-1].strip()
    if binding.startswith("["):
        host_port = binding.rsplit("]:", 1)[-1]
        host = "127.0.0.1"
    else:
        host, host_port = binding.rsplit(":", 1)
        if host in {"0.0.0.0", "::", ""}:
            host = "127.0.0.1"
    return f"http://{host}:{host_port}"


def postgres_version(project: str) -> tuple[str, int, int, int]:
    """Return (server_version, server_version_num, major, minor).

    Exact-minor gate uses integer GUC ``server_version_num`` via
    ``divmod(..., 10000)`` plus a ``server_version`` prefix check for ``18.6``.
    """

    def _psql_show(guc: str) -> str:
        probe = run_cmd(
            [
                *compose_argv(project),
                "exec",
                "-T",
                POSTGRES_NAME,
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
    return version, version_num, major, minor


def run_alembic_upgrade(project: str) -> str:
    upgrade = run_cmd(
        [
            *compose_argv(project),
            "exec",
            "-T",
            SERVICE_NAME,
            "uv",
            "run",
            "--group",
            "dev",
            "alembic",
            "upgrade",
            "head",
        ],
        capture=True,
        timeout=180,
    )
    if upgrade.returncode != 0:
        raise RuntimeError(
            "alembic upgrade failed: "
            + redact(clip((upgrade.stderr or upgrade.stdout or "").strip()))
        )
    current = run_cmd(
        [
            *compose_argv(project),
            "exec",
            "-T",
            SERVICE_NAME,
            "uv",
            "run",
            "--group",
            "dev",
            "alembic",
            "current",
        ],
        capture=True,
        timeout=60,
    )
    if current.returncode != 0:
        raise RuntimeError(
            "alembic current failed: "
            + redact(clip((current.stderr or current.stdout or "").strip()))
        )
    text = (current.stdout or "").strip()
    if EXPECTED_ALEMBIC_REVISION not in text:
        raise RuntimeError(
            "alembic revision mismatch after upgrade: " + redact(clip(text))
        )
    return EXPECTED_ALEMBIC_REVISION


def openapi_expected_protocol() -> dict[str, Any]:
    doc = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    caps = (
        doc.get("components", {})
        .get("schemas", {})
        .get("Capabilities", {})
        .get("properties", {})
    )
    protocol = caps.get("protocol_version", {}).get("const")
    schema = caps.get("schema_revision", {}).get("const")
    if protocol != "1.0" or schema != "0001":
        raise RuntimeError(
            "committed OpenAPI capability consts drifted from Phase 3.1 "
            f"expectations (protocol_version={protocol!r}, schema_revision={schema!r})"
        )
    return {
        "protocol_version": protocol,
        "schema_revision": schema,
        "service_capabilities_observed": False,
    }


def run_selected_pytest(
    *,
    base_url: str,
    database_url: str,
    pytest_args: list[str],
) -> int:
    """Run selected pytest with compose base URL + TEST_DATABASE_URL in env."""
    env = os.environ.copy()
    env["TEST_DATABASE_URL"] = database_url
    env["CONFORMANCE_BASE_URL"] = base_url
    proc = subprocess.run(
        ["uv", "run", "pytest", *pytest_args],
        cwd=str(ROOT),
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if proc.stdout:
        print(proc.stdout, flush=True)
    if proc.stderr:
        print(redact(proc.stderr), file=sys.stderr)
    return int(proc.returncode)


def run_harness(base_url: str) -> dict[str, Any]:
    cases = build_phase_31_skeleton_cases()
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url=base_url)
    for case in cases:
        if case.body is not None:
            harness.validate_request_fixture(case.operation_id, case.body)
        harness.register_adapter(case.operation_id, case)
    report = harness.run_cases(cases)
    payload = report.to_json_dict()
    payload["overall"] = (
        "PASSED" if report.suite_outcome == CaseOutcome.PASS else "FAILED"
    )
    return payload


def summarize_cases(raw: dict[str, Any]) -> dict[str, int]:
    counts = {
        "total": 0,
        "passed": 0,
        "unsupported_operation": 0,
        "contract_failure": 0,
        "harness_error": 0,
        "skipped": 0,
    }
    for case in raw.get("cases") or []:
        counts["total"] += 1
        outcome = case.get("outcome")
        if outcome == CaseOutcome.PASS.value:
            counts["passed"] += 1
        elif outcome == CaseOutcome.UNSUPPORTED_OPERATION.value:
            counts["unsupported_operation"] += 1
        elif outcome == CaseOutcome.CONTRACT_FAILURE.value:
            counts["contract_failure"] += 1
        elif outcome == CaseOutcome.HARNESS_ERROR.value:
            counts["harness_error"] += 1
    return counts


def skeleton_assert_ok(
    raw: dict[str, Any],
    counts: dict[str, int],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if counts["total"] == 0:
        reasons.append("no cases executed")
    if counts["passed"] != 0:
        reasons.append("expected zero passed cases")
    if counts["skipped"] != 0:
        reasons.append("expected zero skipped cases")
    if counts["unsupported_operation"] != counts["total"]:
        reasons.append("every selected operation must be UNSUPPORTED_OPERATION")
    if counts["contract_failure"] != 0 or counts["harness_error"] != 0:
        reasons.append("contract/harness failures are not skeleton-assert success")
    if raw.get("overall") != "FAILED":
        reasons.append("raw conformance overall must be FAILED")
    if int(raw.get("exit_code") or 0) == 0:
        reasons.append("raw conformance exit_code must be non-zero")
    return (not reasons), reasons


def write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True)
    for literal in ("claim-secret", "Bearer secret-token", "queue:queue@"):
        if literal in text:
            raise RuntimeError(f"refusing to write report containing {literal!r}")
    path.write_text(text + "\n", encoding="utf-8")


DEFAULT_PHASE_34_PYTEST = [
    "tests/conformance/test_enqueue_uncertain_commit.py",
    "tests/conformance/test_phase_03_4_intake.py",
    "tests/concurrency/test_enqueue_state_races.py",
    "-q",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skeleton-assert",
        action="store_true",
        help=(
            "Meta-verification mode: exit 0 only when boot/migration succeeded "
            "and every selected operation is explicitly UNSUPPORTED_OPERATION"
        ),
    )
    parser.add_argument(
        "--pytest",
        nargs="*",
        metavar="ARG",
        default=None,
        help=(
            "After boot, run selected pytest with TEST_DATABASE_URL and "
            "CONFORMANCE_BASE_URL set. Omit ARG list to use Phase 3.4 defaults."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / ".conformance-out",
        help="Directory for conformance-report.json",
    )
    args = parser.parse_args(argv)

    project = f"queue-conform-{uuid.uuid4().hex[:8]}"
    report_path = args.output_dir / "conformance-report.json"
    diagnostics: list[str] = []
    exit_code = 1
    cleanup_failed = False
    report: dict[str, Any] = {
        "mode": "skeleton_assert" if args.skeleton_assert else "normal",
        "compose_project": project,
        "boot": {
            "postgres_version": None,
            "postgres_version_num": None,
            "postgres_major": None,
            "postgres_minor": None,
            "migration_ok": False,
            "alembic_revision": None,
            "base_url": None,
        },
        "expected_from_openapi": openapi_expected_protocol(),
        "case_counts": {
            "total": 0,
            "passed": 0,
            "unsupported_operation": 0,
            "contract_failure": 0,
            "harness_error": 0,
            "skipped": 0,
        },
        "raw_conformance": None,
        "selected_pytest": None,
        "skeleton_assert": {"passed": False, "reasons": []},
        "diagnostics": [],
    }

    try:
        preflight()
        print(f"starting isolated compose project {project}", flush=True)
        up = run_cmd(
            [*compose_argv(project), "up", "-d", "--build"],
            capture=True,
            timeout=600,
        )
        if up.returncode != 0:
            raise RuntimeError(
                "compose up failed: "
                + redact(clip((up.stderr or up.stdout or "").strip()))
            )
        wait_service_healthy(project, POSTGRES_NAME)
        wait_service_healthy(project, SERVICE_NAME)
        pg_version, pg_version_num, pg_major, pg_minor = postgres_version(project)
        if (pg_major, pg_minor) != (18, 6) or not pg_version.startswith("18.6"):
            raise RuntimeError(
                "expected PostgreSQL 18.6.x "
                f"(server_version_num=180006), got version={pg_version!r} "
                f"server_version_num={pg_version_num}"
            )
        report["boot"]["postgres_version"] = pg_version
        report["boot"]["postgres_version_num"] = pg_version_num
        report["boot"]["postgres_major"] = pg_major
        report["boot"]["postgres_minor"] = pg_minor
        revision = run_alembic_upgrade(project)
        report["boot"]["migration_ok"] = True
        report["boot"]["alembic_revision"] = revision
        base_url = published_base_url(project)
        report["boot"]["base_url"] = base_url

        if args.pytest is not None:
            database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
            if not database_url:
                raise RuntimeError(
                    "TEST_DATABASE_URL is required when using --pytest "
                    "(host-reachable PostgreSQL URL)"
                )
            pytest_args = list(args.pytest) if args.pytest else list(DEFAULT_PHASE_34_PYTEST)
            pytest_rc = run_selected_pytest(
                base_url=base_url,
                database_url=database_url,
                pytest_args=pytest_args,
            )
            report["selected_pytest"] = {
                "args": pytest_args,
                "exit_code": pytest_rc,
                "base_url": base_url,
            }
            exit_code = pytest_rc
        else:
            raw = run_harness(base_url)
            counts = summarize_cases(raw)
            report["raw_conformance"] = raw
            report["case_counts"] = counts
            ok, reasons = skeleton_assert_ok(raw, counts)
            report["skeleton_assert"] = {"passed": ok, "reasons": reasons}
            if args.skeleton_assert:
                exit_code = 0 if ok and report["boot"]["migration_ok"] else 1
            else:
                exit_code = int(raw.get("exit_code") or 1)
    except Exception as exc:  # noqa: BLE001 — always tear down
        message = redact(str(exc))
        diagnostics.append(clip(message))
        report["diagnostics"] = diagnostics
        print(f"conformance run failed: {message}", file=sys.stderr)
        report["skeleton_assert"] = {"passed": False, "reasons": diagnostics}
        exit_code = 1
    finally:
        try:
            teardown(project)
        except Exception as exc:  # noqa: BLE001
            cleanup_failed = True
            message = redact(str(exc))
            diagnostics.append(f"cleanup failed: {clip(message)}")
            report["diagnostics"] = diagnostics
            print(f"conformance cleanup failed: {message}", file=sys.stderr)

    try:
        write_report(report_path, report)
        print(f"wrote {report_path}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"failed writing report: {redact(str(exc))}", file=sys.stderr)
        return 1

    if cleanup_failed:
        return 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
