"""Single-image deployment topology end-to-end (OPS/SEC/DEP/PKG Phase 3.2 Plan 11).

Builds one runtime image, boots PostgreSQL 18.6 + role services via Compose, and
proves migration-gated readiness, private admin, production plaintext rejection,
SIGTERM shutdown, pool budget, concurrent migrate, credential rotation overlap,
and zero sentinel leakage in captured logs. Always tears down volumes/containers.

Host ports ``:5432`` / ``:8080`` are fixed in ``docker-compose.dev.yml``. This
suite is single-runner: it never stops foreign containers that publish those
ports. Tear down is project-scoped (``docker compose -p <project> down -v``).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest

from workhold import settings
from workhold.roles import migrate as migrate_role

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "docker-compose.dev.yml"
DOCKERFILE = ROOT / "Dockerfile"
ENV_EXAMPLE = ROOT / ".env.example"
IMAGE_TAG = "workhold:local"

PAYLOAD_SENTINEL = "PAYLOAD_SENTINEL_do_not_log_9f3a"
CLAIM_TOKEN_SENTINEL = "CLAIM_TOKEN_SENTINEL_do_not_log_7c2b"
CREDENTIAL_SENTINEL_CURRENT = "CRED_SENTINEL_current_a1b2c3d4"
CREDENTIAL_SENTINEL_PREVIOUS = "CRED_SENTINEL_previous_e5f6g7h8"

# Aggregate budget in compose defaults: 4+2+2+2+2+2 pool + 1 API listener = 15 <= 100-10.
# api, admin, migrate, apply, maintain, relay, plus one dedicated LISTEN per API replica.
COMPOSE_COMMITTED_CONNECTIONS = 15


def _docker_available() -> bool:
    try:
        docker = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        compose = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False
    return docker.returncode == 0 and compose.returncode == 0


def _require_docker() -> None:
    if not _docker_available():
        pytest.fail("Docker Engine + Compose are required for deployment-role tests")


def _run(
    args: Sequence[str],
    *,
    timeout: float = 120.0,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        list(args),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged,
        check=False,
    )


def _compose(
    project: str,
    *args: str,
    timeout: float = 180.0,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return _run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(COMPOSE_FILE),
            *args,
        ],
        timeout=timeout,
        env=env,
    )


def _http_get(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float = 3.0,
) -> tuple[int, str]:
    req = Request(url, method="GET", headers=dict(headers or {}))
    try:
        with urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return int(exc.code), body


def _wait_http(url: str, *, timeout: float = 60.0) -> tuple[int, str]:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return _http_get(url, timeout=2.0)
        except (URLError, TimeoutError, OSError) as exc:
            last_err = exc
            time.sleep(0.5)
    raise AssertionError(f"HTTP not reachable at {url}: {last_err}")


def _wait_http_status(
    url: str,
    expected: int,
    *,
    timeout: float = 60.0,
) -> tuple[int, str]:
    """Poll until ``url`` returns ``expected`` (or raise)."""
    deadline = time.monotonic() + timeout
    last: tuple[int, str] | None = None
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status, body = _http_get(url, timeout=2.0)
            last = (status, body)
            if status == expected:
                return status, body
        except (URLError, TimeoutError, OSError) as exc:
            last_err = exc
        time.sleep(0.5)
    raise AssertionError(
        f"HTTP {url} never reached status {expected}: last={last} err={last_err}"
    )


def _host_port_open(host: str, port: int, *, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _assert_fixed_host_ports_free(*ports: int) -> None:
    """Fail clearly when fixed compose ports are busy; never stop foreign stacks."""
    busy = [port for port in ports if _host_port_open("127.0.0.1", port)]
    if busy:
        pytest.fail(
            f"Host port(s) {busy} already in use. "
            "test_deployment_roles binds fixed :5432/:8080 and must run alone; "
            "cleanup is project-scoped and does not stop unrelated containers."
        )


def _image_inspect(tag: str) -> dict[str, Any]:
    result = _run(["docker", "image", "inspect", tag, "--format", "{{json .}}"])
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _container_id(project: str, service: str) -> str:
    result = _compose(project, "ps", "-q", service)
    assert result.returncode == 0, result.stderr
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    assert lines, f"no container for service {service}"
    return lines[0]


def _container_logs(project: str, *services: str) -> str:
    result = _compose(project, "logs", "--no-color", *services, timeout=60.0)
    return f"{result.stdout}\n{result.stderr}"


def _assert_no_sentinels(blob: str) -> None:
    lowered = blob.lower()
    for sentinel in (
        PAYLOAD_SENTINEL,
        CLAIM_TOKEN_SENTINEL,
        CREDENTIAL_SENTINEL_CURRENT,
        CREDENTIAL_SENTINEL_PREVIOUS,
    ):
        assert sentinel.lower() not in lowered, (
            f"sentinel leaked into diagnostics: {sentinel}"
        )


def _wait_postgres_healthy(project: str, env: Mapping[str, str]) -> None:
    deadline = time.monotonic() + 60.0
    last = ""
    while time.monotonic() < deadline:
        healthy = _compose(project, "ps", "postgres", timeout=30.0, env=env)
        last = healthy.stdout
        if "healthy" in healthy.stdout.lower():
            return
        time.sleep(1.0)
    pytest.fail(f"postgres not healthy: {last}")


def _wait_migrate_ok(project: str, env: Mapping[str, str]) -> None:
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        mig = _compose(project, "ps", "-a", "migrate", timeout=30.0, env=env)
        lower = mig.stdout.lower()
        if "exited (0)" in lower or "exit 0" in lower:
            return
        if "exited (" in lower or "exit " in lower:
            logs = _container_logs(project, "migrate")
            pytest.fail(f"migrate failed: {mig.stdout}\n{logs}")
        time.sleep(1.0)
    pytest.fail(f"migrate did not complete: {_container_logs(project, 'migrate')}")


def _admin_status(network: str, image: str, token: str) -> int:
    script = (
        "import urllib.request\n"
        "req = urllib.request.Request(\n"
        "    'http://api:8081/admin/v1/queues',\n"
        f"    headers={{'Authorization': 'Bearer {token}'}},\n"
        ")\n"
        "try:\n"
        "    with urllib.request.urlopen(req, timeout=3) as r:\n"
        "        print(r.status)\n"
        "except Exception as exc:\n"
        "    print(getattr(exc, 'code', type(exc).__name__))\n"
    )
    probe = _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            network,
            "--entrypoint",
            "python",
            image,
            "-c",
            script,
        ],
        timeout=60.0,
    )
    assert probe.returncode == 0, probe.stderr
    raw = probe.stdout.strip().splitlines()[-1]
    try:
        return int(raw)
    except ValueError as exc:
        raise AssertionError(
            f"admin probe returned non-status {raw!r}: {probe.stdout}\n{probe.stderr}"
        ) from exc


def _wait_admin_status(
    network: str,
    image: str,
    token: str,
    expected: int,
    *,
    timeout: float = 45.0,
) -> int:
    """Retry admin until ``expected`` (e.g. 501/401) after API recreate."""
    deadline = time.monotonic() + timeout
    last: int | Exception | None = None
    while time.monotonic() < deadline:
        try:
            status = _admin_status(network, image, token)
            last = status
            if status == expected:
                return status
        except (AssertionError, ValueError, OSError) as exc:
            last = exc
        time.sleep(0.5)
    raise AssertionError(
        f"admin status never reached {expected} for token probe: last={last}"
    )


def _hold_migrate_lock_on_network(network: str, image: str) -> str:
    """Start a one-off container that holds the migrate advisory lock; return its id."""
    script = (
        "import os, time\n"
        "import psycopg\n"
        f"key = {migrate_role.MIGRATE_ADVISORY_LOCK_KEY}\n"
        "conn = psycopg.connect("
        "'postgresql://queue:queue@postgres:5432/queue', autocommit=True)\n"
        "conn.execute('SELECT pg_advisory_lock(%s)', (key,))\n"
        "print('LOCK_HELD', flush=True)\n"
        "time.sleep(120)\n"
    )
    started = _run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--network",
            network,
            "--entrypoint",
            "python",
            image,
            "-c",
            script,
        ],
        timeout=60.0,
    )
    assert started.returncode == 0, started.stderr
    cid = started.stdout.strip()
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        logs = _run(["docker", "logs", cid], timeout=15.0)
        if "LOCK_HELD" in logs.stdout:
            return cid
        time.sleep(0.5)
    _run(["docker", "rm", "-f", cid], timeout=30.0)
    pytest.fail(f"lock holder did not acquire: {logs.stdout}\n{logs.stderr}")


@pytest.fixture(scope="module")
def runtime_image() -> Iterator[str]:
    _require_docker()
    build = _run(
        [
            "docker",
            "build",
            "-f",
            str(DOCKERFILE),
            "--target",
            "runtime",
            "-t",
            IMAGE_TAG,
            str(ROOT),
        ],
        timeout=600.0,
    )
    assert build.returncode == 0, build.stderr + build.stdout
    inspect = _image_inspect(IMAGE_TAG)
    config = inspect["Config"]
    assert config.get("User") in {"queue", "10001"}, config.get("User")
    stop = (config.get("StopSignal") or "SIGTERM").upper()
    assert stop in {"SIGTERM", "15", "SIGNAL 15"}, stop
    entry = config.get("Entrypoint") or []
    assert entry[:1] == ["/app/entrypoint.sh"], entry
    yield IMAGE_TAG


@pytest.fixture
def compose_project(runtime_image: str) -> Iterator[str]:
    project = f"qdep{uuid.uuid4().hex[:10]}"
    env = {
        "QUEUE_API_BEARER_TOKEN": CREDENTIAL_SENTINEL_CURRENT,
        "QUEUE_API_BEARER_TOKEN_PREVIOUS": CREDENTIAL_SENTINEL_PREVIOUS,
    }
    try:
        yield project
    finally:
        _compose(project, "down", "-v", "--remove-orphans", timeout=120.0, env=env)
        _run(
            [
                "docker",
                "rm",
                "-f",
                f"{project}-preapi",
                f"{project}-horizon",
                f"{project}-maintain-oneshot",
                f"{project}-relay-oneshot",
            ],
            timeout=30.0,
        )


def test_compose_config_and_env_contract(runtime_image: str) -> None:
    _require_docker()
    assert COMPOSE_FILE.is_file()
    assert ENV_EXAMPLE.is_file()
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    env_text = ENV_EXAMPLE.read_text(encoding="utf-8")

    assert "image: workhold:local" in text
    assert "target: runtime" in text
    assert 'command: ["migrate"]' in text
    assert 'command: ["api"]' in text
    assert 'command: ["maintain"]' in text
    assert 'command: ["relay"]' in text
    assert "8080:8080" in text
    assert "8081:8081" not in text
    assert "condition: service_completed_successfully" in text
    assert "plaintext_public" in text
    assert "postgres:18.6-alpine" in text
    assert "queue-pgdata-18:/var/lib/postgresql" in text
    assert "/var/lib/postgresql/data" not in text
    assert "postgres:16-alpine" not in text

    for key in (
        "QUEUE_ENVIRONMENT",
        "QUEUE_LISTENER_TLS_MODE",
        "QUEUE_POSTGRES_MAX_CONNECTIONS",
        "QUEUE_POSTGRES_RESERVED_CONNECTIONS",
        "QUEUE_API_POOL_CEILING",
        "QUEUE_SHUTDOWN_GRACE_SECONDS",
        "QUEUE_PARTITION_PREMAKE_DAYS",
        "QUEUE_CLAIM_MAX_WAIT_SECONDS",
        "QUEUE_CLAIM_WAIT_FALLBACK_SECONDS",
        "QUEUE_CLAIM_CANCELLATION_PROBE_SECONDS",
        "QUEUE_CLAIM_MAX_OUTSTANDING_WAITS",
        "QUEUE_API_BEARER_TOKEN",
        "QUEUE_API_BEARER_TOKEN_PREVIOUS",
    ):
        assert key in env_text, key

    cfg = settings.from_environ(
        {
            "DATABASE_URL": "postgresql+psycopg://queue:queue@localhost:5432/queue",
            "QUEUE_ENVIRONMENT": "development",
            "QUEUE_LISTENER_TLS_MODE": "plaintext_public",
            "QUEUE_POSTGRES_MAX_CONNECTIONS": "100",
            "QUEUE_POSTGRES_RESERVED_CONNECTIONS": "10",
            "QUEUE_API_POOL_CEILING": "4",
            "QUEUE_ADMIN_POOL_CEILING": "2",
            "QUEUE_MIGRATE_POOL_CEILING": "2",
            "QUEUE_MAINTAIN_POOL_CEILING": "2",
            "QUEUE_RELAY_POOL_CEILING": "2",
            "QUEUE_API_BEARER_TOKEN": "token",
        }
    )
    assert cfg is not None
    assert cfg.committed_connections == COMPOSE_COMMITTED_CONNECTIONS
    assert cfg.committed_connections <= cfg.usable_connections

    with pytest.raises(settings.SettingsValidationError):
        settings.from_environ(
            {
                "DATABASE_URL": "postgresql+psycopg://queue:queue@localhost:5432/queue",
                "QUEUE_ENVIRONMENT": "production",
                "QUEUE_LISTENER_TLS_MODE": "plaintext_public",
                "QUEUE_API_BEARER_TOKEN": "token",
            }
        )

    cfg_result = _compose("qdepcfg", "config")
    assert cfg_result.returncode == 0, cfg_result.stderr


def test_single_image_deployment_roles_end_to_end(
    runtime_image: str,
    compose_project: str,
) -> None:
    project = compose_project
    network = f"{project}_queue-internal"
    env = {
        "QUEUE_API_BEARER_TOKEN": CREDENTIAL_SENTINEL_CURRENT,
        "QUEUE_API_BEARER_TOKEN_PREVIOUS": CREDENTIAL_SENTINEL_PREVIOUS,
    }

    # Production plaintext rejection (one-shot, no listeners).
    reject = _run(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "DATABASE_URL=postgresql+psycopg://queue:queue@127.0.0.1:9/queue",
            "-e",
            "QUEUE_ENVIRONMENT=production",
            "-e",
            "QUEUE_LISTENER_TLS_MODE=plaintext_public",
            "-e",
            f"QUEUE_API_BEARER_TOKEN={CREDENTIAL_SENTINEL_CURRENT}",
            runtime_image,
            "api",
        ],
        timeout=30.0,
    )
    assert reject.returncode != 0, reject.stdout + reject.stderr
    combined_reject = f"{reject.stdout}\n{reject.stderr}".lower()
    assert "plaintext" in combined_reject or "production" in combined_reject
    _assert_no_sentinels(f"{reject.stdout}\n{reject.stderr}")

    # Boot postgres; API before migrate must be live but not ready.
    # Fixed host ports — single-runner only; never stop foreign publish=5432/8080.
    _assert_fixed_host_ports_free(5432, 8080)
    up_pg = _compose(project, "up", "-d", "postgres", timeout=120.0, env=env)
    assert up_pg.returncode == 0, up_pg.stderr
    _wait_postgres_healthy(project, env)

    pre_name = f"{project}-preapi"
    _run(["docker", "rm", "-f", pre_name], timeout=30.0)
    pre_api = _run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            pre_name,
            "--network",
            network,
            "-p",
            "18080:8080",
            "-e",
            "DATABASE_URL=postgresql+psycopg://queue:queue@postgres:5432/queue",
            "-e",
            "QUEUE_ENVIRONMENT=development",
            "-e",
            "QUEUE_LISTENER_TLS_MODE=plaintext_public",
            "-e",
            "QUEUE_APPLICATION_HOST=0.0.0.0",
            "-e",
            "QUEUE_APPLICATION_PORT=8080",
            "-e",
            "QUEUE_ADMIN_HOST=0.0.0.0",
            "-e",
            "QUEUE_ADMIN_PORT=8081",
            "-e",
            "QUEUE_PARTITION_PREMAKE_DAYS=30",
            "-e",
            f"QUEUE_API_BEARER_TOKEN={CREDENTIAL_SENTINEL_CURRENT}",
            runtime_image,
            "api",
        ],
        timeout=60.0,
    )
    assert pre_api.returncode == 0, pre_api.stderr
    try:
        status, body = _wait_http("http://127.0.0.1:18080/healthz", timeout=45.0)
        assert status == 200, body
        ready_status, ready_body = _wait_http(
            "http://127.0.0.1:18080/readyz", timeout=45.0
        )
        assert ready_status == 503, ready_body
    finally:
        _run(["docker", "rm", "-f", pre_name], timeout=30.0)

    # Concurrent migrators: held lock → loser exits EXIT_LOCK_TIMEOUT.
    lock_cid = _hold_migrate_lock_on_network(network, runtime_image)
    try:
        loser = _run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                "-e",
                "DATABASE_URL=postgresql+psycopg://queue:queue@postgres:5432/queue",
                "-e",
                "QUEUE_MIGRATE_LOCK_DEADLINE_SECONDS=2",
                runtime_image,
                "migrate",
            ],
            timeout=60.0,
        )
        assert loser.returncode == migrate_role.EXIT_LOCK_TIMEOUT, (
            loser.returncode,
            loser.stdout,
            loser.stderr,
        )
    finally:
        _run(["docker", "rm", "-f", lock_cid], timeout=30.0)

    # Full topology: migrate → api (compose gates readiness on migrate success).
    up = _compose(project, "up", "-d", "migrate", "api", timeout=180.0, env=env)
    assert up.returncode == 0, up.stderr + up.stdout
    _wait_migrate_ok(project, env)

    status, body = _wait_http("http://127.0.0.1:8080/healthz", timeout=60.0)
    assert status == 200, body
    ready_status, ready_body = _wait_http("http://127.0.0.1:8080/readyz", timeout=60.0)
    assert ready_status == 200, ready_body

    api_cid = _container_id(project, "api")
    mig_ps = _compose(project, "ps", "-a", "-q", "migrate", timeout=30.0, env=env)
    assert mig_ps.returncode == 0 and mig_ps.stdout.strip(), mig_ps.stderr
    mig_cid = mig_ps.stdout.strip().splitlines()[0].strip()

    def _image_id(cid: str) -> str:
        out = _run(["docker", "inspect", "-f", "{{.Image}}", cid], timeout=30.0)
        assert out.returncode == 0, out.stderr
        return out.stdout.strip()

    assert _image_id(api_cid) == _image_id(mig_cid)

    user = _run(["docker", "inspect", "-f", "{{.Config.User}}", api_cid], timeout=30.0)
    assert user.returncode == 0
    assert user.stdout.strip() in {"queue", "10001"}

    # Admin unpublished on host; reachable only on Compose network.
    assert _host_port_open("127.0.0.1", 8081) is False
    admin_probe = _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            network,
            "--entrypoint",
            "python",
            runtime_image,
            "-c",
            (
                "import urllib.request;"
                "r=urllib.request.urlopen('http://api:8081/healthz', timeout=3);"
                "print(r.status)"
            ),
        ],
        timeout=60.0,
    )
    assert admin_probe.returncode == 0, admin_probe.stderr
    assert "200" in admin_probe.stdout

    assert _admin_status(network, runtime_image, CREDENTIAL_SENTINEL_CURRENT) == 501
    assert _admin_status(network, runtime_image, CREDENTIAL_SENTINEL_PREVIOUS) == 501
    assert _admin_status(network, runtime_image, "totally-wrong-token") == 401

    leak_script = (
        "import json, urllib.request\n"
        f"body = json.dumps({{'payload': {{'secret': '{PAYLOAD_SENTINEL}'}}}}).encode()\n"
        "req = urllib.request.Request(\n"
        "    'http://api:8080/v1/queues/orders/tasks',\n"
        "    data=body,\n"
        "    method='POST',\n"
        "    headers={\n"
        f"        'Authorization': 'Bearer {CREDENTIAL_SENTINEL_CURRENT}',\n"
        f"        'X-Claim-Token': '{CLAIM_TOKEN_SENTINEL}',\n"
        "        'Content-Type': 'application/json',\n"
        "        'Idempotency-Key': 'deploy-sentinel-1',\n"
        "    },\n"
        ")\n"
        "try:\n"
        "    urllib.request.urlopen(req, timeout=3)\n"
        "except Exception:\n"
        "    pass\n"
    )
    leak_probe = _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            network,
            "--entrypoint",
            "python",
            runtime_image,
            "-c",
            leak_script,
        ],
        timeout=60.0,
    )
    assert leak_probe.returncode == 0, leak_probe.stderr

    # Unsafe horizon → readyz 503 while healthz stays up.
    horizon_name = f"{project}-horizon"
    _run(["docker", "rm", "-f", horizon_name], timeout=30.0)
    horizon = _run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            horizon_name,
            "--network",
            network,
            "-p",
            "18081:8080",
            "-e",
            "DATABASE_URL=postgresql+psycopg://queue:queue@postgres:5432/queue",
            "-e",
            "QUEUE_ENVIRONMENT=development",
            "-e",
            "QUEUE_LISTENER_TLS_MODE=plaintext_public",
            "-e",
            "QUEUE_APPLICATION_HOST=0.0.0.0",
            "-e",
            "QUEUE_ADMIN_HOST=0.0.0.0",
            "-e",
            "QUEUE_PARTITION_PREMAKE_DAYS=3650",
            "-e",
            f"QUEUE_API_BEARER_TOKEN={CREDENTIAL_SENTINEL_CURRENT}",
            runtime_image,
            "api",
        ],
        timeout=60.0,
    )
    assert horizon.returncode == 0, horizon.stderr
    try:
        hz, _ = _wait_http("http://127.0.0.1:18081/healthz", timeout=45.0)
        assert hz == 200
        rz, rbody = _wait_http("http://127.0.0.1:18081/readyz", timeout=45.0)
        assert rz == 503, rbody
    finally:
        _run(["docker", "rm", "-f", horizon_name], timeout=30.0)

    # One-shot maintain/relay against the live stack: --no-deps so Compose does
    # not re-orchestrate migrate/postgres (Windows Docker flakes with exit 5 /
    # "No such container" when profile run restarts the dependency chain).
    maintain_name = f"{project}-maintain-oneshot"
    _run(["docker", "rm", "-f", maintain_name], timeout=30.0)
    maintain = _compose(
        project,
        "--profile",
        "maintain",
        "run",
        "--no-deps",
        "--name",
        maintain_name,
        "maintain",
        timeout=120.0,
        env=env,
    )
    assert maintain.returncode == 0, maintain.stdout + maintain.stderr
    assert _image_id(maintain_name) == _image_id(api_cid)
    _run(["docker", "rm", "-f", maintain_name], timeout=30.0)

    relay_name = f"{project}-relay-oneshot"
    _run(["docker", "rm", "-f", relay_name], timeout=30.0)
    relay = _compose(
        project,
        "--profile",
        "relay",
        "run",
        "--no-deps",
        "--name",
        relay_name,
        "relay",
        timeout=60.0,
        env=env,
    )
    assert relay.returncode != 0
    # Phase 5 activated relay: missing delivery webhook fails closed (was "reserved"
    # before Delivery Outbox landed).
    combined_relay = (relay.stdout + relay.stderr).lower()
    assert (
        "webhook" in combined_relay
        or "delivery" in combined_relay
        or "reserved" in combined_relay
    ), combined_relay
    assert _image_id(relay_name) == _image_id(api_cid)
    _run(["docker", "rm", "-f", relay_name], timeout=30.0)

    # SIGTERM within grace.
    stop_started = time.monotonic()
    stop = _run(["docker", "kill", "--signal=SIGTERM", api_cid], timeout=30.0)
    assert stop.returncode == 0, stop.stderr
    wait = _run(["docker", "wait", api_cid], timeout=30.0)
    elapsed = time.monotonic() - stop_started
    assert wait.returncode == 0, wait.stderr
    assert elapsed <= 12.0, elapsed

    # Old credential revocation does not interrupt the new credential.
    _compose(project, "rm", "-f", "api", timeout=60.0, env=env)
    rotated_env = {
        "QUEUE_API_BEARER_TOKEN": CREDENTIAL_SENTINEL_CURRENT,
        "QUEUE_API_BEARER_TOKEN_PREVIOUS": "",
    }
    up_rot = _compose(project, "up", "-d", "api", timeout=120.0, env=rotated_env)
    assert up_rot.returncode == 0, up_rot.stderr
    # Acceptance readiness is /readyz, not /healthz alone (avoids intermittent 503).
    _wait_http_status("http://127.0.0.1:8080/healthz", 200, timeout=60.0)
    _wait_http_status("http://127.0.0.1:8080/readyz", 200, timeout=60.0)
    _wait_admin_status(
        network, runtime_image, CREDENTIAL_SENTINEL_CURRENT, 501, timeout=45.0
    )
    _wait_admin_status(
        network, runtime_image, CREDENTIAL_SENTINEL_PREVIOUS, 401, timeout=45.0
    )

    logs = _container_logs(project, "api", "migrate")
    _assert_no_sentinels(logs)

    # DB-outage: liveness remains process-only.
    _compose(project, "stop", "postgres", timeout=60.0, env=rotated_env)
    time.sleep(1.0)
    live_status, _ = _http_get("http://127.0.0.1:8080/healthz", timeout=3.0)
    assert live_status == 200
    ready_down, _ = _http_get("http://127.0.0.1:8080/readyz", timeout=3.0)
    assert ready_down == 503
