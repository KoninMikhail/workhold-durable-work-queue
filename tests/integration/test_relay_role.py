"""Process-level `workhold relay` composition and lifecycle (DLVR-03)."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from workhold import db, settings
from workhold.delivery.models import STATE_PENDING, STATE_PUBLISHED
from workhold.roles import relay as relay_role

REPO_ROOT = Path(__file__).resolve().parents[2]


class _SinkHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        self.server.recorded.append(  # type: ignore[attr-defined]
            {
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
                "path": self.path,
            }
        )
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


@pytest.fixture
def http_sink() -> Any:
    server = HTTPServer(("127.0.0.1", 0), _SinkHandler)
    server.recorded = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


@pytest.fixture
def relay_schema(test_database_url: str) -> Iterator[tuple[str, str]]:
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()
    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    try:
        yield schema, test_database_url
    finally:
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def _role_pools() -> dict[str, settings.RolePoolSettings]:
    return {
        role: settings.RolePoolSettings(
            replica_ceiling=1,
            pool_ceiling=2,
            pool_acquisition_timeout_seconds=5.0,
            statement_timeout_seconds=30.0,
        )
        for role in settings.PROCESS_ROLES
    }


def _settings_for_url(database_url: str) -> settings.DeploymentSettings:
    return settings.DeploymentSettings(
        environment=settings.EnvironmentMode.DEVELOPMENT,
        listener_tls_mode=settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        database_url=settings.Secret(database_url),
        postgres_max_connections=100,
        postgres_reserved_connections=10,
        role_pools=_role_pools(),
        credential_generations=(),
    )


def _insert_pending(database_url: str, schema: str) -> uuid.UUID:
    engine = create_engine(database_url)
    eid = uuid.uuid4()
    envelope = {
        "id": str(eid),
        "source": "urn:test:relay-role",
        "specversion": "1.0",
        "type": "com.example.relay.v1",
        "time": "2026-09-19T12:00:00Z",
        "data": {"ok": True},
    }
    envelope_json = json.dumps(envelope, separators=(",", ":"), sort_keys=True)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.execute(text(f'SET search_path TO "{schema}"'))
        now = session.scalar(text("SELECT transaction_timestamp()"))
        session.execute(
            text(
                """
                INSERT INTO delivery_events_active (
                    event_id, source_task_id, ordinal, state_code,
                    envelope, envelope_bytes, available_at, generation,
                    current_claim_id, claimed_at, lease_expires_at,
                    relay_principal_id, delivery_attempt, last_failure_code,
                    created_at, updated_at
                ) VALUES (
                    :eid, :sid, 0, :pending,
                    CAST(:envelope AS jsonb), :ebytes,
                    :now, 0,
                    NULL, NULL, NULL,
                    NULL, 0, NULL,
                    :now, :now
                )
                """
            ),
            {
                "eid": eid,
                "sid": uuid.uuid4(),
                "pending": STATE_PENDING,
                "envelope": envelope_json,
                "ebytes": len(envelope_json.encode("utf-8")),
                "now": now,
            },
        )
        session.commit()
    engine.dispose()
    return eid


def _relay_env(
    database_url: str,
    schema: str,
    webhook_url: str,
    *,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    host = "127.0.0.1"
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": database_url,
            "QUEUE_SCHEMA": schema,
            "QUEUE_ENVIRONMENT": "development",
            "QUEUE_DELIVERY_WEBHOOK_URL": webhook_url,
            "QUEUE_DELIVERY_ALLOWED_HOSTS": host,
            "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8,::1/128",
            "QUEUE_DELIVERY_BEARER_TOKEN": "relay-bearer",
            "QUEUE_DELIVERY_CONNECT_TIMEOUT_SECONDS": "1",
            "QUEUE_DELIVERY_READ_TIMEOUT_SECONDS": "1",
            "QUEUE_DELIVERY_TOTAL_TIMEOUT_SECONDS": "2",
            "QUEUE_DELIVERY_MAX_RESPONSE_BYTES": "4096",
            "QUEUE_DELIVERY_CIRCUIT_FAILURE_THRESHOLD": "5",
            "QUEUE_DELIVERY_CIRCUIT_OPEN_SECONDS": "1",
            "QUEUE_DELIVERY_CIRCUIT_SUCCESS_THRESHOLD": "1",
            "QUEUE_DELIVERY_HALF_OPEN_MAX_PROBES": "1",
            "QUEUE_DELIVERY_RETRY_AFTER_CAP_SECONDS": "30",
            "QUEUE_RELAY_PRINCIPAL_ID": "relay-integration",
            "QUEUE_RELAY_LEASE_SECONDS": "30",
            "QUEUE_RELAY_MAX_ATTEMPTS": "5",
            "QUEUE_RELAY_BACKOFF_BASE_SECONDS": "0.01",
            "QUEUE_RELAY_BACKOFF_MAX_SECONDS": "1",
            "QUEUE_RELAY_RETRY_AFTER_CAP_SECONDS": "30",
            "QUEUE_RELAY_JITTER_RATIO": "0",
            "QUEUE_RELAY_DEFAULT_PROBE_SECONDS": "0.05",
            "QUEUE_RELAY_IDLE_SLEEP_SECONDS": "0.05",
            "QUEUE_RELAY_GRACE_SECONDS": "2",
            "QUEUE_RELAY_MAX_CYCLES": "1",
        }
    )
    if extra:
        env.update(extra)
    return env


def test_run_relay_publishes_committed_event(
    relay_schema: tuple[str, str], http_sink: Any
) -> None:
    schema, url = relay_schema
    eid = _insert_pending(url, schema)
    host, port = http_sink.server_address[:2]
    webhook = f"http://{host}:{port}/delivery"
    code = relay_role.run_relay(
        (),
        deployment=_settings_for_url(url),
        http_config=relay_role.http_config_from_mapping(
            {
                "QUEUE_DELIVERY_WEBHOOK_URL": webhook,
                "QUEUE_DELIVERY_ALLOWED_HOSTS": "127.0.0.1",
                "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8",
                "QUEUE_DELIVERY_BEARER_TOKEN": "relay-bearer",
                "QUEUE_ENVIRONMENT": "development",
            }
        ),
        schema=schema,
        max_cycles=1,
        idle_sleep_seconds=0.01,
        install_signals=False,
    )
    assert code == 0
    assert len(http_sink.recorded) == 1
    body = json.loads(http_sink.recorded[0]["body"].decode("utf-8"))
    assert body["id"] == str(eid)
    assert http_sink.recorded[0]["headers"]["content-type"] == (
        "application/cloudevents+json"
    )
    assert http_sink.recorded[0]["headers"].get("authorization") == (
        "Bearer relay-bearer"
    )

    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text(f'SET search_path TO "{schema}"'))
        active = conn.execute(
            text(
                "SELECT state_code FROM delivery_events_active WHERE event_id = :eid"
            ),
            {"eid": eid},
        ).first()
        terminal = conn.execute(
            text(
                "SELECT state_code FROM delivery_events_terminal WHERE event_id = :eid"
            ),
            {"eid": eid},
        ).first()
    engine.dispose()
    assert active is None
    assert terminal is not None
    assert terminal[0] == STATE_PUBLISHED


def test_open_circuit_skips_claim_before_publish(
    relay_schema: tuple[str, str], http_sink: Any
) -> None:
    schema, url = relay_schema
    _insert_pending(url, schema)
    host, port = http_sink.server_address[:2]
    webhook = f"http://{host}:{port}/delivery"
    http_cfg = relay_role.http_config_from_mapping(
        {
            "QUEUE_DELIVERY_WEBHOOK_URL": webhook,
            "QUEUE_DELIVERY_ALLOWED_HOSTS": "127.0.0.1",
            "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8",
            "QUEUE_ENVIRONMENT": "development",
            "QUEUE_DELIVERY_CIRCUIT_FAILURE_THRESHOLD": "1",
            "QUEUE_DELIVERY_CIRCUIT_OPEN_SECONDS": "30",
        }
    )
    transport = relay_role.build_transport(http_cfg)
    # Force open circuit without claiming.
    transport._breaker.force_open()  # type: ignore[attr-defined]
    result = asyncio.run(
        relay_role.build_service(
            deployment=_settings_for_url(url),
            transport=transport,
            schema=schema,
        ).process_one()
    )
    assert result.kind == "skipped_backpressure"
    assert len(http_sink.recorded) == 0


def test_invalid_config_fails_startup(relay_schema: tuple[str, str]) -> None:
    schema, url = relay_schema
    code = relay_role.run_relay(
        (),
        deployment=_settings_for_url(url),
        environ={
            "QUEUE_DELIVERY_WEBHOOK_URL": "http://evil.example/hook",
            "QUEUE_DELIVERY_ALLOWED_HOSTS": "127.0.0.1",
            "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8",
            "QUEUE_ENVIRONMENT": "development",
            "QUEUE_SCHEMA": schema,
        },
        schema=schema,
        install_signals=False,
    )
    assert code != 0


def test_cli_queue_relay_subprocess(
    relay_schema: tuple[str, str], http_sink: Any
) -> None:
    schema, url = relay_schema
    eid = _insert_pending(url, schema)
    host, port = http_sink.server_address[:2]
    webhook = f"http://{host}:{port}/delivery"
    env = _relay_env(url, schema, webhook)
    proc = subprocess.run(
        ["uv", "run", "workhold", "relay"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert "listening" not in (proc.stdout + proc.stderr).lower()
    assert "admin" not in (proc.stdout + proc.stderr).lower() or "listener" not in (
        proc.stdout + proc.stderr
    ).lower()
    assert len(http_sink.recorded) == 1
    body = json.loads(http_sink.recorded[0]["body"].decode("utf-8"))
    assert body["id"] == str(eid)


def test_sigterm_stops_cleanly(relay_schema: tuple[str, str], http_sink: Any) -> None:
    schema, url = relay_schema
    host, port = http_sink.server_address[:2]
    webhook = f"http://{host}:{port}/delivery"
    stop = threading.Event()

    def _raise_term() -> None:
        time.sleep(0.2)
        stop.set()

    threading.Thread(target=_raise_term, daemon=True).start()
    started = time.monotonic()
    code = relay_role.run_relay(
        (),
        deployment=_settings_for_url(url),
        http_config=relay_role.http_config_from_mapping(
            {
                "QUEUE_DELIVERY_WEBHOOK_URL": webhook,
                "QUEUE_DELIVERY_ALLOWED_HOSTS": "127.0.0.1",
                "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8",
                "QUEUE_ENVIRONMENT": "development",
            }
        ),
        schema=schema,
        max_cycles=None,
        idle_sleep_seconds=0.05,
        grace_seconds=1.0,
        install_signals=False,
        stop_event=stop,
    )
    elapsed = time.monotonic() - started
    assert code == 0
    assert elapsed < 5.0


def test_missing_database_url_exits_dependency() -> None:
    code = relay_role.run_relay([], environ={"DATABASE_URL": ""})
    assert code == relay_role.EXIT_DEPENDENCY


def test_no_api_ports_opened(
    relay_schema: tuple[str, str], http_sink: Any
) -> None:
    schema, url = relay_schema
    host, port = http_sink.server_address[:2]
    webhook = f"http://{host}:{port}/delivery"
    # Bind check ports that api would use by default.
    probe_ports = (18080, 18081)
    for p in probe_ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", p))
    code = relay_role.run_relay(
        (),
        deployment=_settings_for_url(url),
        http_config=relay_role.http_config_from_mapping(
            {
                "QUEUE_DELIVERY_WEBHOOK_URL": webhook,
                "QUEUE_DELIVERY_ALLOWED_HOSTS": "127.0.0.1",
                "QUEUE_DELIVERY_ALLOWED_CIDRS": "127.0.0.0/8",
                "QUEUE_ENVIRONMENT": "development",
            }
        ),
        schema=schema,
        max_cycles=1,
        install_signals=False,
    )
    assert code == 0
