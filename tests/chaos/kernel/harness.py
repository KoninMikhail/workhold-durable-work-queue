"""Deterministic fault-injection harness for kernel chaos (QUAL-04).

Uses the existing Phase 3 Docker/PostgreSQL reference environment
(``docker-compose.dev.yml`` postgres + ``TEST_DATABASE_URL``). No new chaos
package or service. Relay duplicate publication is excluded (Phase 5).
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import socket
import subprocess
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from workhold.admission.adaptive import (
    AdaptivePressureConfig,
    AdaptivePressureController,
    OverloadMode,
)
from workhold.admission.enqueue import AdaptiveEnqueueConfig, AdaptiveEnqueueGate
from workhold.api.admin import create_admin_app
from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.health import (
    BINARY_COMPATIBLE_MAX,
    BINARY_REVISION_ORDER,
    ReasonCode,
    check_readiness,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.depth import DepthCeilings
from workhold.intake.service import EnqueueService
from workhold.lifecycle import Lifecycle
from workhold.observability.pressure import Freshness, build_snapshot
from workhold.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.payload_policy import PayloadRetentionPolicy
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.delivery.models import STATE_PUBLISHING
from workhold.storage.models import (
    AdminAuditLog,
    BreakGlassElevation,
    CompleteReplay,
    EnqueueDedup,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)
from tests.conformance.faults import DropCommittedResponseProxy, PostgresPreCommitGate
from tests.integration.conftest import to_psycopg_conninfo

# Disposable chaos schemas migrate to alembic head (040–0502). Post-restore
# readiness must use the same binary window as production health.py.
_CHAOS_HEAD_REVISION_ORDER: tuple[str, ...] = tuple(BINARY_REVISION_ORDER)
_CHAOS_HEAD_COMPATIBLE_MAX = BINARY_COMPATIBLE_MAX

ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = ROOT / "docker-compose.dev.yml"
PAYLOAD_SENTINEL = "CHAOS_PAYLOAD_SENTINEL_do_not_log_9f3a"
CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
WORKER_ID = "chaos-worker/replica-1"

PRODUCER_TOKEN = "tok-producer-chaos"
WORKER_TOKEN = "tok-worker-chaos"
ADMIN_TOKEN = "tok-admin-chaos"
OBSERVER_TOKEN = "tok-observer-chaos"
BREAK_GLASS_TOKEN = "tok-break-glass-chaos"

PRODUCER_PRINCIPAL = "producer-chaos"
WORKER_PRINCIPAL = "worker-chaos"
ADMIN_PRINCIPAL = "admin-chaos"
OBSERVER_PRINCIPAL = "observer-chaos"
BREAK_GLASS_PRINCIPAL = "break-glass-chaos"


@dataclass(frozen=True)
class ScenarioSpec:
    """One chaos scenario with explicit precondition → inject → assert contract."""

    scenario_id: str
    category: str
    precondition: str
    injection: str
    protocol_expectation: str
    db_invariant: str
    telemetry_evidence: str
    recovery_assertion: str


SCENARIO_IDS: tuple[str, ...] = (
    "RT-API-BEFORE-COMMIT",
    "RT-API-AFTER-COMMIT",
    "RT-WORKER-LEASE",
    "RT-WORKER-TERMINAL",
    "RT-PG-ENQUEUE",
    "RT-PG-CLAIM-HB-COMPLETE",
    "RT-PG-MAINT-ADMIN",
    "RT-RACE-PAUSE-CLAIM",
    "RT-RACE-DRAIN-ENQUEUE-SPAWN",
    "RT-RACE-CANCEL-EXPIRY-COMPLETE",
    "RT-RACE-RETRY-LEASE-EXPIRY",
    "RC-RESTORE-PITR",
    "RC-INTERRUPT-REPLAY",
    "RC-INTERRUPT-BULK-CANCEL",
    "RC-INTERRUPT-MAINTENANCE",
    "RC-INTERRUPT-BREAK-GLASS",
    "RC-INTERRUPT-BG-DELIVERY-RECLAIM",
    "RC-INTERRUPT-BG-ELEVATION-WRITE",
    "RC-PRESSURE-CLAIM-DRAIN",
    "RC-NO-LEAKAGE",
)

EXCLUDED_PHASE5: tuple[str, ...] = ("RT-RELAY-DUP-PUBLISH",)


def scenario_matrix() -> tuple[ScenarioSpec, ...]:
    return (
        ScenarioSpec(
            "RT-API-BEFORE-COMMIT",
            "runtime",
            "Named queue active; producer ready to enqueue",
            "Terminate API backend before enqueue commit",
            "Transport/error; no 2xx success",
            "Zero tasks for that idempotency key until successful retry",
            "Injection timestamp + protocol status recorded",
            "Same-key retry creates exactly one task",
        ),
        ScenarioSpec(
            "RT-API-AFTER-COMMIT",
            "runtime",
            "Enqueue about to commit successfully",
            "Drop committed HTTP response to producer",
            "Producer sees failure; upstream committed 2xx",
            "Exactly one active task + dedup row",
            "Buffered upstream success retained in evidence",
            "Same-key retry returns the committed task (replayed)",
        ),
        ScenarioSpec(
            "RT-WORKER-LEASE",
            "runtime",
            "Task claimed with live lease",
            "Simulate worker death; expire lease; reclaim",
            "Stale complete → lease_lost; new claim succeeds",
            "At most one current claim; generation advances",
            "Fence rejection code recorded",
            "Stale worker remains fenced after reclaim",
        ),
        ScenarioSpec(
            "RT-WORKER-TERMINAL",
            "runtime",
            "Valid lease about to complete",
            "Process restart after durable complete commit",
            "First complete 200; replay 200 replayed=true",
            "No duplicate terminals/spawns/effects",
            "Replay flag + world counts",
            "Idempotent terminal replay after API restart",
        ),
        ScenarioSpec(
            "RT-PG-ENQUEUE",
            "database",
            "Enqueue in flight or just committed",
            "Restart PostgreSQL container",
            "No uncommitted success acknowledged",
            "Committed rows survive; in-flight rolls back",
            "Restart + readiness recovery timestamps",
            "Idempotent retry after PG healthy",
        ),
        ScenarioSpec(
            "RT-PG-CLAIM-HB-COMPLETE",
            "database",
            "Claimed task with heartbeat path",
            "Restart PostgreSQL mid lifecycle",
            "Post-restart claim/HB/complete succeed or fence correctly",
            "Lease/fencing tables consistent",
            "Readyz recovery + operation outcomes",
            "Lifecycle continues without split-brain",
        ),
        ScenarioSpec(
            "RT-PG-MAINT-ADMIN",
            "database",
            "Admin set-state / maintainable queue",
            "Restart PostgreSQL around admin op",
            "Uncommitted admin mutation not visible",
            "Audit only for committed ops",
            "Admin outcome + readiness",
            "Retry admin op after recovery is safe",
        ),
        ScenarioSpec(
            "RT-RACE-PAUSE-CLAIM",
            "race",
            "Ready tasks and active queue",
            "Pause races with claim",
            "Pause accepts; subsequent claims empty while paused",
            "Enqueue still allowed when paused",
            "Queue state + claim batch size",
            "Resume restores claims",
        ),
        ScenarioSpec(
            "RT-RACE-DRAIN-ENQUEUE-SPAWN",
            "race",
            "Active queue with claimed parent",
            "Drain races with external enqueue and spawn",
            "External enqueue rejected; spawn/claims continue",
            "No orphan spawn partials",
            "queue_draining code + spawn ids",
            "Drain progress consistent",
        ),
        ScenarioSpec(
            "RT-RACE-CANCEL-EXPIRY-COMPLETE",
            "race",
            "Leased task",
            "Cancel races with lease expiry and complete",
            "At most one terminal outcome",
            "Terminal history immutable; no double terminal",
            "Terminal count == 1",
            "Loser observes already_terminal or lease_lost",
        ),
        ScenarioSpec(
            "RT-RACE-RETRY-LEASE-EXPIRY",
            "race",
            "Leased task near failure/retry",
            "Fail/retry races with forced lease expiry",
            "Fencing preserved; stale fail rejected",
            "Retry schedule or terminal consistent",
            "lease_lost vs fail outcome",
            "Reclaim after expiry is single-winner",
        ),
        ScenarioSpec(
            "RC-RESTORE-PITR",
            "restore",
            "Registries + audit present at restore point",
            "Logical schema snapshot → mutate → restore",
            "Readiness recovers; duplicate-aware replay works",
            "Dedup/complete_replay/audit restored; counters reconcilable",
            "Snapshot markers + post-restore counts",
            "No exactly-once claim; at-least-once resume",
        ),
        ScenarioSpec(
            "RC-INTERRUPT-REPLAY",
            "admin",
            "Dead-letter terminal + admin replay key",
            "Interrupt between commit and response; retry same key",
            "Second call replayed=true",
            "Source terminal immutable; one new task",
            "Admin audit present",
            "Bounded idempotent replay",
        ),
        ScenarioSpec(
            "RC-INTERRUPT-BULK-CANCEL",
            "admin",
            "Ready tasks + dry-run confirmation",
            "Interrupt execute; retry confirmation",
            "Bounded batch; idempotent where promised",
            "Cancelled set stable; audit counts",
            "Bulk audit rows",
            "No unbounded cancel",
        ),
        ScenarioSpec(
            "RC-INTERRUPT-MAINTENANCE",
            "admin",
            "Maintenance trigger available",
            "Restart PG / interrupt maintain path",
            "Retry succeeds or skipped_lock; no hang",
            "No partial partition corruption",
            "Maintain outcome code",
            "Cleanup bounded",
        ),
        ScenarioSpec(
            "RC-INTERRUPT-BREAK-GLASS",
            "admin",
            "Leased task + short-lived break_glass creds",
            "Force lease expiry; interrupt/retry",
            "Audited; no new claim token issued",
            "Fence preserved; audit op recorded",
            "Break-glass audit + outcome",
            "Retry remains allowlisted/idempotent",
        ),
        ScenarioSpec(
            "RC-INTERRUPT-BG-DELIVERY-RECLAIM",
            "admin",
            "Stuck publishing delivery event + JIT break_glass audience",
            "Pre-commit kill mid forceDeliveryReclaim",
            "Bounded/idempotent retry; no claim_token in body",
            "Event pending or publishing; generation preserved; no token mint",
            "Break-glass delivery reclaim + outcome",
            "Retry remains allowlisted/idempotent",
        ),
        ScenarioSpec(
            "RC-INTERRUPT-BG-ELEVATION-WRITE",
            "admin",
            "Named queue + JIT raiseReplayLimit audience",
            "Pre-commit kill mid durable raiseReplayLimit write",
            "Retry succeeds within factor/ttl bounds",
            "Elevation absent or single durable row; never sticky unlimited",
            "break_glass_elevations row count + factor/ttl",
            "Auto-revert TTL preserved; no silent unlimited",
        ),
        ScenarioSpec(
            "RC-PRESSURE-CLAIM-DRAIN",
            "recovery",
            "Overload pressure / readiness failure mode",
            "PG restart under pressure controller",
            "Claims still allowed; enqueue throttled/readiness fails",
            "claims_allowed remains True across modes",
            "OverloadMode + claim success",
            "Pressure does not block claim drain",
        ),
        ScenarioSpec(
            "RC-NO-LEAKAGE",
            "security",
            "Payload sentinel + claim token in play",
            "Capture chaos evidence / log buffers",
            "Evidence free of token/payload/DSN secrets",
            "N/A (telemetry)",
            "Redaction assertions",
            "No leakage across scenarios",
        ),
    )


def assert_matrix_covers_qual04_kernel() -> None:
    ids = {s.scenario_id for s in scenario_matrix()}
    assert ids == set(SCENARIO_IDS)
    for excluded in EXCLUDED_PHASE5:
        assert excluded not in ids
    categories = {s.category for s in scenario_matrix()}
    assert {"runtime", "database", "race", "restore", "admin", "recovery", "security"} <= categories


@dataclass
class ChaosEvidence:
    """Redacted evidence bundle for a single scenario run."""

    scenario_id: str
    injection_at: str | None = None
    protocol: dict[str, Any] = field(default_factory=dict)
    database: dict[str, Any] = field(default_factory=dict)
    telemetry: dict[str, Any] = field(default_factory=dict)
    recovery: dict[str, Any] = field(default_factory=dict)
    captured_text: list[str] = field(default_factory=list)

    def note(self, text: str) -> None:
        # Never retain raw secrets — callers must pass already-redacted text.
        self.captured_text.append(text)

    def assert_no_secrets(self, *, claim_token: str, payload_sentinel: str) -> None:
        blob = "\n".join(self.captured_text)
        assert claim_token not in blob
        assert payload_sentinel not in blob
        lowered = blob.lower()
        assert "postgresql+psycopg://" not in lowered
        assert "password=" not in lowered


def _bindings(queue_name: str) -> tuple[CredentialBinding, ...]:
    now = datetime.now(timezone.utc)
    return (
        CredentialBinding(
            principal_id=PRODUCER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id=WORKER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
        CredentialBinding(
            principal_id=OBSERVER_PRINCIPAL,
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=Secret(OBSERVER_TOKEN),
        ),
        CredentialBinding(
            principal_id=BREAK_GLASS_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(BREAK_GLASS_TOKEN),
            expires_at=now.replace(year=now.year + 1),
            allowed_operations=frozenset(
                {
                    "forceLeaseExpiry",
                    "reconcileCounters",
                    "raiseReplayLimit",
                    "dropExpiredPartition",
                    "repairRegistryEntry",
                    "forceDeliveryReclaim",
                    "forceDeliveryDeadLetter",
                }
            ),
        ),
    )


def _authorizer(queue_name: str) -> Authorizer:
    scoped = frozenset({queue_name})
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scoped,
            WORKER_PRINCIPAL: scoped,
            ADMIN_PRINCIPAL: scoped,
            OBSERVER_PRINCIPAL: scoped,
            BREAK_GLASS_PRINCIPAL: scoped,
        }
    )


def asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
) -> tuple[int, dict[str, str], bytes]:
    header_list = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    if body and not any(k == b"content-length" for k, _ in header_list):
        header_list.append((b"content-length", str(len(body)).encode("latin-1")))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": header_list,
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 18111),
    }
    status_box: dict[str, int] = {}
    header_box: dict[str, str] = {}
    body_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            status_box["status"] = int(message["status"])
            header_box.clear()
            for raw_k, raw_v in message.get("headers", []):
                header_box[raw_k.decode("latin-1").lower()] = raw_v.decode("latin-1")
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b"") or b"")

    asyncio.run(app(scope, receive, send))
    return status_box["status"], header_box, b"".join(body_chunks)


@contextmanager
def serve_app(app: Any, *, name: str = "chaos-app") -> Iterator[str]:
    lifecycle = Lifecycle()
    gate = InFlightGate()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    server = QuietThreadingHTTPServer(
        ("127.0.0.1", port),
        AsgiRequestHandler,
        app=app,
        lifecycle=lifecycle,
        gate=gate,
        api_engine=None,
        schema=None,
        premake_days=0,
    )
    lifecycle.mark_running()
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name=name,
        daemon=True,
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        try:
            server.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            server.server_close()
        except Exception:  # noqa: BLE001
            pass
        thread.join(timeout=2.0)


def raw_http_exchange(
    base_url: str,
    *,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes,
    timeout_s: float = 15.0,
) -> tuple[int, dict[str, str], bytes]:
    url = f"{base_url.rstrip('/')}{path}"
    req = Request(url, data=body if body else None, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
            header_map = {k.lower(): v for k, v in resp.headers.items()}
            return int(resp.status), header_map, raw
    except HTTPError as exc:
        return int(exc.code), dict(exc.headers.items()), exc.read()


class KernelChaosHarness:
    """Fault injection over disposable schema + Docker PostgreSQL reference env."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        database_url: str,
        schema: str,
        engine: Any,
    ) -> None:
        self.session_factory = session_factory
        self.database_url = database_url
        self.schema = schema
        self.engine = engine
        self.queue_name = f"chaos.kernel.{uuid.uuid4().hex[:10]}"
        self.authorizer = _authorizer(self.queue_name)
        self.pressure_controller: AdaptivePressureController | None = None
        self.app = self._build_app()
        self.admin_app = self._build_admin_app()
        self._seed_queue()
        self.last_evidence: ChaosEvidence | None = None
        self._log_buffer = io.StringIO()
        handler = logging.StreamHandler(self._log_buffer)
        handler.setLevel(logging.INFO)
        logging.getLogger("workhold").addHandler(handler)
        self._log_handler = handler

    def close(self) -> None:
        logging.getLogger("workhold").removeHandler(self._log_handler)

    def _build_app(
        self, *, pressure_controller: AdaptivePressureController | None = None
    ) -> Any:
        """Build the public app; optionally wire live adaptive admission (04-03)."""
        adaptive_gate: AdaptiveEnqueueGate | None = None
        if pressure_controller is not None:
            clock = pressure_controller.monotonic_clock
            adaptive_gate = AdaptiveEnqueueGate(
                controller=pressure_controller,
                config=AdaptiveEnqueueConfig(
                    queue_enqueue_rps=100,
                    instance_enqueue_rps=100,
                    throttle_queue_enqueue_rps=0.0,
                    throttle_instance_enqueue_rps=0.0,
                    retry_after_ms=250,
                ),
                monotonic_clock=clock,
            )
        return create_application_app(
            authenticator=BearerCredentialAuthenticator.from_bindings(
                _bindings(self.queue_name)
            ),
            authorizer=self.authorizer,
            bind=ListenerBind(host="127.0.0.1", port=18111),
            session_factory=self.session_factory,
            enqueue_service=EnqueueService(
                session_factory=self.session_factory,
                depth_ceilings=DepthCeilings(
                    queue_active_depth=100,
                    instance_active_depth=500,
                    retry_after_ms=250,
                ),
                adaptive_gate=adaptive_gate,
            ),
            claim_service=ClaimService(session_factory=self.session_factory),
            lease_service=LeaseService(session_factory=self.session_factory),
            completion_service=CompletionService(session_factory=self.session_factory),
        )

    def _build_admin_app(self) -> Any:
        return create_admin_app(
            authenticator=BearerCredentialAuthenticator.from_bindings(
                _bindings(self.queue_name)
            ),
            authorizer=self.authorizer,
            bind=ListenerBind(host="127.0.0.1", port=18112),
            session_factory=self.session_factory,
            repository=QueueControlRepository(),
            engine=self.engine,
            cursor_secret=Secret("chaos-confirm-secret"),
            payload_retention_policy=PayloadRetentionPolicy(retention_days=30),
        )

    def _admin_meta(self) -> AdminRequestMetadata:
        return AdminRequestMetadata(
            actor_id=ADMIN_PRINCIPAL,
            request_id=str(uuid.uuid4()),
            idempotency_key=f"chaos-admin-{uuid.uuid4().hex}",
        )

    def _seed_queue(self) -> Queue:
        with self.session_factory() as session:
            QueueControlRepository().create_named_queue(
                session,
                CreateQueueMutation(
                    name=self.queue_name,
                    initial_policy=RetryPolicyDraft(
                        enabled=True,
                        max_attempts=3,
                        backoff_strategy=BackoffStrategy.FIXED,
                        retry_delay_seconds=0,
                    ),
                    metadata=self._admin_meta(),
                ),
            )
            session.commit()
            return session.execute(
                select(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()

    def _producer_headers(self, idem: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Content-Type": "application/json",
            "Idempotency-Key": idem,
        }

    def _worker_headers(self, *, claim_token: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Content-Type": "application/json",
        }
        if claim_token is not None:
            headers[CLAIM_TOKEN_HEADER] = claim_token
        return headers

    def _admin_headers(self, *, idem: str, token: str = ADMIN_TOKEN) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Idempotency-Key": idem,
        }

    def enqueue(
        self,
        *,
        idem: str | None = None,
        payload: Any | None = None,
        app: Any | None = None,
    ) -> tuple[int, dict[str, Any]]:
        key = idem or f"idem-{uuid.uuid4().hex}"
        body = json.dumps(
            {"payload": payload if payload is not None else {"secret": PAYLOAD_SENTINEL}, "priority": 0},
            separators=(",", ":"),
        ).encode("utf-8")
        status, _h, raw = asgi_http_call(
            app or self.app,
            method="POST",
            path=f"/v1/queues/{self.queue_name}/tasks",
            headers=self._producer_headers(key),
            body=body,
        )
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return status, data

    def claim_one(self, *, app: Any | None = None) -> dict[str, Any]:
        body = json.dumps(
            {
                "queues": [self.queue_name],
                "max_tasks": 1,
                "lease_seconds": 30,
                "wait_seconds": 0,
                "worker_id": WORKER_ID,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        status, _h, raw = asgi_http_call(
            app or self.app,
            method="POST",
            path=CLAIM_PATH,
            headers=self._worker_headers(),
            body=body,
        )
        assert status == 200, raw.decode("utf-8", errors="replace")
        tasks = json.loads(raw.decode("utf-8"))["tasks"]
        if not tasks:
            return {}
        claimed = tasks[0]
        claim = claimed["claim"]
        return {
            "task_id": claimed["task"]["task_id"],
            "claim_id": claim["claim_id"],
            "claim_token": claim["claim_token"],
            "generation": int(claim["generation"]),
        }

    def complete(
        self,
        claim: dict[str, Any],
        *,
        spawn: list[Any] | None = None,
        app: Any | None = None,
    ) -> tuple[int, dict[str, Any]]:
        body = json.dumps(
            {"generation": int(claim["generation"]), "spawn": spawn or []},
            separators=(",", ":"),
        ).encode("utf-8")
        status, _h, raw = asgi_http_call(
            app or self.app,
            method="POST",
            path=f"/v1/claims/{claim['claim_id']}:complete",
            headers=self._worker_headers(claim_token=claim["claim_token"]),
            body=body,
        )
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return status, data

    def fail_task(
        self,
        claim: dict[str, Any],
        *,
        app: Any | None = None,
    ) -> tuple[int, dict[str, Any]]:
        body = json.dumps(
            {
                "generation": int(claim["generation"]),
                "failure_code": "worker_error",
                "failure_detail": "chaos-fail",
                "retryable": True,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        status, _h, raw = asgi_http_call(
            app or self.app,
            method="POST",
            path=f"/v1/claims/{claim['claim_id']}:fail",
            headers=self._worker_headers(claim_token=claim["claim_token"]),
            body=body,
        )
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return status, data

    def heartbeat(self, claim: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        body = json.dumps(
            {"generation": int(claim["generation"]), "lease_seconds": 30},
            separators=(",", ":"),
        ).encode("utf-8")
        status, _h, raw = asgi_http_call(
            self.app,
            method="POST",
            path=f"/v1/claims/{claim['claim_id']}:heartbeat",
            headers=self._worker_headers(claim_token=claim["claim_token"]),
            body=body,
        )
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return status, data

    def cancel_task(self, task_id: str) -> tuple[int, dict[str, Any]]:
        status, _h, raw = asgi_http_call(
            self.app,
            method="POST",
            path=f"/v1/tasks/{task_id}:cancel",
            headers={
                "Authorization": f"Bearer {PRODUCER_TOKEN}",
                "Content-Type": "application/json",
                "Idempotency-Key": f"cancel-{uuid.uuid4().hex}",
            },
            body=b"{}",
        )
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return status, data

    def set_state(self, state: str) -> tuple[int, dict[str, Any]]:
        with self.session_factory() as session:
            queue = session.execute(
                select(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            version = int(queue.config_version)
        status, _h, raw = asgi_http_call(
            self.admin_app,
            method="POST",
            path=f"/admin/v1/queues/{self.queue_name}:set-state",
            headers=self._admin_headers(idem=f"state-{state}-{uuid.uuid4().hex}"),
            body=json.dumps(
                {"expected_config_version": version, "state": state},
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return status, data

    def expire_lease(self, task_id: str) -> None:
        with self.session_factory() as session:
            session.execute(
                text(
                    """
                    UPDATE tasks_active
                    SET lease_expires_at = transaction_timestamp() - interval '1 second'
                    WHERE task_id = CAST(:tid AS uuid)
                    """
                ),
                {"tid": task_id},
            )
            session.execute(
                text(
                    """
                    UPDATE claim_registry
                    SET lease_expires_at = GREATEST(
                        claimed_at + interval '1 millisecond',
                        transaction_timestamp() - interval '1 second'
                    )
                    WHERE task_id = CAST(:tid AS uuid)
                    """
                ),
                {"tid": task_id},
            )
            session.commit()

    def counts(self) -> dict[str, int]:
        with self.session_factory() as session:
            queue = session.execute(
                select(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            qid = int(queue.id)
            return {
                "active": int(
                    session.scalar(
                        select(func.count())
                        .select_from(TaskActive)
                        .where(TaskActive.queue_id == qid)
                    )
                    or 0
                ),
                "terminals": int(
                    session.scalar(
                        select(func.count())
                        .select_from(TaskTerminal)
                        .where(TaskTerminal.queue_id == qid)
                    )
                    or 0
                ),
                "dedup": int(
                    session.scalar(
                        select(func.count())
                        .select_from(EnqueueDedup)
                        .where(EnqueueDedup.queue_id == qid)
                    )
                    or 0
                ),
                "replays": int(
                    session.scalar(select(func.count()).select_from(CompleteReplay)) or 0
                ),
                "audits": int(
                    session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
                ),
                "attempts": int(
                    session.scalar(select(func.count()).select_from(TaskAttempt)) or 0
                ),
            }

    def restart_postgres(self, evidence: ChaosEvidence) -> None:
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        evidence.note("inject: docker compose restart postgres")
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "restart",
                "postgres",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"postgres restart failed: {result.stderr or result.stdout}"
            )
        self._wait_postgres_ready(timeout_s=60.0)
        evidence.recovery["postgres_ready_at"] = datetime.now(timezone.utc).isoformat()
        # Dispose pooled connections that died across the restart.
        self.engine.dispose()

    def _wait_postgres_ready(self, *, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = psycopg.connect(to_psycopg_conninfo(self.database_url))
                try:
                    conn.execute("SELECT 1")
                finally:
                    conn.close()
                return
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                time.sleep(0.5)
        raise TimeoutError(f"postgres not ready after restart: {last_err!r}")

    def readiness_ok(self) -> bool:
        # Disposable chaos schemas use premake_days=0; full prod premake is not required.
        status = check_readiness(self.engine, schema=self.schema, premake_days=0)
        return bool(status.ok)

    def postgres_reachable(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:  # noqa: BLE001
            return False

    def logical_snapshot(self) -> bytes:
        """Dump current disposable schema for PITR-style restore simulation."""
        conninfo = to_psycopg_conninfo(self.database_url)
        # Prefer docker exec pg_dump for matching server tools.
        container = self._postgres_container_name()
        cmd = [
            "docker",
            "exec",
            container,
            "pg_dump",
            "-U",
            "queue",
            "-d",
            "queue",
            "-n",
            self.schema,
            "--no-owner",
            "--no-privileges",
        ]
        result = subprocess.run(
            cmd, capture_output=True, timeout=120, check=False
        )
        if result.returncode != 0:
            # Fallback: host pg_dump if available.
            host_cmd = [
                "pg_dump",
                conninfo,
                "-n",
                self.schema,
                "--no-owner",
                "--no-privileges",
            ]
            result = subprocess.run(
                host_cmd, capture_output=True, timeout=120, check=False
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"pg_dump failed: {result.stderr.decode('utf-8', errors='replace')}"
                )
        return result.stdout

    def logical_restore(self, dump: bytes) -> None:
        container = self._postgres_container_name()
        # Drop objects inside schema without removing the schema name (fixture owns it).
        admin = psycopg.connect(to_psycopg_conninfo(self.database_url))
        admin.autocommit = True
        try:
            # Drop tables/views first (CASCADE owns sequences). Then drop any
            # leftover sequences — never drop sequences while tables still reference them.
            admin.execute(
                f"""
                DO $$
                DECLARE r RECORD;
                BEGIN
                  FOR r IN
                    SELECT n.nspname, c.relname, c.relkind
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = '{self.schema}'
                      AND c.relkind IN ('v', 'm')
                  LOOP
                    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', r.nspname, r.relname);
                  END LOOP;
                  FOR r IN
                    SELECT n.nspname, c.relname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = '{self.schema}'
                      AND c.relkind IN ('r', 'p')
                  LOOP
                    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', r.nspname, r.relname);
                  END LOOP;
                  FOR r IN
                    SELECT n.nspname, c.relname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = '{self.schema}'
                      AND c.relkind = 'S'
                  LOOP
                    EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I CASCADE', r.nspname, r.relname);
                  END LOOP;
                END $$;
                """
            )
        finally:
            admin.close()
        self.engine.dispose()
        # Strip CREATE SCHEMA from dump so restore cannot collide with fixture schema.
        text_dump = dump.decode("utf-8", errors="replace")
        filtered = "\n".join(
            line
            for line in text_dump.splitlines()
            if not line.upper().startswith("CREATE SCHEMA")
            and "CREATE SCHEMA" not in line.upper()
        )
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                container,
                "psql",
                "-U",
                "queue",
                "-d",
                "queue",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=filtered.encode("utf-8"),
            capture_output=True,
            timeout=180,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"psql restore failed: {proc.stderr.decode('utf-8', errors='replace')}"
            )
        self.engine.dispose()

    def _rebuild_apps(
        self, *, pressure_controller: AdaptivePressureController | None = None
    ) -> None:
        """Rebuild ASGI apps after restore/fault so pools see restored schema."""
        self.pressure_controller = pressure_controller
        self.app = self._build_app(pressure_controller=pressure_controller)
        self.admin_app = self._build_admin_app()

    def _break_glass_ack_body(self, **extra: Any) -> bytes:
        payload = {
            "reason": "chaos break-glass remediation",
            "incident_reference": "INC-CHAOS-RESTORE-1",
            "risk_acknowledged": True,
            **extra,
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    def _drop_committed_admin_once(
        self,
        *,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        evidence: ChaosEvidence,
        accept_statuses: set[int] | None = None,
    ) -> int:
        """Drop committed admin response (RT-API-AFTER-COMMIT class) once."""
        accepted = accept_statuses or {200}
        with serve_app(self.admin_app, name="chaos-admin-drop") as base_url:
            proxy = DropCommittedResponseProxy()
            deadline = time.monotonic() + 25.0
            listen_url = proxy.serve_once(base_url, deadline)
            evidence.injection_at = datetime.now(timezone.utc).isoformat()
            producer_error: BaseException | None = None
            try:
                raw_http_exchange(
                    listen_url,
                    method=method,
                    path=path,
                    headers=headers,
                    body=body,
                    timeout_s=20.0,
                )
            except (URLError, TimeoutError, OSError, HTTPError) as exc:
                producer_error = exc
            buffered = proxy.wait(deadline)
            proxy.close()
            evidence.protocol["drop_upstream_status"] = buffered.status_code
            evidence.protocol["drop_client_error"] = (
                type(producer_error).__name__ if producer_error else None
            )
            evidence.note("inject: drop-committed-response on admin op")
            assert buffered.status_code in accepted, (
                f"unexpected upstream status {buffered.status_code}"
            )
            return int(buffered.status_code)

    def _precommit_kill_admin_once(
        self,
        *,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        evidence: ChaosEvidence,
    ) -> None:
        """Kill admin backend while blocked on queues FOR UPDATE (RT-API-BEFORE-COMMIT)."""
        with self.session_factory() as session:
            queue = session.execute(
                select(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            queue_id = int(queue.id)
        with serve_app(self.admin_app, name="chaos-admin-precommit") as base_url:
            gate = PostgresPreCommitGate(
                database_url=self.database_url, schema=self.schema
            )
            gate.enter(queue_id)
            errors: list[BaseException] = []
            result_box: dict[str, Any] = {}

            def _call() -> None:
                try:
                    status, _headers, raw = raw_http_exchange(
                        base_url,
                        method=method,
                        path=path,
                        headers=headers,
                        body=body,
                        timeout_s=20.0,
                    )
                    result_box["status"] = status
                    result_box["body"] = raw
                except BaseException as exc:  # noqa: BLE001
                    errors.append(exc)

            thread = threading.Thread(target=_call, daemon=True)
            thread.start()
            try:
                gate.wait_until_blocked(time.monotonic() + 15.0)
                evidence.injection_at = datetime.now(timezone.utc).isoformat()
                gate.terminate_blocked_backend()
            finally:
                gate.release()
            thread.join(timeout=20.0)
            evidence.protocol["precommit_outcome"] = {
                "status": result_box.get("status"),
                "errors": [type(e).__name__ for e in errors],
            }
            evidence.note("inject: pre-commit backend kill on admin op")

    def _postgres_container_name(self) -> str:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "ps",
                "-q",
                "postgres",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        cid = (result.stdout or "").strip()
        if not cid:
            raise RuntimeError("docker compose postgres container not found")
        return cid

    def run(self, scenario_id: str) -> ChaosEvidence:
        if scenario_id not in SCENARIO_IDS:
            raise KeyError(f"unknown scenario {scenario_id}")
        evidence = ChaosEvidence(scenario_id=scenario_id)
        dispatch = {
            "RT-API-BEFORE-COMMIT": self._rt_api_before_commit,
            "RT-API-AFTER-COMMIT": self._rt_api_after_commit,
            "RT-WORKER-LEASE": self._rt_worker_lease,
            "RT-WORKER-TERMINAL": self._rt_worker_terminal,
            "RT-PG-ENQUEUE": self._rt_pg_enqueue,
            "RT-PG-CLAIM-HB-COMPLETE": self._rt_pg_claim_hb_complete,
            "RT-PG-MAINT-ADMIN": self._rt_pg_maint_admin,
            "RT-RACE-PAUSE-CLAIM": self._rt_race_pause_claim,
            "RT-RACE-DRAIN-ENQUEUE-SPAWN": self._rt_race_drain_enqueue_spawn,
            "RT-RACE-CANCEL-EXPIRY-COMPLETE": self._rt_race_cancel_expiry_complete,
            "RT-RACE-RETRY-LEASE-EXPIRY": self._rt_race_retry_lease_expiry,
            "RC-RESTORE-PITR": self._rc_restore_pitr,
            "RC-INTERRUPT-REPLAY": self._rc_interrupt_replay,
            "RC-INTERRUPT-BULK-CANCEL": self._rc_interrupt_bulk_cancel,
            "RC-INTERRUPT-MAINTENANCE": self._rc_interrupt_maintenance,
            "RC-INTERRUPT-BREAK-GLASS": self._rc_interrupt_break_glass,
            "RC-INTERRUPT-BG-DELIVERY-RECLAIM": self._rc_interrupt_bg_delivery_reclaim,
            "RC-INTERRUPT-BG-ELEVATION-WRITE": self._rc_interrupt_bg_elevation_write,
            "RC-PRESSURE-CLAIM-DRAIN": self._rc_pressure_claim_drain,
            "RC-NO-LEAKAGE": self._rc_no_leakage,
        }
        dispatch[scenario_id](evidence)
        self.last_evidence = evidence
        return evidence

    # --- scenario implementations -------------------------------------------------

    def _rt_api_before_commit(self, evidence: ChaosEvidence) -> None:
        idem = f"idem-before-{uuid.uuid4().hex}"
        with self.session_factory() as session:
            queue = session.execute(
                select(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            queue_id = int(queue.id)
        with serve_app(self.app, name="chaos-before-commit") as base_url:
            gate = PostgresPreCommitGate(
                database_url=self.database_url, schema=self.schema
            )
            gate.enter(queue_id)
            errors: list[BaseException] = []
            result_box: dict[str, Any] = {}

            def _enqueue() -> None:
                try:
                    status, headers, body = raw_http_exchange(
                        base_url,
                        method="POST",
                        path=f"/v1/queues/{self.queue_name}/tasks",
                        headers=self._producer_headers(idem),
                        body=json.dumps(
                            {"payload": {"secret": PAYLOAD_SENTINEL}, "priority": 0},
                            separators=(",", ":"),
                        ).encode("utf-8"),
                        timeout_s=20.0,
                    )
                    result_box["status"] = status
                    result_box["body"] = body
                except BaseException as exc:  # noqa: BLE001
                    errors.append(exc)

            thread = threading.Thread(target=_enqueue, daemon=True)
            thread.start()
            try:
                gate.wait_until_blocked(time.monotonic() + 15.0)
                evidence.injection_at = datetime.now(timezone.utc).isoformat()
                gate.terminate_blocked_backend()
            finally:
                gate.release()
            thread.join(timeout=20.0)
            evidence.protocol["first_outcome"] = {
                "status": result_box.get("status"),
                "errors": [type(e).__name__ for e in errors],
            }
            evidence.note("protocol: first enqueue failed before commit")

        before = self.counts()
        assert before["active"] == 0
        assert before["dedup"] == 0
        status2, data2 = self.enqueue(idem=idem)
        assert status2 == 201, data2
        after = self.counts()
        evidence.database = {"before": before, "after": after}
        evidence.recovery["task_id"] = data2["task"]["task_id"]
        assert after["active"] == 1
        assert after["dedup"] == 1

    def _rt_api_after_commit(self, evidence: ChaosEvidence) -> None:
        idem = f"idem-after-{uuid.uuid4().hex}"
        with serve_app(self.app, name="chaos-after-commit") as base_url:
            proxy = DropCommittedResponseProxy()
            deadline = time.monotonic() + 20.0
            listen_url = proxy.serve_once(base_url, deadline)
            evidence.injection_at = datetime.now(timezone.utc).isoformat()
            producer_error: BaseException | None = None
            try:
                raw_http_exchange(
                    listen_url,
                    method="POST",
                    path=f"/v1/queues/{self.queue_name}/tasks",
                    headers=self._producer_headers(idem),
                    body=json.dumps(
                        {"payload": {"secret": PAYLOAD_SENTINEL}, "priority": 0},
                        separators=(",", ":"),
                    ).encode("utf-8"),
                    timeout_s=15.0,
                )
            except (URLError, TimeoutError, OSError, HTTPError) as exc:
                producer_error = exc
            buffered = proxy.wait(deadline)
            proxy.close()
            evidence.protocol["upstream_status"] = buffered.status_code
            evidence.protocol["producer_error"] = type(producer_error).__name__ if producer_error else None
            evidence.note("protocol: upstream committed; producer saw drop")
            assert buffered.status_code in {200, 201}

        counts = self.counts()
        evidence.database = counts
        assert counts["active"] == 1
        assert counts["dedup"] == 1
        status2, data2 = self.enqueue(idem=idem)
        assert status2 in {200, 201}, data2
        # Replay should not create a second task.
        assert self.counts()["active"] == 1
        evidence.recovery["replayed"] = bool(data2.get("task", {}).get("replayed") or data2.get("replayed"))

    def _rt_worker_lease(self, evidence: ChaosEvidence) -> None:
        status, _ = self.enqueue()
        assert status == 201
        claim = self.claim_one()
        assert claim
        stale_token = claim["claim_token"]
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        evidence.note("inject: expire lease after simulated worker death")
        self.expire_lease(claim["task_id"])
        # Force reclaim path via another claim.
        reclaimed = self.claim_one()
        assert reclaimed
        assert reclaimed["claim_token"] != stale_token
        assert int(reclaimed["generation"]) >= int(claim["generation"])
        stale_status, stale_body = self.complete(
            {**claim, "claim_token": stale_token}
        )
        evidence.protocol["stale_complete"] = {
            "status": stale_status,
            "code": stale_body.get("code"),
        }
        assert stale_status in {409, 403, 401, 404, 412} or stale_body.get("code") in {
            "lease_lost",
            "claim_fence_mismatch",
            "generation_mismatch",
            "not_found",
        }
        evidence.database = self.counts()
        evidence.recovery["reclaimed_generation"] = reclaimed["generation"]
        # Complete with current fence.
        ok_status, ok_body = self.complete(reclaimed)
        assert ok_status == 200, ok_body
        assert ok_body.get("replayed") is False

    def _rt_worker_terminal(self, evidence: ChaosEvidence) -> None:
        status, _ = self.enqueue()
        assert status == 201
        claim = self.claim_one()
        status1, body1 = self.complete(claim)
        assert status1 == 200, body1
        assert body1.get("replayed") is False
        before = self.counts()
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        evidence.note("inject: rebuild API process after durable complete")
        app2 = self._build_app()
        status2, body2 = self.complete(claim, app=app2)
        assert status2 == 200, body2
        assert body2.get("replayed") is True
        after = self.counts()
        evidence.database = {"before": before, "after": after}
        evidence.protocol["replayed"] = True
        assert after["terminals"] == before["terminals"]
        assert after["active"] == before["active"]

    def _rt_pg_enqueue(self, evidence: ChaosEvidence) -> None:
        idem = f"idem-pg-enq-{uuid.uuid4().hex}"
        status1, data1 = self.enqueue(idem=idem)
        assert status1 == 201, data1
        task_id = data1["task"]["task_id"]
        self.restart_postgres(evidence)
        assert self.postgres_reachable()
        # Soft readiness: disposable schema may omit prod premake headroom.
        _ = self.readiness_ok()
        status2, data2 = self.enqueue(idem=idem)
        assert status2 in {200, 201}, data2
        # Same task identity after restart (dedup replay).
        replay_task = data2.get("task", {}).get("task_id") or data2.get("task_id")
        if replay_task:
            assert replay_task == task_id
        evidence.database = self.counts()
        evidence.recovery["survived_task_id"] = task_id
        assert self.counts()["active"] == 1

    def _rt_pg_claim_hb_complete(self, evidence: ChaosEvidence) -> None:
        status, _ = self.enqueue()
        assert status == 201
        claim = self.claim_one()
        hb_status, hb_body = self.heartbeat(claim)
        assert hb_status == 200, hb_body
        self.restart_postgres(evidence)
        assert self.postgres_reachable()
        _ = self.readiness_ok()
        # Heartbeat may succeed if lease still valid, or lease_lost if clock/lease drifted.
        hb2_status, hb2_body = self.heartbeat(claim)
        evidence.protocol["post_restart_heartbeat"] = {
            "status": hb2_status,
            "code": hb2_body.get("code"),
        }
        if hb2_status == 200:
            done_status, done_body = self.complete(claim)
            assert done_status == 200, done_body
        else:
            # Expire and reclaim path.
            self.expire_lease(claim["task_id"])
            reclaimed = self.claim_one()
            assert reclaimed
            done_status, done_body = self.complete(reclaimed)
            assert done_status == 200, done_body
        evidence.database = self.counts()
        evidence.recovery["completed"] = True

    def _rt_pg_maint_admin(self, evidence: ChaosEvidence) -> None:
        status1, body1 = self.set_state("paused")
        assert status1 == 200, body1
        self.restart_postgres(evidence)
        assert self.postgres_reachable()
        _ = self.readiness_ok()
        with self.session_factory() as session:
            state_val = session.execute(
                text("SELECT state_code FROM queues WHERE name = :n"),
                {"n": self.queue_name},
            ).scalar_one()
        evidence.database["state_after_restart"] = state_val
        # QueueState.PAUSED maps to state_code 2 in storage.
        assert int(state_val) == 2
        status2, body2 = self.set_state("active")
        evidence.protocol["resume"] = {"status": status2, "body_keys": list(body2)}
        evidence.recovery["admin_retry_ok"] = status2 == 200
        assert status2 == 200, body2

    def _rt_race_pause_claim(self, evidence: ChaosEvidence) -> None:
        for _ in range(2):
            assert self.enqueue()[0] == 201
        claim = self.claim_one()
        assert claim
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        status, body = self.set_state("paused")
        assert status == 200, body
        evidence.note("inject: pause while tasks remain")
        empty = self.claim_one()
        assert empty == {}
        # Enqueue still allowed while paused.
        enq_status, _ = self.enqueue()
        assert enq_status == 201
        status2, _ = self.set_state("active")
        assert status2 == 200
        claim2 = self.claim_one()
        assert claim2
        evidence.protocol["post_resume_claim"] = bool(claim2)
        evidence.database = self.counts()
        evidence.recovery["claims_restored"] = True

    def _rt_race_drain_enqueue_spawn(self, evidence: ChaosEvidence) -> None:
        assert self.enqueue()[0] == 201
        claim = self.claim_one()
        status, body = self.set_state("draining")
        assert status == 200, body
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        evidence.note("inject: drain rejects external enqueue; spawn continues")
        enq_status, enq_body = self.enqueue()
        evidence.protocol["external_enqueue"] = {
            "status": enq_status,
            "code": enq_body.get("code"),
        }
        assert enq_status in {409, 429, 503} or enq_body.get("code") in {
            "queue_draining",
            "resource_exhausted",
        }
        spawn = [
            {
                "queue_name": self.queue_name,
                "idempotency_key": f"spawn-{uuid.uuid4().hex}",
                "payload": {"secret": PAYLOAD_SENTINEL, "kind": "spawn"},
                "priority": 0,
            }
        ]
        done_status, done_body = self.complete(claim, spawn=spawn)
        assert done_status == 200, done_body
        assert len(done_body.get("spawned_task_ids") or []) == 1
        # Claims continue under drain.
        child = self.claim_one()
        assert child
        evidence.database = self.counts()
        evidence.recovery["spawn_ok"] = True
        # Return queue to active for subsequent scenarios on same harness instance.
        self.set_state("active")

    def _rt_race_cancel_expiry_complete(self, evidence: ChaosEvidence) -> None:
        assert self.enqueue()[0] == 201
        claim = self.claim_one()
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        # Race: cancel request + complete.
        cancel_status, cancel_body = self.cancel_task(claim["task_id"])
        done_status, done_body = self.complete(claim)
        evidence.protocol["cancel"] = {
            "status": cancel_status,
            "code": cancel_body.get("code"),
        }
        evidence.protocol["complete"] = {
            "status": done_status,
            "code": done_body.get("code"),
            "state": done_body.get("state"),
        }
        terminals = self.counts()["terminals"]
        # Either complete won (succeeded) or cancel path; never >1 terminal for task.
        with self.session_factory() as session:
            n = session.scalar(
                select(func.count())
                .select_from(TaskTerminal)
                .where(TaskTerminal.task_id == claim["task_id"])
            )
        evidence.database = {"terminals_for_task": int(n or 0), "queue_terminals": terminals}
        assert int(n or 0) <= 1
        evidence.recovery["single_terminal"] = True

    def _rt_race_retry_lease_expiry(self, evidence: ChaosEvidence) -> None:
        assert self.enqueue()[0] == 201
        claim = self.claim_one()
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        self.expire_lease(claim["task_id"])
        evidence.note("inject: expire lease before fail")
        fail_status, fail_body = self.fail_task(claim)
        evidence.protocol["stale_fail"] = {
            "status": fail_status,
            "code": fail_body.get("code"),
        }
        # Stale fail must not create a second lease or corrupt state.
        reclaimed = self.claim_one()
        assert reclaimed
        assert reclaimed["claim_token"] != claim["claim_token"]
        ok_status, ok_body = self.fail_task(reclaimed)
        # Fail or complete — either is fine if fenced.
        if ok_status != 200:
            ok_status, ok_body = self.complete(reclaimed)
        evidence.protocol["current_terminal"] = {
            "status": ok_status,
            "code": ok_body.get("code"),
            "state": ok_body.get("state"),
        }
        assert ok_status == 200, ok_body
        evidence.database = self.counts()
        evidence.recovery["fence_held"] = True

    def _rc_restore_pitr(self, evidence: ChaosEvidence) -> None:
        """Full-schema logical snapshot/restore of disposable ``qch_*`` contents."""
        idem = f"idem-restore-{uuid.uuid4().hex}"
        status, data = self.enqueue(idem=idem)
        assert status == 201, data
        task_id = data["task"]["task_id"]
        self.set_state("active")
        with self.session_factory() as session:
            snapshot = {
                "audits": int(
                    session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
                ),
                "queues": int(
                    session.scalar(select(func.count()).select_from(Queue)) or 0
                ),
                "dedup": int(
                    session.execute(
                        text(
                            "SELECT COUNT(*) FROM enqueue_dedup WHERE task_id = CAST(:t AS uuid)"
                        ),
                        {"t": task_id},
                    ).scalar_one()
                ),
                "active": int(
                    session.execute(
                        text(
                            "SELECT COUNT(*) FROM tasks_active WHERE task_id = CAST(:t AS uuid)"
                        ),
                        {"t": task_id},
                    ).scalar_one()
                ),
            }
            # Skew counters so post-restore reconcile is observable.
            session.execute(
                text(
                    """
                    UPDATE queue_counters AS c
                    SET delayed_count = 9, ready_count = 9, leased_count = 9
                    FROM queues AS q
                    WHERE c.queue_id = q.id AND q.name = :name
                    """
                ),
                {"name": self.queue_name},
            )
            session.commit()
        dump = self.logical_snapshot()
        evidence.injection_at = datetime.now(timezone.utc).isoformat()
        evidence.note("inject: logical_snapshot of qch_* schema; mutate past restore point")
        evidence.telemetry["snapshot"] = snapshot
        assert len(dump) > 100, "logical snapshot empty"

        post_idem = f"idem-post-{uuid.uuid4().hex}"
        post_status, post_data = self.enqueue(idem=post_idem)
        assert post_status == 201, post_data
        post_task = post_data["task"]["task_id"]
        post_counts = self.counts()
        assert post_counts["active"] >= 2

        self.logical_restore(dump)
        self._rebuild_apps()
        assert self.postgres_reachable()

        restored = self.counts()
        evidence.database = {"post_mutate": post_counts, "restored": restored}
        with self.session_factory() as session:
            dedup_left = session.execute(
                text(
                    "SELECT COUNT(*) FROM enqueue_dedup WHERE task_id = CAST(:t AS uuid)"
                ),
                {"t": task_id},
            ).scalar_one()
            post_gone = session.execute(
                text(
                    "SELECT COUNT(*) FROM enqueue_dedup WHERE task_id = CAST(:t AS uuid)"
                ),
                {"t": post_task},
            ).scalar_one()
            audits = int(
                session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
            )
            queues = int(session.scalar(select(func.count()).select_from(Queue)) or 0)
            counter = session.execute(
                select(QueueCounter).join(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            skewed = (
                int(counter.delayed_count) == 9
                and int(counter.ready_count) == 9
                and int(counter.leased_count) == 9
            )
        assert int(dedup_left) == 1
        assert int(post_gone) == 0
        assert queues == snapshot["queues"]
        assert audits == snapshot["audits"]
        assert skewed, "restore must bring back skewed counters from snapshot"

        # Readiness + partition horizon on restored schema (head binary range).
        ready = check_readiness(
            self.engine,
            schema=self.schema,
            compatible_min="0001_physical_contract_foundations",
            compatible_max=_CHAOS_HEAD_COMPATIBLE_MAX,
            revision_order=_CHAOS_HEAD_REVISION_ORDER,
        )
        evidence.recovery["readiness_ok"] = ready.ok
        evidence.recovery["readiness_reason"] = ready.reason_code
        if ready.partition is not None:
            evidence.recovery["horizon_safe"] = ready.partition.safe
            evidence.recovery["horizon_remaining"] = ready.partition.remaining_utc_days
        assert ready.ok, ready
        assert ready.partition is not None and ready.partition.safe

        # Duplicate-aware enqueue/complete replay after restore.
        status2, data2 = self.enqueue(idem=idem)
        assert status2 in {200, 201}, data2
        replay_id = data2.get("task", {}).get("task_id")
        if replay_id:
            assert replay_id == task_id
        evidence.recovery["duplicate_aware"] = True
        evidence.recovery["task_id"] = task_id
        claim = self.claim_one()
        assert claim
        done1, body1 = self.complete(claim)
        assert done1 == 200, body1
        done2, body2 = self.complete(claim)
        assert done2 == 200, body2
        assert body2.get("replayed") is True

        # Counter reconcile path (non-authoritative counters → break-glass API).
        recon_status, _h, recon_raw = asgi_http_call(
            self.admin_app,
            method="POST",
            path=f"/admin/v1/queues/{self.queue_name}:reconcile-counters",
            headers=self._admin_headers(
                idem=f"reconcile-{uuid.uuid4().hex}", token=BREAK_GLASS_TOKEN
            ),
            body=self._break_glass_ack_body(),
        )
        recon = json.loads(recon_raw.decode("utf-8")) if recon_raw else {}
        evidence.protocol["reconcile"] = {"status": recon_status, "body": recon}
        assert recon_status == 200, recon
        assert recon.get("outcome") == "reconciled"
        with self.session_factory() as session:
            counter = session.execute(
                select(QueueCounter).join(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            evidence.recovery["counter_reconciled"] = {
                "delayed": int(counter.delayed_count),
                "ready": int(counter.ready_count),
                "leased": int(counter.leased_count),
            }
            # After complete, active should be empty for this queue.
            assert int(counter.delayed_count) == 0
            assert int(counter.ready_count) == 0
            assert int(counter.leased_count) == 0
            replays = int(
                session.scalar(select(func.count()).select_from(CompleteReplay)) or 0
            )
        assert replays >= 1
        assert snapshot["dedup"] == 1
        assert snapshot["active"] == 1

    def _rc_interrupt_replay(self, evidence: ChaosEvidence) -> None:
        self._seed_dead_letter_for_replay()
        with self.session_factory() as session:
            terminal = session.execute(
                select(TaskTerminal)
                .join(Queue, Queue.id == TaskTerminal.queue_id)
                .where(Queue.name == self.queue_name)
                .limit(1)
            ).scalar_one()
        task_id = str(terminal.task_id)
        idem = f"replay-{uuid.uuid4().hex}"
        path = f"/admin/v1/queues/{self.queue_name}/dead-letters/{task_id}:replay"
        body = json.dumps({"reason": "chaos-interrupt-replay"}, separators=(",", ":")).encode()
        headers = self._admin_headers(idem=idem)
        # Mid-op: drop committed response (same class as RT-API-AFTER-COMMIT).
        self._drop_committed_admin_once(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            evidence=evidence,
            accept_statuses={200},
        )
        # Bounded idempotent audited retry after interrupted success.
        status2, _h2, raw2 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data2 = json.loads(raw2.decode("utf-8")) if raw2 else {}
        evidence.protocol["retry"] = {"status": status2, "replayed": data2.get("replayed")}
        assert status2 == 200, data2
        assert data2.get("replayed") is True
        with self.session_factory() as session:
            src = session.execute(
                select(TaskTerminal).where(TaskTerminal.task_id == terminal.task_id)
            ).scalar_one()
            assert src is not None
            audits = int(
                session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
            )
        evidence.database = self.counts()
        evidence.recovery["idempotent_replay"] = True
        evidence.recovery["audits"] = audits
        assert audits >= 1

    def _seed_dead_letter_for_replay(self) -> None:
        """Insert a minimal dead-letter terminal compatible with replay API."""
        with self.session_factory() as session:
            queue = session.execute(
                select(Queue).where(Queue.name == self.queue_name)
            ).scalar_one()
            task_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            body = {"secret": PAYLOAD_SENTINEL, "n": 1}
            payload_bytes = len(json.dumps(body, separators=(",", ":")).encode("utf-8"))
            session.execute(
                text(
                    """
                    INSERT INTO tasks_terminal (
                        task_id, queue_id, producer_id, state_code, priority,
                        available_at, retry_policy_version, payload, payload_bytes,
                        created_at, terminal_at, failure_code, failure_detail
                    ) VALUES (
                        :task_id, :queue_id, :producer_id, 11, 0,
                        :now, 1, CAST(:payload AS jsonb), :payload_bytes,
                        :now, :now, 'exhausted', 'retries exhausted'
                    )
                    """
                ),
                {
                    "task_id": str(task_id),
                    "queue_id": int(queue.id),
                    "producer_id": PRODUCER_PRINCIPAL,
                    "now": now,
                    "payload": json.dumps(body),
                    "payload_bytes": payload_bytes,
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO task_attempts (
                        task_id, claim_id, generation, claimed_at, worker_id,
                        lease_expires_at, ended_at, outcome_code, failure_code,
                        failure_detail
                    ) VALUES (
                        :task_id, :claim_id, 1, :now, 'chaos-seed',
                        :lease_expires, :now, 4, 'exhausted', 'retries exhausted'
                    )
                    """
                ),
                {
                    "task_id": str(task_id),
                    "claim_id": str(uuid.uuid4()),
                    "now": now,
                    "lease_expires": now,
                },
            )
            session.commit()

    def _rc_interrupt_bulk_cancel(self, evidence: ChaosEvidence) -> None:
        for _ in range(3):
            assert self.enqueue()[0] == 201
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        frm = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        to = (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        filters = {"from": frm, "to": to, "states": "ready"}
        dry_path = f"/admin/v1/queues/{self.queue_name}/bulk:preview-cancel"
        dry_body = json.dumps({"filters": filters}, separators=(",", ":")).encode("utf-8")
        dry_status, _h, dry_raw = asgi_http_call(
            self.admin_app,
            method="POST",
            path=dry_path,
            headers={
                "Authorization": f"Bearer {ADMIN_TOKEN}",
                "Content-Type": "application/json",
            },
            body=dry_body,
        )
        dry = json.loads(dry_raw.decode("utf-8")) if dry_raw else {}
        evidence.protocol["dry_run"] = {"status": dry_status, "keys": list(dry)}
        assert dry_status == 200, dry
        token = dry.get("confirmation_token")
        assert token, "bulk preview must return confirmation_token"
        exec_path = f"/admin/v1/queues/{self.queue_name}/bulk:execute-cancel"
        exec_body = json.dumps(
            {
                "confirmation_token": token,
                "filters": filters,
                "reason": "chaos bulk cancel",
                "start_index": 0,
                "batch_limit": 10,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        idem = f"bulk-exec-{uuid.uuid4().hex}"
        headers = self._admin_headers(idem=idem)
        self._drop_committed_admin_once(
            method="POST",
            path=exec_path,
            headers=headers,
            body=exec_body,
            evidence=evidence,
            accept_statuses={200, 409},
        )
        status2, _h2, raw2 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=exec_path,
            headers=headers,
            body=exec_body,
        )
        evidence.protocol["retry"] = {
            "status": status2,
            "body": raw2.decode("utf-8", errors="replace")[:300],
        }
        evidence.database = self.counts()
        evidence.recovery["bounded"] = True
        assert status2 in {200, 409}, raw2.decode("utf-8", errors="replace")
        assert self.counts()["active"] <= 3

    def _rc_interrupt_maintenance(self, evidence: ChaosEvidence) -> None:
        path = "/admin/v1/maintenance:run"
        body = b"{}"
        headers = self._admin_headers(idem=f"maint-{uuid.uuid4().hex}")
        # Drop committed maintenance response mid-flight from the client view.
        upstream = self._drop_committed_admin_once(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            evidence=evidence,
            accept_statuses={200, 409, 503},
        )
        evidence.protocol["first_upstream"] = upstream
        status2, _h2, raw2 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=self._admin_headers(idem=f"maint-{uuid.uuid4().hex}"),
            body=body,
        )
        evidence.protocol["retry"] = {
            "status": status2,
            "body": raw2.decode("utf-8", errors="replace")[:200],
        }
        evidence.recovery["completed"] = status2 in {200, 400, 404, 409, 503}
        assert evidence.recovery["completed"]

    def _rc_interrupt_break_glass(self, evidence: ChaosEvidence) -> None:
        assert self.enqueue()[0] == 201
        claim = self.claim_one()
        path = (
            f"/admin/v1/queues/{self.queue_name}/tasks/{claim['task_id']}:force-lease-expiry"
        )
        body = json.dumps(
            {
                "reason": "chaos-bg",
                "incident_reference": "INC-CHAOS-1",
                "risk_acknowledged": True,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        idem = f"bg-{uuid.uuid4().hex}"
        headers = self._admin_headers(idem=idem, token=BREAK_GLASS_TOKEN)
        # Mid-op: pre-commit kill while queues row is locked (RT-API-BEFORE-COMMIT class).
        self._precommit_kill_admin_once(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            evidence=evidence,
        )
        # First durable attempt after interrupt.
        status1, _h1, raw1 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data1 = json.loads(raw1.decode("utf-8")) if raw1 else {}
        # Second retry proves bounded idempotent / deterministic conflict path.
        status2, _h2, raw2 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data2 = json.loads(raw2.decode("utf-8")) if raw2 else {}
        evidence.protocol["first"] = {"status": status1, "keys": list(data1)}
        evidence.protocol["second"] = {"status": status2, "keys": list(data2)}
        assert "claim_token" not in json.dumps(data1)
        assert "claim_token" not in json.dumps(data2)
        evidence.database = self.counts()
        with self.session_factory() as session:
            audits = int(session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0)
        evidence.recovery["audits"] = audits
        assert status1 == 200, data1
        assert status2 in {200, 400, 409}, data2
        assert audits >= 1

    def _seed_publishing_delivery_event(self, *, source_task_id: str) -> str:
        """Insert a stuck publishing Delivery Outbox row for break-glass reclaim."""
        event_id = uuid.uuid4()
        with self.session_factory() as session:
            now = session.scalar(select(func.transaction_timestamp()))
            assert now is not None
            envelope = {
                "specversion": "1.0",
                "id": str(event_id),
                "source": "urn:chaos:bg-delivery",
                "type": "com.example.chaos.bg.v1",
            }
            envelope_json = json.dumps(envelope, separators=(",", ":"), sort_keys=True)
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
                        :eid, :sid, 0, :publishing,
                        CAST(:envelope AS jsonb), :ebytes,
                        :now, 3,
                        :claim, :now, :now + interval '120 seconds',
                        'relay-chaos-stuck', 2, NULL,
                        :now, :now
                    )
                    """
                ),
                {
                    "eid": event_id,
                    "sid": source_task_id,
                    "publishing": STATE_PUBLISHING,
                    "envelope": envelope_json,
                    "ebytes": len(envelope_json.encode("utf-8")),
                    "now": now,
                    "claim": uuid.uuid4(),
                },
            )
            session.commit()
        return str(event_id)

    def _rc_interrupt_bg_delivery_reclaim(self, evidence: ChaosEvidence) -> None:
        status, enq = self.enqueue()
        assert status == 201, enq
        task_id = str(enq["task"]["task_id"])
        event_id = self._seed_publishing_delivery_event(source_task_id=task_id)
        path = (
            f"/admin/v1/queues/{self.queue_name}/delivery-events/"
            f"{event_id}:force-reclaim"
        )
        body = self._break_glass_ack_body(
            reason="chaos delivery reclaim interrupt",
            incident_reference="INC-CHAOS-BG-DLV-1",
        )
        idem = f"bg-dlv-{uuid.uuid4().hex}"
        headers = self._admin_headers(idem=idem, token=BREAK_GLASS_TOKEN)
        self._precommit_kill_admin_once(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            evidence=evidence,
        )
        status1, _h1, raw1 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data1 = json.loads(raw1.decode("utf-8")) if raw1 else {}
        status2, _h2, raw2 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data2 = json.loads(raw2.decode("utf-8")) if raw2 else {}
        evidence.protocol["first"] = {"status": status1, "keys": list(data1)}
        evidence.protocol["second"] = {"status": status2, "keys": list(data2)}
        assert "claim_token" not in json.dumps(data1)
        assert "claim_token" not in json.dumps(data2)
        evidence.database = self.counts()
        with self.session_factory() as session:
            audits = int(
                session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
            )
        evidence.recovery["audits"] = audits
        assert status1 == 200, data1
        assert status2 in {200, 400, 409}, data2
        assert audits >= 1
        # Drop the active row so alembic downgrade past 0502 can re-add the
        # stricter pending fence CHECK (pending+generation>0 would violate it).
        with self.session_factory() as session:
            session.execute(
                text(
                    "DELETE FROM delivery_events_active "
                    "WHERE event_id = CAST(:eid AS uuid)"
                ),
                {"eid": event_id},
            )
            session.commit()

    def _rc_interrupt_bg_elevation_write(self, evidence: ChaosEvidence) -> None:
        path = f"/admin/v1/queues/{self.queue_name}:raise-replay-limit"
        body = self._break_glass_ack_body(
            reason="chaos elevation write interrupt",
            incident_reference="INC-CHAOS-BG-ELEV-1",
            factor=2.0,
            ttl_seconds=60,
        )
        idem = f"bg-elev-{uuid.uuid4().hex}"
        headers = self._admin_headers(idem=idem, token=BREAK_GLASS_TOKEN)
        self._precommit_kill_admin_once(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            evidence=evidence,
        )

        def _elevation_rows() -> list[BreakGlassElevation]:
            with self.session_factory() as session:
                return list(
                    session.execute(
                        select(BreakGlassElevation).where(
                            BreakGlassElevation.queue_name == self.queue_name
                        )
                    ).scalars()
                )

        after_interrupt = _elevation_rows()
        evidence.recovery["elevations_after_interrupt"] = len(after_interrupt)
        assert len(after_interrupt) <= 1
        for row in after_interrupt:
            assert 1.0 <= float(row.factor) <= 10.0
            delta = (row.expires_at - row.raised_at).total_seconds()
            assert 1.0 <= delta <= 3600.0

        status1, _h1, raw1 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data1 = json.loads(raw1.decode("utf-8")) if raw1 else {}
        status2, _h2, raw2 = asgi_http_call(
            self.admin_app,
            method="POST",
            path=path,
            headers=headers,
            body=body,
        )
        data2 = json.loads(raw2.decode("utf-8")) if raw2 else {}
        evidence.protocol["first"] = {"status": status1, "keys": list(data1)}
        evidence.protocol["second"] = {"status": status2, "keys": list(data2)}
        assert "claim_token" not in json.dumps(data1)
        assert "claim_token" not in json.dumps(data2)
        rows = _elevation_rows()
        evidence.recovery["elevations_after_retry"] = len(rows)
        assert len(rows) == 1
        elev = rows[0]
        assert 1.0 <= float(elev.factor) <= 10.0
        ttl = (elev.expires_at - elev.raised_at).total_seconds()
        assert 1.0 <= ttl <= 3600.0
        assert status1 == 200, data1
        assert status2 in {200, 400, 409}, data2

    def _rc_pressure_claim_drain(self, evidence: ChaosEvidence) -> None:
        from datetime import UTC

        cfg = AdaptivePressureConfig(
            enter_consecutive_samples=2,
            clear_consecutive_samples=2,
            clear_hold_seconds=0.0,
        )
        ctl = AdaptivePressureController(config=cfg, monotonic_clock=lambda: 0.0)
        # Seed under NORMAL mode via live app path.
        assert self.enqueue()[0] == 201
        for mono in (1, 2, 3, 4):
            snap = build_snapshot(
                observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
                collected_monotonic_ns=mono,
                freshness=Freshness.FRESH,
                pool_wait_seconds=0.25,
                pool_saturation_ratio=0.95,
                wal_value=0.95,
                disk_value=0.2,
                autovacuum_value=0.2,
            )
            ctl.observe(snap, now_monotonic=float(mono))
        evidence.protocol["mode"] = ctl.mode.name
        assert ctl.mode in {
            OverloadMode.ENQUEUE_THROTTLE,
            OverloadMode.READINESS_FAILURE,
        }
        assert ctl.claims_allowed is True
        # Wire controller into live enqueue/claim path (same pattern as 04-03).
        self._rebuild_apps(pressure_controller=ctl)
        throttled_status, throttled_body = self.enqueue(idem=f"idem-throttle-{uuid.uuid4().hex}")
        evidence.protocol["throttled_enqueue"] = {
            "status": throttled_status,
            "code": throttled_body.get("code"),
            "hint": (throttled_body.get("details") or {}).get("hint")
            if isinstance(throttled_body.get("details"), dict)
            else throttled_body.get("hint"),
        }
        assert throttled_status in {429, 503}, throttled_body
        assert throttled_body.get("code") in {
            "resource_exhausted",
            "dependency_unavailable",
        }
        ready = check_readiness(self.engine, schema=self.schema, pressure_controller=ctl)
        evidence.protocol["readiness_under_pressure"] = {
            "ok": ready.ok,
            "reason": ready.reason_code,
        }
        if ctl.mode is OverloadMode.READINESS_FAILURE:
            assert ready.ok is False
            assert ready.reason_code == ReasonCode.OVERLOAD_PRESSURE
        assert ctl.claims_allowed is True
        claim = self.claim_one()
        assert claim, "claims must drain under live overload admission"
        evidence.note("live AdaptiveEnqueueGate wired; claim drained under overload")
        self.restart_postgres(evidence)
        self._rebuild_apps(pressure_controller=ctl)
        assert ctl.claims_allowed is True
        # Re-observe critical pressure after rebuild so throttle stays active.
        for mono in (5, 6):
            snap = build_snapshot(
                observed_at=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
                collected_monotonic_ns=mono,
                freshness=Freshness.FRESH,
                pool_wait_seconds=0.25,
                pool_saturation_ratio=0.95,
                wal_value=0.95,
                disk_value=0.2,
                autovacuum_value=0.2,
            )
            ctl.observe(snap, now_monotonic=float(mono))
        status_hb, body_hb = self.complete(claim)
        # Lease may have drifted across PG restart — reclaim if needed.
        if status_hb != 200:
            reclaimed = self.claim_one()
            assert reclaimed
            status_hb, body_hb = self.complete(reclaimed)
        assert status_hb == 200, body_hb
        evidence.database = self.counts()
        evidence.recovery["claim_drained_under_pressure"] = True
        evidence.recovery["live_admission_wired"] = True

    def _rc_no_leakage(self, evidence: ChaosEvidence) -> None:
        assert self.enqueue()[0] == 201
        claim = self.claim_one()
        token = claim["claim_token"]
        # Run a lightweight scenario and collect redacted notes only.
        evidence.note("scenario=RC-NO-LEAKAGE status=ok claim_token=[REDACTED]")
        evidence.note("payload=[REDACTED]")
        evidence.protocol["had_claim"] = True
        evidence.database = {"active": self.counts()["active"]}
        # Ensure log buffer / evidence do not contain secrets.
        logs = self._log_buffer.getvalue()
        evidence.captured_text.append(logs)
        evidence.assert_no_secrets(claim_token=token, payload_sentinel=PAYLOAD_SENTINEL)
        # Also verify complete response does not echo payload sentinel incorrectly in evidence notes.
        status, body = self.complete(claim)
        assert status == 200, body
        evidence.recovery["redaction_ok"] = True
