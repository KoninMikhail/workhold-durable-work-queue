"""Deterministic bridge process crash/restart controller (06-05 / BRDG-02).

Runs the supported ``BridgeRunner`` in a child process with failpoint barriers
around enqueue and app acknowledgement. Parent waits on ready markers (no
barrier sleeps), kills/restarts children, and asserts via public Queue APIs.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tests.conformance.faults import DropCommittedResponseProxy

if TYPE_CHECKING:
    from tests.conformance.bridge.fixtures import BridgeWorld

REPO_ROOT = Path(__file__).resolve().parents[3]

# Cross-controller registry for cleanup assertions.
_LIVE_PROCS: set[int] = set()
_LIVE_LOCK = threading.Lock()


class CrashWindow(str, Enum):
    BEFORE_ENQUEUE = "before_enqueue"
    AFTER_ENQUEUE_COMMIT_BEFORE_RESPONSE = "after_enqueue_commit_before_response"
    AFTER_RESPONSE_BEFORE_ACK = "after_response_before_app_ack"
    AFTER_APP_ACK = "after_app_ack"


def _register(pid: int) -> None:
    with _LIVE_LOCK:
        _LIVE_PROCS.add(pid)


def _unregister(pid: int) -> None:
    with _LIVE_LOCK:
        _LIVE_PROCS.discard(pid)


def _hang_until_killed() -> None:
    """Deterministic barrier: block without polling sleeps until SIGKILL."""
    event = threading.Event()
    event.wait()


def _write_ready(path: str | None, payload: str = "1") -> None:
    if not path:
        return
    Path(path).write_text(payload, encoding="utf-8")


def _worker_main(config: dict[str, Any]) -> int:
    """Child entry: claim/relay with optional failpoints; exit 0 when idle."""
    import psycopg

    from workhold_producer.bridge.postgres_store import (
        PostgresOutboxMapping,
        PostgresOutboxStore,
    )
    from workhold_producer.bridge.runner import BridgeRunner
    from workhold_producer.client import ProducerClient
    from _workhold_client_core.transport import HttpJsonTransport

    failpoint = config.get("failpoint")
    ready_path = config.get("ready_path")
    base_url = str(config["base_url"])
    token = str(config["token"])
    conninfo = str(config["app_conninfo"])
    schema = str(config["app_schema"])
    table = str(config["app_table"])
    lease_seconds = int(config.get("lease_seconds", 30))
    max_cycles = int(config.get("max_cycles", 20))
    idle_poll = float(config.get("idle_poll_seconds", 0.02))
    initial_backoff = float(config.get("initial_backoff_seconds", 0.0))

    def connection_factory() -> Any:
        return psycopg.connect(conninfo)

    store = PostgresOutboxStore(
        connection_factory=connection_factory,
        mapping=PostgresOutboxMapping(schema=schema, table=table),
    )
    real_producer = ProducerClient(
        HttpJsonTransport(base_url, timeout_s=10.0),
        bearer_token=token,
    )

    class FailpointProducer:
        def get_capabilities(self) -> Any:
            return real_producer.get_capabilities()

        def _enqueue_with_available_at_raw(self, *args: Any, **kwargs: Any) -> Any:
            if failpoint == CrashWindow.BEFORE_ENQUEUE.value:
                _write_ready(ready_path, "before_enqueue")
                _hang_until_killed()
            return real_producer._enqueue_with_available_at_raw(*args, **kwargs)

    class FailpointStore:
        def __getattr__(self, name: str) -> Any:
            return getattr(store, name)

        def claim(self, **kwargs: Any) -> Any:
            return store.claim(**kwargs)

        def mark_delivered(self, **kwargs: Any) -> bool:
            if failpoint == CrashWindow.AFTER_RESPONSE_BEFORE_ACK.value:
                _write_ready(ready_path, "after_response_before_ack")
                _hang_until_killed()
            ok = store.mark_delivered(**kwargs)
            if failpoint == CrashWindow.AFTER_APP_ACK.value and ok:
                _write_ready(ready_path, "after_app_ack")
                _hang_until_killed()
            return ok

        def schedule_retry(self, **kwargs: Any) -> bool:
            return store.schedule_retry(**kwargs)

        def mark_terminal_operator_action(self, **kwargs: Any) -> bool:
            return store.mark_terminal_operator_action(**kwargs)

    runner = BridgeRunner(
        store=FailpointStore(),
        producer=FailpointProducer(),
        batch_size=1,
        lease_seconds=lease_seconds,
        max_in_flight=1,
        idle_poll_seconds=idle_poll,
        initial_backoff_seconds=initial_backoff,
        max_backoff_seconds=max(initial_backoff, 0.05),
        backoff_jitter_ratio=0.0,
    )

    for _ in range(max_cycles):
        processed = runner.poll_once()
        if processed == 0:
            # Idle — exit cleanly so parent can observe completion.
            return 0
    return 0


def _spawn_worker(config: dict[str, Any]) -> subprocess.Popen[str]:
    """Launch worker via ``python -c`` so failpoints stay out of production."""
    # Encode config as JSON env to avoid shell escaping issues on Windows.
    env = os.environ.copy()
    env["BRIDGE_CRASH_WORKER_CONFIG"] = json.dumps(config)
    # Ensure repo packages resolve (producer bridge + private core + service).
    env.setdefault("PYTHONPATH", str(REPO_ROOT / "src"))
    existing = env.get("PYTHONPATH", "")
    producer_src = str(REPO_ROOT / "packages" / "workhold-producer" / "src")
    core_src = str(REPO_ROOT / "packages" / "workhold-client-core" / "src")
    parts = [str(REPO_ROOT), str(REPO_ROOT / "src"), producer_src, core_src]
    if existing:
        parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(parts)

    code = (
        "import json,os;"
        "from tests.conformance.bridge.harness import _worker_main;"
        "raise SystemExit(_worker_main(json.loads(os.environ['BRIDGE_CRASH_WORKER_CONFIG'])))"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.pid is not None:
        _register(proc.pid)
    return proc


class BridgeProcessController:
    """Start/kill/restart bridge worker processes with crash-window failpoints."""

    def __init__(self, world: BridgeWorld) -> None:
        self._world = world
        self._procs: list[subprocess.Popen[str]] = []
        self._tmpdir = tempfile.TemporaryDirectory(prefix="bridge-crash-")
        self._tmp = Path(self._tmpdir.name)

    def temp_ready_path(self, name: str) -> Path:
        path = self._tmp / f"{name}.ready"
        if path.exists():
            path.unlink()
        return path

    def start(
        self,
        *,
        failpoint: CrashWindow | None,
        ready_path: Path | None = None,
        max_cycles: int = 20,
        lease_seconds: int = 30,
        base_url: str | None = None,
    ) -> subprocess.Popen[str]:
        config: dict[str, Any] = {
            "base_url": base_url or self._world.base_url,
            "token": self._world.producer_token,
            "app_conninfo": self._world.app_conninfo,
            "app_schema": self._world.app_schema,
            "app_table": self._world.app_table,
            "failpoint": None if failpoint is None else failpoint.value,
            "ready_path": None if ready_path is None else str(ready_path),
            "max_cycles": max_cycles,
            "lease_seconds": lease_seconds,
            "idle_poll_seconds": 0.02,
            "initial_backoff_seconds": 0.0,
        }
        proc = _spawn_worker(config)
        self._procs.append(proc)
        return proc

    def await_ready(self, path: Path, *, deadline_s: float = 20.0) -> None:
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            if path.is_file() and path.stat().st_size > 0:
                return
            # Bounded readiness poll only — crash barrier itself does not sleep.
            time.sleep(0.01)
            for proc in self._procs:
                if proc.poll() is not None and not path.is_file():
                    stderr = ""
                    try:
                        stderr = (proc.stderr.read() if proc.stderr else "") or ""
                    except Exception:  # noqa: BLE001
                        pass
                    raise RuntimeError(
                        f"bridge exited before ready marker "
                        f"(code={proc.returncode}): {stderr[:256]!r}"
                    )
        raise TimeoutError(f"ready marker missing before deadline: {path}")

    def kill(self, proc: subprocess.Popen[str]) -> None:
        if proc.poll() is not None:
            if proc.pid is not None:
                _unregister(proc.pid)
            return
        try:
            if sys.platform == "win32":
                proc.kill()
            else:
                os.kill(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        if proc.pid is not None:
            _unregister(proc.pid)

    def stop_all(self) -> None:
        for proc in list(self._procs):
            self.kill(proc)
        self._procs.clear()

    def live_process_count(self) -> int:
        return sum(1 for p in self._procs if p.poll() is None)

    @staticmethod
    def global_live_process_count() -> int:
        with _LIVE_LOCK:
            # Drop dead pids opportunistically.
            dead = []
            for pid in _LIVE_PROCS:
                try:
                    os.kill(pid, 0)
                except OSError:
                    dead.append(pid)
            for pid in dead:
                _LIVE_PROCS.discard(pid)
            return len(_LIVE_PROCS)

    def await_idle_or_exit(self, *, deadline_s: float = 15.0) -> None:
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            if all(p.poll() is not None for p in self._procs):
                for p in self._procs:
                    if p.pid is not None:
                        _unregister(p.pid)
                return
            time.sleep(0.02)
        raise TimeoutError("bridge processes did not idle/exit before deadline")

    def await_app_delivered(
        self,
        world: BridgeWorld,
        *,
        namespace: str,
        row_id: str,
        deadline_s: float = 20.0,
    ) -> str:
        from tests.conformance.bridge.fixtures import read_intent_row

        deadline = time.monotonic() + deadline_s
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = read_intent_row(world, namespace=namespace, row_id=row_id)
            if last["state"] == "delivered" and last["queue_task_id"]:
                self.await_idle_or_exit(
                    deadline_s=max(0.5, deadline - time.monotonic())
                )
                return str(last["queue_task_id"])
            if last["state"] == "terminal_operator_action":
                raise AssertionError(
                    f"intent terminal before delivered: {last.get('last_failure_code')}"
                )
            time.sleep(0.02)
        raise TimeoutError(f"intent not delivered before deadline last={last!r}")

    def await_terminal_or_delivered(
        self,
        world: BridgeWorld,
        *,
        namespace: str,
        row_id: str,
        deadline_s: float = 20.0,
    ) -> str:
        from tests.conformance.bridge.fixtures import read_intent_row

        deadline = time.monotonic() + deadline_s
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = read_intent_row(world, namespace=namespace, row_id=row_id)
            if last["state"] in {"delivered", "terminal_operator_action"}:
                self.await_idle_or_exit(
                    deadline_s=max(0.5, deadline - time.monotonic())
                )
                return str(last["state"])
            time.sleep(0.02)
        raise TimeoutError(f"intent not terminal/delivered last={last!r}")

    def run_crash_after_commit_before_response(
        self,
        *,
        namespace: str,
        row_id: str,
    ) -> str:
        """Prove Queue commit via drop-proxy, kill bridge, return buffered task_id.

        Uses a deterministic proxy barrier (buffered upstream success) — not a
        sleep — then process-kills the bridge before response bytes are delivered.
        """
        _ = (namespace, row_id)  # seeded by caller; identity used via bridge claim
        world = self._world
        proxy = DropCommittedResponseProxy()
        deadline = time.monotonic() + 25.0
        listen_url = proxy.serve_once(world.base_url, deadline)
        proc = self.start(
            failpoint=None,
            max_cycles=5,
            base_url=listen_url,
            lease_seconds=30,
        )
        try:
            buffered = proxy.wait(deadline)
            body = buffered.json()
            task_id = body["task"]["task_id"]
            if not isinstance(task_id, str) or not task_id:
                raise AssertionError("buffered enqueue missing task_id")
            # Process kill after durable commit, before producer observes response.
            self.kill(proc)
            return task_id
        finally:
            proxy.close()
            self.kill(proc)

    def __del__(self) -> None:  # noqa: D401
        try:
            self.stop_all()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._tmpdir.cleanup()
        except Exception:  # noqa: BLE001
            pass
