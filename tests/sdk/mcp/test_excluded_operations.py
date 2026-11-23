"""Worker and break-glass allowlist aliases exit 2 before stdio.

Aliases come from the ownership file: operationIds, the sync method, and the
consumer, worker, and break-glass prefixes. ``getCapabilities`` stays a tool
under the included clients; only the consumer and worker aliases of that
method are rejected here.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_OWNERSHIP_PATH = _REPO_ROOT / "packages" / "client-operation-ownership.json"
_PROCESS_ENV_KEYS = frozenset(
    {
        "systemroot",
        "systemdrive",
        "windir",
        "path",
        "pathext",
        "comspec",
        "temp",
        "tmp",
        "userprofile",
        "homedrive",
        "homepath",
        "localappdata",
        "appdata",
        "programfiles",
        "programdata",
        "programfiles(x86)",
    }
)
_WORKER_PREFIXES = ("consumer_", "worker_")
_BREAK_GLASS_PREFIXES = ("break_glass_", "breakglass_")
_REQUIRED = frozenset(
    {
        "acknowledgeClaimCancellation",
        "ack_cancel",
        "claimTasks",
        "claim",
        "consumer_claim",
        "worker_claim",
        "completeClaim",
        "complete",
        "failClaim",
        "fail",
        "heartbeatClaim",
        "heartbeat",
        "dropExpiredPartition",
        "drop_expired_partition",
        "forceDeliveryDeadLetter",
        "force_delivery_dead_letter",
        "forceDeliveryReclaim",
        "force_delivery_reclaim",
        "forceLeaseExpiry",
        "force_lease_expiry",
        "break_glass_force_lease_expiry",
        "raiseReplayLimit",
        "raise_replay_limit",
        "reconcileCounters",
        "reconcile_counters",
        "repairRegistryEntry",
        "repair_registry_entry",
        "breakglass_repair_registry_entry",
        "consumer_get_capabilities",
    }
)


def _final_method(sync: str) -> str:
    tail = sync.split("->")[-1].strip()
    return tail.rsplit(".", 1)[-1]


def _excluded_aliases() -> tuple[str, ...]:
    document = json.loads(_OWNERSHIP_PATH.read_text(encoding="utf-8"))
    aliases: set[str] = set()
    for entry in document["operations"]:
        clients = set(entry["clients"])
        exclusive_worker = clients == {"ConsumerClient"}
        break_glass = "BreakGlassClient" in clients
        if exclusive_worker or break_glass:
            aliases.add(entry["operationId"])
        for surface in entry["sync_async_surfaces"]:
            sync = surface["sync"]
            method = _final_method(sync)
            if exclusive_worker:
                aliases.add(method)
                for prefix in _WORKER_PREFIXES:
                    aliases.add(f"{prefix}{method}")
            elif sync.startswith("ConsumerClient."):
                for prefix in _WORKER_PREFIXES:
                    aliases.add(f"{prefix}{method}")
            if break_glass:
                aliases.add(method)
                for prefix in _BREAK_GLASS_PREFIXES:
                    aliases.add(f"{prefix}{method}")
    missing = _REQUIRED - aliases
    assert not missing, f"ownership aliases missing {sorted(missing)}"
    assert "getCapabilities" not in aliases
    assert "observer_get_capabilities" not in aliases
    return tuple(sorted(aliases))


_ALIASES = _excluded_aliases()


def _process_env() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key.lower() in _PROCESS_ENV_KEYS
    }


def _run(alias: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-m", "workhold_mcp", "--allow-tool", alias],
        cwd=_REPO_ROOT,
        env=_process_env(),
        capture_output=True,
        check=False,
        timeout=20,
    )


@pytest.mark.parametrize("alias", _ALIASES)
def test_worker_and_break_glass_aliases_exit_2(alias: str) -> None:
    completed = _run(alias)
    assert completed.returncode == 2
    assert completed.stdout == b""
    assert completed.stderr.splitlines() == [
        b"workhold-mcp: invalid configuration: --allow-tool"
    ]
    assert alias.encode("utf-8") not in completed.stdout + completed.stderr
    assert b"Traceback" not in completed.stderr
    assert b"WORKHOLD_MCP_OBSERVER_TOKEN" not in completed.stderr
