"""Kernel capacity workload loading, matrix expansion and load helpers (QUAL-03).

Stdlib-only YAML subset (mappings + lists + scalars). Never logs payloads,
idempotency keys or claim tokens (T-039-22).
"""

from __future__ import annotations

import hashlib
import math
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

FORBIDDEN_LOG_FIELDS: frozenset[str] = frozenset(
    {
        "payload",
        "idempotency_key",
        "claim_token",
        "claim_tokens",
        "bearer",
        "password",
        "token",
    }
)

QUEUE_COUNTS = (1, 10, 100)
CLAIMER_COUNTS = (1, 8, 32)
PAYLOAD_BYTES = (1024, 262_144, 1_048_576)
SCENARIOS = (
    "ready",
    "empty",
    "reclaim-heavy",
    "heartbeat-heavy",
    "duplicate-enqueue-storm",
    "complete-replay-storm",
)
HOT_OPERATIONS = ("enqueue", "claim", "heartbeat", "complete")


class WorkloadLoadError(ValueError):
    """Invalid workload document."""


@dataclass(frozen=True, slots=True)
class MatrixCell:
    queue_count: int
    claimer_count: int
    scenario: str
    payload_bytes: int

    @property
    def cell_id(self) -> str:
        return (
            f"q{self.queue_count}-c{self.claimer_count}-"
            f"{self.scenario}-p{self.payload_bytes}"
        )


@dataclass(frozen=True, slots=True)
class SmokeModeParams:
    warmup_seconds: int
    sample_seconds: int
    preseed_tasks: int


@dataclass(frozen=True, slots=True)
class FullModeParams:
    warmup_seconds: int
    target_claims_per_second: int
    terminal_lifecycles: int
    min_measured_seconds: int
    max_claimers: int


@dataclass(frozen=True, slots=True)
class ScenarioFixture:
    name: str
    expire_once_ratio: float | None = None
    expire_once_count: int = 0
    expire_once_indices: tuple[int, ...] = ()
    heartbeat_interval_ms: int | None = None
    interval_source: str | None = None
    duplicate_ratio: float | None = None
    request_count: int = 0
    replay_times: int = 0
    accepted_terminal_ids: tuple[str, ...] = ()
    seed: int = 0

    def idempotency_keys(self) -> list[str]:
        if self.name != "duplicate-enqueue-storm":
            raise WorkloadLoadError("idempotency_keys only for duplicate-enqueue-storm")
        unique_n = int(round(self.request_count * (1.0 - float(self.duplicate_ratio or 0))))
        if unique_n <= 0:
            raise WorkloadLoadError("duplicate storm needs at least one unique key")
        unique = [
            deterministic_idempotency_key(seed=self.seed, index=i) for i in range(unique_n)
        ]
        # First unique_n requests are unique; remaining (90%) replay those keys
        # round-robin so the duplicate ratio is exact over request_count.
        keys: list[str] = []
        for i in range(self.request_count):
            keys.append(unique[i % unique_n])
        return keys

    def replay_sequence(self) -> list[str]:
        if self.name != "complete-replay-storm":
            raise WorkloadLoadError("replay_sequence only for complete-replay-storm")
        out: list[str] = []
        for terminal_id in self.accepted_terminal_ids:
            out.extend([terminal_id] * self.replay_times)
        return out


def load_workload(path: Path | str) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    data = loads_document(text)
    if not isinstance(data, dict):
        raise WorkloadLoadError("workload root must be a mapping")
    _validate_axes(data)
    return data


def loads_document(text: str) -> Any:
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        stripped = raw.lstrip(" ")
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw) - len(stripped)
        if "\t" in raw[:indent]:
            raise WorkloadLoadError("tabs are not allowed in workload YAML")
        lines.append((indent, stripped))
    if not lines:
        raise WorkloadLoadError("empty workload")
    value, next_idx = _parse_value(lines, 0, lines[0][0])
    if next_idx != len(lines):
        raise WorkloadLoadError("trailing content after root document")
    return value


def _parse_value(
    lines: list[tuple[int, str]], idx: int, indent: int
) -> tuple[Any, int]:
    if idx >= len(lines):
        raise WorkloadLoadError("unexpected end of document")
    line_indent, content = lines[idx]
    if line_indent != indent:
        raise WorkloadLoadError(f"unexpected indent at: {content!r}")
    if content.startswith("- "):
        return _parse_list(lines, idx, indent)
    if ":" in content:
        return _parse_map(lines, idx, indent)
    raise WorkloadLoadError(f"unsupported YAML node: {content!r}")


def _parse_map(
    lines: list[tuple[int, str]], idx: int, indent: int
) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {}
    while idx < len(lines):
        line_indent, content = lines[idx]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise WorkloadLoadError(f"unexpected indent at: {content!r}")
        if content.startswith("- "):
            break
        if ":" not in content:
            raise WorkloadLoadError(f"expected key: value at: {content!r}")
        key, _, rest = content.partition(":")
        key = key.strip()
        rest = rest.strip()
        idx += 1
        if rest == "":
            if idx < len(lines) and lines[idx][0] > indent:
                child_indent = lines[idx][0]
                child, idx = _parse_value(lines, idx, child_indent)
                result[key] = child
            else:
                result[key] = {}
        elif rest.startswith("[") and rest.endswith("]"):
            result[key] = _parse_flow_list(rest)
        else:
            result[key] = _parse_scalar(rest)
    return result, idx


def _parse_list(
    lines: list[tuple[int, str]], idx: int, indent: int
) -> tuple[list[Any], int]:
    items: list[Any] = []
    while idx < len(lines):
        line_indent, content = lines[idx]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise WorkloadLoadError(f"unexpected indent at: {content!r}")
        if not content.startswith("- "):
            break
        item_text = content[2:].strip()
        idx += 1
        if item_text == "":
            if idx < len(lines) and lines[idx][0] > indent:
                child, idx = _parse_value(lines, idx, lines[idx][0])
                items.append(child)
            else:
                items.append(None)
        elif item_text.startswith("[") and item_text.endswith("]"):
            items.append(_parse_flow_list(item_text))
        else:
            items.append(_parse_scalar(item_text))
    return items, idx


def _parse_flow_list(raw: str) -> list[Any]:
    inner = raw[1:-1].strip()
    if not inner:
        return []
    parts: list[str] = []
    buf: list[str] = []
    in_quote = False
    quote_char = ""
    for ch in inner:
        if ch in ('"', "'") and not in_quote:
            in_quote = True
            quote_char = ch
            buf.append(ch)
            continue
        if in_quote and ch == quote_char:
            in_quote = False
            buf.append(ch)
            continue
        if ch == "," and not in_quote:
            parts.append("".join(buf).strip())
            buf = []
            continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return [_parse_scalar(part) for part in parts if part != ""]


def _parse_scalar(raw: str) -> Any:
    if raw in ("null", "~", "Null", "NULL"):
        return None
    if raw in ("true", "True", "TRUE"):
        return True
    if raw in ("false", "False", "FALSE"):
        return False
    if (raw.startswith('"') and raw.endswith('"')) or (
        raw.startswith("'") and raw.endswith("'")
    ):
        return raw[1:-1]
    try:
        if any(c in raw for c in ".eE") and not raw.isdigit():
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def _validate_axes(data: Mapping[str, Any]) -> None:
    if tuple(data.get("queue_counts") or ()) != QUEUE_COUNTS:
        raise WorkloadLoadError(f"queue_counts must be {QUEUE_COUNTS}")
    if tuple(data.get("claimer_counts") or ()) != CLAIMER_COUNTS:
        raise WorkloadLoadError(f"claimer_counts must be {CLAIMER_COUNTS}")
    if tuple(data.get("payload_bytes") or ()) != PAYLOAD_BYTES:
        raise WorkloadLoadError(f"payload_bytes must be {PAYLOAD_BYTES}")
    if tuple(data.get("scenarios") or ()) != SCENARIOS:
        raise WorkloadLoadError(f"scenarios must be {SCENARIOS}")


def expand_matrix(workload: Mapping[str, Any]) -> list[MatrixCell]:
    _validate_axes(workload)
    cells: list[MatrixCell] = []
    for queue_count in workload["queue_counts"]:
        for claimer_count in workload["claimer_counts"]:
            for scenario in workload["scenarios"]:
                for payload_bytes in workload["payload_bytes"]:
                    cells.append(
                        MatrixCell(
                            queue_count=int(queue_count),
                            claimer_count=int(claimer_count),
                            scenario=str(scenario),
                            payload_bytes=int(payload_bytes),
                        )
                    )
    return cells


def mode_parameters(
    workload: Mapping[str, Any], mode: str
) -> SmokeModeParams | FullModeParams:
    modes = workload.get("modes") or {}
    if mode == "smoke":
        cfg = modes.get("smoke") or {}
        return SmokeModeParams(
            warmup_seconds=int(cfg["warmup_seconds"]),
            sample_seconds=int(cfg["sample_seconds"]),
            preseed_tasks=int(cfg["preseed_tasks"]),
        )
    if mode == "full":
        cfg = modes.get("full") or {}
        return FullModeParams(
            warmup_seconds=int(cfg["warmup_seconds"]),
            target_claims_per_second=int(cfg["target_claims_per_second"]),
            terminal_lifecycles=int(cfg["terminal_lifecycles"]),
            min_measured_seconds=int(cfg["min_measured_seconds"]),
            max_claimers=int(cfg["max_claimers"]),
        )
    raise WorkloadLoadError(f"unknown mode: {mode!r}")


def scenario_fixture(
    name: str,
    *,
    seed: int = 0,
    lease_count: int = 0,
    server_recommended_min_safe_interval_ms: int | None = None,
    accepted_terminal_ids: Sequence[str] = (),
) -> ScenarioFixture:
    if name == "reclaim-heavy":
        if lease_count <= 0:
            raise WorkloadLoadError("reclaim-heavy requires lease_count > 0")
        ratio = 0.20
        count = int(round(lease_count * ratio))
        # Deterministic selection without replacement.
        rng_indices = list(range(lease_count))
        # Fisher–Yates with seeded hash stream.
        for i in range(lease_count - 1, 0, -1):
            digest = hashlib.sha256(f"{seed}:reclaim:{i}".encode()).digest()
            j = int.from_bytes(digest[:8], "big") % (i + 1)
            rng_indices[i], rng_indices[j] = rng_indices[j], rng_indices[i]
        chosen = tuple(sorted(rng_indices[:count]))
        return ScenarioFixture(
            name=name,
            expire_once_ratio=ratio,
            expire_once_count=count,
            expire_once_indices=chosen,
            seed=seed,
        )
    if name == "heartbeat-heavy":
        if server_recommended_min_safe_interval_ms is None:
            raise WorkloadLoadError(
                "heartbeat-heavy requires server_recommended_min_safe_interval_ms"
            )
        return ScenarioFixture(
            name=name,
            heartbeat_interval_ms=int(server_recommended_min_safe_interval_ms),
            interval_source="server_recommended_min_safe",
            seed=seed,
        )
    if name == "duplicate-enqueue-storm":
        return ScenarioFixture(
            name=name,
            duplicate_ratio=0.90,
            request_count=100_000,
            seed=seed,
        )
    if name == "complete-replay-storm":
        return ScenarioFixture(
            name=name,
            replay_times=10,
            accepted_terminal_ids=tuple(accepted_terminal_ids),
            seed=seed,
        )
    if name in {"ready", "empty"}:
        return ScenarioFixture(name=name, seed=seed)
    raise WorkloadLoadError(f"unknown scenario: {name!r}")


def deterministic_payload(*, seed: int, index: int, size_bytes: int) -> bytes:
    if size_bytes <= 0:
        raise WorkloadLoadError("size_bytes must be positive")
    out = bytearray(size_bytes)
    offset = 0
    counter = 0
    while offset < size_bytes:
        block = hashlib.sha256(f"{seed}:{index}:{counter}".encode()).digest()
        take = min(len(block), size_bytes - offset)
        out[offset : offset + take] = block[:take]
        offset += take
        counter += 1
    return bytes(out)


def deterministic_idempotency_key(*, seed: int, index: int) -> str:
    digest = hashlib.sha256(f"idem:{seed}:{index}".encode()).hexdigest()
    return f"idem-{digest[:32]}"


def redact_log_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Drop forbidden credential/payload fields from loggable metadata."""
    return {
        key: value
        for key, value in fields.items()
        if key not in FORBIDDEN_LOG_FIELDS and not str(key).lower().endswith("_token")
    }


def clamp_claimers(claimer_count: int, *, max_claimers: int = 32) -> int:
    return min(int(claimer_count), int(max_claimers))


def assign_queues_round_robin(*, queue_count: int, claimer_count: int) -> list[int]:
    if queue_count <= 0 or claimer_count <= 0:
        raise WorkloadLoadError("queue_count and claimer_count must be positive")
    claimers = clamp_claimers(claimer_count)
    return [i % queue_count for i in range(claimers)]


class MonotonicRateController:
    """Token-bucket style rate limiter that never exceeds the target rate.

    Uses a monotone acquired counter and wall-clock budget so the instantaneous
    permitted rate cannot run ahead of ``target_per_second * elapsed``.
    """

    def __init__(
        self,
        *,
        target_per_second: float,
        now: Callable[[], float] | None = None,
    ) -> None:
        if target_per_second <= 0:
            raise WorkloadLoadError("target_per_second must be positive")
        self._target = float(target_per_second)
        self._now = now or (lambda: __import__("time").monotonic())
        self._started = self._now()
        self.acquired = 0

    def try_acquire(self) -> bool:
        elapsed = max(0.0, self._now() - self._started)
        budget = math.floor(self._target * elapsed + 1e-9)
        if self.acquired < budget:
            self.acquired += 1
            return True
        # At t=0 allow the first permit so warm starts are not deadlocked.
        if self.acquired == 0 and elapsed == 0.0:
            self.acquired += 1
            return True
        return False


def workload_hash(workload: Mapping[str, Any]) -> str:
    import json

    canonical = json.dumps(workload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def new_run_id() -> str:
    return f"run-{secrets.token_hex(8)}"


_RELEASE_MIX_KEYS = (
    "claim",
    "enqueue",
    "heartbeat",
    "complete_fanout_0",
    "complete_fanout_8",
    "fail_cancel_inspect",
)


def load_release_gate(path: Path | str) -> dict[str, Any]:
    """Load and validate the Phase 3.9 release-gate workload document."""
    text = Path(path).read_text(encoding="utf-8")
    data = loads_document(text)
    if not isinstance(data, dict):
        raise WorkloadLoadError("release-gate root must be a mapping")

    required = (
        "profile",
        "named_queues",
        "claimers",
        "payload_bytes_baseline",
        "warmup_seconds",
        "target_claims_per_second",
        "terminal_lifecycles",
        "min_measured_seconds",
        "operation_mix",
        "fanout_64_sample",
    )
    missing = [key for key in required if key not in data]
    if missing:
        raise WorkloadLoadError(f"release-gate missing keys: {missing}")

    if data["profile"] != "phase-3.9-linux-x86_64-v1":
        raise WorkloadLoadError(
            "release-gate.profile must be phase-3.9-linux-x86_64-v1"
        )
    if int(data["named_queues"]) != 100:
        raise WorkloadLoadError("release-gate.named_queues must be 100")
    if int(data["claimers"]) != 32:
        raise WorkloadLoadError("release-gate.claimers must be 32")
    if int(data["payload_bytes_baseline"]) != 1024:
        raise WorkloadLoadError("release-gate.payload_bytes_baseline must be 1024")
    if int(data["warmup_seconds"]) != 120:
        raise WorkloadLoadError("release-gate.warmup_seconds must be 120")
    if int(data["target_claims_per_second"]) != 500:
        raise WorkloadLoadError("release-gate.target_claims_per_second must be 500")
    if int(data["terminal_lifecycles"]) != 1_000_000:
        raise WorkloadLoadError("release-gate.terminal_lifecycles must be 1000000")
    if int(data["min_measured_seconds"]) != 300:
        raise WorkloadLoadError("release-gate.min_measured_seconds must be 300")

    mix = data["operation_mix"]
    if not isinstance(mix, dict):
        raise WorkloadLoadError("release-gate.operation_mix must be a mapping")
    if tuple(mix.keys()) != _RELEASE_MIX_KEYS:
        raise WorkloadLoadError(
            f"release-gate.operation_mix keys must be {_RELEASE_MIX_KEYS}"
        )
    expected = (0.35, 0.20, 0.20, 0.10, 0.10, 0.05)
    for key, want in zip(_RELEASE_MIX_KEYS, expected, strict=True):
        if abs(float(mix[key]) - want) > 1e-9:
            raise WorkloadLoadError(
                f"release-gate.operation_mix[{key!r}] must be {want}"
            )
    if abs(sum(float(mix[k]) for k in _RELEASE_MIX_KEYS) - 1.0) > 1e-9:
        raise WorkloadLoadError("release-gate.operation_mix must sum to 1.0")

    sample = data["fanout_64_sample"]
    if not isinstance(sample, dict):
        raise WorkloadLoadError("release-gate.fanout_64_sample must be a mapping")
    if int(sample.get("completes", -1)) != 10_000:
        raise WorkloadLoadError("release-gate.fanout_64_sample.completes must be 10000")
    if int(sample.get("fan_out", -1)) != 64:
        raise WorkloadLoadError("release-gate.fanout_64_sample.fan_out must be 64")

    return data
