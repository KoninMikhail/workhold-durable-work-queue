"""Golden vectors and boundary tests for bridge:v1: idempotency mapping (BRDG-02)."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from queue_service_producer.bridge.idempotency import bridge_idempotency_key

# Frozen protocol vectors — accidental algorithm changes MUST fail these pins.
GOLDEN_VECTORS: list[tuple[str, str, str]] = [
    (
        "ab",
        "c",
        "bridge:v1:8pofkdaw5bspsesmhnvtdiimodokgas5mv8tuplgpkc",
    ),
    (
        "a",
        "bc",
        "bridge:v1:tttofqycizacpzmjlc6odjx62wbbgrvetxjk2s0hel0",
    ),
    (
        "orders.checkout",
        "outbox-row-42",
        "bridge:v1:lljzhy4jy0katn6jwvbcy3x9_saiwljh2wwp6c-lnl0",
    ),
    (
        " ns ",
        " row ",
        "bridge:v1:wcbupddk40f6bqnansnmqh8gyidiwtgwnjk9boxzgvc",
    ),
    (
        "café-名前",
        "row-Ω",
        "bridge:v1:h0dnzkebo6oc8kxmyxjctseoefrotrg6v-ms21epnby",
    ),
    (
        "a" * 256,
        "b" * 256,
        "bridge:v1:fqj3s95mp7kncjm1nibnngy7yrqg29o2fsz5zhawszi",
    ),
]

_KEY_RE = re.compile(r"^bridge:v1:[a-z0-9_-]+$")
_REPO_ROOT = Path(__file__).resolve().parents[4]
_FRESH_PROCESS_SCRIPT = """\
from queue_service_producer.bridge.idempotency import bridge_idempotency_key
vectors = {vectors!r}
for ns, row, expected in vectors:
    got = bridge_idempotency_key(ns, row)
    assert got == expected, (ns, row, got, expected)
"""


def test_golden_vectors() -> None:
    for namespace, row_id, expected in GOLDEN_VECTORS:
        got = bridge_idempotency_key(namespace, row_id)
        assert got == expected
        assert _KEY_RE.fullmatch(got)
        assert 1 <= len(got) <= 256
        assert len(got) == 53  # "bridge:v1:" (10) + 43-char SHA-256 base64url


def test_length_prefix_distinguishes_ambiguous_concatenation() -> None:
    left = bridge_idempotency_key("ab", "c")
    right = bridge_idempotency_key("a", "bc")
    assert left != right
    assert left == GOLDEN_VECTORS[0][2]
    assert right == GOLDEN_VECTORS[1][2]


def test_replay_same_identity_is_stable() -> None:
    ns, row, expected = GOLDEN_VECTORS[2]
    assert bridge_idempotency_key(ns, row) == expected
    assert bridge_idempotency_key(ns, row) == bridge_idempotency_key(ns, row)


def test_whitespace_is_significant_not_trimmed() -> None:
    padded = bridge_idempotency_key(" ns ", " row ")
    trimmed = bridge_idempotency_key("ns", "row")
    assert padded == GOLDEN_VECTORS[3][2]
    assert padded != trimmed


@pytest.mark.parametrize(
    ("namespace", "row_id"),
    [
        ("", "row"),
        ("ns", ""),
        ("ab\x00c", "row"),
        ("ns", "row\nwith\nnewline"),
        ("ns", "row\x7f"),
        ("a" * 257, "row"),
        ("ns", "b" * 257),
    ],
)
def test_contract_limit_and_control_char_violations_rejected(
    namespace: str, row_id: str
) -> None:
    with pytest.raises(ValueError):
        bridge_idempotency_key(namespace, row_id)


@pytest.mark.parametrize(
    ("namespace", "row_id"),
    [
        (123, "row"),
        ("ns", None),
        (None, "row"),
        (b"ns", "row"),
        ("ns", b"row"),
    ],
)
def test_non_string_identities_rejected(namespace: object, row_id: object) -> None:
    with pytest.raises(TypeError):
        bridge_idempotency_key(namespace, row_id)  # type: ignore[arg-type]


def test_surrogate_code_points_rejected() -> None:
    # Lone surrogates are valid Python str code points but not UTF-8.
    with pytest.raises(ValueError):
        bridge_idempotency_key("ns", "\ud800")
    with pytest.raises(ValueError):
        bridge_idempotency_key("\udfff", "row")


def test_golden_vectors_across_1000_fresh_processes() -> None:
    """Mapping output must be identical across 1,000 fresh interpreter processes."""
    script = _FRESH_PROCESS_SCRIPT.format(vectors=GOLDEN_VECTORS)
    env = os.environ.copy()
    # Prefer the workspace editable producer package when PYTHONPATH is needed.
    producer_src = str(_REPO_ROOT / "packages" / "queue-service-producer" / "src")
    core_src = str(_REPO_ROOT / "packages" / "queue-service-client-core" / "src")
    prefix = producer_src + os.pathsep + core_src
    env["PYTHONPATH"] = (
        prefix + os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else prefix
    )
    for i in range(1000):
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=_REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, (
            f"fresh process {i + 1}/1000 failed:\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
