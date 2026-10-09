"""Core wheel metadata: no runtime deps, no server modules."""

from __future__ import annotations

import email.message
import email.parser
import os
import subprocess
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DIST_DIR = REPO_ROOT / "dist"


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        check=True,
        text=True,
        capture_output=True,
    )


def _latest_wheel(dist_name: str) -> Path:
    wheels = sorted(DIST_DIR.glob(f"{dist_name.replace('-', '_')}-*.whl"))
    if not wheels:
        wheels = sorted(DIST_DIR.glob(f"{dist_name}-*.whl"))
    assert wheels, f"no wheel for {dist_name}"
    return wheels[-1]


def _metadata(wheel: Path) -> email.message.Message:
    with zipfile.ZipFile(wheel) as zf:
        meta_names = [n for n in zf.namelist() if n.endswith(".dist-info/METADATA")]
        assert len(meta_names) == 1
        return email.parser.Parser().parsestr(zf.read(meta_names[0]).decode("utf-8"))


def _top_level(wheel: Path) -> set[str]:
    with zipfile.ZipFile(wheel) as zf:
        tops: set[str] = set()
        for name in zf.namelist():
            if name.endswith("/") or ".dist-info/" in name or ".data/" in name:
                continue
            parts = Path(name).parts
            if parts:
                tops.add(parts[0])
        return tops


@pytest.fixture(scope="module")
def core_wheel() -> Path:
    DIST_DIR.mkdir(exist_ok=True)
    for stale in DIST_DIR.glob("workhold_client_core-*.whl"):
        stale.unlink()
    for stale in DIST_DIR.glob("workhold-client-core-*.whl"):
        stale.unlink()
    _run(["uv", "build", "--package", "workhold-client-core", "--out-dir", str(DIST_DIR)])
    return _latest_wheel("workhold_client_core")


def test_core_wheel_name_and_no_runtime_dependencies(core_wheel: Path) -> None:
    meta = _metadata(core_wheel)
    assert meta.get("Name") == "workhold-client-core"
    requires = meta.get_all("Requires-Dist") or []
    pure: list[str] = []
    for req in requires:
        if ";" in req and "extra" in req.split(";", 1)[1]:
            continue
        pure.append(req)
    assert pure == [], f"core must have empty runtime deps, got {pure}"


def test_core_wheel_contains_private_core_and_public_testkit(core_wheel: Path) -> None:
    tops = _top_level(core_wheel)
    assert tops == {"_workhold_client_core", "workhold_client_testing"}
    assert "workhold" not in tops
    assert "queue_service_client" not in tops
    assert "workhold_producer" not in tops
    assert "workhold_admin" not in tops
