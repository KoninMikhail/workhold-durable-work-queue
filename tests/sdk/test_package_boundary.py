"""Package boundary proofs for server vs coordinated client distributions (SDK-01)."""

from __future__ import annotations

import email.message
import email.parser
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DIST_DIR = REPO_ROOT / "dist"

CLIENT_DISTRIBUTIONS: tuple[str, ...] = (
    "workhold-client-core",
    "workhold-producer",
    "workhold-consumer",
    "workhold-admin",
)
CLIENT_IMPORTS: dict[str, frozenset[str]] = {
    "workhold-client-core": frozenset(
        {"_workhold_client_core", "workhold_client_testing"}
    ),
    "workhold-producer": frozenset({"workhold_producer"}),
    "workhold-consumer": frozenset({"workhold_consumer"}),
    "workhold-admin": frozenset({"workhold_admin"}),
}


def _run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        cmd,
        cwd=cwd or REPO_ROOT,
        env=merged,
        check=True,
        text=True,
        capture_output=True,
    )


def _latest_wheel(dist_name: str) -> Path:
    wheels = sorted(DIST_DIR.glob(f"{dist_name.replace('-', '_')}-*.whl"))
    if not wheels:
        wheels = sorted(DIST_DIR.glob(f"{dist_name}-*.whl"))
    assert wheels, f"no wheel found for distribution {dist_name!r} under {DIST_DIR}"
    return wheels[-1]


def _wheel_metadata(wheel: Path) -> email.message.Message:
    with zipfile.ZipFile(wheel) as zf:
        meta_names = [n for n in zf.namelist() if n.endswith(".dist-info/METADATA")]
        assert len(meta_names) == 1, f"expected one METADATA in {wheel.name}, got {meta_names}"
        raw = zf.read(meta_names[0]).decode("utf-8")
    return email.parser.Parser().parsestr(raw)


def _top_level_packages(wheel: Path) -> set[str]:
    with zipfile.ZipFile(wheel) as zf:
        tops: set[str] = set()
        for name in zf.namelist():
            if name.endswith("/") or ".dist-info/" in name or ".data/" in name:
                continue
            parts = Path(name).parts
            if not parts:
                continue
            tops.add(parts[0])
        return tops


@pytest.fixture(scope="module")
def built_wheels() -> dict[str, Path]:
    DIST_DIR.mkdir(exist_ok=True)
    for stale in DIST_DIR.glob("*.whl"):
        stale.unlink()
    _run(["uv", "build", "--package", "workhold", "--out-dir", str(DIST_DIR)])
    for dist in CLIENT_DISTRIBUTIONS:
        _run(["uv", "build", "--package", dist, "--out-dir", str(DIST_DIR)])
    wheels = {"workhold": _latest_wheel("workhold")}
    for dist in CLIENT_DISTRIBUTIONS:
        wheels[dist] = _latest_wheel(dist.replace("-", "_"))
    return wheels


def test_no_legacy_queue_client_wheel(built_wheels: dict[str, Path]) -> None:
    assert "queue-client" not in built_wheels
    leftovers = list(DIST_DIR.glob("queue_client-*.whl")) + list(DIST_DIR.glob("queue-client-*.whl"))
    assert leftovers == [], f"legacy prototype wheel still present: {leftovers}"


def test_client_wheels_have_no_dependency_on_queue(built_wheels: dict[str, Path]) -> None:
    for dist in CLIENT_DISTRIBUTIONS:
        meta = _wheel_metadata(built_wheels[dist])
        requires = meta.get_all("Requires-Dist") or []
        for req in requires:
            name = req.split(";", 1)[0].strip().split(" ", 1)[0].lower().replace("_", "-")
            assert name not in {"queue", "workhold"}, (
                f"{dist} must not depend on server distribution: {req}"
            )


def test_client_wheels_contain_only_own_import_package(built_wheels: dict[str, Path]) -> None:
    for dist in CLIENT_DISTRIBUTIONS:
        tops = _top_level_packages(built_wheels[dist])
        assert tops == CLIENT_IMPORTS[dist], f"{dist} tops={tops}"
        assert "workhold" not in tops
        assert "queue_service_client" not in tops


def test_server_wheel_does_not_ship_client_packages(built_wheels: dict[str, Path]) -> None:
    tops = _top_level_packages(built_wheels["workhold"])
    assert "workhold" in tops
    for import_names in CLIENT_IMPORTS.values():
        assert tops.isdisjoint(import_names)
    assert "queue_service_client" not in tops


def test_clean_venv_imports_producer_without_server_or_admin(
    built_wheels: dict[str, Path],
) -> None:
    producer = built_wheels["workhold-producer"]
    core = built_wheels["workhold-client-core"]
    with tempfile.TemporaryDirectory(prefix="queue-role-boundary-") as tmp:
        venv = Path(tmp) / "venv"
        _run(["uv", "venv", str(venv)])
        _run(["uv", "pip", "install", "--python", str(venv), str(core), str(producer)])
        probe = r"""
import importlib
import sys

client = importlib.import_module("workhold_producer")
assert getattr(client, "__version__", None), "missing __version__"
assert hasattr(client, "ProducerClient")

try:
    importlib.import_module("workhold")
except ModuleNotFoundError:
    pass
else:
    raise SystemExit("workhold must not be importable from producer-only install")

try:
    importlib.import_module("workhold_admin")
except ModuleNotFoundError:
    pass
else:
    raise SystemExit("workhold_admin must not be importable")

try:
    importlib.import_module("queue_service_client")
except ModuleNotFoundError:
    pass
else:
    raise SystemExit("legacy queue_service_client must not be importable")

print("ok", client.__version__)
"""
        py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        result = _run([str(py), "-c", probe])
        assert "ok" in result.stdout
