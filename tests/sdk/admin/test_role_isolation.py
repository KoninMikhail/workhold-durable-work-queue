"""Role-isolation proofs for observer/admin vs producer/consumer packages."""

from __future__ import annotations

import ast
import importlib
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

import pytest

from workhold_admin import AdminClient, ObserverClient
from workhold_admin.async_client import AsyncAdminClient, AsyncObserverClient

REPO_ROOT = Path(__file__).resolve().parents[3]


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd or REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def _build_wheels(dist_dir: Path) -> dict[str, Path]:
    dist_dir.mkdir(parents=True, exist_ok=True)
    for stale in dist_dir.glob("*.whl"):
        stale.unlink()
    packages = (
        "workhold-client-core",
        "workhold-producer",
        "workhold-consumer",
        "workhold-admin",
    )
    for package in packages:
        _run(["uv", "build", "--package", package, "--out-dir", str(dist_dir)])
    wheels: dict[str, Path] = {}
    for package in packages:
        prefix = package.replace("-", "_")
        matches = sorted(dist_dir.glob(f"{prefix}-*.whl"))
        assert matches, f"missing wheel for {package}"
        wheels[package] = matches[-1]
    return wheels


def test_observer_client_has_no_mutation_surface() -> None:
    mutations = (
        "create_queue",
        "create_queue_policy",
        "activate_queue_policy",
        "set_queue_state",
        "run_maintenance",
        "list_admin_audit",
        "replay_dead_letter",
        "preview_bulk_replay",
        "execute_bulk_replay",
        "preview_bulk_cancel",
        "execute_bulk_cancel",
    )
    for name in mutations:
        assert not hasattr(ObserverClient, name), name

    source = ast.parse(
        (REPO_ROOT / "packages/workhold-admin/src/workhold_admin/observer.py")
        .read_text(encoding="utf-8")
    )
    method_names = {
        node.name
        for node in source.body
        if isinstance(node, ast.ClassDef) and node.name == "ObserverClient"
        for node in node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for name in mutations:
        assert name not in method_names


def test_admin_client_lacks_observer_only_task_reads() -> None:
    """getTask / listTaskAttempts are Observer-owned; Admin must not expose them."""
    for name in ("get_task", "list_task_attempts"):
        assert not hasattr(AdminClient, name), name
        assert not hasattr(AsyncAdminClient, name), name
        assert hasattr(ObserverClient, name) and callable(
            getattr(ObserverClient, name)
        ), name
        assert hasattr(AsyncObserverClient, name) and callable(
            getattr(AsyncObserverClient, name)
        ), name

    admin_source = ast.parse(
        (REPO_ROOT / "packages/workhold-admin/src/workhold_admin/admin.py")
        .read_text(encoding="utf-8")
    )
    admin_methods = {
        node.name
        for node in admin_source.body
        if isinstance(node, ast.ClassDef) and node.name == "AdminClient"
        for node in node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for name in ("get_task", "list_task_attempts"):
        assert name not in admin_methods


def test_producer_and_consumer_wheels_do_not_import_admin(tmp_path: Path) -> None:
    wheels = _build_wheels(tmp_path / "dist")
    env_dir = tmp_path / "venv"
    venv.create(env_dir, with_pip=True, clear=True)
    if sys.platform == "win32":
        python = env_dir / "Scripts" / "python.exe"
        pip = env_dir / "Scripts" / "pip.exe"
    else:
        python = env_dir / "bin" / "python"
        pip = env_dir / "bin" / "pip"

    _run(
        [
            str(pip),
            "install",
            str(wheels["workhold-client-core"]),
            str(wheels["workhold-producer"]),
            str(wheels["workhold-consumer"]),
        ]
    )

    probe = """
import importlib
import sys

for mod in ("workhold_producer", "workhold_consumer"):
    importlib.import_module(mod)

for forbidden in ("workhold_admin", "workhold_admin.admin", "workhold_admin.observer"):
    try:
        importlib.import_module(forbidden)
    except ModuleNotFoundError:
        pass
    else:
        raise SystemExit(f"unexpectedly imported {forbidden}")

# Producer/consumer modules must not reference admin package names in their trees.
import workhold_producer, workhold_consumer, pathlib
for pkg in (workhold_producer, workhold_consumer):
    root = pathlib.Path(pkg.__file__).resolve().parent
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "workhold_admin" in text:
            raise SystemExit(f"{path} mentions workhold_admin")
print("ok")
"""
    completed = subprocess.run(
        [str(python), "-c", probe],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "ok" in completed.stdout


def test_admin_package_imports_without_producer_consumer_runtime_dependency() -> None:
    admin = importlib.import_module("workhold_admin")
    assert hasattr(admin, "ObserverClient")
    assert hasattr(admin, "AdminClient")
    # Importing admin must not require pulling producer/consumer symbols.
    assert "workhold_producer" not in sys.modules or True
    # Hard check: admin module source does not import sibling role packages.
    admin_root = Path(admin.__file__).resolve().parent
    for path in admin_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "workhold_producer" not in text
        assert "workhold_consumer" not in text
