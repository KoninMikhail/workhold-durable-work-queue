"""Isolated workhold-mcp wheel, source imports, and runtime image boundary."""

from __future__ import annotations

import ast
import email.parser
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_ROOT = REPO_ROOT / "packages" / "workhold-mcp"

_FORBIDDEN_ROOTS = frozenset({"workhold", "sqlalchemy", "psycopg", "psycopg2"})
_REPOSITORY_PARTS = frozenset({"repository", "repositories"})
_REQUIRED_DEPS = frozenset({"mcp", "workhold-admin", "workhold-client-core"})
_FORBIDDEN_DEPS = frozenset({"workhold", "sqlalchemy", "psycopg", "psycopg2"})


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    merged.pop("WORKHOLD_PUBLISH", None)
    if env:
        merged.update(env)
        merged.pop("WORKHOLD_PUBLISH", None)
    return subprocess.run(
        cmd,
        cwd=cwd or REPO_ROOT,
        env=merged,
        check=True,
        text=True,
        capture_output=True,
    )


def _requirement_name(req: str) -> str:
    base = req.split(";", 1)[0].strip()
    for index, char in enumerate(base):
        if char in " \t[<>=!~":
            base = base[:index]
            break
    return base.lower().replace("_", "-")


def _wheel_metadata(wheel: Path) -> email.message.Message:
    with zipfile.ZipFile(wheel) as zf:
        meta_names = [n for n in zf.namelist() if n.endswith(".dist-info/METADATA")]
        assert len(meta_names) == 1, f"expected one METADATA in {wheel.name}, got {meta_names}"
        raw = zf.read(meta_names[0]).decode("utf-8")
    return email.parser.Parser().parsestr(raw)


def _entry_points(wheel: Path) -> str:
    with zipfile.ZipFile(wheel) as zf:
        names = [n for n in zf.namelist() if n.endswith(".dist-info/entry_points.txt")]
        assert len(names) == 1, f"expected entry_points.txt in {wheel.name}, got {names}"
        return zf.read(names[0]).decode("utf-8")


def _forbidden_module(module: str) -> bool:
    parts = [part for part in module.split(".") if part]
    if not parts:
        return False
    if parts[0] in _FORBIDDEN_ROOTS:
        return True
    return bool(_REPOSITORY_PARTS.intersection(parts))


def _build_mcp_wheel(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    _run(["uv", "build", "--package", "workhold-mcp", "--out-dir", str(out_dir)])
    wheels = sorted(out_dir.glob("workhold_mcp-*.whl"))
    assert wheels, f"no workhold-mcp wheel under {out_dir}"
    return wheels[-1]


def test_mcp_source_does_not_import_the_server() -> None:
    offenders: list[str] = []
    for path in MCP_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str | None]
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module]
            else:
                continue
            for module in modules:
                if module and _forbidden_module(module):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}:{module}")
    assert offenders == [], f"forbidden imports: {offenders}"


def test_mcp_wheel_depends_on_clients_not_the_server() -> None:
    with tempfile.TemporaryDirectory(prefix="workhold-mcp-wheel-") as tmp:
        wheel = _build_mcp_wheel(Path(tmp))
        meta = _wheel_metadata(wheel)
        assert meta.get("Name") == "workhold-mcp"
        requires = meta.get_all("Requires-Dist") or []
        names = {_requirement_name(req) for req in requires}
        assert _REQUIRED_DEPS <= names, f"missing required deps in {requires}"
        assert names.isdisjoint(_FORBIDDEN_DEPS), f"forbidden deps in {requires}"
        entry_points = _entry_points(wheel)
        assert "workhold-mcp" in entry_points


def test_isolated_mcp_wheel_imports_adapter_without_server() -> None:
    with tempfile.TemporaryDirectory(prefix="workhold-mcp-venv-") as tmp:
        root = Path(tmp)
        wheel = _build_mcp_wheel(root / "dist")
        venv = root / "venv"
        _run(["uv", "venv", str(venv)])
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(venv),
                "--no-deps",
                str(wheel),
            ]
        )
        probe = r"""
import importlib

mod = importlib.import_module("workhold_mcp")
assert getattr(mod, "__version__", None), "missing __version__"
try:
    importlib.import_module("workhold")
except ModuleNotFoundError:
    pass
else:
    raise SystemExit("workhold must not be importable from the MCP wheel")
print("ok", mod.__version__)
"""
        py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        result = _run([str(py), "-c", probe])
        assert "ok" in result.stdout
        assert "WORKHOLD_PUBLISH" not in result.stdout


def test_runtime_image_does_not_install_the_mcp_package() -> None:
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    entrypoint = (REPO_ROOT / "docker" / "entrypoint.sh").read_text(encoding="utf-8")
    assert "workhold-mcp" not in dockerfile
    assert "workhold-mcp" not in entrypoint
    runtime = dockerfile.split("FROM base AS runtime", 1)[1].split("FROM ", 1)[0]
    sync_lines = [line.strip() for line in runtime.splitlines() if "uv sync" in line]
    assert sync_lines, "runtime stage must keep its uv sync line"
    assert all("--all-packages" not in line for line in sync_lines)
