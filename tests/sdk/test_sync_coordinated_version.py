"""Version sync keeps the server and client-set release trains independent."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_PATH = REPO_ROOT / "tools" / "sync_coordinated_version.py"
PUBLISH_PATH = REPO_ROOT / "tools" / "publish_client_packages.py"


def _load_sync():
    spec = importlib.util.spec_from_file_location("sync_coordinated_version", SYNC_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_minor_bounds_follow_root_version() -> None:
    sync = _load_sync()
    assert sync.exact_minor_bounds("0.1.0") == ("0.1.0", "0.2.0")
    assert sync.exact_minor_bounds("0.2.3") == ("0.2.0", "0.3.0")
    assert sync.exact_minor_bounds("1.4.1") == ("1.4.0", "1.5.0")


def test_role_pyproject_rewrite_moves_bounds_on_minor_bump() -> None:
    sync = _load_sync()
    source = '\n'.join(
        [
            '[project]',
            'name = "workhold-producer"',
            'version = "0.1.0"',
            'dependencies = [',
            '    "workhold-client-core>=0.1.0,<0.2.0",',
            ']',
            '[project.optional-dependencies]',
            'async = [',
            '    "workhold-client-core[async]>=0.1.0,<0.2.0",',
            ']',
            '',
        ]
    )
    rewritten = sync.rewrite_pyproject(source, "0.2.0", update_bounds=True)
    assert 'version = "0.2.0"' in rewritten
    assert "workhold-client-core>=0.2.0,<0.3.0" in rewritten
    assert "workhold-client-core[async]>=0.2.0,<0.3.0" in rewritten
    assert sync.rewrite_pyproject(rewritten, "0.2.0", update_bounds=True) == rewritten


def test_version_module_rewrite() -> None:
    sync = _load_sync()
    source = '"""pkg"""\n\n__version__ = "0.1.0"\n'
    assert sync.rewrite_version_module(source, "0.1.1") == '"""pkg"""\n\n__version__ = "0.1.1"\n'


def test_repository_versions_already_coordinated() -> None:
    sync = _load_sync()
    assert sync.sync_repository(REPO_ROOT) == []


def _write_release_tree(root: Path, *, server: str, core: str, role: str) -> None:
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "workhold"\nversion = "{server}"\n', encoding="utf-8"
    )
    (root / "src" / "workhold").mkdir(parents=True)
    (root / "src" / "workhold" / "__init__.py").write_text(
        '__version__ = "0.0.1"\n', encoding="utf-8"
    )
    for dist, version, module in (
        ("workhold-client-core", core, "_workhold_client_core"),
        ("workhold-producer", role, "workhold_producer"),
    ):
        package = root / "packages" / dist
        (package / "src" / module).mkdir(parents=True)
        requirements = (
            ""
            if dist == "workhold-client-core"
            else 'dependencies = ["workhold-client-core>=0.1.0,<0.2.0"]\n'
            '[project.optional-dependencies]\n'
            'async = ["workhold-client-core[async]>=0.1.0,<0.2.0"]\n'
        )
        (package / "pyproject.toml").write_text(
            f'[project]\nname = "{dist}"\nversion = "{version}"\n{requirements}',
            encoding="utf-8",
        )
        (package / "src" / module / "__init__.py").write_text(
            '__version__ = "0.0.1"\n', encoding="utf-8"
        )
    (root / "release-packages.json").write_text(
        json.dumps(
            {
                "pypi_packages": ["workhold-client-core", "workhold-producer"],
                "extra_version_files": [
                    "packages/workhold-client-core/pyproject.toml",
                    "packages/workhold-producer/pyproject.toml",
                ],
                "version_py_files": [
                    "packages/workhold-client-core/src/_workhold_client_core/__init__.py",
                    "packages/workhold-producer/src/workhold_producer/__init__.py",
                ],
                "server_version_py_files": ["src/workhold/__init__.py"],
            }
        ),
        encoding="utf-8",
    )


def test_sync_keeps_server_and_client_versions_independent(tmp_path: Path) -> None:
    sync = _load_sync()
    _write_release_tree(tmp_path, server="1.4.2", core="1.1.0", role="1.0.9")
    sync.sync_repository(tmp_path)
    producer = (tmp_path / "packages/workhold-producer/pyproject.toml").read_text(
        encoding="utf-8"
    )
    assert 'version = "1.1.0"' in producer
    assert "workhold-client-core>=1.1.0,<1.2.0" in producer
    assert "workhold-client-core[async]>=1.1.0,<1.2.0" in producer
    assert 'version = "1.4.2"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert (tmp_path / "src/workhold/__init__.py").read_text(
        encoding="utf-8"
    ) == '__version__ = "1.4.2"\n'
    for module in (
        "packages/workhold-client-core/src/_workhold_client_core/__init__.py",
        "packages/workhold-producer/src/workhold_producer/__init__.py",
    ):
        assert (tmp_path / module).read_text(encoding="utf-8") == '__version__ = "1.1.0"\n'
    assert sync.sync_repository(tmp_path) == []


def test_publish_refuses_without_opt_in() -> None:
    completed = subprocess.run(
        [sys.executable, str(PUBLISH_PATH)],
        cwd=REPO_ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 1
    assert "WORKHOLD_PUBLISH" in completed.stderr
