"""Coordinated version sync stays aligned with the root project version."""

from __future__ import annotations

import importlib.util
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
