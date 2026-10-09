#!/usr/bin/env python3
"""Align package versions and exact-minor core bounds to the root version.

``pyproject.toml`` ``project.version`` is the source of truth. Client
pyprojects, ``__version__`` assignments, and role-package core bounds
(``>=MAJOR.MINOR.0,<MAJOR.(MINOR+1).0``) are rewritten to match it.
"""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_PACKAGES_PATH = REPO_ROOT / "release-packages.json"

_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_VERSION_LINE = re.compile(r'^(version\s*=\s*")([^"]+)(")', re.MULTILINE)
_PY_VERSION = re.compile(r'^(__version__\s*=\s*")([^"]+)(")', re.MULTILINE)
_CORE_REQ = re.compile(
    r"workhold-client-core(?P<extras>\[async\])?>=\d+\.\d+\.\d+,<\d+\.\d+\.\d+"
)


class SyncError(RuntimeError):
    """Coordinated version files cannot be rewritten."""


def exact_minor_bounds(version: str) -> tuple[str, str]:
    match = _VERSION_RE.fullmatch(version)
    if not match:
        raise SyncError(f"unexpected version format: {version!r}")
    major, minor, _patch = (int(value) for value in match.groups())
    return f"{major}.{minor}.0", f"{major}.{minor + 1}.0"


def rewrite_pyproject(text: str, version: str, *, update_bounds: bool) -> str:
    if not _VERSION_LINE.search(text):
        raise SyncError("missing project version assignment")

    def _version(match: re.Match[str]) -> str:
        return f"{match.group(1)}{version}{match.group(3)}"

    rewritten = _VERSION_LINE.sub(_version, text, count=1)
    if not update_bounds:
        return rewritten
    lower, upper = exact_minor_bounds(version)

    def _bound(match: re.Match[str]) -> str:
        extras = match.group("extras") or ""
        return f"workhold-client-core{extras}>={lower},<{upper}"

    rewritten, count = _CORE_REQ.subn(_bound, rewritten)
    if count != 2:
        raise SyncError(f"expected 2 core requirements, rewrote {count}")
    return rewritten


def rewrite_version_module(text: str, version: str) -> str:
    if not _PY_VERSION.search(text):
        raise SyncError("missing __version__ assignment")

    def _replace(match: re.Match[str]) -> str:
        return f"{match.group(1)}{version}{match.group(3)}"

    return _PY_VERSION.sub(_replace, text, count=1)


def _root_version(root: Path) -> str:
    with (root / "pyproject.toml").open("rb") as fh:
        version = tomllib.load(fh)["project"]["version"]
    if not isinstance(version, str):
        raise SyncError("root project.version must be a string")
    exact_minor_bounds(version)
    return version


def _release_contract(root: Path) -> dict[str, list[str]]:
    payload = json.loads((root / "release-packages.json").read_text(encoding="utf-8"))
    packages = payload.get("pypi_packages")
    version_files = payload.get("extra_version_files")
    version_modules = payload.get("version_py_files")
    if not isinstance(packages, list) or not packages:
        raise SyncError("release-packages.json pypi_packages must be a non-empty list")
    if not isinstance(version_files, list) or not isinstance(version_modules, list):
        raise SyncError("release-packages.json version file lists must be arrays")
    return {
        "pypi_packages": [str(item) for item in packages],
        "extra_version_files": [str(item) for item in version_files],
        "version_py_files": [str(item) for item in version_modules],
    }


def sync_repository(root: Path) -> list[Path]:
    """Rewrite drifted files. Return paths that changed."""
    version = _root_version(root)
    contract = _release_contract(root)
    role_names = set(contract["pypi_packages"][1:])
    changed: list[Path] = []
    for relative in contract["extra_version_files"]:
        path = root / relative
        package_name = path.parent.name
        updated = rewrite_pyproject(
            path.read_text(encoding="utf-8"),
            version,
            update_bounds=package_name in role_names,
        )
        if _write_if_changed(path, updated):
            changed.append(path)
    for relative in contract["version_py_files"]:
        path = root / relative
        updated = rewrite_version_module(path.read_text(encoding="utf-8"), version)
        if _write_if_changed(path, updated):
            changed.append(path)
    return changed


def _write_if_changed(path: Path, updated: str) -> bool:
    current = path.read_text(encoding="utf-8")
    if current == updated:
        return False
    path.write_text(updated, encoding="utf-8", newline="\n")
    return True


def main() -> int:
    changed = sync_repository(REPO_ROOT)
    for path in changed:
        print(path.relative_to(REPO_ROOT).as_posix())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SyncError as exc:
        print(f"sync_coordinated_version: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
