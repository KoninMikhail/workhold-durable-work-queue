"""Coordinated four-package client release matrix (SDK-01 / SDK-03 / SDK-16).

Asserts workspace members, version alignment, exact-minor core bounds, and CI
publication discovery list core before roles with no partial/legacy set.
Does not publish, tag, or write to any registry.
"""

from __future__ import annotations

import ast
import email.message
import email.parser
import importlib.util
import json
import os
import re
import subprocess
import sys
import tomllib
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
ROLE_DISTRIBUTIONS: tuple[str, ...] = CLIENT_DISTRIBUTIONS[1:]
FORBIDDEN_DISTRIBUTIONS: frozenset[str] = frozenset({"queue-client", "queue_client"})

_CORE_REQUIREMENT_RE = re.compile(
    r"^workhold-client-core"
    r"(?P<extras>\[async\])?"
    r"\s*(?P<spec>[^;]+?)"
    r"(?:\s*;\s*(?P<marker>.+))?$",
    re.IGNORECASE,
)
_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_BOUND_RE = re.compile(r"^(>=|<)\s*(\d+\.\d+\.\d+)$")
_ASYNC_MARKER_RE = re.compile(r"""^extra\s*==\s*(['"])async\1$""")


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        check=True,
        text=True,
        capture_output=True,
    )


def _load_toml(path: Path) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _latest_wheel(dist_name: str) -> Path:
    underscore = dist_name.replace("-", "_")
    wheels = sorted(DIST_DIR.glob(f"{underscore}-*.whl"))
    if not wheels:
        wheels = sorted(DIST_DIR.glob(f"{dist_name}-*.whl"))
    assert wheels, f"no wheel for {dist_name!r} under {DIST_DIR}"
    return wheels[-1]


def _wheel_metadata(wheel: Path) -> email.message.Message:
    with zipfile.ZipFile(wheel) as zf:
        meta_names = [n for n in zf.namelist() if n.endswith(".dist-info/METADATA")]
        assert len(meta_names) == 1, f"expected one METADATA in {wheel.name}"
        return email.parser.Parser().parsestr(zf.read(meta_names[0]).decode("utf-8"))


def _parse_ci_input(name: str) -> str:
    payload = json.loads((REPO_ROOT / "release-packages.json").read_text(encoding="utf-8"))
    value = payload[name]
    if isinstance(value, list):
        return " ".join(value)
    return str(value)


def _tokenize_ci_list(raw: str) -> list[str]:
    parts = re.split(r"[\s,]+", raw.strip())
    return [p for p in parts if p]


def _workspace_members() -> list[str]:
    root = _load_toml(REPO_ROOT / "pyproject.toml")
    members = root.get("tool", {}).get("uv", {}).get("workspace", {}).get("members")
    assert isinstance(members, list), "tool.uv.workspace.members missing"
    return [str(m).replace("\\", "/") for m in members]


def _package_pyproject(dist: str) -> Path:
    return REPO_ROOT / "packages" / dist / "pyproject.toml"


def _project_version(path: Path) -> str:
    data = _load_toml(path)
    version = data["project"]["version"]
    assert isinstance(version, str)
    return version


def _core_requirements(path: Path, *, section: str) -> list[str]:
    data = _load_toml(path)
    if section == "dependencies":
        deps = data["project"].get("dependencies") or []
    else:
        deps = (
            data["project"].get("optional-dependencies", {}).get("async") or []
        )
    found: list[str] = []
    for dep in deps:
        assert isinstance(dep, str)
        name = re.split(r"[<>=!~\[]", dep, maxsplit=1)[0].strip().lower().replace("_", "-")
        if name == "workhold-client-core":
            found.append(dep)
    return found


def _expected_bounds(version: str) -> tuple[str, str]:
    match = _VERSION_RE.fullmatch(version)
    assert match, f"unexpected version format: {version}"
    major, minor, _patch = (int(value) for value in match.groups())
    return f"{major}.{minor}.0", f"{major}.{minor + 1}.0"


def _parsed_core_requirement(requirement: str) -> tuple[bool, str, str, str | None]:
    match = _CORE_REQUIREMENT_RE.fullmatch(requirement.strip())
    assert match, f"malformed core requirement: {requirement!r}"
    bounds: dict[str, str] = {}
    clauses = [clause.strip() for clause in match.group("spec").split(",")]
    assert len(clauses) == 2
    for clause in clauses:
        parsed = _BOUND_RE.fullmatch(clause)
        assert parsed, f"malformed core bound: {requirement!r}"
        assert parsed.group(1) not in bounds
        bounds[parsed.group(1)] = parsed.group(2)
    assert set(bounds) == {">=", "<"}
    return (
        match.group("extras") is not None,
        bounds[">="],
        bounds["<"],
        match.group("marker"),
    )


@pytest.fixture(scope="module")
def built_client_wheels() -> dict[str, Path]:
    DIST_DIR.mkdir(exist_ok=True)
    for stale in DIST_DIR.glob("*.whl"):
        if any(
            stale.name.startswith(prefix)
            for prefix in (
                "workhold_client_core-",
                "workhold_producer-",
                "workhold_consumer-",
                "workhold_admin-",
                "queue_client-",
            )
        ):
            stale.unlink()
    for dist in CLIENT_DISTRIBUTIONS:
        _run(["uv", "build", "--package", dist, "--out-dir", str(DIST_DIR)])
    return {dist: _latest_wheel(dist.replace("-", "_")) for dist in CLIENT_DISTRIBUTIONS}


def test_prototype_package_tree_absent() -> None:
    assert not (REPO_ROOT / "packages" / "queue-client").exists()
    assert not (REPO_ROOT / "packages" / "queue-client" / "src" / "queue_service_client").exists()


def test_runtime_import_of_old_package_fails() -> None:
    assert importlib.util.find_spec("queue_service_client") is None
    with pytest.raises(ModuleNotFoundError):
        __import__("queue_service_client")


def test_workspace_members_are_core_and_three_roles_only() -> None:
    members = _workspace_members()
    expected = [f"packages/{name}" for name in CLIENT_DISTRIBUTIONS]
    assert members == expected
    assert all("queue-client" not in m for m in members)


def test_lockfile_members_exclude_prototype() -> None:
    lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    # Manifest members block lists workspace packages (includes server root).
    member_block = re.search(r"members = \[([^\]]+)\]", lock, re.DOTALL)
    assert member_block, "uv.lock missing members list"
    listed = ast.literal_eval("[" + member_block.group(1) + "]")
    assert "queue-client" not in listed
    for dist in CLIENT_DISTRIBUTIONS:
        assert dist in listed
    # No editable source for the deleted distribution.
    assert 'editable = "packages/queue-client"' not in lock
    assert 'name = "queue-client"' not in lock


def test_coordinated_repository_versions_match() -> None:
    versions = {_project_version(_package_pyproject(dist)) for dist in CLIENT_DISTRIBUTIONS}
    assert len(versions) == 1, f"client package versions drifted: {versions}"
    version = next(iter(versions))
    assert _VERSION_RE.match(version), f"unexpected version format: {version}"
    root_version = _project_version(REPO_ROOT / "pyproject.toml")
    assert version == root_version, "client set must share root repository version"


def test_role_packages_declare_exact_minor_core_bounds() -> None:
    for dist in ROLE_DISTRIBUTIONS:
        path = _package_pyproject(dist)
        expected_lower, expected_upper = _expected_bounds(_project_version(path))
        base = _core_requirements(path, section="dependencies")
        async_reqs = _core_requirements(
            path, section="optional-dependencies.async"
        )
        assert len(base) == 1, f"{dist} dependencies core count: {base!r}"
        assert len(async_reqs) == 1, (
            f"{dist} optional-dependencies.async core count: {async_reqs!r}"
        )
        base_async, base_lower, base_upper, base_marker = (
            _parsed_core_requirement(base[0])
        )
        assert (base_async, base_lower, base_upper, base_marker) == (
            False,
            expected_lower,
            expected_upper,
            None,
        )
        async_flag, async_lower, async_upper, async_marker = (
            _parsed_core_requirement(async_reqs[0])
        )
        assert (async_flag, async_lower, async_upper, async_marker) == (
            True,
            expected_lower,
            expected_upper,
            None,
        )


def test_ci_publication_lists_core_before_roles_only() -> None:
    packages = _tokenize_ci_list(_parse_ci_input("pypi_packages"))
    assert packages == list(CLIENT_DISTRIBUTIONS), (
        "CI pypi_packages must enumerate core then producer/consumer/admin only"
    )
    assert packages[0] == "workhold-client-core"
    assert FORBIDDEN_DISTRIBUTIONS.isdisjoint(packages)

    version_files = _tokenize_ci_list(_parse_ci_input("extra_version_files"))
    expected_files = [f"packages/{name}/pyproject.toml" for name in CLIENT_DISTRIBUTIONS]
    assert version_files == expected_files
    assert all("queue-client" not in path for path in version_files)


def test_ci_rejects_partial_role_publication_shape() -> None:
    packages = _tokenize_ci_list(_parse_ci_input("pypi_packages"))
    # Partial = missing any role, or roles without core first.
    assert set(packages) == set(CLIENT_DISTRIBUTIONS)
    assert packages.index("workhold-client-core") == 0
    for role in ROLE_DISTRIBUTIONS:
        assert packages.index(role) > 0


def test_built_wheels_share_version_and_metadata(
    built_client_wheels: dict[str, Path],
) -> None:
    versions: set[str] = set()
    for dist, wheel in built_client_wheels.items():
        meta = _wheel_metadata(wheel)
        assert meta.get("Name") == dist
        version = meta.get("Version")
        assert version
        versions.add(version)
        if dist == "workhold-client-core":
            requires = meta.get_all("Requires-Dist") or []
            pure = [r for r in requires if ";" not in r or "extra" not in r.split(";", 1)[1]]
            assert pure == []
        else:
            requires = meta.get_all("Requires-Dist") or []
            core_reqs = [
                requirement
                for requirement in requires
                if requirement.lower().startswith("workhold-client-core")
            ]
            parsed = [_parsed_core_requirement(req) for req in core_reqs]
            base = [item for item in parsed if not item[0]]
            async_extra = [item for item in parsed if item[0]]
            expected_lower, expected_upper = _expected_bounds(version)
            assert base == [(False, expected_lower, expected_upper, None)]
            assert len(async_extra) == 1
            assert async_extra[0][0:3] == (
                True,
                expected_lower,
                expected_upper,
            )
            assert async_extra[0][3] is not None
            assert _ASYNC_MARKER_RE.fullmatch(async_extra[0][3].strip())
    assert len(versions) == 1, f"built wheel versions drifted: {versions}"


def test_no_source_tree_references_runtime_old_import() -> None:
    """Runtime Python under packages/ and tests/sdk must not import the prototype."""
    roots = [
        REPO_ROOT / "packages",
        REPO_ROOT / "tests" / "sdk",
    ]
    pattern = re.compile(
        r"^\s*(?:from\s+queue_service_client\b|import\s+queue_service_client\b)",
        re.MULTILINE,
    )
    offenders: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if pattern.search(text):
                offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == [], f"old runtime imports remain: {offenders}"


def test_sys_modules_cleared_prototype_stays_unimportable() -> None:
    sys.modules.pop("queue_service_client", None)
    assert "queue_service_client" not in sys.modules
    with pytest.raises(ModuleNotFoundError):
        __import__("queue_service_client")
