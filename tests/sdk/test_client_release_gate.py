"""Atomic client release gate: dry-run publish + wheel inventory (SDK-16)."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from email.message import Message
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GATE_PATH = REPO_ROOT / "tools" / "client_release_gate.py"
INVENTORY_PATH = REPO_ROOT / "dist" / "client-wheel-inventory.json"

CLIENT_DISTRIBUTIONS = (
    "workhold-client-core",
    "workhold-producer",
    "workhold-consumer",
    "workhold-admin",
)


def _load_gate():
    spec = importlib.util.spec_from_file_location("client_release_gate", GATE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_synthetic_release_tree(
    root: Path,
    *,
    version: str = "1.2.7",
    server_version: str | None = None,
    overrides: dict[tuple[str, str], list[str]] | None = None,
) -> None:
    overrides = overrides or {}
    package_versions = {
        "workhold-client-core": version,
        "workhold-producer": version,
        "workhold-consumer": version,
        "workhold-admin": version,
    }
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "queue"\nversion = "{server_version or version}"\n',
        encoding="utf-8",
    )
    for dist, package_version in package_versions.items():
        package_dir = root / "packages" / dist
        package_dir.mkdir(parents=True)
        if dist == "workhold-client-core":
            dependencies: list[str] = []
            async_dependencies = ["httpx>=0.28"]
        else:
            dependencies = [
                "workhold-client-core>=1.2.0,<1.3.0"
            ]
            async_dependencies = [
                "workhold-client-core[async]>=1.2.0,<1.3.0"
            ]
        dependencies = overrides.get((dist, "dependencies"), dependencies)
        async_dependencies = overrides.get(
            (dist, "optional-dependencies.async"), async_dependencies
        )
        deps_toml = ",\n".join(f'  "{dep}"' for dep in dependencies)
        async_toml = ",\n".join(f'  "{dep}"' for dep in async_dependencies)
        (package_dir / "pyproject.toml").write_text(
            f"""[project]
name = "{dist}"
version = "{package_version}"
dependencies = [
{deps_toml}
]

[project.optional-dependencies]
async = [
{async_toml}
]
""",
            encoding="utf-8",
        )


def test_synthetic_nonzero_patch_derives_and_accepts_coordinated_minor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = _load_gate()
    _write_synthetic_release_tree(tmp_path)
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    version, expected = gate._assert_coordinated_versions()
    assert version == "1.2.7"
    assert expected == ">=1.2.0,<1.3.0"


def test_client_set_version_is_independent_of_server_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = _load_gate()
    _write_synthetic_release_tree(tmp_path, server_version="2.5.0")
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    version, expected = gate._assert_coordinated_versions()
    assert version == "1.2.7"
    assert expected == ">=1.2.0,<1.3.0"


def test_extra_file_paths_are_relative_to_client_release_path() -> None:
    gate = _load_gate()
    assert gate._repo_path(".", "pyproject.toml") == "pyproject.toml"
    assert (
        gate._repo_path("packages", "workhold-admin/pyproject.toml")
        == "packages/workhold-admin/pyproject.toml"
    )
    assert gate._repo_path("packages", "/uv.lock") == "uv.lock"


@pytest.mark.parametrize(
    ("section", "requirements", "diagnostic"),
    [
        (
            "dependencies",
            ["workhold-client-core>=0.1.0,<0.2.0"],
            "dependencies",
        ),
        (
            "optional-dependencies.async",
            ["workhold-client-core[async]>=0.1.0,<0.2.0"],
            "optional-dependencies.async",
        ),
        (
            "dependencies",
            ["workhold-client-core>=1.2.1,<1.3.0"],
            "dependencies",
        ),
        (
            "dependencies",
            ["workhold-client-core>=1.2.0,<1.4.0"],
            "dependencies",
        ),
        (
            "dependencies",
            [],
            "dependencies",
        ),
        (
            "dependencies",
            [
                "workhold-client-core>=1.2.0,<1.3.0",
                "workhold-client-core>=1.2.0,<1.3.0",
            ],
            "dependencies",
        ),
    ],
)
def test_synthetic_release_rejects_core_dependency_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    section: str,
    requirements: list[str],
    diagnostic: str,
) -> None:
    gate = _load_gate()
    role = "workhold-consumer"
    _write_synthetic_release_tree(
        tmp_path,
        overrides={(role, section): requirements},
    )
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    with pytest.raises(
        gate.GateError,
        match=rf"{role}.*{re.escape(diagnostic)}",
    ):
        gate._assert_coordinated_versions()


def _role_wheel_metadata(
    *,
    base: str = "workhold-client-core<1.3.0,>=1.2.0",
    async_requirement: str = (
        "workhold-client-core[async]<1.3.0,>=1.2.0; extra == 'async'"
    ),
) -> Message:
    metadata = Message()
    metadata["Requires-Dist"] = base
    metadata["Requires-Dist"] = async_requirement
    return metadata


def test_release_gate_rejects_version_module_drift(tmp_path: Path) -> None:
    gate = _load_gate()
    module = tmp_path / "src" / "workhold" / "__init__.py"
    module.parent.mkdir(parents=True)
    module.write_text('__version__ = "1.0.0"\n', encoding="utf-8")
    relative = "src/workhold/__init__.py"
    with pytest.raises(gate.GateError, match=r"__version__ 1\.0\.0"):
        gate._assert_version_modules(tmp_path, [relative], "1.0.1")
    gate._assert_version_modules(tmp_path, [relative], "1.0.0")


def test_repository_version_modules_match_manifest() -> None:
    gate = _load_gate()
    gate._assert_ci_atomic_publish_shape()


def test_role_wheel_metadata_accepts_matching_base_and_async_bounds() -> None:
    gate = _load_gate()
    gate._assert_role_wheel_core_dependencies(
        "workhold-producer",
        _role_wheel_metadata(),
        expected_lower="1.2.0",
        expected_upper="1.3.0",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("base", "workhold-client-core<0.2.0,>=0.1.0"),
        (
            "async",
            "workhold-client-core[async]<0.2.0,>=0.1.0; extra == 'async'",
        ),
        (
            "async",
            "workhold-client-core[async]<1.3.0,>=1.2.0; extra == 'other'",
        ),
    ],
)
def test_role_wheel_metadata_rejects_stale_or_mismarked_core_requirement(
    field: str,
    value: str,
) -> None:
    gate = _load_gate()
    metadata = (
        _role_wheel_metadata(base=value)
        if field == "base"
        else _role_wheel_metadata(async_requirement=value)
    )
    with pytest.raises(gate.GateError, match=rf"workhold-admin.*{field}"):
        gate._assert_role_wheel_core_dependencies(
            "workhold-admin",
            metadata,
            expected_lower="1.2.0",
            expected_upper="1.3.0",
        )


def test_ci_invokes_client_release_gate() -> None:
    payload = json.loads((REPO_ROOT / "release-packages.json").read_text(encoding="utf-8"))
    assert "queue-client" not in payload["pypi_packages"]
    workflow = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    )
    assert "tools/client_release_gate.py" in workflow
    assert "tools/publish_client_packages.py" in workflow
    assert "--require-qualification-pass" not in workflow


def test_client_release_gate_builds_inventory_and_dry_run() -> None:
    gate = _load_gate()
    result = gate.run()
    assert result["ok"] is True
    assert result["version"]
    assert INVENTORY_PATH.is_file()
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    assert inventory["coordinated_version"] == result["version"]
    assert inventory["distributions"] == list(CLIENT_DISTRIBUTIONS)
    assert inventory["forbidden_absent"]
    assert "queue-client" in inventory["forbidden_absent"]
    assert len(inventory["wheels"]) == 4
    names = [entry["distribution"] for entry in inventory["wheels"]]
    assert names == list(CLIENT_DISTRIBUTIONS)
    for entry in inventory["wheels"]:
        assert re_sha256(entry["sha256"])
        assert entry["metadata"]["name"] == entry["distribution"]
        assert entry["metadata"]["version"] == inventory["coordinated_version"]
    # Dependency graph: roles depend on core only.
    assert inventory["dependency_graph"]["workhold-client-core"] == []
    for role in CLIENT_DISTRIBUTIONS[1:]:
        assert inventory["dependency_graph"][role] == ["workhold-client-core"]
    dry_run = result["dry_run"]
    assert isinstance(dry_run, str)
    assert "workhold_mcp-" in dry_run or "workhold-mcp-" in dry_run, (
        "uv publish --dry-run must name the workhold-mcp wheel"
    )


def test_mcp_package_stays_outside_the_coordinated_set() -> None:
    payload = json.loads((REPO_ROOT / "release-packages.json").read_text(encoding="utf-8"))
    assert payload.get("mcp_package") == "workhold-mcp", (
        "release-packages.json mcp_package must be workhold-mcp"
    )
    assert payload["pypi_packages"] == list(CLIENT_DISTRIBUTIONS)
    assert "workhold-mcp" not in payload["pypi_packages"]


def test_mcp_publisher_script_is_separate() -> None:
    path = REPO_ROOT / "tools" / "publish_mcp_package.py"
    assert path.is_file(), "missing publisher tools/publish_mcp_package.py"
    text = path.read_text(encoding="utf-8")
    assert "workhold-mcp" in text
    assert "WORKHOLD_PUBLISH" in text
    assert "pypi_packages" in text
    assert "--trusted-publishing" in text
    assert "always" in text


def test_release_workflow_has_an_mcp_dispatch_choice() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert re.search(r"(?m)^\s+- mcp\s*$", workflow), "missing mcp dispatch choice"
    assert "publish-mcp:" in workflow, "missing publish-mcp job"
    assert "publish_mcp:" in workflow, "missing publish_mcp output"
    assert "pypi-workhold-mcp" in workflow, "missing environment pypi-workhold-mcp"
    assert "tools/publish_mcp_package.py" in workflow, (
        "missing publisher tools/publish_mcp_package.py"
    )
    job = workflow.split("publish-mcp:", 1)[1]
    assert "packages--release_created" not in job
    assert "publish_clients" not in job


def test_dispatch_all_does_not_select_mcp() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    for line in workflow.splitlines():
        if "mcp=true" in line.replace(" ", ""):
            assert "all" not in line, "all dispatch must not set the MCP flag"
        if (
            "PUBLISH_EXISTING" in line
            and re.search(r'=\s*"all"', line)
            and "mcp" in line
        ):
            raise AssertionError(f"all dispatch selects mcp: {line.strip()}")


def re_sha256(value: str) -> bool:
    return len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _git_missing(*_args: object, **_kwargs: object) -> None:
    raise FileNotFoundError(2, "No such file or directory", "git")


def test_git_sha_uses_ci_commit_sha_when_git_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_gate()
    monkeypatch.setattr(gate, "_run", _git_missing)
    sha = "b" * 40
    monkeypatch.setenv("CI_COMMIT_SHA", sha.upper())
    assert gate._git_sha() == sha


def test_git_sha_reads_checkout_without_git_or_ci_sha(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    gate = _load_gate()
    monkeypatch.setattr(gate, "_run", _git_missing)
    monkeypatch.delenv("CI_COMMIT_SHA", raising=False)
    sha = "c" * 40
    git_dir = tmp_path / ".git" / "refs" / "heads"
    git_dir.mkdir(parents=True)
    (tmp_path / ".git" / "HEAD").write_text("ref: refs/heads/dev\n", encoding="utf-8")
    (git_dir / "dev").write_text(sha + "\n", encoding="utf-8")
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    assert gate._git_sha() == sha


def test_git_sha_fails_closed_without_git_ci_sha_or_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    gate = _load_gate()
    monkeypatch.setattr(gate, "_run", _git_missing)
    monkeypatch.delenv("CI_COMMIT_SHA", raising=False)
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    with pytest.raises(gate.GateError, match="git is not installed"):
        gate._git_sha()


def test_cli_exit_zero_without_qualification_requirement() -> None:
    completed = subprocess.run(
        [sys.executable, str(GATE_PATH)],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["ok"] is True
    assert "qualification_required" not in payload
