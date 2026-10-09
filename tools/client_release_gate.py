#!/usr/bin/env python3
"""Atomic client package release gate (SDK-16).

Builds core then the three role wheels as one coordinated version set, writes a
wheel checksum / metadata inventory, runs a no-upload package-index dry run, and
fails closed on missing artifacts, version/dependency drift, or legacy
``queue-client`` presence.

Does not publish, tag, or bump versions.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
import zipfile
from email import parser as email_parser
from email.message import Message
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"
INVENTORY_PATH = DIST_DIR / "client-wheel-inventory.json"

CLIENT_DISTRIBUTIONS: tuple[str, ...] = (
    "workhold-client-core",
    "workhold-producer",
    "workhold-consumer",
    "workhold-admin",
)
FORBIDDEN_DISTRIBUTIONS = frozenset(
    {"queue-client", "queue_client", "queue-service-client", "queue_service_client"}
)
_CORE_REQUIREMENT_RE = re.compile(
    r"^workhold-client-core"
    r"(?P<extras>\[[A-Za-z0-9_,.-]+\])?"
    r"\s*(?P<spec>[^;]+?)"
    r"(?:\s*;\s*(?P<marker>.+))?$",
    re.IGNORECASE,
)
_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_BOUND_RE = re.compile(r"^(>=|<)\s*(\d+\.\d+\.\d+)$")
_ASYNC_MARKER_RE = re.compile(r"""^extra\s*==\s*(['"])async\1$""")
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class GateError(RuntimeError):
    """Fail-closed gate failure."""


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd or REPO_ROOT,
        env=os.environ.copy(),
        check=True,
        text=True,
        capture_output=True,
    )


def _load_toml(path: Path) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _project_version(path: Path) -> str:
    version = _load_toml(path)["project"]["version"]
    if not isinstance(version, str) or not _VERSION_RE.match(version):
        raise GateError(f"invalid version in {path}: {version!r}")
    return version


def _package_pyproject(dist: str) -> Path:
    return REPO_ROOT / "packages" / dist / "pyproject.toml"


def _assert_no_legacy_tree() -> None:
    if (REPO_ROOT / "packages" / "queue-client").exists():
        raise GateError("packages/queue-client must be absent")
    lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    for forbidden in FORBIDDEN_DISTRIBUTIONS:
        if f'name = "{forbidden}"' in lock:
            raise GateError(f"uv.lock references forbidden distribution {forbidden!r}")
    members = (
        _load_toml(REPO_ROOT / "pyproject.toml")
        .get("tool", {})
        .get("uv", {})
        .get("workspace", {})
        .get("members")
    )
    if not isinstance(members, list):
        raise GateError("tool.uv.workspace.members missing")
    expected = [f"packages/{name}" for name in CLIENT_DISTRIBUTIONS]
    if [str(m).replace("\\", "/") for m in members] != expected:
        raise GateError(f"workspace members drifted: {members!r}")


def _expected_core_interval(version: str) -> tuple[str, str, str]:
    match = _VERSION_RE.fullmatch(version)
    if match is None:
        raise GateError(f"invalid coordinated version: {version!r}")
    major, minor, _patch = (int(part) for part in match.groups())
    lower = f"{major}.{minor}.0"
    upper = f"{major}.{minor + 1}.0"
    return lower, upper, f">={lower},<{upper}"


def _is_core_requirement(requirement: object) -> bool:
    if not isinstance(requirement, str):
        return False
    name = re.split(r"[\s\[<>=!~;]", requirement, maxsplit=1)[0]
    return name.lower().replace("_", "-") == "workhold-client-core"


def _parse_core_requirement(
    requirement: str,
    *,
    dist: str,
    section: str,
) -> tuple[frozenset[str], str, str, str | None]:
    match = _CORE_REQUIREMENT_RE.fullmatch(requirement.strip())
    if match is None:
        raise GateError(
            f"{dist} {section} has malformed workhold-client-core requirement"
        )
    extras_raw = match.group("extras")
    extras = frozenset(
        part.strip().lower()
        for part in (extras_raw or "[]")[1:-1].split(",")
        if part.strip()
    )
    clauses = [clause.strip() for clause in match.group("spec").split(",")]
    parsed_bounds: dict[str, str] = {}
    for clause in clauses:
        bound = _BOUND_RE.fullmatch(clause)
        if bound is None or bound.group(1) in parsed_bounds:
            raise GateError(
                f"{dist} {section} must contain exactly one >= lower and one < upper bound"
            )
        parsed_bounds[bound.group(1)] = bound.group(2)
    if set(parsed_bounds) != {">=", "<"} or len(clauses) != 2:
        raise GateError(
            f"{dist} {section} must contain exactly one >= lower and one < upper bound"
        )
    return extras, parsed_bounds[">="], parsed_bounds["<"], match.group("marker")


def _assert_requirement_section(
    dist: str,
    section: str,
    requirements: object,
    *,
    expected_lower: str,
    expected_upper: str,
    expected_extras: frozenset[str],
) -> None:
    if not isinstance(requirements, list):
        raise GateError(f"{dist} {section} must be an array")
    core = [requirement for requirement in requirements if _is_core_requirement(requirement)]
    if len(core) != 1:
        raise GateError(
            f"{dist} {section} must contain exactly one "
            f"workhold-client-core requirement; got {len(core)}"
        )
    requirement = core[0]
    assert isinstance(requirement, str)
    extras, lower, upper, marker = _parse_core_requirement(
        requirement,
        dist=dist,
        section=section,
    )
    if marker is not None:
        raise GateError(f"{dist} {section} source requirement must not use a marker")
    if extras != expected_extras or lower != expected_lower or upper != expected_upper:
        expected = (
            "workhold-client-core"
            + ("[async]" if expected_extras else "")
            + f">={expected_lower},<{expected_upper}"
        )
        raise GateError(
            f"{dist} {section} core dependency drift; expected {expected}"
        )


def _assert_coordinated_versions() -> tuple[str, str]:
    versions = {_project_version(_package_pyproject(dist)) for dist in CLIENT_DISTRIBUTIONS}
    if len(versions) != 1:
        raise GateError(f"client package versions drifted: {versions}")
    version = next(iter(versions))
    root_version = _project_version(REPO_ROOT / "pyproject.toml")
    if version != root_version:
        raise GateError(
            f"client set version {version} != root repository version {root_version}"
        )
    expected_lower, expected_upper, expected_spec = _expected_core_interval(version)
    for dist in CLIENT_DISTRIBUTIONS[1:]:
        project = _load_toml(_package_pyproject(dist))["project"]
        _assert_requirement_section(
            dist,
            "dependencies",
            project.get("dependencies"),
            expected_lower=expected_lower,
            expected_upper=expected_upper,
            expected_extras=frozenset(),
        )
        optional = project.get("optional-dependencies")
        async_requirements = (
            optional.get("async") if isinstance(optional, dict) else None
        )
        _assert_requirement_section(
            dist,
            "optional-dependencies.async",
            async_requirements,
            expected_lower=expected_lower,
            expected_upper=expected_upper,
            expected_extras=frozenset({"async"}),
        )
    return version, expected_spec


def _string_list(data: dict[str, object], key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise GateError(f"release-packages.json {key} must be a list of strings")
    return list(value)


def _workflow_text() -> str:
    workflow_dir = REPO_ROOT / ".github" / "workflows"
    paths = sorted(workflow_dir.glob("*.yml"))
    if not paths:
        raise GateError("missing .github/workflows")
    return "\n".join(path.read_text(encoding="utf-8") for path in paths)


def _assert_ci_atomic_publish_shape() -> None:
    contract_path = REPO_ROOT / "release-packages.json"
    if not contract_path.is_file():
        raise GateError("missing release-packages.json")
    data = json.loads(contract_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise GateError("release-packages.json must be an object")
    packages = _string_list(data, "pypi_packages")
    if packages != list(CLIENT_DISTRIBUTIONS):
        raise GateError(
            "CI pypi_packages must list core then producer/consumer/admin only; "
            f"got {packages!r}"
        )
    if FORBIDDEN_DISTRIBUTIONS.intersection(packages):
        raise GateError("CI pypi_packages includes forbidden legacy distribution")
    version_files = _string_list(data, "extra_version_files")
    expected = [f"packages/{name}/pyproject.toml" for name in CLIENT_DISTRIBUTIONS]
    if version_files != expected:
        raise GateError(f"CI extra_version_files drifted: {version_files!r}")
    for relative in _string_list(data, "version_py_files"):
        if not (REPO_ROOT / relative).is_file():
            raise GateError(f"missing version module {relative}")

    config = json.loads((REPO_ROOT / "release-please-config.json").read_text(encoding="utf-8"))
    package_config = config["packages"]["."]
    extra = package_config["extra-files"]
    extra_paths = [item["path"] for item in extra]
    expected_extra = ["pyproject.toml", *version_files]
    if extra_paths != expected_extra:
        raise GateError(f"release-please extra-files drifted: {extra_paths!r}")
    with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
        root_version = tomllib.load(fh)["project"]["version"]
    initial_version = package_config.get("initial-version") or config.get("initial-version")
    if initial_version != "1.0.0":
        raise GateError(
            "release-please initial-version must be 1.0.0 so the first release "
            f"is 1.0.0; got {initial_version!r}"
        )
    manifest = json.loads(
        (REPO_ROOT / ".release-please-manifest.json").read_text(encoding="utf-8")
    )
    manifest_version = manifest.get(".")
    if manifest_version == "0.0.0":
        if root_version != initial_version:
            raise GateError(
                "until the first release, root project.version must equal "
                f"initial-version {initial_version}; got {root_version!r}"
            )
    elif manifest_version != root_version:
        raise GateError(
            "release-please manifest drifted from root project.version: "
            f"{manifest_version!r} != {root_version!r}"
        )

    workflow_text = _workflow_text()
    for token in (
        "tools/client_release_gate.py",
        "tools/publish_client_packages.py",
        "release-please-config.json",
    ):
        if token not in workflow_text:
            raise GateError(f"GitHub workflows missing {token!r}")
    release_yml = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )
    gate_at = release_yml.find("tools/client_release_gate.py")
    publish_at = release_yml.find("tools/publish_client_packages.py")
    if gate_at < 0 or publish_at < 0 or gate_at > publish_at:
        raise GateError("release workflow must run the client release gate before client publish")
    publish_script = (REPO_ROOT / "tools" / "publish_client_packages.py").read_text(
        encoding="utf-8"
    )
    if "id-token: write" not in release_yml:
        raise GateError("release workflow must grant id-token: write for PyPI trusted publishing")
    if "release-packages.json" not in publish_script or "WORKHOLD_PUBLISH" not in publish_script:
        raise GateError(
            "publish_client_packages.py must read release-packages.json and require WORKHOLD_PUBLISH"
        )


def _latest_wheel(dist_name: str) -> Path:
    underscore = dist_name.replace("-", "_")
    wheels = sorted(DIST_DIR.glob(f"{underscore}-*.whl"))
    if not wheels:
        wheels = sorted(DIST_DIR.glob(f"{dist_name}-*.whl"))
    if not wheels:
        raise GateError(f"missing wheel for {dist_name!r} under {DIST_DIR}")
    return wheels[-1]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _wheel_metadata(wheel: Path) -> dict[str, object]:
    with zipfile.ZipFile(wheel) as zf:
        meta_names = [n for n in zf.namelist() if n.endswith(".dist-info/METADATA")]
        if len(meta_names) != 1:
            raise GateError(f"expected one METADATA in {wheel.name}")
        meta = email_parser.Parser().parsestr(zf.read(meta_names[0]).decode("utf-8"))
    requires = meta.get_all("Requires-Dist") or []
    return {
        "name": meta.get("Name"),
        "version": meta.get("Version"),
        "requires_dist": list(requires),
        "summary": meta.get("Summary"),
        "requires_python": meta.get("Requires-Python"),
    }


def _clean_client_wheels() -> None:
    DIST_DIR.mkdir(exist_ok=True)
    prefixes = tuple(
        name.replace("-", "_") + "-" for name in (*CLIENT_DISTRIBUTIONS, "queue_client")
    )
    for stale in DIST_DIR.glob("*.whl"):
        if stale.name.startswith(prefixes) or stale.name.startswith("queue-client-"):
            stale.unlink()


def _build_wheels() -> dict[str, Path]:
    _clean_client_wheels()
    wheels: dict[str, Path] = {}
    # Core first, then roles — dependents must not publish without core artifacts.
    for dist in CLIENT_DISTRIBUTIONS:
        _run(["uv", "build", "--package", dist, "--out-dir", str(DIST_DIR)])
        wheels[dist] = _latest_wheel(dist)
    return wheels


def _assert_role_wheel_core_dependencies(
    dist: str,
    metadata: Message | list[str],
    *,
    expected_lower: str,
    expected_upper: str,
) -> None:
    requirements = (
        metadata.get_all("Requires-Dist") or []
        if isinstance(metadata, Message)
        else metadata
    )
    core = [requirement for requirement in requirements if _is_core_requirement(requirement)]
    base: list[tuple[str, str, str, str | None]] = []
    async_extra: list[tuple[str, str, str, str | None]] = []
    for requirement in core:
        assert isinstance(requirement, str)
        parsed = _parse_core_requirement(
            requirement,
            dist=dist,
            section="wheel Requires-Dist",
        )
        extras, _lower, _upper, _marker = parsed
        if not extras:
            base.append(parsed)
        elif extras == frozenset({"async"}):
            async_extra.append(parsed)
        else:
            raise GateError(
                f"{dist} wheel Requires-Dist has unsupported core extras"
            )

    if len(base) != 1:
        raise GateError(
            f"{dist} wheel base core requirement count is {len(base)}; expected 1"
        )
    base_extras, base_lower, base_upper, base_marker = base[0]
    if (
        base_extras
        or base_marker is not None
        or base_lower != expected_lower
        or base_upper != expected_upper
    ):
        raise GateError(
            f"{dist} wheel base core dependency drift; expected "
            f">={expected_lower},<{expected_upper}"
        )

    if len(async_extra) != 1:
        raise GateError(
            f"{dist} wheel async core requirement count is {len(async_extra)}; expected 1"
        )
    async_extras, async_lower, async_upper, async_marker = async_extra[0]
    if (
        async_extras != frozenset({"async"})
        or async_marker is None
        or _ASYNC_MARKER_RE.fullmatch(async_marker.strip()) is None
        or async_lower != expected_lower
        or async_upper != expected_upper
    ):
        raise GateError(
            f"{dist} wheel async core dependency drift; expected "
            f"workhold-client-core[async]>={expected_lower},<{expected_upper} "
            "with extra == 'async'"
        )


def _bounds_from_expected_spec(expected_spec: str) -> tuple[str, str]:
    clauses = expected_spec.split(",")
    if len(clauses) != 2:
        raise GateError(f"invalid derived core interval: {expected_spec!r}")
    lower = _BOUND_RE.fullmatch(clauses[0])
    upper = _BOUND_RE.fullmatch(clauses[1])
    if lower is None or upper is None or lower.group(1) != ">=" or upper.group(1) != "<":
        raise GateError(f"invalid derived core interval: {expected_spec!r}")
    return lower.group(2), upper.group(2)


def _write_inventory(
    wheels: dict[str, Path],
    *,
    version: str,
    git_sha: str,
    expected_core_spec: str,
) -> Path:
    expected_lower, expected_upper = _bounds_from_expected_spec(expected_core_spec)
    entries = []
    for dist in CLIENT_DISTRIBUTIONS:
        wheel = wheels[dist]
        meta = _wheel_metadata(wheel)
        if meta["name"] != dist:
            raise GateError(f"wheel name {meta['name']!r} != distribution {dist!r}")
        if meta["version"] != version:
            raise GateError(
                f"wheel version drift for {dist}: {meta['version']!r} != {version!r}"
            )
        if dist != "workhold-client-core":
            requires_dist = meta["requires_dist"]
            assert isinstance(requires_dist, list)
            _assert_role_wheel_core_dependencies(
                dist,
                requires_dist,
                expected_lower=expected_lower,
                expected_upper=expected_upper,
            )
        entries.append(
            {
                "distribution": dist,
                "filename": wheel.name,
                "sha256": _sha256(wheel),
                "size_bytes": wheel.stat().st_size,
                "metadata": meta,
            }
        )
    dependency_graph = {
        "workhold-client-core": [],
        "workhold-producer": ["workhold-client-core"],
        "workhold-consumer": ["workhold-client-core"],
        "workhold-admin": ["workhold-client-core"],
    }
    payload = {
        "git_sha": git_sha,
        "coordinated_version": version,
        "distributions": list(CLIENT_DISTRIBUTIONS),
        "dependency_graph": dependency_graph,
        "forbidden_absent": sorted(FORBIDDEN_DISTRIBUTIONS),
        "wheels": entries,
    }
    INVENTORY_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return INVENTORY_PATH


def _dry_run_publish(wheels: dict[str, Path]) -> str:
    files = [str(wheels[dist]) for dist in CLIENT_DISTRIBUTIONS]
    # No-upload package-index dry run: validates artifacts without registry write.
    # Trusted-publishing discovery can exit non-zero outside CI OIDC; still require
    # every wheel to be enumerated for the dry-run upload plan.
    result = subprocess.run(
        [
            "uv",
            "publish",
            "--dry-run",
            "--trusted-publishing",
            "never",
            *files,
        ],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            # Satisfy credential presence checks without contacting a registry write.
            "UV_PUBLISH_TOKEN": os.environ.get("UV_PUBLISH_TOKEN", "dry-run-not-a-secret"),
        },
        check=False,
        text=True,
        capture_output=True,
    )
    combined = (result.stdout or "") + (result.stderr or "")
    lowered = combined.lower().replace("\\", "/")
    for wheel in files:
        if Path(wheel).name.lower() not in lowered:
            raise GateError(
                f"uv publish --dry-run did not enumerate {Path(wheel).name}:\n{combined}"
            )
    # Reject legacy distribution names as path tokens (not substrings of
    # workhold-client-core / workhold_client_testing).
    legacy = re.compile(
        r"(?<![a-z0-9_-])(?:queue-client|queue_client|queue_service_client)(?![a-z0-9_-])",
        re.IGNORECASE,
    )
    if legacy.search(combined):
        raise GateError("dry-run output mentions forbidden legacy package")
    if result.returncode != 0 and "checking" not in lowered:
        raise GateError(f"uv publish --dry-run failed:\n{combined}")
    return combined.strip() or "uv publish --dry-run ok"



def _git_sha() -> str:
    """Full HEAD SHA.

    ``git rev-parse`` when the binary exists. The publication test image has no
    git; GitLab still sets ``CI_COMMIT_SHA``, and a normal checkout can be read
    from ``.git`` without the binary.
    """
    try:
        result = _run(["git", "rev-parse", "HEAD"])
    except FileNotFoundError:
        return _git_sha_without_git()
    sha = result.stdout.strip().lower()
    if not _FULL_SHA_RE.fullmatch(sha):
        raise GateError(f"git rev-parse HEAD returned {sha!r}")
    return sha


def _git_sha_without_git() -> str:
    ci_sha = os.environ.get("CI_COMMIT_SHA", "").strip().lower()
    if _FULL_SHA_RE.fullmatch(ci_sha):
        return ci_sha
    checked_out = _checked_out_sha()
    if checked_out is not None:
        return checked_out
    raise GateError(
        "git is not installed and CI_COMMIT_SHA is unset; cannot record the release commit"
    )


def _checked_out_sha() -> str | None:
    git_dir = REPO_ROOT / ".git"
    if not git_dir.is_dir():
        return None
    head_path = git_dir / "HEAD"
    if not head_path.is_file():
        return None
    head = head_path.read_text(encoding="utf-8").strip()
    direct = head.lower()
    if _FULL_SHA_RE.fullmatch(direct):
        return direct
    if not head.startswith("ref: "):
        return None
    ref = head.removeprefix("ref: ").strip()
    ref_file = git_dir / ref
    if ref_file.is_file():
        sha = ref_file.read_text(encoding="utf-8").strip().lower()
        if _FULL_SHA_RE.fullmatch(sha):
            return sha
    packed = git_dir / "packed-refs"
    if not packed.is_file():
        return None
    for line in packed.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or line.startswith("^"):
            continue
        sha, _, name = line.partition(" ")
        if name.strip() == ref and _FULL_SHA_RE.fullmatch(sha.strip().lower()):
            return sha.strip().lower()
    return None


def run() -> dict[str, object]:
    _assert_no_legacy_tree()
    version, expected_core_spec = _assert_coordinated_versions()
    _assert_ci_atomic_publish_shape()
    git_sha = _git_sha()
    wheels = _build_wheels()
    inventory = _write_inventory(
        wheels,
        version=version,
        git_sha=git_sha,
        expected_core_spec=expected_core_spec,
    )
    dry_run = _dry_run_publish(wheels)
    return {
        "ok": True,
        "git_sha": git_sha,
        "version": version,
        "inventory": str(inventory.relative_to(REPO_ROOT)),
        "wheels": {dist: path.name for dist, path in wheels.items()},
        "dry_run": dry_run.splitlines()[-1] if dry_run else "ok",
    }


def main() -> int:
    try:
        result = run()
    except (GateError, subprocess.CalledProcessError) as exc:
        print(f"client_release_gate: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
