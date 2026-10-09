"""Role package boundary proofs (SDK-03 / SDK-04).

Public distributions are independently installable shells that depend only on
compatible exact-minor ``workhold-client-core``. Operation implementations
belong to later phases; this file guards packaging isolation only.
"""

from __future__ import annotations

import email.message
import email.parser
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DIST_DIR = REPO_ROOT / "dist"

ROLE_PACKAGES: dict[str, dict[str, object]] = {
    "workhold-producer": {
        "import": "workhold_producer",
        "wheel_prefix": "workhold_producer",
        "public_names": ("ProducerClient",),
        "forbidden_imports": (
            "workhold_admin",
            "workhold_consumer",
            "workhold",
            "queue_service_client",
        ),
    },
    "workhold-consumer": {
        "import": "workhold_consumer",
        "wheel_prefix": "workhold_consumer",
        "public_names": ("ConsumerClient", "ConsumerSupervisor"),
        "forbidden_imports": (
            "workhold_admin",
            "workhold_producer",
            "workhold",
            "queue_service_client",
        ),
    },
    "workhold-admin": {
        "import": "workhold_admin",
        "wheel_prefix": "workhold_admin",
        "public_names": ("ObserverClient", "AdminClient", "BreakGlassClient"),
        "forbidden_imports": (
            "workhold_producer",
            "workhold_consumer",
            "workhold",
            "queue_service_client",
        ),
    },
}

FORBIDDEN_TOP_LEVEL = {
    "workhold",
    "queue_service_client",
    "_workhold_client_core",
}
SERVER_DEP_NAMES = {"queue", "workhold"}
OTHER_ROLE_DEPS = {
    "workhold-producer",
    "workhold-consumer",
    "workhold-admin",
    "queue-client",
}

_CORE_EXACT_MINOR = re.compile(
    r"^workhold-client-core(?P<spec>\s*[^;]*?)(?:\s*;.*)?$",
    re.IGNORECASE,
)


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
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
        assert len(meta_names) == 1, f"expected one METADATA in {wheel.name}"
        raw = zf.read(meta_names[0]).decode("utf-8")
    return email.parser.Parser().parsestr(raw)


def _top_level_packages(wheel: Path) -> set[str]:
    with zipfile.ZipFile(wheel) as zf:
        tops: set[str] = set()
        for name in zf.namelist():
            if name.endswith("/") or ".dist-info/" in name or ".data/" in name:
                continue
            parts = Path(name).parts
            if parts:
                tops.add(parts[0])
        return tops


def _requirement_name(req: str) -> str:
    token = req.split(";", 1)[0].strip()
    name = re.split(r"[<>=!~\[]", token, maxsplit=1)[0].strip()
    return name.lower().replace("_", "-")


def _assert_exact_minor_core(requires: list[str]) -> None:
    core_reqs = [r for r in requires if _requirement_name(r) == "workhold-client-core"]
    assert len(core_reqs) == 1, f"expected exactly one core dependency, got {requires}"
    m = _CORE_EXACT_MINOR.match(core_reqs[0].strip())
    assert m is not None, core_reqs[0]
    spec = m.group("spec").strip()
    assert spec, f"core dependency must pin a compatible exact-minor range: {core_reqs[0]}"
    ok = False
    if re.search(r"~=\s*0\.1(\.0)?\b", spec):
        ok = True
    if re.search(r">=\s*0\.1(\.0)?\b", spec) and re.search(r"<\s*0\.2(\.0)?\b", spec):
        ok = True
    assert ok, f"core dependency must be exact-minor compatible with 0.1.x: {core_reqs[0]}"


@pytest.fixture(scope="module")
def role_and_core_wheels() -> dict[str, Path]:
    DIST_DIR.mkdir(exist_ok=True)
    patterns = (
        "workhold_producer-*.whl",
        "workhold_consumer-*.whl",
        "workhold_admin-*.whl",
        "workhold_client_core-*.whl",
        "workhold-producer-*.whl",
        "workhold-consumer-*.whl",
        "workhold-admin-*.whl",
        "workhold-client-core-*.whl",
    )
    for pattern in patterns:
        for stale in DIST_DIR.glob(pattern):
            stale.unlink()

    _run(
        [
            "uv",
            "build",
            "--package",
            "workhold-client-core",
            "--out-dir",
            str(DIST_DIR),
        ]
    )
    for dist in ROLE_PACKAGES:
        _run(["uv", "build", "--package", dist, "--out-dir", str(DIST_DIR)])

    wheels = {
        "workhold-client-core": _latest_wheel("workhold_client_core"),
    }
    for dist, meta in ROLE_PACKAGES.items():
        wheels[dist] = _latest_wheel(str(meta["wheel_prefix"]))
    return wheels


@pytest.mark.parametrize("dist_name", list(ROLE_PACKAGES))
def test_role_wheel_name_and_top_level(
    role_and_core_wheels: dict[str, Path],
    dist_name: str,
) -> None:
    meta_cfg = ROLE_PACKAGES[dist_name]
    wheel = role_and_core_wheels[dist_name]
    meta = _wheel_metadata(wheel)
    assert meta.get("Name") == dist_name
    assert wheel.name.startswith(f"{meta_cfg['wheel_prefix']}-")

    tops = _top_level_packages(wheel)
    assert tops == {meta_cfg["import"]}
    for forbidden in FORBIDDEN_TOP_LEVEL | {
        ROLE_PACKAGES[other]["import"]
        for other in ROLE_PACKAGES
        if other != dist_name
    }:
        assert forbidden not in tops, f"{dist_name} leaked top-level {forbidden}"


@pytest.mark.parametrize("dist_name", list(ROLE_PACKAGES))
def test_role_wheel_depends_only_on_exact_minor_core(
    role_and_core_wheels: dict[str, Path],
    dist_name: str,
) -> None:
    meta = _wheel_metadata(role_and_core_wheels[dist_name])
    requires = meta.get_all("Requires-Dist") or []
    pure: list[str] = []
    for req in requires:
        if ";" in req and "extra" in req.split(";", 1)[1]:
            continue
        pure.append(req)

    _assert_exact_minor_core(pure)
    for req in pure:
        name = _requirement_name(req)
        assert name == "workhold-client-core", f"unexpected runtime dep: {req}"
        assert name not in SERVER_DEP_NAMES
        assert name not in (OTHER_ROLE_DEPS - {dist_name})


@pytest.mark.parametrize("dist_name", list(ROLE_PACKAGES))
def test_clean_venv_imports_role_shell_without_cross_role_or_server(
    role_and_core_wheels: dict[str, Path],
    dist_name: str,
) -> None:
    cfg = ROLE_PACKAGES[dist_name]
    import_name = str(cfg["import"])
    public_names = tuple(cfg["public_names"])  # type: ignore[arg-type]
    forbidden = tuple(cfg["forbidden_imports"])  # type: ignore[arg-type]
    role_wheel = role_and_core_wheels[dist_name]
    core_wheel = role_and_core_wheels["workhold-client-core"]

    with tempfile.TemporaryDirectory(prefix=f"{import_name}-boundary-") as tmp:
        venv = Path(tmp) / "venv"
        _run(["uv", "venv", str(venv)])
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(venv),
                str(core_wheel),
                str(role_wheel),
            ]
        )
        public_repr = ", ".join(repr(n) for n in public_names)
        forbidden_repr = ", ".join(repr(n) for n in forbidden)
        probe = f"""
import importlib

mod = importlib.import_module({import_name!r})
assert getattr(mod, "__version__", None), "missing __version__"
for name in ({public_repr},):
    assert hasattr(mod, name), f"missing public shell {{name}}"
    cls = getattr(mod, name)
    assert isinstance(cls, type), f"{{name}} must be a class shell"

for name in ({forbidden_repr},):
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        pass
    else:
        raise SystemExit(f"{{name}} must not be importable from {{mod.__name__}}-only install")

if {import_name!r} != "workhold_admin":
    for name in ("workhold_admin", "workhold_admin.break_glass"):
        try:
            importlib.import_module(name)
        except ModuleNotFoundError:
            pass
        else:
            raise SystemExit(f"{{name}} leaked into non-admin install")

print("ok", mod.__version__)
"""
        py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        result = _run([str(py), "-c", probe])
        assert "ok" in result.stdout
