"""Clean-wheel isolation matrix for core + three role clients (SDK-16 / QUAL-01).

Builds fresh wheels, installs core and each role independently, and rejects:
server dependencies, cross-role imports, async HTTP deps in base installs, and
every ``queue-client`` / ``queue_service_client`` artifact.
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

CLIENT_DISTRIBUTIONS: tuple[str, ...] = (
    "queue-service-client-core",
    "queue-service-producer",
    "queue-service-consumer",
    "queue-service-admin",
)
ROLE_DISTRIBUTIONS: tuple[str, ...] = CLIENT_DISTRIBUTIONS[1:]
FORBIDDEN_DISTS = frozenset({"queue-client", "queue_client", "queue-service-client"})
SERVER_DEP_NAMES = frozenset({"queue", "queue-service", "queue_service"})
ASYNC_MARKERS = ("httpx", "anyio", "httpcore")


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd or REPO_ROOT,
        env=os.environ.copy(),
        check=True,
        text=True,
        capture_output=True,
    )


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


def _requirement_name(req: str) -> str:
    token = req.split(";", 1)[0].strip()
    name = re.split(r"[<>=!~\[]", token, maxsplit=1)[0].strip()
    return name.lower().replace("_", "-")


def _base_requires(meta: email.message.Message) -> list[str]:
    requires = meta.get_all("Requires-Dist") or []
    pure: list[str] = []
    for req in requires:
        if ";" in req and "extra" in req.split(";", 1)[1]:
            continue
        pure.append(req)
    return pure


@pytest.fixture(scope="module")
def clean_wheels() -> dict[str, Path]:
    DIST_DIR.mkdir(exist_ok=True)
    for stale in DIST_DIR.glob("*.whl"):
        if any(
            stale.name.startswith(prefix)
            for prefix in (
                "queue_service_client_core-",
                "queue_service_producer-",
                "queue_service_consumer-",
                "queue_service_admin-",
                "queue_client-",
                "queue-client-",
            )
        ):
            stale.unlink()
    for dist in CLIENT_DISTRIBUTIONS:
        _run(["uv", "build", "--package", dist, "--out-dir", str(DIST_DIR)])
    wheels = {dist: _latest_wheel(dist.replace("-", "_")) for dist in CLIENT_DISTRIBUTIONS}
    return wheels


def test_no_prototype_or_legacy_client_wheels(clean_wheels: dict[str, Path]) -> None:
    assert FORBIDDEN_DISTS.isdisjoint(clean_wheels)
    leftovers = (
        list(DIST_DIR.glob("queue_client-*.whl"))
        + list(DIST_DIR.glob("queue-client-*.whl"))
        + list(DIST_DIR.glob("queue_service_client-*.whl"))
    )
    assert leftovers == [], f"forbidden client artifacts present: {leftovers}"
    assert not (REPO_ROOT / "packages" / "queue-client").exists()


@pytest.mark.parametrize("dist_name", list(CLIENT_DISTRIBUTIONS))
def test_base_wheel_excludes_server_and_async_http_deps(
    clean_wheels: dict[str, Path],
    dist_name: str,
) -> None:
    meta = _wheel_metadata(clean_wheels[dist_name])
    assert meta.get("Name") == dist_name
    for req in _base_requires(meta):
        name = _requirement_name(req)
        assert name not in SERVER_DEP_NAMES, f"{dist_name} depends on server: {req}"
        assert name not in FORBIDDEN_DISTS, f"{dist_name} depends on legacy: {req}"
        lowered = req.lower()
        for marker in ASYNC_MARKERS:
            assert marker not in lowered or "extra" in lowered, (
                f"{dist_name} base install must not pull async HTTP dep: {req}"
            )


@pytest.mark.parametrize("dist_name", list(ROLE_DISTRIBUTIONS))
def test_role_base_install_cannot_import_server_or_cross_role(
    clean_wheels: dict[str, Path],
    dist_name: str,
) -> None:
    import_name = dist_name.replace("-", "_")
    role_wheel = clean_wheels[dist_name]
    core_wheel = clean_wheels["queue-service-client-core"]
    other_roles = [d.replace("-", "_") for d in ROLE_DISTRIBUTIONS if d != dist_name]
    forbidden = ["queue_service", "queue_service_client", *other_roles]

    with tempfile.TemporaryDirectory(prefix=f"clean-{import_name}-") as tmp:
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
        py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        probe = f"""
import importlib
import sys

mod = importlib.import_module({import_name!r})
assert getattr(mod, "__version__", None)

# Base install must not have httpx until [async] extra is selected.
try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    pass
else:
    # Core/role base wheels must not depend on httpx; if present it came from env leak.
    raise SystemExit("httpx must not be importable from base role install")

for name in {forbidden!r}:
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        pass
    else:
        raise SystemExit(f"forbidden import leaked: {{name}}")

print("ok", mod.__name__, mod.__version__)
"""
        result = _run([str(py), "-c", probe])
        assert "ok" in result.stdout


def test_async_extra_installs_httpx_for_core(clean_wheels: dict[str, Path]) -> None:
    core_wheel = clean_wheels["queue-service-client-core"]
    with tempfile.TemporaryDirectory(prefix="clean-core-async-") as tmp:
        venv = Path(tmp) / "venv"
        _run(["uv", "venv", str(venv)])
        # Install wheel then add the documented async extra from the built dist.
        _run(["uv", "pip", "install", "--python", str(venv), str(core_wheel)])
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(venv),
                f"{core_wheel}[async]",
            ]
        )
        py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        result = _run([str(py), "-c", "import httpx; print(httpx.__version__)"])
        assert result.stdout.strip()
