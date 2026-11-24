#!/usr/bin/env python3
"""Publish workhold-mcp to PyPI on its own release train.

Refuses to upload unless ``WORKHOLD_PUBLISH=1``. Uses GitHub Actions
trusted publishing (OIDC); no API token is stored. Reads the single
package name from ``release-packages.json`` ``mcp_package``. Does not
publish the coordinated client set and does not bump versions.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_PACKAGES_PATH = REPO_ROOT / "release-packages.json"


class PublishError(RuntimeError):
    """Publication cannot start or a package upload failed."""


def _mcp_package() -> str:
    payload = json.loads(RELEASE_PACKAGES_PATH.read_text(encoding="utf-8"))
    name = payload.get("mcp_package")
    if not isinstance(name, str) or not name:
        raise PublishError("release-packages.json mcp_package must name one package")
    coordinated = payload.get("pypi_packages")
    if not isinstance(coordinated, list):
        raise PublishError("release-packages.json pypi_packages must be a list")
    coordinated_names = [str(item) for item in coordinated]
    if name in coordinated_names:
        raise PublishError(
            f"{name} is in pypi_packages; the MCP publisher refuses coordinated clients"
        )
    legacy = {"queue-client", "queue_client", "queue-service-client", "queue_service_client"}
    if name in legacy:
        raise PublishError("refusing to publish a legacy queue-client distribution")
    return name


def publish_package() -> None:
    if os.environ.get("WORKHOLD_PUBLISH") != "1":
        raise PublishError("refusing to publish without WORKHOLD_PUBLISH=1")
    package = _mcp_package()
    staging = REPO_ROOT / "dist" / "publish-mcp"
    if staging.exists():
        shutil.rmtree(staging)
    out = staging / package
    out.mkdir(parents=True)
    print(f"build {package}")
    subprocess.run(
        ["uv", "build", "--package", package, "--out-dir", str(out)],
        cwd=REPO_ROOT,
        check=True,
    )
    artifacts = sorted(
        path
        for path in out.iterdir()
        if path.is_file() and (path.suffix == ".whl" or path.name.endswith(".tar.gz"))
    )
    if not artifacts:
        raise PublishError(f"uv build produced no artifacts for {package}")
    print(f"publish {package} -> https://pypi.org/project/{package}/")
    subprocess.run(
        [
            "uv",
            "publish",
            "--trusted-publishing",
            "always",
            "--check-url",
            "https://pypi.org/simple",
            *[str(path) for path in artifacts],
        ],
        cwd=REPO_ROOT,
        check=True,
    )


def main() -> int:
    try:
        publish_package()
    except PublishError as exc:
        print(f"publish_mcp_package: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
