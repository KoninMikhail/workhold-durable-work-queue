#!/usr/bin/env python3
"""Publish the coordinated client set, core first, to PyPI.

Refuses to upload unless ``WORKHOLD_PUBLISH=1``. Uses GitHub Actions
trusted publishing (OIDC); no API token is stored. Reads the package
order from ``release-packages.json``. Does not bump versions.
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


def _packages() -> list[str]:
    payload = json.loads(RELEASE_PACKAGES_PATH.read_text(encoding="utf-8"))
    packages = payload.get("pypi_packages")
    if not isinstance(packages, list) or not packages:
        raise PublishError("release-packages.json pypi_packages must be a non-empty list")
    names = [str(item) for item in packages]
    legacy = {"queue-client", "queue_client", "queue-service-client", "queue_service_client"}
    if legacy.intersection(names):
        raise PublishError("refusing to publish a legacy queue-client distribution")
    return names


def publish_packages() -> None:
    if os.environ.get("WORKHOLD_PUBLISH") != "1":
        raise PublishError("refusing to publish without WORKHOLD_PUBLISH=1")
    selected = os.environ.get("WORKHOLD_PUBLISH_ONLY", "").strip()
    packages = _packages()
    if selected:
        if selected not in packages:
            raise PublishError(f"WORKHOLD_PUBLISH_ONLY {selected!r} is not in release-packages.json")
        packages = [selected]
    staging = REPO_ROOT / "dist" / "publish"
    if staging.exists():
        shutil.rmtree(staging)
    for package in packages:
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
        publish_packages()
    except PublishError as exc:
        print(f"publish_client_packages: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
