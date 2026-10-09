"""Docker runtime image _FILE secret matrix (Phase 08 Plan 02 / DEP-05).

Proves NAME-only start, NAME_FILE materialization, fail-closed both-set /
missing / unreadable / empty / relative, CR/LF strip, symlink success, and
optional previous-token behavior. Value equality is exit-code only; fixture
sentinels must never appear in combined stdout+stderr.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "Dockerfile"
IMAGE_TAG = "queue:local"

CREDENTIAL_SENTINEL_CURRENT = "CRED_SENTINEL_current_a1b2c3d4"
CREDENTIAL_SENTINEL_PREVIOUS = "CRED_SENTINEL_previous_e5f6g7h8"
DSN_SENTINEL = "DSN_SENTINEL_p4ssw0rd_9z8y"
MANIFEST_SENTINEL = "MANIFEST_SENTINEL_q7w8e9r0"

_ALL_SENTINELS = (
    CREDENTIAL_SENTINEL_CURRENT,
    CREDENTIAL_SENTINEL_PREVIOUS,
    DSN_SENTINEL,
    MANIFEST_SENTINEL,
)


def _docker_available() -> bool:
    try:
        docker = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        compose = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False
    return docker.returncode == 0 and compose.returncode == 0


def _require_docker() -> None:
    if not _docker_available():
        pytest.fail(
            "Docker Engine + Compose are required for secret-file entrypoint tests"
        )


def _run(
    args: Sequence[str],
    *,
    timeout: float = 120.0,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        list(args),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged,
        check=False,
    )


def _image_inspect(tag: str) -> dict[str, Any]:
    result = _run(["docker", "image", "inspect", tag, "--format", "{{json .}}"])
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _assert_no_sentinels(blob: str) -> None:
    lowered = blob.lower()
    for sentinel in _ALL_SENTINELS:
        assert sentinel.lower() not in lowered, (
            f"sentinel leaked into diagnostics: {sentinel}"
        )


def _combined(result: subprocess.CompletedProcess[str]) -> str:
    return f"{result.stdout}\n{result.stderr}"


def _write_secret(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    # 0644 must not be rejected (C-05/C-06); Windows may not honor mode bits,
    # but Linux mount of a regular file is readable for uid 10001 in practice.
    try:
        path.chmod(0o644)
    except OSError:
        pass


def _compare_env_to_file_script(env_name: str, file_path: str) -> str:
    """One-liner: exit 0/1 only — never print secret values."""
    return (
        "import os; from pathlib import Path; "
        f"expected = Path({file_path!r}).read_bytes().rstrip(b'\\r\\n').decode(); "
        f"raise SystemExit(0 if os.environ.get({env_name!r}) == expected else 1)"
    )


def _materialize_and_compare(
    image: str,
    *,
    env_name: str,
    file_env: str,
    host_file: Path,
    container_file: str = "/secrets/value",
    extra_env: Sequence[str] = (),
) -> subprocess.CompletedProcess[str]:
    """Source helpers via --entrypoint /bin/sh, materialize, compare exit code only."""
    script = (
        ". /app/entrypoint.sh && "
        "materialize_secrets && "
        f"python -c {_compare_env_to_file_script(env_name, container_file)!r}"
    )
    args: list[str] = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "/bin/sh",
        "-v",
        f"{host_file.resolve()}:{container_file}:ro",
        "-e",
        f"{file_env}={container_file}",
        *extra_env,
        image,
        "-c",
        script,
    ]
    return _run(args, timeout=60.0)


@pytest.fixture(scope="module")
def runtime_image() -> Iterator[str]:
    _require_docker()
    build = _run(
        [
            "docker",
            "build",
            "-f",
            str(DOCKERFILE),
            "--target",
            "runtime",
            "-t",
            IMAGE_TAG,
            str(ROOT),
        ],
        timeout=600.0,
    )
    assert build.returncode == 0, build.stderr + build.stdout
    inspect = _image_inspect(IMAGE_TAG)
    config = inspect["Config"]
    assert config.get("User") in {"queue", "10001"}, config.get("User")
    entry = config.get("Entrypoint") or []
    assert entry[:1] == ["/app/entrypoint.sh"], entry
    yield IMAGE_TAG


def test_name_only_help_exits_zero(runtime_image: str) -> None:
    """(1) NAME set, NAME_FILE unset → default ENTRYPOINT --help exits 0."""
    result = _run(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            f"DATABASE_URL=postgresql://queue:queue@127.0.0.1:9/queue",
            "-e",
            f"QUEUE_API_BEARER_TOKEN={CREDENTIAL_SENTINEL_CURRENT}",
            runtime_image,
            "--help",
        ],
        timeout=60.0,
    )
    _assert_no_sentinels(_combined(result))
    assert result.returncode == 0, _combined(result)


def test_name_file_materializes_without_printing(runtime_image: str) -> None:
    """(2) NAME unset, NAME_FILE absolute readable → materialize; exit 0/1 only."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "db"
        _write_secret(host, DSN_SENTINEL.encode("utf-8"))
        result = _materialize_and_compare(
            runtime_image,
            env_name="DATABASE_URL",
            file_env="DATABASE_URL_FILE",
            host_file=host,
        )
        _assert_no_sentinels(_combined(result))
        assert result.returncode == 0, _combined(result)


def test_both_set_fails_closed_names_only(runtime_image: str) -> None:
    """(3) Both NAME and NAME_FILE non-empty → nonzero; names in stderr; no sentinels."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "db"
        _write_secret(host, DSN_SENTINEL.encode("utf-8"))
        result = _run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{host.resolve()}:/secrets/db:ro",
                "-e",
                f"DATABASE_URL={DSN_SENTINEL}",
                "-e",
                "DATABASE_URL_FILE=/secrets/db",
                runtime_image,
                "--help",
            ],
            timeout=60.0,
        )
        blob = _combined(result)
        _assert_no_sentinels(blob)
        assert result.returncode != 0, blob
        assert "DATABASE_URL" in result.stderr
        assert "DATABASE_URL_FILE" in result.stderr


def test_missing_path_fails_closed(runtime_image: str) -> None:
    """(4) NAME_FILE set, path missing → nonzero; names only."""
    result = _run(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "DATABASE_URL_FILE=/secrets/does-not-exist",
            runtime_image,
            "--help",
        ],
        timeout=60.0,
    )
    blob = _combined(result)
    _assert_no_sentinels(blob)
    assert result.returncode != 0, blob
    assert "DATABASE_URL_FILE" in result.stderr


def test_directory_path_fails_closed(runtime_image: str) -> None:
    """(5) NAME_FILE points at a directory → nonzero (not a regular file)."""
    result = _run(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "DATABASE_URL_FILE=/tmp",
            runtime_image,
            "--help",
        ],
        timeout=60.0,
    )
    blob = _combined(result)
    _assert_no_sentinels(blob)
    assert result.returncode != 0, blob
    assert "DATABASE_URL_FILE" in result.stderr


def test_empty_file_fails_closed(runtime_image: str) -> None:
    """(6) Empty secret file → nonzero."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "empty"
        _write_secret(host, b"")
        result = _run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{host.resolve()}:/secrets/empty:ro",
                "-e",
                "DATABASE_URL_FILE=/secrets/empty",
                runtime_image,
                "--help",
            ],
            timeout=60.0,
        )
        blob = _combined(result)
        _assert_no_sentinels(blob)
        assert result.returncode != 0, blob
        assert "DATABASE_URL_FILE" in result.stderr


@pytest.mark.parametrize(
    "suffix",
    [
        b"\n",
        b"\r\n",
    ],
    ids=["lf", "crlf"],
)
def test_trailing_crlf_stripped(runtime_image: str, suffix: bytes) -> None:
    """(7) Trailing LF / CRLF stripped — in-container compare equals stripped bytes."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "db"
        _write_secret(host, DSN_SENTINEL.encode("utf-8") + suffix)
        result = _materialize_and_compare(
            runtime_image,
            env_name="DATABASE_URL",
            file_env="DATABASE_URL_FILE",
            host_file=host,
        )
        _assert_no_sentinels(_combined(result))
        assert result.returncode == 0, _combined(result)


def test_relative_name_file_fails_closed(runtime_image: str) -> None:
    """(8) Relative NAME_FILE → nonzero."""
    result = _run(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "DATABASE_URL_FILE=secrets/db",
            runtime_image,
            "--help",
        ],
        timeout=60.0,
    )
    blob = _combined(result)
    _assert_no_sentinels(blob)
    assert result.returncode != 0, blob
    assert "DATABASE_URL_FILE" in result.stderr


def test_symlink_to_regular_file_succeeds(runtime_image: str) -> None:
    """(9) In-container symlink to a regular file → materialize success."""
    # Windows hosts cannot supply Linux symlinks; create them inside the container.
    # Mount into /tmp (sticky, writable by uid 10001) so ln -s succeeds.
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "real"
        _write_secret(host, DSN_SENTINEL.encode("utf-8"))
        inner = (
            "ln -sf /tmp/real /tmp/link && "
            ". /app/entrypoint.sh && "
            "materialize_secrets && "
            "python -c "
            + repr(_compare_env_to_file_script("DATABASE_URL", "/tmp/real"))
        )
        result = _run(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "/bin/sh",
                "-v",
                f"{host.resolve()}:/tmp/real:ro",
                "-e",
                "DATABASE_URL_FILE=/tmp/link",
                runtime_image,
                "-c",
                inner,
            ],
            timeout=60.0,
        )
        _assert_no_sentinels(_combined(result))
        assert result.returncode == 0, _combined(result)


def test_optional_previous_neither_set_help_ok(runtime_image: str) -> None:
    """(10a) Neither QUEUE_API_BEARER_TOKEN nor _FILE set → --help exits 0."""
    result = _run(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            f"DATABASE_URL=postgresql://queue:queue@127.0.0.1:9/queue",
            runtime_image,
            "--help",
        ],
        timeout=60.0,
    )
    _assert_no_sentinels(_combined(result))
    assert result.returncode == 0, _combined(result)


def test_optional_previous_file_materializes(runtime_image: str) -> None:
    """(10b) QUEUE_API_BEARER_TOKEN_FILE set to a good file → materialize success."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "prev"
        _write_secret(host, CREDENTIAL_SENTINEL_PREVIOUS.encode("utf-8"))
        result = _materialize_and_compare(
            runtime_image,
            env_name="QUEUE_API_BEARER_TOKEN_PREVIOUS",
            file_env="QUEUE_API_BEARER_TOKEN_PREVIOUS_FILE",
            host_file=host,
            container_file="/secrets/prev",
        )
        _assert_no_sentinels(_combined(result))
        assert result.returncode == 0, _combined(result)


def test_principal_manifest_file_materializes_and_unsets_file_name(
    runtime_image: str,
) -> None:
    """Manifest JSON is materialized without disclosure; *_FILE is removed."""
    manifest = json.dumps(
        {
            "schema_version": 1,
            "principals": [
                {
                    "principal_id": "producer-orders",
                    "role": "PRODUCER",
                    "queue_scopes": ["orders"],
                    "credentials": [
                        {
                            "generation_id": "current",
                            "secret": MANIFEST_SENTINEL,
                        }
                    ],
                }
            ],
        }
    )
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "principals.json"
        _write_secret(host, manifest.encode("utf-8"))
        script = (
            ". /app/entrypoint.sh && materialize_secrets && "
            "test -z \"${QUEUE_API_PRINCIPALS_MANIFEST_FILE+x}\" && "
            f"python -c {_compare_env_to_file_script('QUEUE_API_PRINCIPALS_MANIFEST', '/secrets/principals.json')!r}"
        )
        result = _run(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "/bin/sh",
                "-v",
                f"{host.resolve()}:/secrets/principals.json:ro",
                "-e",
                "QUEUE_API_PRINCIPALS_MANIFEST_FILE=/secrets/principals.json",
                runtime_image,
                "-c",
                script,
            ],
            timeout=60.0,
        )
        _assert_no_sentinels(_combined(result))
        assert result.returncode == 0, _combined(result)


def test_principal_manifest_name_and_file_are_exclusive(runtime_image: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "principals.json"
        _write_secret(host, b'{"schema_version":1,"principals":[]}')
        result = _run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{host.resolve()}:/secrets/principals.json:ro",
                "-e",
                f"QUEUE_API_PRINCIPALS_MANIFEST={MANIFEST_SENTINEL}",
                "-e",
                "QUEUE_API_PRINCIPALS_MANIFEST_FILE=/secrets/principals.json",
                runtime_image,
                "--help",
            ],
            timeout=60.0,
        )
        blob = _combined(result)
        _assert_no_sentinels(blob)
        assert result.returncode != 0
        assert "QUEUE_API_PRINCIPALS_MANIFEST" in result.stderr
        assert "QUEUE_API_PRINCIPALS_MANIFEST_FILE" in result.stderr


def test_empty_name_plus_file_takes_file(runtime_image: str) -> None:
    """Empty NAME + set NAME_FILE follows ${var:-} / empty-as-unset → file wins."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "db"
        _write_secret(host, DSN_SENTINEL.encode("utf-8"))
        result = _materialize_and_compare(
            runtime_image,
            env_name="DATABASE_URL",
            file_env="DATABASE_URL_FILE",
            host_file=host,
            extra_env=("-e", "DATABASE_URL="),
        )
        _assert_no_sentinels(_combined(result))
        assert result.returncode == 0, _combined(result)


def test_combined_logs_never_contain_sentinels(runtime_image: str) -> None:
    """(11) Combined stdout+stderr across happy + fail paths never contain sentinels."""
    with tempfile.TemporaryDirectory() as tmp:
        host = Path(tmp) / "db"
        _write_secret(host, DSN_SENTINEL.encode("utf-8"))
        cases = [
            _run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-e",
                    f"DATABASE_URL={DSN_SENTINEL}",
                    "-e",
                    f"QUEUE_API_BEARER_TOKEN={CREDENTIAL_SENTINEL_CURRENT}",
                    runtime_image,
                    "--help",
                ],
                timeout=60.0,
            ),
            _materialize_and_compare(
                runtime_image,
                env_name="DATABASE_URL",
                file_env="DATABASE_URL_FILE",
                host_file=host,
            ),
            _run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{host.resolve()}:/secrets/db:ro",
                    "-e",
                    f"DATABASE_URL={DSN_SENTINEL}",
                    "-e",
                    "DATABASE_URL_FILE=/secrets/db",
                    runtime_image,
                    "--help",
                ],
                timeout=60.0,
            ),
            _run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-e",
                    "DATABASE_URL_FILE=/secrets/missing",
                    runtime_image,
                    "--help",
                ],
                timeout=60.0,
            ),
        ]
        for result in cases:
            _assert_no_sentinels(_combined(result))
