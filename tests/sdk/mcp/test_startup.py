"""Process startup for the local MCP observer server."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_CANARY = "WHMCP-TOKEN-CANARY"
_PUBLIC = "http://127.0.0.1:9"
_ADMIN = "http://127.0.0.1:10"
_USERINFO_URL = "https://user:secret@host"
_FTP_URL = "ftp://example.com"
_BLANK_PUBLIC = "http://127.0.0.1:9/WHMCP-TOKEN-CANARY"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PROCESS_ENV_KEYS = frozenset(
    {
        "systemroot",
        "systemdrive",
        "windir",
        "path",
        "pathext",
        "comspec",
        "temp",
        "tmp",
        "userprofile",
        "homedrive",
        "homepath",
        "localappdata",
        "appdata",
        "programfiles",
        "programdata",
        "programfiles(x86)",
    }
)


def _process_env() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key.lower() in _PROCESS_ENV_KEYS
    }


def _run(env: dict[str, str], args: tuple[str, ...]) -> subprocess.CompletedProcess[bytes]:
    merged = _process_env()
    merged.update(env)
    return subprocess.run(
        [sys.executable, "-m", "workhold_mcp", *args],
        cwd=_REPO_ROOT,
        env=merged,
        capture_output=True,
        check=False,
        timeout=20,
    )


def _valid_env() -> dict[str, str]:
    return {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
        "WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY,
    }


@pytest.mark.parametrize(
    ("env", "args", "forbidden"),
    [
        pytest.param(
            {"WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY},
            (),
            (_CANARY,),
            id="missing-env",
        ),
        pytest.param(
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _BLANK_PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": "",
            },
            (),
            (_CANARY, _BLANK_PUBLIC, _ADMIN),
            id="blank-token",
        ),
        pytest.param(
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _USERINFO_URL,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY,
            },
            (),
            (_CANARY, _USERINFO_URL, _ADMIN),
            id="userinfo-url",
        ),
        pytest.param(
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
                "WORKHOLD_MCP_ADMIN_BASE_URL": _FTP_URL,
                "WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY,
            },
            (),
            (_CANARY, _PUBLIC, _FTP_URL),
            id="non-http-scheme",
        ),
        pytest.param(
            {
                **_valid_env(),
                "WORKHOLD_MCP_CONNECT_TIMEOUT_S": "0",
            },
            (),
            (_CANARY, _PUBLIC, _ADMIN),
            id="non-positive-timeout",
        ),
        pytest.param(
            {
                **_valid_env(),
                "WORKHOLD_MCP_EXTRA": _CANARY,
            },
            (),
            (_CANARY, _PUBLIC, _ADMIN),
            id="unknown-setting",
        ),
        pytest.param(
            {
                **_valid_env(),
                "WORKHOLD_MCP_VERIFY_TLS": "yes",
            },
            (),
            (_CANARY, _PUBLIC, _ADMIN, "yes"),
            id="verify-tls",
        ),
        pytest.param(
            _valid_env(),
            ("not-a-flag",),
            (_CANARY, _PUBLIC, _ADMIN, "not-a-flag"),
            id="bare-argument",
        ),
        pytest.param(
            _valid_env(),
            ("--token", _CANARY),
            (_CANARY, _PUBLIC, _ADMIN),
            id="token-flag",
        ),
    ],
)
def test_invalid_startup_exits_2(
    env: dict[str, str],
    args: tuple[str, ...],
    forbidden: tuple[str, ...],
) -> None:
    completed = _run(env, args)
    combined = completed.stdout + completed.stderr
    assert completed.returncode == 2
    assert completed.stdout == b""
    lines = completed.stderr.splitlines()
    assert len(lines) == 1
    assert lines[0].startswith(b"workhold-mcp: invalid configuration")
    for item in forbidden:
        assert item.encode("utf-8") not in combined
    assert _CANARY.encode("utf-8") not in combined
