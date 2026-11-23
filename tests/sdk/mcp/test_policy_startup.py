"""Startup tool policy: omitted observer set, allowlists, and exit 2.

Success paths call ``resolve_tool_policy`` directly. A valid policy would
block on stdio, so subprocess covers failure paths only.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_CANARY = "WHMCP-TOKEN-CANARY"
_KEY_CANARY = "WHMCP-KEY-CANARY"
_PUBLIC = "http://127.0.0.1:9"
_ADMIN = "http://127.0.0.1:10"
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
_PRESET_ENV = "WORKHOLD_MCP_PRESET"
_ALLOWED_ENV = "WORKHOLD_MCP_ALLOWED_TOOLS"
_KNOWN_ENV = (
    _PRESET_ENV,
    _ALLOWED_ENV,
    "WORKHOLD_MCP_PRODUCER_TOKEN",
    "WORKHOLD_MCP_ADMIN_TOKEN",
    "WORKHOLD_MCP_CONTENT_KEY",
)


def _process_env() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key.lower() in _PROCESS_ENV_KEYS
    }


def _run(
    env: dict[str, str],
    args: tuple[str, ...],
) -> subprocess.CompletedProcess[bytes]:
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


def _ready_env(**extra: str) -> dict[str, str]:
    env = {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
        "WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY,
    }
    env.update(extra)
    return env


def _known_names() -> frozenset[str]:
    from workhold_mcp.registry import load_manifest

    return frozenset(row.name for row in load_manifest())


def _resolve(
    argv: tuple[str, ...] = (),
    env: dict[str, str] | None = None,
) -> frozenset[str]:
    from workhold_mcp.policy import resolve_tool_policy

    return resolve_tool_policy(argv, {} if env is None else env, _known_names())


def _assert_exit_2(
    completed: subprocess.CompletedProcess[bytes],
    setting: str | None,
    hidden: tuple[str, ...],
) -> None:
    assert completed.returncode == 2
    assert completed.stdout == b""
    lines = completed.stderr.splitlines()
    assert len(lines) == 1
    if setting is None:
        assert lines[0] == b"workhold-mcp: invalid configuration"
    else:
        assert lines[0] == f"workhold-mcp: invalid configuration: {setting}".encode()
    combined = completed.stdout + completed.stderr
    for item in hidden:
        assert item.encode("utf-8") not in combined
    assert b"Traceback" not in completed.stderr
    assert b"ConfigError" not in completed.stderr


def test_omitted_policy_is_observer() -> None:
    from workhold_mcp.registry import preset_names

    observer = preset_names("observer")
    resolved = _resolve()
    assert resolved == frozenset(observer)
    assert tuple(sorted(resolved)) == observer
    assert len(resolved) == 9
    assert _resolve(("--preset", "observer")) == resolved
    assert _resolve(env={_PRESET_ENV: "observer"}) == resolved
    producer = preset_names("producer")
    assert _resolve(("--preset", "producer")) == frozenset(producer)
    assert frozenset(producer).isdisjoint(resolved)
    assert _resolve(env={_PRESET_ENV: "operator"}) == frozenset(preset_names("operator"))
    routine = preset_names("routine")
    assert _resolve(("--preset", "routine")) == frozenset(routine)
    assert len(routine) == 33


def test_allowlist_replaces_default() -> None:
    from workhold_mcp.registry import preset_names

    only = _resolve(("--allow-tool", "producer_enqueue"))
    assert only == frozenset({"producer_enqueue"})
    assert only.isdisjoint(preset_names("observer"))
    expected = frozenset({"observer_get_task", "producer_enqueue"})
    comma = _resolve(("--allow-tool", " observer_get_task, producer_enqueue "))
    repeated = _resolve(
        ("--allow-tool", "observer_get_task", "--allow-tool", "producer_enqueue")
    )
    from_env = _resolve(
        env={_ALLOWED_ENV: "observer_get_task, producer_enqueue"}
    )
    assert comma == repeated == from_env == expected


def test_comma_and_repeat_reject_duplicates() -> None:
    from workhold_mcp.config import ConfigError

    cases = (
        (
            ("--allow-tool", "observer_get_task,observer_get_task"),
            {},
            "--allow-tool",
            "observer_get_task",
        ),
        (
            ("--allow-tool", "observer_get_task", "--allow-tool", "observer_get_task"),
            {},
            "--allow-tool",
            "observer_get_task",
        ),
        (
            (
                "--allow-tool",
                "observer_get_task,producer_enqueue",
                "--allow-tool",
                "observer_get_task",
            ),
            {},
            "--allow-tool",
            "observer_get_task",
        ),
        (
            ("--allow-tool", "Observer_get_task"),
            {},
            "--allow-tool",
            "Observer_get_task",
        ),
        (
            ("--allow-tool", "observer_get_task,"),
            {},
            "--allow-tool",
            "observer_get_task",
        ),
        (
            ("--allow-tool", ""),
            {},
            "--allow-tool",
            "",
        ),
        (
            ("--allow-tool", "*"),
            {},
            "--allow-tool",
            "*",
        ),
        (
            ("--allow-tool", "!observer_get_task"),
            {},
            "--allow-tool",
            "!observer_get_task",
        ),
        (
            (),
            {_ALLOWED_ENV: ""},
            _ALLOWED_ENV,
            "",
        ),
        (
            (),
            {_ALLOWED_ENV: "observer_get_task, observer_get_task"},
            _ALLOWED_ENV,
            "observer_get_task",
        ),
        (
            ("--preset", "Observer"),
            {},
            "--preset",
            "Observer",
        ),
        (
            (),
            {_PRESET_ENV: "Observer"},
            _PRESET_ENV,
            "Observer",
        ),
    )
    for argv, env, setting, leaked in cases:
        with pytest.raises(ConfigError) as caught:
            _resolve(argv, env)
        assert str(caught.value) == setting
        assert caught.value.setting_name == setting
        if leaked:
            assert leaked not in str(caught.value)


@pytest.mark.parametrize(
    ("args", "env", "setting", "hidden"),
    [
        pytest.param(
            ("--preset", "producer"),
            _ready_env(**{_PRESET_ENV: "producer"}),
            _PRESET_ENV,
            (_CANARY, "producer", _PUBLIC, _ADMIN),
            id="cli-and-env-preset",
        ),
        pytest.param(
            (),
            _ready_env(
                **{
                    _PRESET_ENV: "producer",
                    _ALLOWED_ENV: "producer_enqueue",
                }
            ),
            _PRESET_ENV,
            (_CANARY, "producer", "producer_enqueue", _PUBLIC, _ADMIN),
            id="both-env",
        ),
        pytest.param(
            ("--preset", "observer", "--allow-tool", "observer_get_task"),
            _ready_env(),
            "--preset",
            (_CANARY, "observer_get_task", _PUBLIC, _ADMIN),
            id="preset-and-allow-tool",
        ),
    ],
)
def test_contradictory_channels_exit_2(
    args: tuple[str, ...],
    env: dict[str, str],
    setting: str,
    hidden: tuple[str, ...],
) -> None:
    _assert_exit_2(_run(env, args), setting, hidden)


@pytest.mark.parametrize(
    ("args", "env", "setting", "hidden"),
    [
        pytest.param(
            ("--allow-tool", "routine"),
            {},
            "--allow-tool",
            ("routine",),
            id="preset-id",
        ),
        pytest.param(
            ("--allow-tool", "enqueueTask"),
            {},
            "--allow-tool",
            ("enqueueTask",),
            id="operation-id",
        ),
        pytest.param(
            ("--allow-tool", "claim"),
            {},
            "--allow-tool",
            ("claim",),
            id="claim",
        ),
        pytest.param(
            ("--allow-tool", "claimTasks"),
            {},
            "--allow-tool",
            ("claimTasks",),
            id="claim-tasks",
        ),
        pytest.param(
            ("--allow-tool", "consumer_claim"),
            {},
            "--allow-tool",
            ("consumer_claim",),
            id="consumer-claim",
        ),
        pytest.param(
            ("--allow-tool", "break_glass_force_lease_expiry"),
            {},
            "--allow-tool",
            ("break_glass_force_lease_expiry",),
            id="break-glass-alias",
        ),
        pytest.param(
            ("--allow-tool", "breakglass_repair_registry_entry"),
            {},
            "--allow-tool",
            ("breakglass_repair_registry_entry",),
            id="breakglass-alias",
        ),
        pytest.param(
            ("--allow-tool", "consumer_get_capabilities"),
            {},
            "--allow-tool",
            ("consumer_get_capabilities",),
            id="consumer-capabilities",
        ),
        pytest.param(
            ("--allow-tool", "not_a_manifest_tool"),
            {},
            "--allow-tool",
            ("not_a_manifest_tool", "WORKHOLD_MCP_OBSERVER_TOKEN"),
            id="unknown-without-observer-token",
        ),
        pytest.param(
            ("--preset", "Observer"),
            {},
            "--preset",
            ("Observer",),
            id="preset-case",
        ),
    ],
)
def test_unknown_and_alias_forms_exit_2(
    args: tuple[str, ...],
    env: dict[str, str],
    setting: str,
    hidden: tuple[str, ...],
) -> None:
    completed = _run(env, args)
    _assert_exit_2(completed, setting, hidden)
    assert b"WORKHOLD_MCP_OBSERVER_TOKEN" not in completed.stderr


@pytest.mark.parametrize(
    ("args", "hidden"),
    [
        pytest.param(("--token", _CANARY), (_CANARY,), id="token"),
        pytest.param(("--content-key", _KEY_CANARY), (_KEY_CANARY,), id="content-key"),
        pytest.param((f"--token={_CANARY}",), (_CANARY,), id="token-equals"),
        pytest.param(("--help",), (), id="help"),
        pytest.param(("-h",), (), id="short-help"),
        pytest.param(("not-a-flag",), (), id="bare-argument"),
        pytest.param(("--pre", "observer"), ("observer",), id="abbreviation"),
    ],
)
def test_secrets_stay_off_argv(args: tuple[str, ...], hidden: tuple[str, ...]) -> None:
    completed = _run(_ready_env(), args)
    line = completed.stderr.splitlines()
    assert completed.returncode == 2
    assert completed.stdout == b""
    assert len(line) == 1
    assert line[0].startswith(b"workhold-mcp: invalid configuration")
    combined = completed.stdout + completed.stderr
    for item in (*hidden, _CANARY, _KEY_CANARY, _PUBLIC, _ADMIN):
        assert item.encode("utf-8") not in combined
    assert b"Traceback" not in completed.stderr


def test_new_env_names_are_known() -> None:
    from workhold_mcp.config import ConfigError, load_settings

    base = {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": _PUBLIC,
        "WORKHOLD_MCP_ADMIN_BASE_URL": _ADMIN,
        "WORKHOLD_MCP_OBSERVER_TOKEN": "observer-token",
    }
    for name in _KNOWN_ENV:
        load_settings({**base, name: "present"})
    with pytest.raises(ConfigError) as caught:
        load_settings({**base, "WORKHOLD_MCP_TOKEN": "present"})
    assert caught.value.setting_name == "WORKHOLD_MCP_TOKEN"
    assert str(caught.value) == "WORKHOLD_MCP_TOKEN"
    assert "present" not in str(caught.value)
