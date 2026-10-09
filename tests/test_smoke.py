import tomllib
from pathlib import Path

from workhold import __version__
from workhold.cli import CLI_ROLES, main

_ROOT = Path(__file__).resolve().parents[1]


def test_version() -> None:
    with (_ROOT / "pyproject.toml").open("rb") as fh:
        expected = tomllib.load(fh)["project"]["version"]
    assert __version__ == expected


def test_main_help_exits_zero_without_greeting(capsys) -> None:
    assert main(["--help"]) == 0
    captured = capsys.readouterr()
    combined = f"{captured.out}\n{captured.err}".lower()
    assert "hello from queue!" not in combined
    for role in CLI_ROLES:
        assert role in combined
