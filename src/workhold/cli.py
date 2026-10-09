"""Explicit multi-role process dispatcher (PKG-01 / ADR-015).

One installed command selects exactly one process role. Role runners are resolved
only after the role name is validated so ``--help`` and invalid input never open
database or network resources. Later plans replace placeholder runners under
``workhold.roles`` without changing CLI syntax.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from typing import Final

CLI_ROLES: Final[tuple[str, ...]] = ("api", "migrate", "maintain", "relay", "apply")

RoleRunner = Callable[[Sequence[str]], int]

_ROLE_HELP: Final[dict[str, str]] = {
    "api": "Application and private-admin HTTP listeners",
    "migrate": "One-shot Alembic schema upgrade",
    "maintain": "Partition premake, retention, and registry purge",
    "relay": "Delivery Outbox publication (HTTP webhook)",
    "apply": "One-shot ensure-exists named-queue catalog apply",
}


def _normalize_exit(code: object) -> int:
    """Clamp runner results to a deterministic process exit status."""
    if not isinstance(code, int):
        return 1
    if code < 0:
        return 1
    if code > 255:
        return 255
    return code


def _placeholder_api(_argv: Sequence[str]) -> int:
    return 0


def _placeholder_migrate(_argv: Sequence[str]) -> int:
    return 0


def _placeholder_maintain(_argv: Sequence[str]) -> int:
    return 0


def _run_relay_unavailable(_argv: Sequence[str]) -> int:
    """Fallback when the relay role module cannot be imported."""
    print("relay role is unavailable", file=sys.stderr)
    return 2


_PLACEHOLDERS: Final[dict[str, RoleRunner]] = {
    "api": _placeholder_api,
    "migrate": _placeholder_migrate,
    "maintain": _placeholder_maintain,
    "relay": _run_relay_unavailable,
}


def _load_runner(role: str) -> RoleRunner:
    """Lazy-load a role runner; fall back to placeholders when roles are absent."""
    try:
        module = __import__(f"workhold.roles.{role}", fromlist=["run"])
    except ImportError:
        return _PLACEHOLDERS[role]
    runner = getattr(module, "run", None)
    if not callable(runner):
        return _PLACEHOLDERS[role]
    return runner  # type: ignore[return-value]


def _print_help(stream=sys.stdout) -> None:
    print("usage: workhold [-h] <role> [args...]", file=stream)
    print(file=stream)
    print(
        "Workhold process-role entry point. Select exactly one role;",
        file=stream,
    )
    print("roles scale and terminate independently.", file=stream)
    print(file=stream)
    print("Roles:", file=stream)
    width = max(len(role) for role in CLI_ROLES)
    for role in CLI_ROLES:
        print(f"  {role:<{width}}  {_ROLE_HELP[role]}", file=stream)
    print(file=stream)
    print(f"Roles: {', '.join(CLI_ROLES)}", file=stream)


def _print_usage(stream=sys.stderr) -> None:
    print("usage: workhold [-h] <role> [args...]", file=stream)
    print(f"Roles: {', '.join(CLI_ROLES)}", file=stream)


def main(
    argv: Sequence[str] | None = None,
    *,
    runners: Mapping[str, RoleRunner] | None = None,
) -> int:
    """Dispatch to one process role and return a bounded exit code."""
    args_list = list(sys.argv[1:] if argv is None else argv)

    if not args_list or args_list[0] in {"-h", "--help"}:
        # Bare --help / -h: usage only, no runner import.
        if args_list and args_list[0] in {"-h", "--help"}:
            _print_help(sys.stdout)
            return 0
        _print_usage(sys.stderr)
        print(
            f"workhold: missing role; choose one of: {', '.join(CLI_ROLES)}",
            file=sys.stderr,
        )
        return 2

    role = args_list[0]
    if role not in CLI_ROLES:
        _print_usage(sys.stderr)
        print(f"workhold: unknown role {role!r}", file=sys.stderr)
        return 2

    role_args = tuple(args_list[1:])
    if role_args and role_args[0] == "--":
        role_args = role_args[1:]

    if runners is not None:
        try:
            runner = runners[role]
        except KeyError:
            _print_usage(sys.stderr)
            print(f"workhold: unknown role {role!r}", file=sys.stderr)
            return 2
    else:
        runner = _load_runner(role)

    return _normalize_exit(runner(role_args))


def run(argv: Sequence[str] | None = None) -> None:
    """Process entry point used by ``python -m workhold`` and console scripts."""
    raise SystemExit(main(argv))


if __name__ == "__main__":
    run()
