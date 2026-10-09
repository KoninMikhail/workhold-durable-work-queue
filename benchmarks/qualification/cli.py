"""CLI for immutable qualification artifact lifecycle (QUAL-03 / QUAL-05)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from benchmarks.qualification.artifacts import (
    ArtifactError,
    derive,
    validate_final,
    validate_raw,
    write_checksums,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.qualification.cli",
        description="Validate, derive and checksum Queue qualification evidence bundles.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("validate-raw", "Validate the eight raw evidence classes (no derived writes)."),
        ("derive", "Derive summary.json, qualification.json and report.md."),
        ("checksums", "Promote bundle_stage to final and write SHA256SUMS."),
        ("validate-final", "Read-only validation of a final 12-class bundle."),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("bundle", type=Path, help="Path to the evidence bundle directory")
        if name in {"derive", "validate-final"}:
            cmd.add_argument(
                "--allow-synthetic",
                action="store_true",
                help=(
                    "Allow evidence_mode synthetic|compressed for CI-only derive/final. "
                    "Production qualification must omit this flag and use live evidence."
                ),
            )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    bundle: Path = args.bundle
    allow_synthetic = bool(getattr(args, "allow_synthetic", False))
    try:
        if args.command == "validate-raw":
            digest = validate_raw(bundle)
            print(f"validate-raw OK digest={digest}")
        elif args.command == "derive":
            derive(bundle, allow_synthetic=allow_synthetic)
            print("derive OK")
        elif args.command == "checksums":
            write_checksums(bundle)
            print("checksums OK")
        elif args.command == "validate-final":
            validate_final(bundle, allow_synthetic=allow_synthetic)
            print("validate-final OK")
        else:
            parser.error(f"unknown command: {args.command}")
    except ArtifactError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
