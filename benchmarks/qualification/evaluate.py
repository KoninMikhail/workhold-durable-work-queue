"""Release qualification evaluator (QUAL-03 / QUAL-05 / SDK-02).

Reads a finalized 12-class evidence bundle and emits a fail-closed gate decision.
Synthetic / compressed evidence requires ``--allow-synthetic`` and can never be
reported as a live production PASS.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from benchmarks.qualification.artifacts import (
    ArtifactError,
    assert_evidence_mode_for_qualification,
    validate_final,
)

ACCEPTABLE_VERDICTS = frozenset({"PASS", "CI_SYNTHETIC_PASS"})


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def evaluate_bundle(
    bundle: Path,
    *,
    allow_synthetic: bool = False,
    check: bool = False,
) -> dict[str, Any]:
    """Validate the final bundle and return a machine-readable gate result."""
    bundle = bundle.resolve()
    if not bundle.is_dir():
        raise ArtifactError(f"bundle directory not found: {bundle}")

    manifest = _load(bundle / "manifest.json")
    qualification = _load(bundle / "qualification.json")
    evidence_mode = str(
        manifest.get("evidence_mode")
        or qualification.get("evidence_mode")
        or ""
    )

    assert_evidence_mode_for_qualification(
        evidence_mode,
        allow_synthetic=allow_synthetic,
    )
    validate_final(bundle, allow_synthetic=allow_synthetic)

    verdict = str(qualification.get("verdict") or "")
    production_qualified = bool(qualification.get("production_qualified"))
    if evidence_mode != "live" and verdict == "PASS":
        raise ArtifactError(
            "synthetic/compressed evidence must not report a silent production PASS"
        )
    if evidence_mode != "live" and production_qualified:
        raise ArtifactError(
            "production_qualified must be false for non-live evidence"
        )
    if evidence_mode == "live" and verdict == "CI_SYNTHETIC_PASS":
        raise ArtifactError("live evidence must not report CI_SYNTHETIC_PASS")

    if check and verdict not in ACCEPTABLE_VERDICTS:
        raise ArtifactError(f"release gate blocked: verdict={verdict!r}")

    result = {
        "bundle": str(bundle),
        "evidence_mode": evidence_mode,
        "verdict": verdict,
        "production_qualified": production_qualified,
        "verdict_note": qualification.get("verdict_note"),
        "validated_raw_set_digest": qualification.get("validated_raw_set_digest"),
        "checks": qualification.get("checks"),
        "gate": "PASS" if verdict in ACCEPTABLE_VERDICTS else "BLOCK",
    }
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.qualification.evaluate",
        description=(
            "Evaluate a finalized qualification bundle for the release gate. "
            "Synthetic evidence requires --allow-synthetic and yields "
            "CI_SYNTHETIC_PASS, never a silent production PASS."
        ),
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the finalized qualification bundle directory",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero unless verdict is PASS or CI_SYNTHETIC_PASS",
    )
    parser.add_argument(
        "--allow-synthetic",
        action="store_true",
        help=(
            "Allow evidence_mode synthetic|compressed for CI-only evaluation. "
            "Omit for live production qualification."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = evaluate_bundle(
            args.input,
            allow_synthetic=bool(args.allow_synthetic),
            check=bool(args.check),
        )
    except ArtifactError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    print(
        f"evaluate OK gate={result['gate']} verdict={result['verdict']} "
        f"evidence_mode={result['evidence_mode']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
