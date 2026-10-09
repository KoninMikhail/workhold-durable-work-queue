"""Pure in-memory named-queue catalog parse (CTRL-10 / D-06..D-08 / D-10).

Fail-closes the entire catalog before any apply write. No SQLAlchemy, ASGI,
settings, or HTTP admin route modules.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from queue_service.domain.queue_control import (
    DomainValidationError,
    RetryPolicyDraft,
    validate_retry_policy_draft,
)

CATALOG_MAX_BYTES: Final[int] = 1_048_576

_ENVELOPE_KEYS: Final[frozenset[str]] = frozenset({"schema_version", "queues"})
_ENTRY_KEYS: Final[frozenset[str]] = frozenset({"name", "initial_policy"})
_POLICY_KEYS: Final[frozenset[str]] = frozenset(
    {"enabled", "max_attempts", "backoff_strategy", "retry_delay_seconds"}
)

# Same OpenAPI pattern as CreateQueueMutation.__post_init__ (do not extend that type).
_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_QUEUE_NAME_MAX_LEN: Final[int] = 128


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One named-queue ensure-exists entry from a catalog file."""

    name: str
    initial_policy: RetryPolicyDraft


def _reject_unknown_keys(
    payload: Mapping[str, Any],
    allowed: frozenset[str],
    *,
    path: str,
) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise DomainValidationError(
            "validation_failed",
            f"{path}: unknown property '{unknown[0]}'",
        )


def _validate_queue_name(name: object, *, path: str) -> str:
    if (
        not isinstance(name, str)
        or not (1 <= len(name) <= _QUEUE_NAME_MAX_LEN)
        or _QUEUE_NAME_RE.fullmatch(name) is None
    ):
        raise DomainValidationError(
            "validation_failed",
            f"{path}: queue name must match OpenAPI pattern "
            "^[a-z0-9][a-z0-9._-]*$ (1..128)",
        )
    return name


def parse_catalog_bytes(raw: bytes, *, ceiling: int) -> tuple[CatalogEntry, ...]:
    """Parse and fail-close an entire catalog in memory.

    ``ceiling`` is passed to ``validate_retry_policy_draft`` as
    ``deployment_retry_delay_ceiling_seconds``.
    """
    if len(raw) > CATALOG_MAX_BYTES:
        raise DomainValidationError(
            "validation_failed",
            "catalog exceeds maximum size",
        )
    if not raw.strip():
        raise DomainValidationError(
            "validation_failed",
            "catalog file is empty",
        )

    try:
        parsed = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "catalog must be valid JSON",
        ) from exc

    if not isinstance(parsed, dict):
        raise DomainValidationError(
            "validation_failed",
            "catalog root must be an object",
        )

    _reject_unknown_keys(parsed, _ENVELOPE_KEYS, path="$")

    # Integer identity: JSON true must not pass as schema_version 1.
    schema_version = parsed.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        raise DomainValidationError(
            "validation_failed",
            "$.schema_version must be 1",
        )

    queues = parsed.get("queues")
    if not isinstance(queues, list):
        raise DomainValidationError(
            "validation_failed",
            "$.queues must be an array",
        )

    names: set[str] = set()
    entries: list[CatalogEntry] = []
    for index, item in enumerate(queues):
        entry_path = f"$.queues[{index}]"
        if not isinstance(item, dict):
            raise DomainValidationError(
                "validation_failed",
                f"{entry_path} must be an object",
            )
        _reject_unknown_keys(item, _ENTRY_KEYS, path=entry_path)

        if "name" not in item:
            raise DomainValidationError(
                "validation_failed",
                f"{entry_path} missing required property 'name'",
            )
        if "initial_policy" not in item:
            raise DomainValidationError(
                "validation_failed",
                f"{entry_path} missing required property 'initial_policy'",
            )

        name = _validate_queue_name(item["name"], path=f"{entry_path}.name")
        if name in names:
            raise DomainValidationError(
                "validation_failed",
                f"{entry_path}.name: duplicate queue name",
            )

        initial = item["initial_policy"]
        policy_path = f"{entry_path}.initial_policy"
        if not isinstance(initial, dict):
            raise DomainValidationError(
                "validation_failed",
                f"{policy_path} must be an object",
            )
        _reject_unknown_keys(initial, _POLICY_KEYS, path=policy_path)
        for required in _POLICY_KEYS:
            if required not in initial:
                raise DomainValidationError(
                    "validation_failed",
                    f"{policy_path} missing required property '{required}'",
                )

        policy = validate_retry_policy_draft(
            enabled=initial["enabled"],
            max_attempts=initial["max_attempts"],
            backoff_strategy=initial["backoff_strategy"],
            retry_delay_seconds=initial["retry_delay_seconds"],
            deployment_retry_delay_ceiling_seconds=ceiling,
        )
        names.add(name)
        entries.append(CatalogEntry(name=name, initial_policy=policy))

    return tuple(entries)
