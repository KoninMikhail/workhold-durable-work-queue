"""Stable service principal and role contracts.

Authentication establishes identity only. Operation and queue authorization are
separate (Plan 03.2-04). Diagnostic ``worker_id`` values are never principals.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ServiceRole(str, Enum):
    """Distinct least-privilege service roles (SEC-01).

    Values are non-interchangeable: a producer credential never authenticates as
    admin (or any other role). Break-glass is a separate short-lived role (REC-03).
    """

    PRODUCER = "producer"
    WORKER = "worker"
    RELAY = "relay"
    OBSERVER = "observer"
    ADMIN = "admin"
    BREAK_GLASS = "break_glass"
    MIGRATOR = "migrator"
    MAINTAINER = "maintainer"


@dataclass(frozen=True, slots=True)
class Principal:
    """Authenticated stable service identity.

    Contains no credential material. For ``ServiceRole.BREAK_GLASS``, timezone-aware
    ``credential_expires_at`` and a non-empty ``operation_audience`` are mandatory
    (REC-03 / D-01). Other roles may omit those fields. Values never include secrets.
    """

    principal_id: str
    role: ServiceRole
    credential_expires_at: datetime | None = None
    operation_audience: frozenset[str] | None = None

    def __post_init__(self) -> None:
        if not self.principal_id:
            raise ValueError("principal_id must be non-empty")
        if not isinstance(self.role, ServiceRole):
            raise TypeError("role must be a ServiceRole")
        if self.role is ServiceRole.BREAK_GLASS:
            if self.credential_expires_at is None:
                raise ValueError(
                    "BREAK_GLASS principals require timezone-aware "
                    "credential_expires_at"
                )
            if self.credential_expires_at.tzinfo is None:
                raise ValueError(
                    "credential_expires_at must be timezone-aware when set"
                )
            if not self.operation_audience:
                raise ValueError(
                    "BREAK_GLASS principals require non-empty operation_audience"
                )
            object.__setattr__(
                self, "operation_audience", frozenset(self.operation_audience)
            )
            return
        if self.operation_audience is not None:
            object.__setattr__(
                self, "operation_audience", frozenset(self.operation_audience)
            )

    def __repr__(self) -> str:
        return f"Principal(principal_id={self.principal_id!r}, role={self.role.value!r})"

    def __str__(self) -> str:
        return f"{self.role.value}:{self.principal_id}"
