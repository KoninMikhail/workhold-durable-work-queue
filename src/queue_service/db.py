"""SQLAlchemy metadata hook and role-scoped engine factory.

Domain models are not defined yet. When they appear, declare them on ``Base``.

Engines are never created at import time. Callers must pass validated
``DeploymentSettings`` and an explicit process role so every pool is bounded.
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import QueuePool

from queue_service.settings import DeploymentSettings, PROCESS_ROLES, SettingsValidationError


def api_dedicated_listener_budget(settings: DeploymentSettings) -> int:
    """Non-pooled autocommit LISTEN connections reserved for the API role.

    One dedicated psycopg connection per API replica is counted in the deployment
    connection budget but is never drawn from the SQLAlchemy role pool created by
    :func:`create_role_engine`.
    """
    return settings.api_listener_connections


def to_psycopg_dsn(database_url: str) -> str:
    """Normalize SQLAlchemy-style URLs for a direct psycopg connection."""
    if database_url.startswith("postgresql+psycopg://"):
        return "postgresql://" + database_url.removeprefix("postgresql+psycopg://")
    return database_url


def psycopg_connect_timeout_seconds(pool_acquisition_timeout_seconds: float) -> int:
    """TCP connect timeout (seconds) for direct and pooled psycopg connections.

    Reuses the role pool acquisition budget so SQLAlchemy engines and the
    dedicated claim LISTEN reconnect loop share one bounded connect ceiling.
    """
    return max(1, int(pool_acquisition_timeout_seconds))


class Base(DeclarativeBase):
    """Declarative base for ORM models and Alembic autogenerate."""


def create_role_engine(settings: DeploymentSettings, role: str) -> Engine:
    """Build a SQLAlchemy engine for one process role.

    Pool size equals the role's validated ``pool_ceiling`` with
    ``max_overflow=0`` so the deployment budget cannot be exceeded at runtime.
    Acquisition and statement timeouts are applied from the same settings.
    """
    if role not in PROCESS_ROLES:
        raise SettingsValidationError(f"unknown process role: {role!r}")

    pool = settings.pool_for(role)
    url = settings.database_url.get_secret_value()
    # Convert seconds to whole milliseconds for PostgreSQL statement_timeout.
    statement_timeout_ms = int(pool.statement_timeout_seconds * 1000)
    if statement_timeout_ms <= 0:
        raise SettingsValidationError(
            f"role {role!r}: statement_timeout_seconds must be positive"
        )

    # Bound TCP connect as well as pool wait so readiness cannot hang on a dead host.
    connect_timeout_s = psycopg_connect_timeout_seconds(
        pool.pool_acquisition_timeout_seconds
    )
    return create_engine(
        url,
        poolclass=QueuePool,
        pool_size=pool.pool_ceiling,
        max_overflow=0,
        pool_timeout=pool.pool_acquisition_timeout_seconds,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": connect_timeout_s,
            "options": f"-c statement_timeout={statement_timeout_ms}",
        },
    )
