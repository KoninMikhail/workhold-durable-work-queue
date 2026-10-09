"""Immutable deployment settings with fail-closed cross-field validation.

Deployment-only configuration (TLS mode, PostgreSQL pool ceilings, credential
generations) is validated at construction. Runtime/admin APIs must not mutate
these values — instances are frozen dataclasses.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from workhold.security.credentials import CredentialBinding
    from workhold.security.payload_policy import PayloadRetentionPolicy


PROCESS_ROLES: Final[frozenset[str]] = frozenset(
    {"api", "admin", "migrate", "maintain", "relay", "apply"}
)

# Deployment-configurable opaque payload retention (storage-topology 30–90 days).
PAYLOAD_RETENTION_DAYS_MIN: Final[int] = 30
PAYLOAD_RETENTION_DAYS_MAX: Final[int] = 90
DEFAULT_PAYLOAD_RETENTION_DAYS: Final[int] = 90

# Phase 3.9 QUAL-03 accepted payload/request ceilings (ADR 023).
# Bound by the hard 1 MiB request maximum; equals the measured recommendation.
QUALIFIED_PAYLOAD_CEILING_BYTES: Final[int] = 1_048_576
QUALIFIED_REQUEST_MAX_BYTES: Final[int] = 1_048_576
REQUEST_MAX_BYTES_HARD_LIMIT: Final[int] = 1_048_576

# Correctness registry TTLs in seconds (ADR 017 / Phase 3.1 capability consts).
ENQUEUE_DEDUP_TTL_SECONDS_DEFAULT: Final[int] = 7_776_000  # 90 days
ENQUEUE_DEDUP_TTL_SECONDS_MIN: Final[int] = 2_592_000  # 30 days
ENQUEUE_DEDUP_TTL_SECONDS_MAX: Final[int] = 31_536_000  # 365 days

TERMINAL_REPLAY_TTL_SECONDS_DEFAULT: Final[int] = 604_800  # 7 days
TERMINAL_REPLAY_TTL_SECONDS_MIN: Final[int] = 86_400  # 1 day
TERMINAL_REPLAY_TTL_SECONDS_MAX: Final[int] = 2_592_000  # 30 days

ADMIN_REPLAY_TTL_SECONDS_DEFAULT: Final[int] = 2_592_000  # 30 days
ADMIN_REPLAY_TTL_SECONDS_MIN: Final[int] = 604_800  # 7 days
ADMIN_REPLAY_TTL_SECONDS_MAX: Final[int] = 7_776_000  # 90 days

REGISTRY_PURGE_BATCH_SIZE_DEFAULT: Final[int] = 1000
REGISTRY_PURGE_BATCH_SIZE_MIN: Final[int] = 1
REGISTRY_PURGE_BATCH_SIZE_MAX: Final[int] = 10_000

# Phase 11 WORK-15 client scheduling horizon (LPD-2).
SCHEDULE_HORIZON_SECONDS_DEFAULT: Final[int] = 86_400
SCHEDULE_HORIZON_SECONDS_MIN: Final[int] = 0
SCHEDULE_HORIZON_SECONDS_MAX: Final[int] = 86_400

# Phase 20.1 WORK-17 / API-09 bounded long-poll deployment contract (Wave 0).
CLAIM_MAX_WAIT_SECONDS_DEFAULT: Final[int] = 20
CLAIM_MAX_WAIT_SECONDS_MIN: Final[int] = 0
CLAIM_MAX_WAIT_SECONDS_MAX: Final[int] = 20

CLAIM_WAIT_FALLBACK_SECONDS_DEFAULT: Final[float] = 1.0
CLAIM_WAIT_FALLBACK_SECONDS_MIN: Final[float] = 0.1
CLAIM_WAIT_FALLBACK_SECONDS_MAX: Final[float] = 20.0

CLAIM_CANCELLATION_PROBE_SECONDS_DEFAULT: Final[float] = 0.25
CLAIM_CANCELLATION_PROBE_SECONDS_MIN: Final[float] = 0.05
CLAIM_CANCELLATION_PROBE_SECONDS_MAX: Final[float] = 5.0

CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT: Final[int] = 64
CLAIM_MAX_OUTSTANDING_WAITS_MIN: Final[int] = 1
CLAIM_MAX_OUTSTANDING_WAITS_MAX: Final[int] = 256


class SettingsValidationError(ValueError):
    """Raised when deployment settings are unsafe or incomplete."""


class EnvironmentMode(str, Enum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"


class ListenerTlsMode(str, Enum):
    """Listener / public-exposure transport mode.

    Production rejects ``PLAINTEXT_PUBLIC``. Safe production modes are direct
    TLS termination in-process or trusted private-network TLS termination.
    """

    PLAINTEXT_PUBLIC = "plaintext_public"
    DIRECT_TLS = "direct_tls"
    TRUSTED_PRIVATE_TLS_TERMINATION = "trusted_private_tls_termination"


@dataclass(frozen=True, slots=True)
class Secret:
    """Opaque credential / connection-string material.

    Cleartext is available only via ``get_secret_value`` and never via
    ``str`` / ``repr``.
    """

    _value: str

    def get_secret_value(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('***')"

    def __str__(self) -> str:
        return "***"


@dataclass(frozen=True, slots=True)
class CredentialGeneration:
    """One active credential generation for a stable principal ID."""

    principal_id: str
    generation_id: str
    secret: Secret

    def __post_init__(self) -> None:
        if not self.principal_id:
            raise SettingsValidationError("credential principal_id must be non-empty")
        if not self.generation_id:
            raise SettingsValidationError("credential generation_id must be non-empty")


@dataclass(frozen=True, slots=True)
class RolePoolSettings:
    """Bounded per-role replica and pool ceilings with fail-fast timeouts."""

    replica_ceiling: int
    pool_ceiling: int
    pool_acquisition_timeout_seconds: float
    statement_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class DeploymentSettings:
    """Validated deployment configuration for all process roles.

    Cross-field rules:
    - every role in ``PROCESS_ROLES`` has an explicit pool policy;
    - ``sum(replica_ceiling × pool_ceiling) <= max_connections - reserved``;
    - production rejects plaintext public exposure;
    - overlapping credential generations may share a principal_id.
    """

    environment: EnvironmentMode
    listener_tls_mode: ListenerTlsMode
    database_url: Secret
    postgres_max_connections: int
    postgres_reserved_connections: int
    role_pools: Mapping[str, RolePoolSettings]
    credential_generations: tuple[CredentialGeneration, ...]
    payload_retention_days: int = DEFAULT_PAYLOAD_RETENTION_DAYS
    enqueue_dedup_ttl_seconds: int = ENQUEUE_DEDUP_TTL_SECONDS_DEFAULT
    terminal_replay_ttl_seconds: int = TERMINAL_REPLAY_TTL_SECONDS_DEFAULT
    admin_replay_ttl_seconds: int = ADMIN_REPLAY_TTL_SECONDS_DEFAULT
    registry_purge_batch_size: int = REGISTRY_PURGE_BATCH_SIZE_DEFAULT
    schedule_horizon_seconds: int = SCHEDULE_HORIZON_SECONDS_DEFAULT
    claim_max_wait_seconds: int = CLAIM_MAX_WAIT_SECONDS_DEFAULT
    claim_wait_fallback_seconds: float = CLAIM_WAIT_FALLBACK_SECONDS_DEFAULT
    claim_cancellation_probe_seconds: float = CLAIM_CANCELLATION_PROBE_SECONDS_DEFAULT
    claim_max_outstanding_waits: int = CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT
    sentry_dsn: Secret | None = None
    api_credential_bindings: tuple[CredentialBinding, ...] | None = None
    api_queue_scopes: Mapping[str, frozenset[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "role_pools",
            MappingProxyType(dict(self.role_pools)),
        )
        object.__setattr__(
            self,
            "credential_generations",
            tuple(self.credential_generations),
        )
        if self.api_credential_bindings is not None:
            object.__setattr__(
                self,
                "api_credential_bindings",
                tuple(self.api_credential_bindings),
            )
        object.__setattr__(
            self,
            "api_queue_scopes",
            MappingProxyType(dict(self.api_queue_scopes)),
        )
        self._validate()

    @property
    def usable_connections(self) -> int:
        return self.postgres_max_connections - self.postgres_reserved_connections

    @property
    def api_listener_connections(self) -> int:
        """One dedicated autocommit LISTEN connection per API replica (non-pooled)."""
        return self.pool_for("api").replica_ceiling

    @property
    def committed_connections(self) -> int:
        pool_connections = sum(
            pool.replica_ceiling * pool.pool_ceiling
            for pool in self.role_pools.values()
        )
        return pool_connections + self.api_listener_connections

    def pool_for(self, role: str) -> RolePoolSettings:
        if role not in PROCESS_ROLES:
            raise SettingsValidationError(f"unknown process role: {role!r}")
        try:
            return self.role_pools[role]
        except KeyError as exc:
            raise SettingsValidationError(
                f"missing pool settings for process role: {role!r}"
            ) from exc

    def _validate(self) -> None:
        if self.postgres_max_connections <= 0:
            raise SettingsValidationError(
                "postgres_max_connections must be positive"
            )
        if self.postgres_reserved_connections < 0:
            raise SettingsValidationError(
                "postgres_reserved_connections must be non-negative"
            )
        if self.postgres_reserved_connections >= self.postgres_max_connections:
            raise SettingsValidationError(
                "postgres_reserved_connections must be less than "
                "postgres_max_connections"
            )

        pool_keys = set(self.role_pools)
        unknown = pool_keys - PROCESS_ROLES
        if unknown:
            raise SettingsValidationError(
                f"unknown process role(s) in role_pools: {sorted(unknown)}"
            )
        missing = PROCESS_ROLES - pool_keys
        if missing:
            raise SettingsValidationError(
                f"missing pool settings for process role(s): {sorted(missing)}"
            )

        for role, pool in self.role_pools.items():
            self._validate_role_pool(role, pool)

        if self.committed_connections > self.usable_connections:
            raise SettingsValidationError(
                "PostgreSQL connection budget overcommitted: "
                f"committed={self.committed_connections} "
                f"usable={self.usable_connections} "
                f"(max={self.postgres_max_connections} "
                f"reserved={self.postgres_reserved_connections})"
            )

        if (
            self.environment is EnvironmentMode.PRODUCTION
            and self.listener_tls_mode is ListenerTlsMode.PLAINTEXT_PUBLIC
        ):
            raise SettingsValidationError(
                "production rejects plaintext public exposure; configure "
                "direct_tls or trusted_private_tls_termination"
            )

        self._validate_credentials(self.credential_generations)
        self._validate_payload_retention(self.payload_retention_days)
        self._validate_bounded_int(
            self.enqueue_dedup_ttl_seconds,
            name="enqueue_dedup_ttl_seconds",
            minimum=ENQUEUE_DEDUP_TTL_SECONDS_MIN,
            maximum=ENQUEUE_DEDUP_TTL_SECONDS_MAX,
        )
        self._validate_bounded_int(
            self.terminal_replay_ttl_seconds,
            name="terminal_replay_ttl_seconds",
            minimum=TERMINAL_REPLAY_TTL_SECONDS_MIN,
            maximum=TERMINAL_REPLAY_TTL_SECONDS_MAX,
        )
        self._validate_bounded_int(
            self.admin_replay_ttl_seconds,
            name="admin_replay_ttl_seconds",
            minimum=ADMIN_REPLAY_TTL_SECONDS_MIN,
            maximum=ADMIN_REPLAY_TTL_SECONDS_MAX,
        )
        self._validate_bounded_int(
            self.registry_purge_batch_size,
            name="registry_purge_batch_size",
            minimum=REGISTRY_PURGE_BATCH_SIZE_MIN,
            maximum=REGISTRY_PURGE_BATCH_SIZE_MAX,
        )
        self._validate_bounded_int(
            self.schedule_horizon_seconds,
            name="schedule_horizon_seconds",
            minimum=SCHEDULE_HORIZON_SECONDS_MIN,
            maximum=SCHEDULE_HORIZON_SECONDS_MAX,
        )
        self._validate_bounded_int(
            self.claim_max_wait_seconds,
            name="claim_max_wait_seconds",
            minimum=CLAIM_MAX_WAIT_SECONDS_MIN,
            maximum=CLAIM_MAX_WAIT_SECONDS_MAX,
        )
        self._validate_bounded_float(
            self.claim_wait_fallback_seconds,
            name="claim_wait_fallback_seconds",
            minimum=CLAIM_WAIT_FALLBACK_SECONDS_MIN,
            maximum=CLAIM_WAIT_FALLBACK_SECONDS_MAX,
        )
        self._validate_bounded_float(
            self.claim_cancellation_probe_seconds,
            name="claim_cancellation_probe_seconds",
            minimum=CLAIM_CANCELLATION_PROBE_SECONDS_MIN,
            maximum=CLAIM_CANCELLATION_PROBE_SECONDS_MAX,
        )
        self._validate_bounded_int(
            self.claim_max_outstanding_waits,
            name="claim_max_outstanding_waits",
            minimum=CLAIM_MAX_OUTSTANDING_WAITS_MIN,
            maximum=CLAIM_MAX_OUTSTANDING_WAITS_MAX,
        )

    def payload_retention_policy(self) -> PayloadRetentionPolicy:
        """Build the Phase 3.8 handoff contract from validated retention days.

        Returns:
            ``workhold.security.payload_policy.PayloadRetentionPolicy``
        """
        from workhold.security.payload_policy import PayloadRetentionPolicy

        return PayloadRetentionPolicy(retention_days=self.payload_retention_days)

    @staticmethod
    def _validate_payload_retention(days: int) -> None:
        if not (
            PAYLOAD_RETENTION_DAYS_MIN <= days <= PAYLOAD_RETENTION_DAYS_MAX
        ):
            raise SettingsValidationError(
                "payload_retention_days must be between "
                f"{PAYLOAD_RETENTION_DAYS_MIN} and {PAYLOAD_RETENTION_DAYS_MAX} "
                "inclusive"
            )

    @staticmethod
    def _validate_bounded_int(
        value: object,
        *,
        name: str,
        minimum: int,
        maximum: int,
    ) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise SettingsValidationError(f"{name} must be an int")
        if value < minimum or value > maximum:
            raise SettingsValidationError(
                f"{name} must be between {minimum} and {maximum} inclusive"
            )

    @staticmethod
    def _validate_bounded_float(
        value: object,
        *,
        name: str,
        minimum: float,
        maximum: float,
    ) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SettingsValidationError(f"{name} must be a float")
        numeric = float(value)
        if numeric < minimum or numeric > maximum:
            raise SettingsValidationError(
                f"{name} must be between {minimum} and {maximum} inclusive"
            )

    @staticmethod
    def _validate_role_pool(role: str, pool: RolePoolSettings) -> None:
        if pool.replica_ceiling <= 0:
            raise SettingsValidationError(
                f"role {role!r}: replica_ceiling must be positive"
            )
        if pool.pool_ceiling <= 0:
            raise SettingsValidationError(
                f"role {role!r}: pool_ceiling must be positive"
            )
        if pool.pool_acquisition_timeout_seconds <= 0:
            raise SettingsValidationError(
                f"role {role!r}: pool_acquisition_timeout_seconds must be positive"
            )
        if pool.statement_timeout_seconds <= 0:
            raise SettingsValidationError(
                f"role {role!r}: statement_timeout_seconds must be positive"
            )

    @staticmethod
    def _validate_credentials(
        generations: Sequence[CredentialGeneration],
    ) -> None:
        seen_generation_ids: set[str] = set()
        for generation in generations:
            if generation.generation_id in seen_generation_ids:
                raise SettingsValidationError(
                    "duplicate credential generation_id: "
                    f"{generation.generation_id!r}"
                )
            seen_generation_ids.add(generation.generation_id)


def _env_int(environ: Mapping[str, str], key: str, default: int) -> int:
    raw = environ.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SettingsValidationError(f"invalid integer for {key}: {raw!r}") from exc


def _env_float(environ: Mapping[str, str], key: str, default: float) -> float:
    raw = environ.get(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SettingsValidationError(f"invalid float for {key}: {raw!r}") from exc


def role_pools_from_environ(
    environ: Mapping[str, str] | None = None,
) -> dict[str, RolePoolSettings]:
    """Load per-role pool ceilings from ``QUEUE_<ROLE>_*`` variables."""
    env = os.environ if environ is None else environ
    pools: dict[str, RolePoolSettings] = {}
    for role in sorted(PROCESS_ROLES):
        prefix = f"QUEUE_{role.upper()}_"
        pools[role] = RolePoolSettings(
            replica_ceiling=_env_int(env, f"{prefix}REPLICA_CEILING", 1),
            pool_ceiling=_env_int(env, f"{prefix}POOL_CEILING", 2),
            pool_acquisition_timeout_seconds=_env_float(
                env, f"{prefix}POOL_ACQUISITION_TIMEOUT_SECONDS", 5.0
            ),
            statement_timeout_seconds=_env_float(
                env, f"{prefix}STATEMENT_TIMEOUT_SECONDS", 30.0
            ),
        )
    return pools


def credential_generations_from_environ(
    environ: Mapping[str, str] | None = None,
) -> tuple[CredentialGeneration, ...]:
    """Load overlapping bearer generations from env placeholders.

    ``QUEUE_API_BEARER_TOKEN`` is the current generation. Optional
    ``QUEUE_API_BEARER_TOKEN_PREVIOUS`` keeps the prior generation active for
    rotation overlap. Cleartext never appears in exceptions from this helper.
    """
    env = os.environ if environ is None else environ
    current = env.get("QUEUE_API_BEARER_TOKEN", "dev-token").strip() or "dev-token"
    principal_id = (
        env.get("QUEUE_API_PRINCIPAL_ID", "api-dev").strip() or "api-dev"
    )
    generations: list[CredentialGeneration] = [
        CredentialGeneration(
            principal_id=principal_id,
            generation_id=env.get("QUEUE_API_GENERATION_ID", "current").strip()
            or "current",
            secret=Secret(current),
        )
    ]
    previous = env.get("QUEUE_API_BEARER_TOKEN_PREVIOUS", "").strip()
    if previous:
        generations.append(
            CredentialGeneration(
                principal_id=principal_id,
                generation_id=env.get(
                    "QUEUE_API_PREVIOUS_GENERATION_ID", "previous"
                ).strip()
                or "previous",
                secret=Secret(previous),
            )
        )
    return tuple(generations)


def api_principals_from_environ(
    environ: Mapping[str, str] | None = None,
) -> tuple[tuple[CredentialBinding, ...] | None, Mapping[str, frozenset[str]]]:
    """Load the exclusive deployment principal manifest when non-blank.

    ``None`` bindings means the manifest is absent and the caller must retain
    the legacy ADMIN fallback. Any configured non-blank value is parsed
    strictly; parse failure never falls back to legacy credentials.
    """
    env = os.environ if environ is None else environ
    raw = env.get("QUEUE_API_PRINCIPALS_MANIFEST")
    if raw is None or not raw.strip():
        return None, MappingProxyType({})

    from workhold.security.principal_manifest import parse_principal_manifest

    parsed = parse_principal_manifest(raw.encode("utf-8"))
    return parsed.bindings, parsed.queue_scopes


def sentry_dsn_from_environ(
    environ: Mapping[str, str] | None = None,
) -> Secret | None:
    """Load optional GlitchTip/Sentry DSN from ``SENTRY_DSN``.

    Missing, empty, or whitespace-only values map to ``None``. Cleartext never
    appears in exceptions from this helper.
    """
    env = os.environ if environ is None else environ
    raw = env.get("SENTRY_DSN", "").strip()
    return Secret(raw) if raw else None


def from_environ(environ: Mapping[str, str] | None = None) -> DeploymentSettings | None:
    """Build validated deployment settings from process environment.

    Returns ``None`` when ``DATABASE_URL`` is unset/empty so role CLIs can fail
    closed with a dependency exit. Raises :class:`SettingsValidationError` for
    unsafe combinations (including production plaintext public exposure).
    """
    env = os.environ if environ is None else environ
    url = env.get("DATABASE_URL", "").strip()
    if not url:
        return None

    environment_raw = env.get("QUEUE_ENVIRONMENT", "development").strip().lower()
    try:
        environment = EnvironmentMode(environment_raw)
    except ValueError as exc:
        raise SettingsValidationError(
            f"unknown QUEUE_ENVIRONMENT: {environment_raw!r}"
        ) from exc

    tls_raw = env.get("QUEUE_LISTENER_TLS_MODE", "plaintext_public").strip().lower()
    try:
        listener_tls_mode = ListenerTlsMode(tls_raw)
    except ValueError as exc:
        raise SettingsValidationError(
            f"unknown QUEUE_LISTENER_TLS_MODE: {tls_raw!r}"
        ) from exc

    payload_days = _env_int(
        env, "QUEUE_PAYLOAD_RETENTION_DAYS", DEFAULT_PAYLOAD_RETENTION_DAYS
    )
    api_bindings, api_queue_scopes = api_principals_from_environ(env)

    return DeploymentSettings(
        environment=environment,
        listener_tls_mode=listener_tls_mode,
        database_url=Secret(url),
        postgres_max_connections=_env_int(env, "QUEUE_POSTGRES_MAX_CONNECTIONS", 100),
        postgres_reserved_connections=_env_int(
            env, "QUEUE_POSTGRES_RESERVED_CONNECTIONS", 10
        ),
        role_pools=role_pools_from_environ(env),
        credential_generations=(
            credential_generations_from_environ(env)
            if api_bindings is None
            else ()
        ),
        payload_retention_days=payload_days,
        enqueue_dedup_ttl_seconds=_env_int(
            env, "QUEUE_ENQUEUE_DEDUP_TTL_SECONDS", ENQUEUE_DEDUP_TTL_SECONDS_DEFAULT
        ),
        terminal_replay_ttl_seconds=_env_int(
            env,
            "QUEUE_TERMINAL_REPLAY_TTL_SECONDS",
            TERMINAL_REPLAY_TTL_SECONDS_DEFAULT,
        ),
        admin_replay_ttl_seconds=_env_int(
            env, "QUEUE_ADMIN_REPLAY_TTL_SECONDS", ADMIN_REPLAY_TTL_SECONDS_DEFAULT
        ),
        registry_purge_batch_size=_env_int(
            env, "QUEUE_REGISTRY_PURGE_BATCH_SIZE", REGISTRY_PURGE_BATCH_SIZE_DEFAULT
        ),
        schedule_horizon_seconds=_env_int(
            env,
            "QUEUE_SCHEDULE_HORIZON_SECONDS",
            SCHEDULE_HORIZON_SECONDS_DEFAULT,
        ),
        claim_max_wait_seconds=_env_int(
            env,
            "QUEUE_CLAIM_MAX_WAIT_SECONDS",
            CLAIM_MAX_WAIT_SECONDS_DEFAULT,
        ),
        claim_wait_fallback_seconds=_env_float(
            env,
            "QUEUE_CLAIM_WAIT_FALLBACK_SECONDS",
            CLAIM_WAIT_FALLBACK_SECONDS_DEFAULT,
        ),
        claim_cancellation_probe_seconds=_env_float(
            env,
            "QUEUE_CLAIM_CANCELLATION_PROBE_SECONDS",
            CLAIM_CANCELLATION_PROBE_SECONDS_DEFAULT,
        ),
        claim_max_outstanding_waits=_env_int(
            env,
            "QUEUE_CLAIM_MAX_OUTSTANDING_WAITS",
            CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT,
        ),
        sentry_dsn=sentry_dsn_from_environ(env),
        api_credential_bindings=api_bindings,
        api_queue_scopes=api_queue_scopes,
    )
