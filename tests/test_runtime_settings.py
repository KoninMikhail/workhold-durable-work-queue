"""Binary configuration and PostgreSQL connection-budget tests (DEP-03, SEC-04)."""

from __future__ import annotations

import json

import pytest
from sqlalchemy.engine import Engine

from workhold import db, settings


def _role_pools(
    *,
    pool_ceiling: int = 2,
    replica_ceiling: int = 1,
    acquisition_timeout: float = 5.0,
    statement_timeout: float = 30.0,
) -> dict[str, settings.RolePoolSettings]:
    return {
        role: settings.RolePoolSettings(
            replica_ceiling=replica_ceiling,
            pool_ceiling=pool_ceiling,
            pool_acquisition_timeout_seconds=acquisition_timeout,
            statement_timeout_seconds=statement_timeout,
        )
        for role in settings.PROCESS_ROLES
    }


def _valid_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "environment": settings.EnvironmentMode.DEVELOPMENT,
        "listener_tls_mode": settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        "database_url": settings.Secret("postgresql+psycopg://queue:s3cret@localhost:5432/queue"),
        "postgres_max_connections": 100,
        "postgres_reserved_connections": 10,
        "role_pools": _role_pools(pool_ceiling=2, replica_ceiling=1),
        # 6 roles × 1 replica × 2 pool = 12 <= 90 usable
        "credential_generations": (
            settings.CredentialGeneration(
                principal_id="producer-a",
                generation_id="gen-1",
                secret=settings.Secret("token-old"),
            ),
        ),
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    ("max_connections", "reserved", "replica_ceiling", "pool_ceiling", "expect_ok"),
    [
        # exact boundary: pool 48 + api listeners 2 = 50; usable = 60 - 10 = 50
        (60, 10, 2, 4, True),
        # one-connection overcommit: committed 50; usable = 60 - 11 = 49
        (60, 11, 2, 4, False),
        # under budget — pool 12 + api listener 1 = 13 <= 90
        (100, 10, 1, 2, True),
        # clear overcommit — pool 36 + api listeners 3 = 39 > 15
        (20, 5, 3, 2, False),
    ],
)
def test_connection_budget_boundary(
    max_connections: int,
    reserved: int,
    replica_ceiling: int,
    pool_ceiling: int,
    expect_ok: bool,
) -> None:
    kwargs = _valid_kwargs(
        postgres_max_connections=max_connections,
        postgres_reserved_connections=reserved,
        role_pools=_role_pools(
            replica_ceiling=replica_ceiling,
            pool_ceiling=pool_ceiling,
        ),
    )
    # Pool budget plus one dedicated LISTEN connection per API replica.
    committed = (6 * replica_ceiling * pool_ceiling) + replica_ceiling
    usable = max_connections - reserved
    assert (committed <= usable) is expect_ok
    if expect_ok:
        cfg = settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert cfg.committed_connections <= cfg.usable_connections
        engine = db.create_role_engine(cfg, "api")
        assert isinstance(engine, Engine)
        engine.dispose()
    else:
        with pytest.raises(settings.SettingsValidationError) as exc_info:
            settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert "s3cret" not in str(exc_info.value)
        assert "token-old" not in str(exc_info.value)


def test_overcommit_fails_before_engine_creation(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[object] = []

    def _fake_create_engine(*args: object, **kwargs: object) -> object:
        created.append((args, kwargs))
        raise AssertionError("create_engine must not run for invalid settings")

    monkeypatch.setattr(db, "create_engine", _fake_create_engine)

    with pytest.raises(settings.SettingsValidationError):
        cfg = settings.DeploymentSettings(
            **_valid_kwargs(  # type: ignore[arg-type]
                postgres_max_connections=20,
                postgres_reserved_connections=5,
                role_pools=_role_pools(replica_ceiling=3, pool_ceiling=2),
            )
        )
        db.create_role_engine(cfg, "api")

    assert created == []


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"postgres_reserved_connections": -1},
        {"postgres_reserved_connections": 100, "postgres_max_connections": 50},
        {
            "role_pools": _role_pools(acquisition_timeout=0.0),
        },
        {
            "role_pools": _role_pools(statement_timeout=-1.0),
        },
        {
            "role_pools": _role_pools(pool_ceiling=0),
        },
        {
            "role_pools": _role_pools(replica_ceiling=0),
        },
    ],
)
def test_invalid_numeric_settings_fail(bad_kwargs: dict[str, object]) -> None:
    kwargs = _valid_kwargs(**bad_kwargs)
    with pytest.raises(settings.SettingsValidationError) as exc_info:
        settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
    message = str(exc_info.value)
    assert "s3cret" not in message
    assert "token-old" not in message


def test_unknown_role_in_pools_fails() -> None:
    pools = _role_pools()
    pools["shadow"] = settings.RolePoolSettings(
        replica_ceiling=1,
        pool_ceiling=1,
        pool_acquisition_timeout_seconds=5.0,
        statement_timeout_seconds=30.0,
    )
    with pytest.raises(settings.SettingsValidationError, match="unknown"):
        settings.DeploymentSettings(**_valid_kwargs(role_pools=pools))  # type: ignore[arg-type]


def test_missing_role_pool_fails() -> None:
    pools = _role_pools()
    del pools["relay"]
    with pytest.raises(settings.SettingsValidationError):
        settings.DeploymentSettings(**_valid_kwargs(role_pools=pools))  # type: ignore[arg-type]


def test_every_process_role_has_explicit_bounded_pool() -> None:
    cfg = settings.DeploymentSettings(**_valid_kwargs())  # type: ignore[arg-type]
    assert set(cfg.role_pools) == set(settings.PROCESS_ROLES)
    for role in settings.PROCESS_ROLES:
        pool = cfg.role_pools[role]
        assert pool.pool_ceiling > 0
        assert pool.replica_ceiling > 0
        assert pool.pool_acquisition_timeout_seconds > 0
        assert pool.statement_timeout_seconds > 0
        engine = db.create_role_engine(cfg, role)
        assert engine.pool.size() == pool.pool_ceiling
        assert engine.pool._max_overflow == 0  # noqa: SLF001 — hard ceiling, no overflow
        engine.dispose()


def test_unknown_role_engine_rejected() -> None:
    cfg = settings.DeploymentSettings(**_valid_kwargs())  # type: ignore[arg-type]
    with pytest.raises(settings.SettingsValidationError, match="unknown"):
        db.create_role_engine(cfg, "worker")


def test_no_engine_at_import() -> None:
    assert getattr(db, "engine", None) is None
    assert not hasattr(db, "get_engine") or callable(getattr(db, "create_role_engine"))


@pytest.mark.parametrize(
    ("environment", "tls_mode", "expect_ok"),
    [
        (
            settings.EnvironmentMode.PRODUCTION,
            settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
            False,
        ),
        (
            settings.EnvironmentMode.PRODUCTION,
            settings.ListenerTlsMode.DIRECT_TLS,
            True,
        ),
        (
            settings.EnvironmentMode.PRODUCTION,
            settings.ListenerTlsMode.TRUSTED_PRIVATE_TLS_TERMINATION,
            True,
        ),
        (
            settings.EnvironmentMode.DEVELOPMENT,
            settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
            True,
        ),
    ],
)
def test_production_transport_policy(
    environment: settings.EnvironmentMode,
    tls_mode: settings.ListenerTlsMode,
    expect_ok: bool,
) -> None:
    kwargs = _valid_kwargs(environment=environment, listener_tls_mode=tls_mode)
    if expect_ok:
        settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
    else:
        with pytest.raises(settings.SettingsValidationError) as exc_info:
            settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert "s3cret" not in str(exc_info.value)


def test_overlapping_credential_generations_keep_stable_principal() -> None:
    generations = (
        settings.CredentialGeneration(
            principal_id="worker-1",
            generation_id="g1",
            secret=settings.Secret("super-secret-old"),
        ),
        settings.CredentialGeneration(
            principal_id="worker-1",
            generation_id="g2",
            secret=settings.Secret("super-secret-new"),
        ),
    )
    cfg = settings.DeploymentSettings(
        **_valid_kwargs(credential_generations=generations)  # type: ignore[arg-type]
    )
    assert [g.principal_id for g in cfg.credential_generations] == ["worker-1", "worker-1"]
    assert {g.generation_id for g in cfg.credential_generations} == {"g1", "g2"}

    rendered = repr(cfg) + str(cfg) + repr(cfg.credential_generations)
    assert "super-secret-old" not in rendered
    assert "super-secret-new" not in rendered
    for generation in cfg.credential_generations:
        assert "super-secret" not in repr(generation)
        assert "super-secret" not in str(generation.secret)


def test_settings_are_immutable() -> None:
    cfg = settings.DeploymentSettings(**_valid_kwargs())  # type: ignore[arg-type]
    with pytest.raises(Exception):
        cfg.postgres_max_connections = 1  # type: ignore[misc]


def test_secret_redacts_in_errors_and_repr() -> None:
    secret = settings.Secret("cleartext-token-xyz")
    assert "cleartext-token-xyz" not in repr(secret)
    assert "cleartext-token-xyz" not in str(secret)
    assert secret.get_secret_value() == "cleartext-token-xyz"


_SENTRY_SENTINEL = "SENTRY_DSN_SENTINEL_9z8y"


@pytest.mark.parametrize(
    "env_value",
    [None, "", "   ", "\t\n"],
    ids=["missing", "empty", "whitespace", "tab_newline"],
)
def test_sentry_dsn_from_environ_absent_or_blank_yields_none(
    env_value: str | None,
) -> None:
    env: dict[str, str] = {}
    if env_value is not None:
        env["SENTRY_DSN"] = env_value
    assert settings.sentry_dsn_from_environ(env) is None


def test_sentry_dsn_from_environ_non_empty_yields_secret() -> None:
    dsn = settings.sentry_dsn_from_environ({"SENTRY_DSN": _SENTRY_SENTINEL})
    assert isinstance(dsn, settings.Secret)
    assert dsn.get_secret_value() == _SENTRY_SENTINEL
    assert _SENTRY_SENTINEL not in repr(dsn)
    assert _SENTRY_SENTINEL not in str(dsn)


def test_sentry_dsn_from_environ_strips_surrounding_whitespace() -> None:
    dsn = settings.sentry_dsn_from_environ({"SENTRY_DSN": f"  {_SENTRY_SENTINEL}  "})
    assert isinstance(dsn, settings.Secret)
    assert dsn.get_secret_value() == _SENTRY_SENTINEL


def test_sentry_dsn_secret_redacts_in_deployment_settings_repr() -> None:
    cfg = settings.DeploymentSettings(
        **_valid_kwargs(sentry_dsn=settings.Secret(_SENTRY_SENTINEL))  # type: ignore[arg-type]
    )
    rendered = repr(cfg) + str(cfg)
    assert _SENTRY_SENTINEL not in rendered
    assert cfg.sentry_dsn is not None
    assert cfg.sentry_dsn.get_secret_value() == _SENTRY_SENTINEL


def test_sentry_dsn_from_environ_without_database_url_still_returns_none() -> None:
    env = {"SENTRY_DSN": _SENTRY_SENTINEL}
    assert settings.from_environ(env) is None
    dsn = settings.sentry_dsn_from_environ(env)
    assert isinstance(dsn, settings.Secret)
    assert dsn.get_secret_value() == _SENTRY_SENTINEL


def test_sentry_dsn_from_environ_with_database_url_sets_secret() -> None:
    env = {
        "DATABASE_URL": "postgresql+psycopg://queue:s3cret@localhost:5432/queue",
        "SENTRY_DSN": _SENTRY_SENTINEL,
    }
    cfg = settings.from_environ(env)
    assert cfg is not None
    assert cfg.sentry_dsn is not None
    assert cfg.sentry_dsn.get_secret_value() == _SENTRY_SENTINEL
    assert _SENTRY_SENTINEL not in repr(cfg)
    assert _SENTRY_SENTINEL not in str(cfg)


def test_sentry_dsn_defaults_to_none_in_valid_settings() -> None:
    cfg = settings.DeploymentSettings(**_valid_kwargs())  # type: ignore[arg-type]
    assert cfg.sentry_dsn is None


@pytest.mark.parametrize(
    ("horizon", "expect_ok"),
    [
        (settings.SCHEDULE_HORIZON_SECONDS_DEFAULT, True),
        (settings.SCHEDULE_HORIZON_SECONDS_MIN, True),
        (settings.SCHEDULE_HORIZON_SECONDS_MAX, True),
        (settings.SCHEDULE_HORIZON_SECONDS_MIN - 1, False),
        (settings.SCHEDULE_HORIZON_SECONDS_MAX + 1, False),
        (True, False),
    ],
)
def test_schedule_horizon_seconds_boundary(horizon: object, expect_ok: bool) -> None:
    kwargs = _valid_kwargs(schedule_horizon_seconds=horizon)
    if expect_ok:
        cfg = settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert cfg.schedule_horizon_seconds == horizon
    else:
        with pytest.raises(settings.SettingsValidationError) as exc_info:
            settings.DeploymentSettings(**kwargs)  # type: ignore[arg-type]
        assert "s3cret" not in str(exc_info.value)


def test_schedule_horizon_seconds_from_environ_defaults_and_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = {
        "DATABASE_URL": "postgresql+psycopg://queue:s3cret@localhost:5432/queue",
    }
    cfg = settings.from_environ(env)
    assert cfg is not None
    assert cfg.schedule_horizon_seconds == settings.SCHEDULE_HORIZON_SECONDS_DEFAULT

    env["QUEUE_SCHEDULE_HORIZON_SECONDS"] = "0"
    cfg_zero = settings.from_environ(env)
    assert cfg_zero is not None
    assert cfg_zero.schedule_horizon_seconds == 0

    env["QUEUE_SCHEDULE_HORIZON_SECONDS"] = "86400"
    cfg_max = settings.from_environ(env)
    assert cfg_max is not None
    assert cfg_max.schedule_horizon_seconds == 86400


def test_schedule_horizon_seconds_invalid_env_rejects_before_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[object] = []

    def _fake_create_engine(*args: object, **kwargs: object) -> object:
        created.append((args, kwargs))
        raise AssertionError("create_engine must not run for invalid settings")

    monkeypatch.setattr(db, "create_engine", _fake_create_engine)

    env = {
        "DATABASE_URL": "postgresql+psycopg://queue:s3cret@localhost:5432/queue",
        "QUEUE_SCHEDULE_HORIZON_SECONDS": "-1",
    }
    with pytest.raises(settings.SettingsValidationError):
        cfg = settings.from_environ(env)
        assert cfg is not None
        db.create_role_engine(cfg, "api")

    assert created == []


def _principal_manifest_json(*, secret: str = "manifest-token") -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "principals": [
                {
                    "principal_id": "producer-orders",
                    "role": "PRODUCER",
                    "queue_scopes": ["orders"],
                    "credentials": [
                        {"generation_id": "producer-current", "secret": secret}
                    ],
                }
            ],
        }
    )


def test_manifest_is_exclusive_with_legacy_generations() -> None:
    env = {
        "DATABASE_URL": "postgresql+psycopg://queue:queue@localhost/queue",
        "QUEUE_API_PRINCIPALS_MANIFEST": _principal_manifest_json(),
        "QUEUE_API_BEARER_TOKEN": "legacy-current",
        "QUEUE_API_BEARER_TOKEN_PREVIOUS": "legacy-previous",
    }
    cfg = settings.from_environ(env)
    assert cfg is not None
    assert cfg.credential_generations == ()
    assert cfg.api_credential_bindings is not None
    assert [b.principal_id for b in cfg.api_credential_bindings] == [
        "producer-orders"
    ]
    assert cfg.api_queue_scopes == {
        "producer-orders": frozenset({"orders"})
    }
    rendered = repr(cfg) + str(cfg)
    for secret in ("manifest-token", "legacy-current", "legacy-previous"):
        assert secret not in rendered


@pytest.mark.parametrize("manifest", [None, "", " \r\n"])
def test_absent_or_blank_manifest_retains_legacy_admin_fallback_inputs(
    manifest: str | None,
) -> None:
    env = {
        "DATABASE_URL": "postgresql+psycopg://queue:queue@localhost/queue",
        "QUEUE_API_BEARER_TOKEN": "legacy-current",
        "QUEUE_API_BEARER_TOKEN_PREVIOUS": "legacy-previous",
    }
    if manifest is not None:
        env["QUEUE_API_PRINCIPALS_MANIFEST"] = manifest
    cfg = settings.from_environ(env)
    assert cfg is not None
    assert cfg.api_credential_bindings is None
    assert cfg.api_queue_scopes == {}
    assert [g.generation_id for g in cfg.credential_generations] == [
        "current",
        "previous",
    ]


def test_invalid_manifest_fails_without_legacy_fallback_or_secret_disclosure() -> None:
    sentinel = "MANIFEST_SENTINEL_never_log"
    env = {
        "DATABASE_URL": "postgresql+psycopg://queue:queue@localhost/queue",
        "QUEUE_API_PRINCIPALS_MANIFEST": (
            '{"schema_version":1,"principals":[{"principal_id":"p",'
            '"role":"PRODUCER","queue_scopes":[],"credentials":'
            f'[{{"generation_id":"g","secret":"{sentinel}"}}]}}]}}'
        ),
        "QUEUE_API_BEARER_TOKEN": "legacy-must-not-fallback",
    }
    with pytest.raises(settings.SettingsValidationError) as exc_info:
        settings.from_environ(env)
    assert sentinel not in str(exc_info.value)
    assert "legacy-must-not-fallback" not in str(exc_info.value)
