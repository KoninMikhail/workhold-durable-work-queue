"""Integration: catalog apply role ensure-exists (CTRL-10 / D-12..D-19).

Owned by 13-04 except ``test_api_readiness_ignores_catalog`` (13-05).
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold import db, health, settings
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.storage.models import AdminAuditLog, Queue, QueuePolicyVersion

ROOT = Path(__file__).resolve().parents[2]
APPLY_ADVISORY_LOCK_KEY = 0x5155455541504C59
MIGRATE_ADVISORY_LOCK_KEY = 0x515545554D494752
MAINTAIN_ADVISORY_LOCK_KEY = 0x515545554D41494E
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_LOCK_TIMEOUT = 4

assert APPLY_ADVISORY_LOCK_KEY != MIGRATE_ADVISORY_LOCK_KEY
assert APPLY_ADVISORY_LOCK_KEY != MAINTAIN_ADVISORY_LOCK_KEY
assert APPLY_ADVISORY_LOCK_KEY != 0


def _require_apply():
    return pytest.importorskip(
        "workhold.roles.apply",
        reason="implemented by 13-04",
    )


def _role_pools(
    *,
    pool_ceiling: int = 2,
    replica_ceiling: int = 1,
    acquisition_timeout: float = 5.0,
    statement_timeout: float = 60.0,
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


def _settings_for_url(
    database_url: str,
    *,
    pool_ceiling: int = 2,
    acquisition_timeout: float = 5.0,
    statement_timeout: float = 60.0,
) -> settings.DeploymentSettings:
    return settings.DeploymentSettings(
        environment=settings.EnvironmentMode.DEVELOPMENT,
        listener_tls_mode=settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        database_url=settings.Secret(database_url),
        postgres_max_connections=100,
        postgres_reserved_connections=10,
        role_pools=_role_pools(
            pool_ceiling=pool_ceiling,
            acquisition_timeout=acquisition_timeout,
            statement_timeout=statement_timeout,
        ),
        credential_generations=(
            settings.CredentialGeneration(
                principal_id="apply-test",
                generation_id="gen-1",
                secret=settings.Secret("token"),
            ),
        ),
    )


@pytest.fixture
def sa_session(migrated_schema) -> Iterator[Session]:
    """SQLAlchemy session bound to the isolated Alembic-migrated schema."""
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for tests/integration")
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _catalog_bytes(*, names: list[str] | None = None, **overrides: Any) -> bytes:
    queues = []
    for name in names or ["orders"]:
        queues.append(
            {
                "name": name,
                "initial_policy": {
                    "enabled": True,
                    "max_attempts": 5,
                    "backoff_strategy": "fixed",
                    "retry_delay_seconds": 30,
                },
            }
        )
    body: dict[str, Any] = {"schema_version": 1, "queues": queues}
    body.update(overrides)
    return json.dumps(body).encode("utf-8")


def _write_catalog(tmp_path: Path, raw: bytes) -> Path:
    path = tmp_path / "catalog.json"
    path.write_bytes(raw)
    return path.resolve()


def _queue_count(session: Session) -> int:
    return int(session.scalar(select(func.count()).select_from(Queue)) or 0)


def _policy_count(session: Session) -> int:
    return int(session.scalar(select(func.count()).select_from(QueuePolicyVersion)) or 0)


def _meta(actor_id: str = "admin-seed") -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )


def _create_queue(
    session: Session,
    name: str,
    *,
    enabled: bool = True,
    max_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> Any:
    return QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=enabled,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=retry_delay_seconds,
            ),
            metadata=_meta(),
        ),
    )


def _hold_apply_lock(database_url: str) -> psycopg.Connection:
    from tests.integration.conftest import to_psycopg_conninfo

    conn = psycopg.connect(to_psycopg_conninfo(database_url))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (APPLY_ADVISORY_LOCK_KEY,))
    return conn


def _run_apply(
    apply_role: Any,
    *,
    database_url: str,
    catalog_path: Path,
    schema: str,
    lock_deadline_seconds: float = 5.0,
    monkeypatch: pytest.MonkeyPatch,
) -> int:
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("QUEUE_CATALOG_PATH", str(catalog_path))
    monkeypatch.setenv("ALEMBIC_VERSION_TABLE_SCHEMA", schema)
    monkeypatch.setenv(
        "QUEUE_APPLY_LOCK_DEADLINE_SECONDS",
        str(lock_deadline_seconds),
    )
    return int(apply_role.run([]))


def test_create_if_absent(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    before = _queue_count(sa_session)
    catalog = _write_catalog(tmp_path, _catalog_bytes(names=["orders.bootstrap"]))

    # Create must go through create_named_queue, not raw INSERT.
    create_calls: list[str] = []
    real_create = QueueControlRepository.create_named_queue

    def _spy(self: QueueControlRepository, session: Session, mutation: CreateQueueMutation):
        create_calls.append(mutation.name)
        return real_create(self, session, mutation)

    monkeypatch.setattr(QueueControlRepository, "create_named_queue", _spy)

    code = _run_apply(
        apply_role,
        database_url=database_url,
        catalog_path=catalog,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    assert code == EXIT_OK
    sa_session.expire_all()
    assert _queue_count(sa_session) == before + 1
    assert create_calls == ["orders.bootstrap"]
    cfg = QueueControlRepository().get_queue_configuration(
        sa_session, name="orders.bootstrap"
    )
    assert cfg is not None
    assert cfg.state is QueueState.ACTIVE


def test_skip_existing_state_and_policy(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    repo = QueueControlRepository()

    paused = _create_queue(
        sa_session, "skip.paused", enabled=False, max_attempts=1, retry_delay_seconds=0
    )
    sa_session.commit()
    repo.set_queue_state(
        sa_session,
        queue_name="skip.paused",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=1),
            state=QueueState.PAUSED,
            metadata=_meta("seed-pause"),
        ),
    )
    sa_session.commit()

    draining = _create_queue(
        sa_session, "skip.draining", enabled=True, max_attempts=9, retry_delay_seconds=7
    )
    sa_session.commit()
    repo.set_queue_state(
        sa_session,
        queue_name="skip.draining",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=1),
            state=QueueState.DRAINING,
            metadata=_meta("seed-drain"),
        ),
    )
    sa_session.commit()

    paused_row = sa_session.execute(
        select(Queue).where(Queue.name == "skip.paused")
    ).scalar_one()
    draining_row = sa_session.execute(
        select(Queue).where(Queue.name == "skip.draining")
    ).scalar_one()
    before_policies = _policy_count(sa_session)
    snap = {
        "paused": (
            paused_row.config_version,
            paused_row.state_code,
            paused_row.active_policy_version_id,
        ),
        "draining": (
            draining_row.config_version,
            draining_row.state_code,
            draining_row.active_policy_version_id,
        ),
    }

    # Catalog policy differs from seeded foreign policy — must still skip.
    catalog = _write_catalog(
        tmp_path,
        _catalog_bytes(names=["skip.paused", "skip.draining"]),
    )
    code = _run_apply(
        apply_role,
        database_url=database_url,
        catalog_path=catalog,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    assert code == EXIT_OK
    sa_session.expire_all()
    paused_after = sa_session.execute(
        select(Queue).where(Queue.name == "skip.paused")
    ).scalar_one()
    draining_after = sa_session.execute(
        select(Queue).where(Queue.name == "skip.draining")
    ).scalar_one()
    assert (
        paused_after.config_version,
        paused_after.state_code,
        paused_after.active_policy_version_id,
    ) == snap["paused"]
    assert (
        draining_after.config_version,
        draining_after.state_code,
        draining_after.active_policy_version_id,
    ) == snap["draining"]
    assert _policy_count(sa_session) == before_policies
    assert paused.queue_id is not None and draining.queue_id is not None


def test_invalid_catalog_fail_closed(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    before = _queue_count(sa_session)

    cases: list[tuple[str, Path | None]] = []
    # unset QUEUE_CATALOG_PATH (reachable assertion; Wave 0 had a dead None branch)
    cases.append(("unset", None))
    # missing path
    missing = tmp_path / "missing-catalog.json"
    cases.append(("missing", missing.resolve()))
    # relative path
    relative = Path("relative-catalog.json")
    (tmp_path / relative.name).write_bytes(_catalog_bytes())
    cases.append(("relative", relative))
    # empty file
    empty = tmp_path / "empty.json"
    empty.write_bytes(b"")
    cases.append(("empty", empty.resolve()))
    # invalid JSON / schema
    invalid = tmp_path / "invalid-schema.json"
    invalid.write_bytes(b'{"schema_version":2,"queues":[]}')
    cases.append(("invalid", invalid.resolve()))

    for _label, path in cases:
        monkeypatch.setenv("DATABASE_URL", database_url)
        if path is None:
            monkeypatch.delenv("QUEUE_CATALOG_PATH", raising=False)
        else:
            monkeypatch.setenv("QUEUE_CATALOG_PATH", str(path))
        monkeypatch.setenv("ALEMBIC_VERSION_TABLE_SCHEMA", schema)
        code = int(apply_role.run([]))
        assert code == EXIT_USAGE
        err = capsys.readouterr().err
        assert err.startswith("apply:") or "apply: " in err
        assert "postgresql://" not in err.lower()
        assert "schema_version" not in err
        assert '"queues"' not in err
        sa_session.expire_all()
        assert _queue_count(sa_session) == before


def test_lock_timeout(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apply_role = _require_apply()
    assert apply_role.APPLY_ADVISORY_LOCK_KEY == APPLY_ADVISORY_LOCK_KEY
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    before = _queue_count(sa_session)
    catalog = _write_catalog(tmp_path, _catalog_bytes(names=["lock.loser"]))

    holder = _hold_apply_lock(database_url)
    try:
        started = time.monotonic()
        code = _run_apply(
            apply_role,
            database_url=database_url,
            catalog_path=catalog,
            schema=schema,
            lock_deadline_seconds=1.0,
            monkeypatch=monkeypatch,
        )
        elapsed = time.monotonic() - started
        assert code == EXIT_LOCK_TIMEOUT
        assert elapsed < 2.5
        err = capsys.readouterr().err
        assert "lock_timeout" in err
        assert "postgresql://" not in err.lower()
        assert database_url not in err
        sa_session.expire_all()
        assert _queue_count(sa_session) == before
    finally:
        holder.close()


def test_create_audit_row(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    catalog = _write_catalog(
        tmp_path,
        _catalog_bytes(names=["audit.one", "audit.two"]),
    )
    code = _run_apply(
        apply_role,
        database_url=database_url,
        catalog_path=catalog,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    assert code == EXIT_OK
    sa_session.expire_all()
    audits = list(
        sa_session.scalars(
            select(AdminAuditLog).where(AdminAuditLog.actor_id == "catalog_apply")
        )
    )
    assert len(audits) >= 2
    request_ids = {a.request_id for a in audits[-2:]}
    assert len(request_ids) == 1  # same run request_id
    for audit in audits[-2:]:
        assert audit.actor_id == "catalog_apply"
        assert audit.operation_code == 1
        assert audit.previous_config_version is None
        assert audit.new_config_version == 1


def test_apply_never_starts_http_listener(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    catalog = _write_catalog(tmp_path, _catalog_bytes(names=["no.http"]))
    original_bind = socket.socket.bind

    def _forbid_bind(self: socket.socket, address: object) -> None:  # noqa: ANN001
        raise AssertionError(f"apply must not bind sockets; got {address!r}")

    socket.socket.bind = _forbid_bind  # type: ignore[method-assign]
    try:
        code = _run_apply(
            apply_role,
            database_url=database_url,
            catalog_path=catalog,
            schema=schema,
            monkeypatch=monkeypatch,
        )
        assert code == EXIT_OK
    finally:
        socket.socket.bind = original_bind  # type: ignore[method-assign]


def test_all_already_exist_ok(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    _create_queue(sa_session, "already.here")
    sa_session.commit()
    before = _queue_count(sa_session)
    catalog = _write_catalog(tmp_path, _catalog_bytes(names=["already.here"]))
    code = _run_apply(
        apply_role,
        database_url=database_url,
        catalog_path=catalog,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    assert code == EXIT_OK
    sa_session.expire_all()
    assert _queue_count(sa_session) == before


def test_api_readiness_ignores_catalog(
    migrated_schema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CTRL-10 / D-12: readiness never reads QUEUE_CATALOG_PATH (13-05)."""
    forbidden = ("QUEUE_CATALOG", "catalog_apply", "roles.apply")
    health_src = (ROOT / "src/workhold/health.py").read_text(encoding="utf-8")
    api_src = (ROOT / "src/workhold/roles/api.py").read_text(encoding="utf-8")
    for needle in forbidden:
        assert needle not in health_src, f"health.py must not reference {needle}"
        assert needle not in api_src, f"roles/api.py must not reference {needle}"

    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    engine: Engine = db.create_role_engine(_settings_for_url(database_url), "api")
    try:
        monkeypatch.delenv("QUEUE_CATALOG_PATH", raising=False)
        ready_unset = health.check_readiness(engine, schema=schema)
        # Readiness depends on schema/partitions only — unset catalog must not fail for catalog.
        assert ready_unset.reason_code != "catalog_missing"
        assert "catalog" not in (ready_unset.reason_code or "").lower()

        missing_catalog = (ROOT / "missing-catalog-for-readyz.json").resolve()
        assert not missing_catalog.exists()
        monkeypatch.setenv("QUEUE_CATALOG_PATH", str(missing_catalog))
        ready_missing = health.check_readiness(engine, schema=schema)
        assert ready_missing.reason_code != "catalog_missing"
        assert "catalog" not in (ready_missing.reason_code or "").lower()
        # Outcome must match schema readiness, not catalog presence.
        assert ready_missing.ok == ready_unset.ok
        assert ready_missing.reason_code == ready_unset.reason_code
    finally:
        engine.dispose()


def test_mid_catalog_failure_keeps_earlier_creates(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    catalog = _write_catalog(
        tmp_path,
        _catalog_bytes(names=["partial.first", "partial.second"]),
    )

    real_create = QueueControlRepository.create_named_queue
    calls = {"n": 0}

    def _fail_second(
        self: QueueControlRepository,
        session: Session,
        mutation: CreateQueueMutation,
    ):
        calls["n"] += 1
        if mutation.name == "partial.second":
            raise RuntimeError("forced mid-catalog failure")
        return real_create(self, session, mutation)

    monkeypatch.setattr(QueueControlRepository, "create_named_queue", _fail_second)
    code = _run_apply(
        apply_role,
        database_url=database_url,
        catalog_path=catalog,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    assert code == apply_role.EXIT_APPLY_FAILED
    err = capsys.readouterr().err
    assert "apply_failed" in err
    assert "postgresql://" not in err.lower()
    assert "forced mid-catalog" not in err
    assert database_url not in err
    sa_session.expire_all()
    first = QueueControlRepository().get_queue_configuration(
        sa_session, name="partial.first"
    )
    second = QueueControlRepository().get_queue_configuration(
        sa_session, name="partial.second"
    )
    assert first is not None
    assert second is None


def test_removed_name_is_not_deleted(
    migrated_schema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sa_session: Session,
) -> None:
    apply_role = _require_apply()
    _conn, schema = migrated_schema
    database_url = os.environ["TEST_DATABASE_URL"].strip()
    _create_queue(sa_session, "keep.me")
    _create_queue(sa_session, "also.keep")
    sa_session.commit()
    # Catalog omits keep.me — must remain present (D-03).
    catalog = _write_catalog(tmp_path, _catalog_bytes(names=["also.keep"]))
    code = _run_apply(
        apply_role,
        database_url=database_url,
        catalog_path=catalog,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    assert code == EXIT_OK
    sa_session.expire_all()
    assert (
        QueueControlRepository().get_queue_configuration(sa_session, name="keep.me")
        is not None
    )
    assert (
        QueueControlRepository().get_queue_configuration(sa_session, name="also.keep")
        is not None
    )
