"""Real-PostgreSQL correctness registry TTL + incremental purge (STOR-08).

Proves ADR-017 / Phase 3.1 defaults and inclusive bounds, reuse of Phase 3.1
``admin_replay`` only, and bounded concurrent-safe expiry purge on a held session.
"""

from __future__ import annotations

import ast
import inspect
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from workhold import db, settings

# Under test — remapped from plan storage/postgres → infrastructure/postgres.
from workhold.infrastructure.postgres import registry_retention

UTC = timezone.utc

_RETENTION_SRC = Path(inspect.getsourcefile(registry_retention) or "")

# Phase 3.1 / storage-contract seconds.
ENQUEUE_DEFAULT = 7_776_000
ENQUEUE_MIN = 2_592_000
ENQUEUE_MAX = 31_536_000
TERMINAL_DEFAULT = 604_800
TERMINAL_MIN = 86_400
TERMINAL_MAX = 2_592_000
ADMIN_DEFAULT = 2_592_000
ADMIN_MIN = 604_800
ADMIN_MAX = 7_776_000
BATCH_DEFAULT = 1000
BATCH_MIN = 1
BATCH_MAX = 10_000

ADMIN_OPS = (1, 2, 3, 4, 5)


def _role_pools() -> dict[str, settings.RolePoolSettings]:
    return {
        role: settings.RolePoolSettings(
            replica_ceiling=1,
            pool_ceiling=2,
            pool_acquisition_timeout_seconds=5.0,
            statement_timeout_seconds=30.0,
        )
        for role in settings.PROCESS_ROLES
    }


def _valid_settings(**overrides: object) -> settings.DeploymentSettings:
    base: dict[str, object] = {
        "environment": settings.EnvironmentMode.DEVELOPMENT,
        "listener_tls_mode": settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        "database_url": settings.Secret(
            "postgresql+psycopg://queue:s3cret@localhost:5432/queue"
        ),
        "postgres_max_connections": 100,
        "postgres_reserved_connections": 10,
        "role_pools": _role_pools(),
        "credential_generations": (
            settings.CredentialGeneration(
                principal_id="producer-a",
                generation_id="gen-1",
                secret=settings.Secret("token-old"),
            ),
        ),
    }
    base.update(overrides)
    return settings.DeploymentSettings(**base)  # type: ignore[arg-type]


@pytest.fixture
def retention_schema(test_database_url: str) -> Iterator[tuple[str, str, Engine]]:
    """Fresh migrated schema; yield (schema, url, engine). Always DROP."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    engine = create_engine(test_database_url, pool_pre_ping=True)
    try:
        yield schema, test_database_url, engine
    finally:
        engine.dispose()
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def _set_search_path(conn: Connection, schema: str) -> None:
    conn.execute(text(f'SET search_path TO "{schema}"'))


def _store_now(conn: Connection) -> datetime:
    value = conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
    assert isinstance(value, datetime)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _seed_queue(conn: Connection) -> int:
    return int(
        conn.execute(
            text(
                """
                INSERT INTO queues (queue_id, name)
                VALUES (gen_random_uuid(), :name)
                RETURNING id
                """
            ),
            {"name": f"reg-q-{uuid.uuid4().hex[:8]}"},
        ).scalar_one()
    )


def _hash32(seed: int) -> bytes:
    return bytes([(seed + i) % 256 for i in range(32)])


def _insert_enqueue(
    conn: Connection,
    *,
    queue_id: int,
    created_at: datetime,
    expires_at: datetime,
    producer_id: str | None = None,
) -> int:
    return int(
        conn.execute(
            text(
                """
                INSERT INTO enqueue_dedup (
                  producer_id, queue_id, key_hash, request_fingerprint,
                  task_id, created_at, expires_at
                ) VALUES (
                  :producer, :qid, :kh, :fp, gen_random_uuid(), :created, :expires
                )
                RETURNING id
                """
            ),
            {
                "producer": producer_id or f"p-{uuid.uuid4().hex[:8]}",
                "qid": queue_id,
                "kh": _hash32(1),
                "fp": _hash32(2),
                "created": created_at,
                "expires": expires_at,
            },
        ).scalar_one()
    )


def _insert_complete(
    conn: Connection,
    *,
    created_at: datetime,
    expires_at: datetime,
    claim_id: uuid.UUID | None = None,
) -> int:
    return int(
        conn.execute(
            text(
                """
                INSERT INTO complete_replay (
                  claim_id, operation_code, request_fingerprint, task_id,
                  result_state_code, available_at, terminal_at,
                  spawned_task_ids, event_ids, created_at, expires_at
                ) VALUES (
                  :claim, 1, :fp, gen_random_uuid(),
                  10, NULL, :created,
                  '{}'::uuid[], '{}'::uuid[], :created, :expires
                )
                RETURNING id
                """
            ),
            {
                "claim": claim_id or uuid.uuid4(),
                "fp": _hash32(3),
                "created": created_at,
                "expires": expires_at,
            },
        ).scalar_one()
    )


def _insert_admin(
    conn: Connection,
    *,
    operation_code: int,
    created_at: datetime,
    expires_at: datetime,
    http_status: int = 200,
    response_body: dict | None = None,
    principal_id: str | None = None,
    key_seed: int = 10,
) -> int:
    body = response_body if response_body is not None else {"op": operation_code, "ok": True}
    return int(
        conn.execute(
            text(
                """
                INSERT INTO admin_replay (
                  admin_principal_id, operation_code, key_hash, request_fingerprint,
                  http_status, response_body, created_at, expires_at
                ) VALUES (
                  :principal, :op, :kh, :fp,
                  :status, CAST(:body AS jsonb), :created, :expires
                )
                RETURNING id
                """
            ),
            {
                "principal": principal_id or f"admin-{uuid.uuid4().hex[:8]}",
                "op": operation_code,
                "kh": _hash32(key_seed + operation_code),
                "fp": _hash32(20 + operation_code),
                "status": http_status,
                "body": __import__("json").dumps(body),
                "created": created_at,
                "expires": expires_at,
            },
        ).scalar_one()
    )


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        base = _call_name(func.value)
        return f"{base}.{func.attr}" if base else func.attr
    return ""


def _forbidden_ast_hits(module_path: Path) -> list[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name in {
                "pg_advisory_lock",
                "pg_try_advisory_lock",
                "pg_advisory_unlock",
                "create_engine",
                "create_role_engine",
            }:
                hits.append(name)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            lowered = node.value.lower()
            if "pg_advisory" in lowered:
                hits.append(node.value)
    return hits


def test_omitted_settings_default_to_90d_7d_30d_and_batch_1000() -> None:
    cfg = _valid_settings()
    assert cfg.enqueue_dedup_ttl_seconds == ENQUEUE_DEFAULT
    assert cfg.terminal_replay_ttl_seconds == TERMINAL_DEFAULT
    assert cfg.admin_replay_ttl_seconds == ADMIN_DEFAULT
    assert cfg.registry_purge_batch_size == BATCH_DEFAULT
    assert ENQUEUE_DEFAULT == 90 * 24 * 3600
    assert TERMINAL_DEFAULT == 7 * 24 * 3600
    assert ADMIN_DEFAULT == 30 * 24 * 3600


@pytest.mark.parametrize(
    ("field", "ok_lo", "ok_hi", "bad_lo", "bad_hi"),
    [
        (
            "enqueue_dedup_ttl_seconds",
            ENQUEUE_MIN,
            ENQUEUE_MAX,
            ENQUEUE_MIN - 1,
            ENQUEUE_MAX + 1,
        ),
        (
            "terminal_replay_ttl_seconds",
            TERMINAL_MIN,
            TERMINAL_MAX,
            TERMINAL_MIN - 1,
            TERMINAL_MAX + 1,
        ),
        (
            "admin_replay_ttl_seconds",
            ADMIN_MIN,
            ADMIN_MAX,
            ADMIN_MIN - 1,
            ADMIN_MAX + 1,
        ),
        (
            "registry_purge_batch_size",
            BATCH_MIN,
            BATCH_MAX,
            BATCH_MIN - 1,
            BATCH_MAX + 1,
        ),
    ],
)
def test_ttl_and_batch_bounds_inclusive_one_second_out(
    field: str,
    ok_lo: int,
    ok_hi: int,
    bad_lo: int,
    bad_hi: int,
) -> None:
    _valid_settings(**{field: ok_lo})
    _valid_settings(**{field: ok_hi})
    with pytest.raises(settings.SettingsValidationError):
        _valid_settings(**{field: bad_lo})
    with pytest.raises(settings.SettingsValidationError):
        _valid_settings(**{field: bad_hi})


@pytest.mark.parametrize(
    "field",
    [
        "enqueue_dedup_ttl_seconds",
        "terminal_replay_ttl_seconds",
        "admin_replay_ttl_seconds",
        "registry_purge_batch_size",
    ],
)
def test_ttl_rejects_booleans_and_fractions(field: str) -> None:
    with pytest.raises(settings.SettingsValidationError):
        _valid_settings(**{field: True})
    with pytest.raises(settings.SettingsValidationError):
        _valid_settings(**{field: False})
    with pytest.raises(settings.SettingsValidationError):
        _valid_settings(**{field: 1000.5})  # type: ignore[arg-type]


def test_invalid_ttl_fails_before_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[object] = []

    def _fake_create_engine(*args: object, **kwargs: object) -> object:
        created.append((args, kwargs))
        raise AssertionError("create_engine must not run for invalid TTL")

    monkeypatch.setattr(db, "create_engine", _fake_create_engine)
    with pytest.raises(settings.SettingsValidationError):
        cfg = _valid_settings(enqueue_dedup_ttl_seconds=ENQUEUE_MIN - 1)
        db.create_role_engine(cfg, "maintain")
    assert created == []


def test_modules_forbid_advisory_locks_and_engine_creation() -> None:
    assert _RETENTION_SRC.is_file()
    hits = _forbidden_ast_hits(_RETENTION_SRC)
    assert hits == [], f"registry_retention must not lock/create engines: {hits}"
    src = _RETENTION_SRC.read_text(encoding="utf-8")
    assert "create_engine" not in src
    assert "create_role_engine" not in src
    assert "pg_advisory" not in src.lower()
    assert "FOR UPDATE" in src.upper()
    assert "SKIP LOCKED" in src.upper()


def test_no_alternate_admin_registry_model_or_migration() -> None:
    models_src = Path("src/workhold/storage/models.py").read_text(encoding="utf-8")
    assert models_src.count('__tablename__ = "admin_replay"') == 1
    assert "admin_replay_alt" not in models_src
    assert "AdminReplayAlt" not in models_src
    assert 'class AdminReplay' in models_src

    mig_root = Path("alembic/versions")
    alt_hits: list[str] = []
    for path in mig_root.glob("*.py"):
        text_body = path.read_text(encoding="utf-8")
        for needle in (
            "admin_replay_v2",
            "admin_idempotency",
            "create table admin_replay_",
            "CREATE TABLE admin_replay_",
        ):
            if needle.lower() in text_body.lower() and "admin_replay" in needle.lower():
                # Only flag alternate names, not the canonical relation.
                if "admin_replay" in needle and needle != "admin_replay":
                    alt_hits.append(f"{path.name}:{needle}")
        if "admin_replay_alt" in text_body or "AdminReplayAlt" in text_body:
            alt_hits.append(path.name)
    assert alt_hits == []

    # Retention module must target the exact relation name.
    ret = _RETENTION_SRC.read_text(encoding="utf-8")
    assert "admin_replay" in ret
    assert "admin_replay_alt" not in ret


def test_admin_replay_preserves_five_ops_status_body_until_exact_expiry(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        now = _store_now(conn)
        # Unexpired: created recently, TTL 30d within CHECK 7..90.
        keep_created = now - timedelta(days=1)
        keep_expires = keep_created + timedelta(days=30)
        # Exact-boundary expired: expires_at == store_now (within CHECK vs created).
        expired_created = now - timedelta(days=30)
        expired_expires = now  # exact expiry → purgeable

        preserved: dict[int, dict] = {}
        for op in ADMIN_OPS:
            row_id = _insert_admin(
                conn,
                operation_code=op,
                created_at=keep_created,
                expires_at=keep_expires,
                http_status=200 + (op % 10),
                response_body={"operation": op, "payload": f"body-{op}"},
                key_seed=100,
            )
            preserved[op] = {
                "id": row_id,
                "http_status": 200 + (op % 10),
                "response_body": {"operation": op, "payload": f"body-{op}"},
            }
            _insert_admin(
                conn,
                operation_code=op,
                created_at=expired_created,
                expires_at=expired_expires,
                http_status=201,
                response_body={"gone": op},
                principal_id=f"exp-admin-{op}",
                key_seed=200,
            )
        conn.commit()

        result = registry_retention.purge_expired_registries(
            conn, batch_size=BATCH_DEFAULT
        )
        admin_outcome = next(o for o in result.outcomes if o.registry == "admin_replay")
        assert admin_outcome.deleted == 5

        for op in ADMIN_OPS:
            row = conn.execute(
                text(
                    """
                    SELECT operation_code, http_status, response_body,
                           request_fingerprint, expires_at
                    FROM admin_replay WHERE id = :id
                    """
                ),
                {"id": preserved[op]["id"]},
            ).mappings().one()
            assert int(row["operation_code"]) == op
            assert int(row["http_status"]) == preserved[op]["http_status"]
            assert row["response_body"] == preserved[op]["response_body"]
            assert row["request_fingerprint"] == _hash32(20 + op)
            assert row["expires_at"] == keep_expires

        remaining_expired = conn.execute(
            text("SELECT count(*) FROM admin_replay WHERE expires_at <= :now"),
            {"now": result.store_now},
        ).scalar_one()
        assert int(remaining_expired) == 0


def test_purge_respects_batch_and_only_expired_at_store_cutoff(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        now = _store_now(conn)
        qid = _seed_queue(conn)

        # Per-registry CHECK windows: enqueue 30–365d, complete 1–30d, admin 7–90d.
        enq_created = now - timedelta(days=100)
        enq_expired_at = enq_created + timedelta(days=90)
        term_created = now - timedelta(days=40)
        term_expired_at = term_created + timedelta(days=7)
        admin_created = now - timedelta(days=60)
        admin_expired_at = admin_created + timedelta(days=30)

        keep_created = now - timedelta(days=1)
        enq_keep_at = keep_created + timedelta(days=90)
        term_keep_at = keep_created + timedelta(days=7)
        admin_keep_at = keep_created + timedelta(days=30)

        expired_ids = [
            _insert_enqueue(
                conn, queue_id=qid, created_at=enq_created, expires_at=enq_expired_at
            )
            for _ in range(5)
        ]
        keep_id = _insert_enqueue(
            conn, queue_id=qid, created_at=keep_created, expires_at=enq_keep_at
        )
        for _ in range(5):
            _insert_complete(
                conn, created_at=term_created, expires_at=term_expired_at
            )
        _insert_complete(conn, created_at=keep_created, expires_at=term_keep_at)
        for op in ADMIN_OPS:
            _insert_admin(
                conn,
                operation_code=op,
                created_at=admin_created,
                expires_at=admin_expired_at,
                principal_id=f"batch-exp-{op}",
            )
            _insert_admin(
                conn,
                operation_code=op,
                created_at=keep_created,
                expires_at=admin_keep_at,
                principal_id=f"batch-keep-{op}",
            )
        conn.commit()

        batch = 2
        result = registry_retention.purge_expired_registries(conn, batch_size=batch)
        by_name = {o.registry: o for o in result.outcomes}
        assert by_name["enqueue_dedup"].deleted == batch
        assert by_name["enqueue_dedup"].examined == batch
        assert by_name["enqueue_dedup"].more_work is True
        assert by_name["complete_replay"].deleted == batch
        assert by_name["complete_replay"].more_work is True
        assert by_name["admin_replay"].deleted == batch
        assert by_name["admin_replay"].more_work is True

        still = conn.execute(
            text("SELECT id FROM enqueue_dedup WHERE id = ANY(:ids)"),
            {"ids": expired_ids},
        ).scalars().all()
        assert len(still) == 3
        assert conn.execute(
            text("SELECT count(*) FROM enqueue_dedup WHERE id = :id"),
            {"id": keep_id},
        ).scalar_one() == 1

        # Second pass drains more; never touches unexpired.
        result2 = registry_retention.purge_expired_registries(conn, batch_size=batch)
        assert result2.outcomes[0].deleted <= batch
        assert (
            conn.execute(
                text("SELECT count(*) FROM enqueue_dedup WHERE id = :id"),
                {"id": keep_id},
            ).scalar_one()
            == 1
        )


def test_repeated_and_concurrent_purge_converges_without_unexpired_loss(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        now = _store_now(conn)
        qid = _seed_queue(conn)
        expired_created = now - timedelta(days=40)
        expired_at = expired_created + timedelta(days=30)
        keep_created = now - timedelta(hours=1)
        keep_expires = keep_created + timedelta(days=90)

        for i in range(20):
            _insert_enqueue(
                conn,
                queue_id=qid,
                created_at=expired_created,
                expires_at=expired_at,
                producer_id=f"exp-{i}",
            )
        keep_id = _insert_enqueue(
            conn,
            queue_id=qid,
            created_at=keep_created,
            expires_at=keep_expires,
            producer_id="keep-me",
        )
        conn.commit()

    errors: list[BaseException] = []

    def _worker() -> None:
        try:
            with engine.connect() as c:
                _set_search_path(c, schema)
                for _ in range(15):
                    registry_retention.purge_expired_registries(c, batch_size=3)
                    c.commit()
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()

    assert errors == []
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        expired_left = conn.execute(
            text(
                """
                SELECT count(*) FROM enqueue_dedup
                WHERE expires_at <= CURRENT_TIMESTAMP
                """
            )
        ).scalar_one()
        assert int(expired_left) == 0
        assert (
            conn.execute(
                text("SELECT count(*) FROM enqueue_dedup WHERE id = :id"),
                {"id": keep_id},
            ).scalar_one()
            == 1
        )
        # Idle repeated run is a no-op.
        idle = registry_retention.purge_expired_registries(conn, batch_size=5)
        enq = next(o for o in idle.outcomes if o.registry == "enqueue_dedup")
        assert enq.deleted == 0
        assert enq.more_work is False


def test_held_session_only_no_advisory_or_engine(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        marker = id(conn)
        registry_retention.purge_expired_registries(conn, batch_size=10)
        assert id(conn) == marker
        assert not conn.closed
        assert _store_now(conn) is not None
        conn.commit()
