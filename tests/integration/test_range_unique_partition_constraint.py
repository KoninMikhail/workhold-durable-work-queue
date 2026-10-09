"""Live uniqueness-on-partition re-validation on PostgreSQL 18.6 (STOR-09 / D-09).

Isolated ``uniq_probe`` only — never ALTER product RANGE parents.
"""

from __future__ import annotations

import psycopg
import psycopg.errors
import pytest

PRODUCT_RANGE_PARENTS = frozenset(
    {
        "task_attempts",
        "tasks_terminal",
        "admin_audit_log",
        "delivery_events_terminal",
    }
)


def test_range_parent_rejects_unique_without_partition_key(migrated_schema) -> None:
    """UNIQUE (claim_id) on RANGE(claimed_at) must fail SQLSTATE 0A000 on 18.6."""
    conn, _schema = migrated_schema

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE uniq_probe (
                id bigint NOT NULL,
                claim_id uuid NOT NULL,
                claimed_at timestamptz NOT NULL
            ) PARTITION BY RANGE (claimed_at)
            """
        )
    conn.commit()

    try:
        unique_succeeded = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE uniq_probe
                    ADD CONSTRAINT uniq_probe_claim_id_key UNIQUE (claim_id)
                    """
                )
            conn.commit()
            unique_succeeded = True
        except psycopg.errors.FeatureNotSupported as exc:
            conn.rollback()
            assert exc.sqlstate == "0A000", (
                f"expected SQLSTATE 0A000, got {exc.sqlstate!r}: {exc}"
            )
            detail = (exc.diag.message_detail or "") + " " + (str(exc) or "")
            assert "claimed_at" in detail.lower() or "partition" in detail.lower(), (
                f"expected DETAIL/message to mention partition key claimed_at: {exc!r}"
            )
        except Exception as exc:
            conn.rollback()
            pytest.fail(
                f"UNIQUE (claim_id) raised unexpected error "
                f"(sqlstate={getattr(exc, 'sqlstate', None)!r}): {exc!r}"
            )

        if unique_succeeded:
            pytest.fail(
                "BLOCK to planning (D-09): UNIQUE (claim_id) unexpectedly succeeded "
                "on RANGE (claimed_at) parent under PostgreSQL 18.6. Do not move "
                "uniqueness into product history parents — re-open phase planning."
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE uniq_probe
                ADD CONSTRAINT uniq_probe_pk PRIMARY KEY (id, claimed_at)
                """
            )
            cur.execute(
                """
                CREATE TABLE uniq_probe_d20260101
                    PARTITION OF uniq_probe
                    FOR VALUES FROM ('2026-01-01') TO ('2026-01-02')
                """
            )
        conn.commit()

        with pytest.raises(psycopg.errors.CheckViolation) as raised:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO uniq_probe (id, claim_id, claimed_at)
                    VALUES (1, gen_random_uuid(), TIMESTAMPTZ '2026-02-01 00:00:00+00')
                    """
                )
            conn.commit()
        conn.rollback()
        message = str(raised.value)
        assert 'no partition of relation "uniq_probe" found for row' in message, (
            f"expected no-partition INSERT failure, got: {message!r}"
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relkind = 'p'
                  AND c.relname = ANY(%s)
                """,
                (list(PRODUCT_RANGE_PARENTS),),
            )
            product_parents = {row[0] for row in cur.fetchall()}
        # Product parents may exist from Alembic; assert we never renamed/dropped them.
        assert product_parents == PRODUCT_RANGE_PARENTS or product_parents <= PRODUCT_RANGE_PARENTS
        with conn.cursor() as cur:
            # Probe must exist as its own parent, not a product table rename.
            cur.execute(
                """
                SELECT c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = 'uniq_probe'
                  AND c.relkind = 'p'
                """
            )
            assert cur.fetchone() is not None
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS uniq_probe CASCADE")
        conn.commit()
