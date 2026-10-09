"""Durable break-glass replay rate elevations (multi-replica TTL).

Revision ID: 044_break_glass_elevations
Revises: 043_break_glass_delivery_ops
Create Date: 2026-09-21

Phase 14 OPS-09 / REC-03 / D-10..D-12: ``raiseReplayLimit`` persists a
queue-scoped elevation readable by every API worker on admit; expires_at uses
Queue-store time.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "044_break_glass_elevations"
down_revision: Union[str, Sequence[str], None] = "043_break_glass_delivery_ops"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE break_glass_elevations (
            queue_name TEXT NOT NULL,
            factor DOUBLE PRECISION NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            actor_id TEXT NOT NULL,
            incident_ref_hash TEXT NOT NULL,
            raised_at TIMESTAMPTZ NOT NULL,
            CONSTRAINT break_glass_elevations_pkey PRIMARY KEY (queue_name),
            CONSTRAINT break_glass_elevations_queue_name_check
                CHECK (char_length(queue_name) BETWEEN 1 AND 128),
            CONSTRAINT break_glass_elevations_factor_check
                CHECK (factor >= 1.0 AND factor <= 10.0),
            CONSTRAINT break_glass_elevations_actor_id_check
                CHECK (char_length(actor_id) BETWEEN 1 AND 128),
            CONSTRAINT break_glass_elevations_incident_ref_hash_check
                CHECK (char_length(incident_ref_hash) = 16),
            CONSTRAINT break_glass_elevations_expires_after_raised_check
                CHECK (expires_at > raised_at)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX break_glass_elevations_expires_at_idx
            ON break_glass_elevations (expires_at)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS break_glass_elevations")
