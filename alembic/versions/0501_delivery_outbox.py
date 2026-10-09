"""Activate Delivery Outbox relay fencing columns and state constraints.

Revision ID: 0501_delivery_outbox
Revises: 042_admin_break_glass_ops
Create Date: 2026-09-19

Extends Phase 3.1 ``delivery_events_active`` / ``delivery_events_terminal`` with
the Phase 5 relay fencing/attempt fields and pending-vs-publishing CHECK
constraints. Does not create a second physical contract: keeps Phase 3.1
column names (``ordinal``, ``generation``, ``current_claim_id``, …).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0501_delivery_outbox"
down_revision: Union[str, Sequence[str], None] = "042_admin_break_glass_ops"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE delivery_events_active
            ADD COLUMN IF NOT EXISTS relay_principal_id text,
            ADD COLUMN IF NOT EXISTS delivery_attempt integer NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS last_failure_code text
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_claim_fields_nullability_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_state_claim_fence_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            ADD CONSTRAINT delivery_events_active_state_claim_fence_check CHECK (
                (
                    state_code = 1
                    AND generation = 0
                    AND current_claim_id IS NULL
                    AND claimed_at IS NULL
                    AND lease_expires_at IS NULL
                    AND relay_principal_id IS NULL
                ) OR (
                    state_code = 2
                    AND generation >= 1
                    AND current_claim_id IS NOT NULL
                    AND claimed_at IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                    AND relay_principal_id IS NOT NULL
                )
            )
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_delivery_attempt_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            ADD CONSTRAINT delivery_events_active_delivery_attempt_check
            CHECK (delivery_attempt >= 0)
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_relay_principal_id_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            ADD CONSTRAINT delivery_events_active_relay_principal_id_check
            CHECK (
                relay_principal_id IS NULL
                OR char_length(relay_principal_id) BETWEEN 1 AND 128
            )
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_last_failure_code_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            ADD CONSTRAINT delivery_events_active_last_failure_code_check
            CHECK (
                last_failure_code IS NULL
                OR char_length(last_failure_code) BETWEEN 1 AND 128
            )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            delivery_events_active_source_task_ordinal_key
            ON delivery_events_active (source_task_id, ordinal)
        """
    )

    op.execute(
        """
        ALTER TABLE delivery_events_terminal
            ADD COLUMN IF NOT EXISTS delivery_attempt integer NOT NULL DEFAULT 0
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_terminal
            DROP CONSTRAINT IF EXISTS delivery_events_terminal_delivery_attempt_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_terminal
            ADD CONSTRAINT delivery_events_terminal_delivery_attempt_check
            CHECK (delivery_attempt >= 0)
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE delivery_events_terminal
            DROP CONSTRAINT IF EXISTS delivery_events_terminal_delivery_attempt_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_terminal
            DROP COLUMN IF EXISTS delivery_attempt
        """
    )
    op.execute(
        "DROP INDEX IF EXISTS delivery_events_active_source_task_ordinal_key"
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_state_claim_fence_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_delivery_attempt_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_relay_principal_id_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP CONSTRAINT IF EXISTS delivery_events_active_last_failure_code_check
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            DROP COLUMN IF EXISTS last_failure_code,
            DROP COLUMN IF EXISTS delivery_attempt,
            DROP COLUMN IF EXISTS relay_principal_id
        """
    )
    op.execute(
        """
        ALTER TABLE delivery_events_active
            ADD CONSTRAINT delivery_events_active_claim_fields_nullability_check CHECK (
                (
                    current_claim_id IS NULL AND claimed_at IS NULL
                    AND lease_expires_at IS NULL
                ) OR (
                    current_claim_id IS NOT NULL AND claimed_at IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                )
            )
        """
    )
