"""Allow pending delivery rows to preserve generation after retry.

Revision ID: 0502_delivery_pending_generation
Revises: 0501_delivery_outbox
Create Date: 2026-09-19

Pending rows clear claim fields on retry/backoff but must retain generation so
reclaim continues monotonic fencing (05-03 / DLVR-02).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0502_delivery_pending_generation"
down_revision: Union[str, Sequence[str], None] = "0501_delivery_outbox"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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


def downgrade() -> None:
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
