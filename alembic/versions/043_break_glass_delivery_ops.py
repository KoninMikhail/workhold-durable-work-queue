"""Extend admin audit operation codes for delivery break-glass ops.

Revision ID: 043_break_glass_delivery_ops
Revises: 1201_bounded_priority_claim_ordering
Create Date: 2026-09-21

Phase 14 REC-03 / CTRL-06: delivery reclaim/dead-letter use
``admin_audit_log.operation_code`` 14–15 (RESEARCH: codes 9–13 used → 14+).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "043_break_glass_delivery_ops"
down_revision: Union[str, Sequence[str], None] = "1201_bounded_priority_claim_ordering"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE admin_replay
            DROP CONSTRAINT IF EXISTS admin_replay_operation_code_check
        """
    )
    op.execute(
        """
        ALTER TABLE admin_replay
            ADD CONSTRAINT admin_replay_operation_code_check
            CHECK (operation_code IN (
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
            ))
        """
    )
    op.execute(
        """
        ALTER TABLE admin_audit_log
            DROP CONSTRAINT IF EXISTS admin_audit_log_operation_code_check
        """
    )
    op.execute(
        """
        ALTER TABLE admin_audit_log
            ADD CONSTRAINT admin_audit_log_operation_code_check
            CHECK (operation_code BETWEEN 1 AND 15)
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM admin_replay WHERE operation_code BETWEEN 14 AND 15")
    op.execute("DELETE FROM admin_audit_log WHERE operation_code BETWEEN 14 AND 15")
    op.execute(
        """
        ALTER TABLE admin_audit_log
            DROP CONSTRAINT IF EXISTS admin_audit_log_operation_code_check
        """
    )
    op.execute(
        """
        ALTER TABLE admin_audit_log
            ADD CONSTRAINT admin_audit_log_operation_code_check
            CHECK (operation_code BETWEEN 1 AND 13)
        """
    )
    op.execute(
        """
        ALTER TABLE admin_replay
            DROP CONSTRAINT IF EXISTS admin_replay_operation_code_check
        """
    )
    op.execute(
        """
        ALTER TABLE admin_replay
            ADD CONSTRAINT admin_replay_operation_code_check
            CHECK (operation_code IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
        """
    )
