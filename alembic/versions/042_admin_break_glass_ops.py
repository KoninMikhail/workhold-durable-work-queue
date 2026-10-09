"""Extend admin audit operation codes for break-glass repairs.

Revision ID: 042_admin_break_glass_ops
Revises: 041_admin_bulk_ops
Create Date: 2026-09-19

Phase 4 REC-03: break-glass ops use ``admin_audit_log.operation_code`` 9–13.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "042_admin_break_glass_ops"
down_revision: Union[str, Sequence[str], None] = "041_admin_bulk_ops"
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
            CHECK (operation_code IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
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
            CHECK (operation_code BETWEEN 1 AND 13)
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM admin_replay WHERE operation_code BETWEEN 9 AND 13")
    op.execute("DELETE FROM admin_audit_log WHERE operation_code BETWEEN 9 AND 13")
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
            CHECK (operation_code BETWEEN 1 AND 8)
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
            CHECK (operation_code IN (1, 2, 3, 4, 5, 6, 7, 8))
        """
    )
