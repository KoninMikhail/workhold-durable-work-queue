"""Extend admin audit operation codes for bulk replay/cancel.

Revision ID: 041_admin_bulk_ops
Revises: 040_admin_dead_letter_replay_ops
Create Date: 2026-09-19

Phase 4 REC-02: bulk replay uses ``admin_audit_log.operation_code = 7`` and
bulk cancel uses ``8``. Admin replay registry remains keyed per-item via
existing dead-letter replay code 6.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "041_admin_bulk_ops"
down_revision: Union[str, Sequence[str], None] = "040_admin_dead_letter_replay_ops"
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
            CHECK (operation_code IN (1, 2, 3, 4, 5, 6, 7, 8))
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
            CHECK (operation_code BETWEEN 1 AND 8)
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM admin_replay WHERE operation_code IN (7, 8)")
    op.execute("DELETE FROM admin_audit_log WHERE operation_code IN (7, 8)")
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
            CHECK (operation_code BETWEEN 1 AND 6)
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
            CHECK (operation_code IN (1, 2, 3, 4, 5, 6))
        """
    )
