"""Extend admin replay/audit operation codes for dead-letter replay.

Revision ID: 040_admin_dead_letter_replay_ops
Revises: 039_apply_qualified_storage_layout
Create Date: 2026-09-19

Phase 4 CTRL-06 / REC-01: single-task dead-letter replay uses
``admin_replay.operation_code = 6`` and matching ``admin_audit_log`` rows.
Physical contract previously allowed only codes 1..5 (queue control + maintenance).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "040_admin_dead_letter_replay_ops"
down_revision: Union[str, Sequence[str], None] = "039_apply_qualified_storage_layout"
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
            CHECK (operation_code IN (1, 2, 3, 4, 5, 6))
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
            CHECK (operation_code BETWEEN 1 AND 6)
        """
    )


def downgrade() -> None:
    # Remove Phase 4 dead-letter replay rows before restoring the 1..5 bound.
    op.execute("DELETE FROM admin_replay WHERE operation_code = 6")
    op.execute("DELETE FROM admin_audit_log WHERE operation_code = 6")
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
            CHECK (operation_code BETWEEN 1 AND 5)
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
            CHECK (operation_code IN (1, 2, 3, 4, 5))
        """
    )
