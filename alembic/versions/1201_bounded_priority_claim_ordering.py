"""Bounded priority CHECKs and priority-first claim index (WORK-16).

Revision ID: 1201_bounded_priority_claim_ordering
Revises: 0502_delivery_pending_generation
Create Date: 2026-09-19

Replaces zero-only priority CHECKs with inclusive signed smallint bounds and
reorders ``tasks_active_claim_idx`` to ``(queue_id, state_code, priority DESC,
available_at, id)``. Downgrade preflights ``tasks_active`` and partitioned
parent ``tasks_terminal`` for non-zero priority before any catalog mutation.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "1201_bounded_priority_claim_ordering"
down_revision: Union[str, Sequence[str], None] = "0502_delivery_pending_generation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767


def _assert_downgrade_allowed() -> None:
    bind = op.get_bind()
    active_nonzero = bind.execute(
        text("SELECT EXISTS (SELECT 1 FROM tasks_active WHERE priority <> 0)")
    ).scalar()
    terminal_nonzero = bind.execute(
        text("SELECT EXISTS (SELECT 1 FROM tasks_terminal WHERE priority <> 0)")
    ).scalar()
    if active_nonzero or terminal_nonzero:
        raise RuntimeError(
            "downgrade blocked: tasks_active or tasks_terminal contains "
            "non-zero priority rows; refusing catalog downgrade"
        )


def upgrade() -> None:
    op.execute(
        "ALTER TABLE tasks_active DROP CONSTRAINT IF EXISTS tasks_active_priority_check"
    )
    op.execute(
        f"""
        ALTER TABLE tasks_active
            ADD CONSTRAINT tasks_active_priority_check CHECK (
                priority BETWEEN {_PRIORITY_MIN} AND {_PRIORITY_MAX}
            )
        """
    )
    op.execute(
        "ALTER TABLE tasks_terminal DROP CONSTRAINT IF EXISTS tasks_terminal_priority_check"
    )
    op.execute(
        f"""
        ALTER TABLE tasks_terminal
            ADD CONSTRAINT tasks_terminal_priority_check CHECK (
                priority BETWEEN {_PRIORITY_MIN} AND {_PRIORITY_MAX}
            )
        """
    )
    # Blocking rebuild: ordinary transactional DROP/CREATE on the hot claim index.
    op.execute("DROP INDEX IF EXISTS tasks_active_claim_idx")
    op.execute(
        """
        CREATE INDEX tasks_active_claim_idx ON tasks_active (
            queue_id,
            state_code,
            priority DESC,
            available_at,
            id
        )
        """
    )


def downgrade() -> None:
    _assert_downgrade_allowed()
    op.execute(
        "ALTER TABLE tasks_active DROP CONSTRAINT IF EXISTS tasks_active_priority_check"
    )
    op.execute(
        """
        ALTER TABLE tasks_active
            ADD CONSTRAINT tasks_active_priority_check CHECK (priority = 0)
        """
    )
    op.execute(
        "ALTER TABLE tasks_terminal DROP CONSTRAINT IF EXISTS tasks_terminal_priority_check"
    )
    op.execute(
        """
        ALTER TABLE tasks_terminal
            ADD CONSTRAINT tasks_terminal_priority_check CHECK (priority = 0)
        """
    )
    op.execute("DROP INDEX IF EXISTS tasks_active_claim_idx")
    op.execute(
        """
        CREATE INDEX tasks_active_claim_idx ON tasks_active (
            queue_id,
            state_code,
            available_at,
            priority DESC,
            id
        )
        """
    )
