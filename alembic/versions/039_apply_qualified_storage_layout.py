"""Apply Phase 3.9 qualified storage layout from measured recommendation.

Revision ID: 039_apply_qualified_storage_layout
Revises: 0001_physical_contract_foundations
Create Date: 2026-09-19

Transforms the Phase 3.8/3.1 foundations head to the checksum-valid PASS
recommendation in ``benchmarks/results/phase-3.9-candidates/recommendation.json``:

- indexes: drop ``enqueue_dedup_expires_at_idx`` (selected signature omits it)
- HASH: keep ``enqueue_dedup`` and ``complete_replay`` unpartitioned (count=1)
- does not alter RANGE time-partition topology, retention windows, or queue policies
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "039_apply_qualified_storage_layout"
down_revision: Union[str, Sequence[str], None] = "0001_physical_contract_foundations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Exact measured selections (QUAL-03 / ADR 023).
QUALIFIED_INDEX_SIGNATURE = (
    "admin_audit_log_queue_audit_idx,"
    "admin_replay_expires_at_idx,"
    "complete_replay_expires_at_idx,"
    "delivery_events_terminal_event_idx,"
    "task_attempts_task_claimed_idx,"
    "tasks_active_claim_idx,"
    "tasks_terminal_spawn_lineage_idx,"
    "tasks_terminal_task_terminal_idx"
)
QUALIFIED_ENQUEUE_DEDUP_HASH_COUNT = 1
QUALIFIED_COMPLETE_REPLAY_HASH_COUNT = 1
OMITTED_INDEX = "enqueue_dedup_expires_at_idx"


def upgrade() -> None:
    """Apply the qualified index layout; HASH count=1 remains unpartitioned."""
    # Locking: DROP INDEX CONCURRENTLY is unavailable inside a transaction.
    # This index is on the unpartitioned registry table and is not required for
    # uniqueness; a brief AccessExclusiveLock on the index is acceptable for MVP.
    op.execute(f"DROP INDEX IF EXISTS {OMITTED_INDEX}")

    # HASH partition counts are already 1 (ordinary tables). Document the
    # accepted modulus without rewriting topology.
    op.execute(
        f"""
        DO $qualified$
        BEGIN
          -- enqueue_dedup HASH modulus = {QUALIFIED_ENQUEUE_DEDUP_HASH_COUNT}
          -- complete_replay HASH modulus = {QUALIFIED_COMPLETE_REPLAY_HASH_COUNT}
          -- QUALIFIED_INDEX_SIGNATURE = {QUALIFIED_INDEX_SIGNATURE}
          NULL;
        END
        $qualified$;
        """
    )


def downgrade() -> None:
    """Restore the prior reviewed foundations index layout."""
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {OMITTED_INDEX}
            ON enqueue_dedup (expires_at)
        """
    )
