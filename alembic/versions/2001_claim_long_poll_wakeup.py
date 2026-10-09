"""Transactional claim long-poll wake hints (LISTEN/NOTIFY).

Revision ID: 2001_claim_long_poll_wakeup
Revises: 044_break_glass_elevations
Create Date: 2026-09-22

Installs schema-local trigger functions that ``pg_notify`` a fixed channel with
the bounded queue name only when:

* ``tasks_active`` rows are inserted/updated into delayed|ready (state_code 1|2)
* ``queues.state_code`` changes

Heartbeat / unrelated ``tasks_active`` updates do not notify. Downgrade drops
triggers before functions.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "2001_claim_long_poll_wakeup"
down_revision: Union[str, Sequence[str], None] = "044_break_glass_elevations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Must match queue_service.infrastructure.postgres.claim_wakeup.CLAIM_WAKE_CHANNEL
_CHANNEL = "queue_claim_wakeup"


def upgrade() -> None:
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION claim_wakeup_notify_task()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $fn$
        DECLARE
            queue_name text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.state_code IN (1, 2) THEN
                    SELECT q.name INTO queue_name
                    FROM queues AS q
                    WHERE q.id = NEW.queue_id;
                    IF queue_name IS NOT NULL THEN
                        PERFORM pg_notify('{_CHANNEL}', queue_name);
                    END IF;
                END IF;
                RETURN NEW;
            END IF;

            IF TG_OP = 'UPDATE' THEN
                IF NEW.state_code IN (1, 2)
                   AND OLD.state_code IS DISTINCT FROM NEW.state_code THEN
                    SELECT q.name INTO queue_name
                    FROM queues AS q
                    WHERE q.id = NEW.queue_id;
                    IF queue_name IS NOT NULL THEN
                        PERFORM pg_notify('{_CHANNEL}', queue_name);
                    END IF;
                END IF;
                RETURN NEW;
            END IF;

            RETURN NEW;
        END;
        $fn$;
        """
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION claim_wakeup_notify_queue_state()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $fn$
        BEGIN
            IF TG_OP = 'UPDATE'
               AND OLD.state_code IS DISTINCT FROM NEW.state_code THEN
                PERFORM pg_notify('{_CHANNEL}', NEW.name);
            END IF;
            RETURN NEW;
        END;
        $fn$;
        """
    )
    op.execute(
        """
        DROP TRIGGER IF EXISTS tasks_active_claim_wakeup_trg ON tasks_active;
        CREATE TRIGGER tasks_active_claim_wakeup_trg
            AFTER INSERT OR UPDATE OF state_code ON tasks_active
            FOR EACH ROW
            EXECUTE FUNCTION claim_wakeup_notify_task();
        """
    )
    op.execute(
        """
        DROP TRIGGER IF EXISTS queues_claim_wakeup_trg ON queues;
        CREATE TRIGGER queues_claim_wakeup_trg
            AFTER UPDATE OF state_code ON queues
            FOR EACH ROW
            EXECUTE FUNCTION claim_wakeup_notify_queue_state();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS queues_claim_wakeup_trg ON queues")
    op.execute("DROP TRIGGER IF EXISTS tasks_active_claim_wakeup_trg ON tasks_active")
    op.execute("DROP FUNCTION IF EXISTS claim_wakeup_notify_queue_state()")
    op.execute("DROP FUNCTION IF EXISTS claim_wakeup_notify_task()")
