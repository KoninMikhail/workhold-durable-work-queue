"""Initial physical contract foundations.

Revision ID: 0001_physical_contract_foundations
Revises: None
Create Date: 2026-09-18

Deterministic PostgreSQL 18.6 DDL for the accepted Phase 3.1 storage contract.
Partition child dates are computed at upgrade time from
``(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date`` (not file or client
wall-clock, and not session-local ``CURRENT_DATE``).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_physical_contract_foundations"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Migration-day UTC through 30 days ahead inclusive → generate_series(0, 30).
_HORIZON_SQL = """
DO $premake$
DECLARE
  day_offset integer;
  partition_day date;
  bound_from timestamptz;
  bound_to timestamptz;
  child_suffix text;
BEGIN
  FOR day_offset IN SELECT generate_series(0, 30) LOOP
    partition_day := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date + day_offset;
    bound_from := partition_day::timestamp AT TIME ZONE 'UTC';
    bound_to := (partition_day + interval '1 day')::timestamp AT TIME ZONE 'UTC';
    child_suffix := to_char(partition_day, 'YYYYMMDD');

    EXECUTE format(
      'CREATE TABLE %I PARTITION OF admin_audit_log FOR VALUES FROM (%L) TO (%L)',
      'admin_audit_log_' || child_suffix,
      bound_from,
      bound_to
    );
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF task_attempts FOR VALUES FROM (%L) TO (%L)',
      'task_attempts_' || child_suffix,
      bound_from,
      bound_to
    );
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF tasks_terminal FOR VALUES FROM (%L) TO (%L)',
      'tasks_terminal_' || child_suffix,
      bound_from,
      bound_to
    );
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF delivery_events_terminal FOR VALUES FROM (%L) TO (%L)',
      'delivery_events_terminal_' || child_suffix,
      bound_from,
      bound_to
    );
  END LOOP;
END
$premake$;
"""

_DROP_CHILDREN_SQL = """
DO $drop_children$
DECLARE
  day_offset integer;
  partition_day date;
  child_suffix text;
  parent_name text;
BEGIN
  FOR day_offset IN SELECT generate_series(0, 30) LOOP
    partition_day := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date + day_offset;
    child_suffix := to_char(partition_day, 'YYYYMMDD');
    FOREACH parent_name IN ARRAY ARRAY[
      'admin_audit_log',
      'task_attempts',
      'tasks_terminal',
      'delivery_events_terminal'
    ] LOOP
      EXECUTE format(
        'DROP TABLE IF EXISTS %I',
        parent_name || '_' || child_suffix
      );
    END LOOP;
  END LOOP;
END
$drop_children$;
"""


def upgrade() -> None:
    """Upgrade schema."""
    # --- independent lookup/config tables ---
    op.execute(
        """
        CREATE TABLE queues (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            queue_id uuid NOT NULL,
            name text NOT NULL,
            state_code smallint NOT NULL DEFAULT 1,
            config_version bigint NOT NULL DEFAULT 1,
            active_policy_version_id bigint,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT queues_queue_id_key UNIQUE (queue_id),
            CONSTRAINT queues_name_key UNIQUE (name),
            CONSTRAINT queues_name_format_check CHECK (
                char_length(name) BETWEEN 1 AND 128
                AND name = lower(name)
                AND name ~ '^[a-z0-9][a-z0-9._-]*$'
            ),
            CONSTRAINT queues_state_code_check CHECK (state_code IN (1, 2, 3)),
            CONSTRAINT queues_config_version_check CHECK (config_version >= 1)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE queue_policy_versions (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            queue_id bigint NOT NULL,
            version integer NOT NULL,
            enabled boolean NOT NULL,
            max_attempts integer NOT NULL,
            backoff_strategy_code smallint NOT NULL DEFAULT 1,
            retry_delay_seconds integer NOT NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT queue_policy_versions_queue_id_fkey
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE RESTRICT,
            CONSTRAINT queue_policy_versions_queue_id_version_key
                UNIQUE (queue_id, version),
            CONSTRAINT queue_policy_versions_version_check CHECK (version >= 1),
            CONSTRAINT queue_policy_versions_max_attempts_check
                CHECK (max_attempts >= 1),
            CONSTRAINT queue_policy_versions_backoff_strategy_code_check
                CHECK (backoff_strategy_code = 1),
            CONSTRAINT queue_policy_versions_retry_delay_seconds_check
                CHECK (retry_delay_seconds BETWEEN 0 AND 86400)
        )
        """
    )
    op.execute(
        """
        ALTER TABLE queues
            ADD CONSTRAINT queues_active_policy_version_id_fkey
            FOREIGN KEY (active_policy_version_id)
            REFERENCES queue_policy_versions(id)
            DEFERRABLE INITIALLY DEFERRED
        """
    )

    # --- active / payload / correctness tables ---
    op.execute(
        """
        CREATE TABLE tasks_active (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            task_id uuid NOT NULL,
            queue_id bigint NOT NULL,
            producer_id text NOT NULL,
            state_code smallint NOT NULL,
            priority smallint NOT NULL DEFAULT 0,
            available_at timestamptz NOT NULL,
            retry_policy_version_id bigint NOT NULL,
            generation integer NOT NULL DEFAULT 0,
            current_claim_id uuid,
            claimed_at timestamptz,
            lease_expires_at timestamptz,
            cancel_requested_at timestamptz,
            worker_id text,
            source_task_id uuid,
            spawn_ordinal integer,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT tasks_active_task_id_key UNIQUE (task_id),
            CONSTRAINT tasks_active_queue_id_fkey
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE RESTRICT,
            CONSTRAINT tasks_active_retry_policy_version_id_fkey
                FOREIGN KEY (retry_policy_version_id)
                REFERENCES queue_policy_versions(id) ON DELETE RESTRICT,
            CONSTRAINT tasks_active_producer_id_check
                CHECK (char_length(producer_id) BETWEEN 1 AND 128),
            CONSTRAINT tasks_active_state_code_check
                CHECK (state_code IN (1, 2, 3)),
            CONSTRAINT tasks_active_priority_check CHECK (priority = 0),
            CONSTRAINT tasks_active_generation_check CHECK (generation >= 0),
            CONSTRAINT tasks_active_worker_id_check CHECK (
                worker_id IS NULL OR char_length(worker_id) BETWEEN 1 AND 128
            ),
            CONSTRAINT tasks_active_spawn_ordinal_check CHECK (
                spawn_ordinal IS NULL OR spawn_ordinal >= 0
            ),
            CONSTRAINT tasks_active_claim_fields_nullability_check CHECK (
                (
                    current_claim_id IS NULL AND claimed_at IS NULL
                    AND lease_expires_at IS NULL AND worker_id IS NULL
                ) OR (
                    current_claim_id IS NOT NULL AND claimed_at IS NOT NULL
                    AND lease_expires_at IS NOT NULL AND worker_id IS NOT NULL
                )
            ),
            CONSTRAINT tasks_active_spawn_lineage_nullability_check CHECK (
                (
                    source_task_id IS NULL AND spawn_ordinal IS NULL
                ) OR (
                    source_task_id IS NOT NULL AND spawn_ordinal IS NOT NULL
                )
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX tasks_active_claim_idx
            ON tasks_active (queue_id, state_code, available_at, priority DESC, id)
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX tasks_active_spawn_lineage_uidx
            ON tasks_active (source_task_id, spawn_ordinal)
            WHERE source_task_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE TABLE task_payloads_active (
            task_id bigint PRIMARY KEY,
            payload jsonb NOT NULL,
            payload_bytes integer NOT NULL,
            CONSTRAINT task_payloads_active_task_id_fkey
                FOREIGN KEY (task_id) REFERENCES tasks_active(id) ON DELETE CASCADE,
            CONSTRAINT task_payloads_active_payload_bytes_check
                CHECK (payload_bytes BETWEEN 1 AND 1048576)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE delivery_events_active (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_id uuid NOT NULL,
            source_task_id uuid NOT NULL,
            ordinal integer NOT NULL,
            state_code smallint NOT NULL,
            envelope jsonb NOT NULL,
            envelope_bytes integer NOT NULL,
            available_at timestamptz NOT NULL,
            generation integer NOT NULL DEFAULT 0,
            current_claim_id uuid,
            claimed_at timestamptz,
            lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT delivery_events_active_event_id_key UNIQUE (event_id),
            CONSTRAINT delivery_events_active_ordinal_check CHECK (ordinal >= 0),
            CONSTRAINT delivery_events_active_state_code_check
                CHECK (state_code IN (1, 2)),
            CONSTRAINT delivery_events_active_envelope_bytes_check
                CHECK (envelope_bytes BETWEEN 1 AND 1048576),
            CONSTRAINT delivery_events_active_generation_check
                CHECK (generation >= 0),
            CONSTRAINT delivery_events_active_claim_fields_nullability_check CHECK (
                (
                    current_claim_id IS NULL AND claimed_at IS NULL
                    AND lease_expires_at IS NULL
                ) OR (
                    current_claim_id IS NOT NULL AND claimed_at IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                )
            )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE enqueue_dedup (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            producer_id text NOT NULL,
            queue_id bigint NOT NULL,
            key_hash bytea NOT NULL,
            request_fingerprint bytea NOT NULL,
            task_id uuid NOT NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            expires_at timestamptz NOT NULL,
            CONSTRAINT enqueue_dedup_queue_id_fkey
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE RESTRICT,
            CONSTRAINT enqueue_dedup_producer_id_queue_id_key_hash_key
                UNIQUE (producer_id, queue_id, key_hash),
            CONSTRAINT enqueue_dedup_producer_id_check
                CHECK (char_length(producer_id) BETWEEN 1 AND 128),
            CONSTRAINT enqueue_dedup_key_hash_check
                CHECK (octet_length(key_hash) = 32),
            CONSTRAINT enqueue_dedup_request_fingerprint_check
                CHECK (octet_length(request_fingerprint) = 32),
            CONSTRAINT enqueue_dedup_expires_at_check CHECK (
                expires_at >= created_at + interval '30 days'
                AND expires_at <= created_at + interval '365 days'
            )
        )
        """
    )
    op.execute("CREATE INDEX enqueue_dedup_expires_at_idx ON enqueue_dedup (expires_at)")
    op.execute(
        """
        CREATE TABLE claim_registry (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            claim_id uuid NOT NULL,
            task_id uuid NOT NULL,
            claim_token uuid NOT NULL,
            generation integer NOT NULL,
            claimed_at timestamptz NOT NULL,
            lease_expires_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL,
            CONSTRAINT claim_registry_claim_id_key UNIQUE (claim_id),
            CONSTRAINT claim_registry_claim_token_key UNIQUE (claim_token),
            CONSTRAINT claim_registry_generation_check CHECK (generation >= 1),
            CONSTRAINT claim_registry_lease_expires_at_check
                CHECK (lease_expires_at > claimed_at)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE complete_replay (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            claim_id uuid NOT NULL,
            operation_code smallint NOT NULL,
            request_fingerprint bytea NOT NULL,
            task_id uuid NOT NULL,
            result_state_code smallint NOT NULL,
            available_at timestamptz,
            terminal_at timestamptz,
            spawned_task_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
            event_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
            created_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            CONSTRAINT complete_replay_claim_id_operation_code_key
                UNIQUE (claim_id, operation_code),
            CONSTRAINT complete_replay_operation_code_check
                CHECK (operation_code IN (1, 2, 3)),
            CONSTRAINT complete_replay_request_fingerprint_check
                CHECK (octet_length(request_fingerprint) = 32),
            CONSTRAINT complete_replay_result_state_code_check
                CHECK (result_state_code IN (3, 10, 11, 12)),
            CONSTRAINT complete_replay_expires_at_check CHECK (
                expires_at >= created_at + interval '1 day'
                AND expires_at <= created_at + interval '30 days'
            ),
            CONSTRAINT complete_replay_result_shape_check CHECK (
                (
                    result_state_code = 3
                    AND available_at IS NOT NULL
                    AND terminal_at IS NULL
                ) OR (
                    result_state_code IN (10, 11, 12)
                    AND terminal_at IS NOT NULL
                    AND available_at IS NULL
                )
            )
        )
        """
    )
    op.execute(
        "CREATE INDEX complete_replay_expires_at_idx ON complete_replay (expires_at)"
    )
    op.execute(
        """
        CREATE TABLE admin_replay (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            admin_principal_id text NOT NULL,
            operation_code smallint NOT NULL,
            key_hash bytea NOT NULL,
            request_fingerprint bytea NOT NULL,
            http_status smallint NOT NULL,
            response_body jsonb NOT NULL,
            created_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            CONSTRAINT admin_replay_principal_operation_key_hash_key
                UNIQUE (admin_principal_id, operation_code, key_hash),
            CONSTRAINT admin_replay_admin_principal_id_check
                CHECK (char_length(admin_principal_id) BETWEEN 1 AND 128),
            CONSTRAINT admin_replay_operation_code_check
                CHECK (operation_code IN (1, 2, 3, 4, 5)),
            CONSTRAINT admin_replay_key_hash_check
                CHECK (octet_length(key_hash) = 32),
            CONSTRAINT admin_replay_request_fingerprint_check
                CHECK (octet_length(request_fingerprint) = 32),
            CONSTRAINT admin_replay_http_status_check
                CHECK (http_status BETWEEN 200 AND 299),
            CONSTRAINT admin_replay_expires_at_check CHECK (
                expires_at >= created_at + interval '7 days'
                AND expires_at <= created_at + interval '90 days'
            )
        )
        """
    )
    op.execute("CREATE INDEX admin_replay_expires_at_idx ON admin_replay (expires_at)")
    op.execute(
        """
        CREATE TABLE completion_effects (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source_claim_id uuid NOT NULL,
            effect_kind_code smallint NOT NULL,
            ordinal integer NOT NULL,
            resource_id uuid NOT NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT completion_effects_source_claim_kind_ordinal_key
                UNIQUE (source_claim_id, effect_kind_code, ordinal),
            CONSTRAINT completion_effects_resource_id_key UNIQUE (resource_id),
            CONSTRAINT completion_effects_effect_kind_code_check
                CHECK (effect_kind_code IN (1, 2)),
            CONSTRAINT completion_effects_ordinal_check CHECK (ordinal >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE queue_counters (
            queue_id bigint PRIMARY KEY,
            delayed_count bigint NOT NULL DEFAULT 0,
            ready_count bigint NOT NULL DEFAULT 0,
            leased_count bigint NOT NULL DEFAULT 0,
            as_of timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT queue_counters_queue_id_fkey
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
            CONSTRAINT queue_counters_delayed_count_check CHECK (delayed_count >= 0),
            CONSTRAINT queue_counters_ready_count_check CHECK (ready_count >= 0),
            CONSTRAINT queue_counters_leased_count_check CHECK (leased_count >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE partition_maintenance_status (
            singleton_id smallint PRIMARY KEY,
            last_started_at timestamptz,
            last_succeeded_at timestamptz,
            premade_through date,
            retained_from date,
            last_error_code text,
            last_error_detail text,
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT partition_maintenance_status_singleton_id_check
                CHECK (singleton_id = 1),
            CONSTRAINT partition_maintenance_status_last_error_code_check CHECK (
                last_error_code IS NULL
                OR char_length(last_error_code) BETWEEN 1 AND 128
            ),
            CONSTRAINT partition_maintenance_status_last_error_detail_check CHECK (
                last_error_detail IS NULL
                OR char_length(last_error_detail) <= 4096
            )
        )
        """
    )

    # --- partition parents (indexes before children so PG attaches child indexes) ---
    op.execute(
        """
        CREATE TABLE admin_audit_log (
            id bigint GENERATED ALWAYS AS IDENTITY,
            audit_at timestamptz NOT NULL,
            queue_id bigint,
            actor_id text NOT NULL,
            operation_code smallint NOT NULL,
            previous_config_version bigint,
            new_config_version bigint,
            request_id uuid NOT NULL,
            details jsonb NOT NULL DEFAULT '{}'::jsonb,
            CONSTRAINT admin_audit_log_pkey PRIMARY KEY (id, audit_at),
            CONSTRAINT admin_audit_log_actor_id_check
                CHECK (char_length(actor_id) BETWEEN 1 AND 128),
            CONSTRAINT admin_audit_log_operation_code_check
                CHECK (operation_code BETWEEN 1 AND 5)
        ) PARTITION BY RANGE (audit_at)
        """
    )
    op.execute(
        """
        CREATE INDEX admin_audit_log_queue_audit_idx
            ON admin_audit_log (queue_id, audit_at DESC, id)
        """
    )
    op.execute(
        """
        CREATE TABLE task_attempts (
            id bigint GENERATED ALWAYS AS IDENTITY,
            task_id uuid NOT NULL,
            claim_id uuid NOT NULL,
            generation integer NOT NULL,
            claimed_at timestamptz NOT NULL,
            worker_id text NOT NULL,
            lease_expires_at timestamptz NOT NULL,
            ended_at timestamptz,
            outcome_code smallint NOT NULL,
            failure_code text,
            failure_detail text,
            CONSTRAINT task_attempts_pkey PRIMARY KEY (id, claimed_at),
            CONSTRAINT task_attempts_generation_check CHECK (generation >= 1),
            CONSTRAINT task_attempts_worker_id_check
                CHECK (char_length(worker_id) BETWEEN 1 AND 128),
            CONSTRAINT task_attempts_outcome_code_check
                CHECK (outcome_code IN (1, 2, 3, 4, 5, 6)),
            CONSTRAINT task_attempts_failure_code_check CHECK (
                failure_code IS NULL
                OR char_length(failure_code) BETWEEN 1 AND 128
            ),
            CONSTRAINT task_attempts_failure_detail_check CHECK (
                failure_detail IS NULL OR char_length(failure_detail) <= 4096
            )
        ) PARTITION BY RANGE (claimed_at)
        """
    )
    op.execute(
        """
        CREATE INDEX task_attempts_task_claimed_idx
            ON task_attempts (task_id, claimed_at DESC, id)
        """
    )
    op.execute(
        """
        CREATE TABLE tasks_terminal (
            id bigint GENERATED ALWAYS AS IDENTITY,
            task_id uuid NOT NULL,
            queue_id bigint NOT NULL,
            producer_id text NOT NULL,
            state_code smallint NOT NULL,
            priority smallint NOT NULL,
            available_at timestamptz NOT NULL,
            retry_policy_version integer NOT NULL,
            payload jsonb NOT NULL,
            payload_bytes integer NOT NULL,
            created_at timestamptz NOT NULL,
            terminal_at timestamptz NOT NULL,
            failure_code text,
            failure_detail text,
            source_task_id uuid,
            spawn_ordinal integer,
            CONSTRAINT tasks_terminal_pkey PRIMARY KEY (id, terminal_at),
            CONSTRAINT tasks_terminal_producer_id_check
                CHECK (char_length(producer_id) BETWEEN 1 AND 128),
            CONSTRAINT tasks_terminal_state_code_check
                CHECK (state_code IN (10, 11, 12)),
            CONSTRAINT tasks_terminal_priority_check CHECK (priority = 0),
            CONSTRAINT tasks_terminal_retry_policy_version_check
                CHECK (retry_policy_version >= 1),
            CONSTRAINT tasks_terminal_payload_bytes_check
                CHECK (payload_bytes BETWEEN 1 AND 1048576),
            CONSTRAINT tasks_terminal_failure_code_check CHECK (
                failure_code IS NULL
                OR char_length(failure_code) BETWEEN 1 AND 128
            ),
            CONSTRAINT tasks_terminal_failure_detail_check CHECK (
                failure_detail IS NULL OR char_length(failure_detail) <= 4096
            ),
            CONSTRAINT tasks_terminal_spawn_ordinal_check CHECK (
                spawn_ordinal IS NULL OR spawn_ordinal >= 0
            ),
            CONSTRAINT tasks_terminal_spawn_lineage_nullability_check CHECK (
                (
                    source_task_id IS NULL AND spawn_ordinal IS NULL
                ) OR (
                    source_task_id IS NOT NULL AND spawn_ordinal IS NOT NULL
                )
            )
        ) PARTITION BY RANGE (terminal_at)
        """
    )
    op.execute(
        """
        CREATE INDEX tasks_terminal_task_terminal_idx
            ON tasks_terminal (task_id, terminal_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX tasks_terminal_spawn_lineage_idx
            ON tasks_terminal (source_task_id, spawn_ordinal, terminal_at)
            WHERE source_task_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE TABLE delivery_events_terminal (
            id bigint GENERATED ALWAYS AS IDENTITY,
            event_id uuid NOT NULL,
            source_task_id uuid NOT NULL,
            ordinal integer NOT NULL,
            state_code smallint NOT NULL,
            envelope jsonb NOT NULL,
            envelope_bytes integer NOT NULL,
            created_at timestamptz NOT NULL,
            terminal_at timestamptz NOT NULL,
            failure_code text,
            failure_detail text,
            CONSTRAINT delivery_events_terminal_pkey PRIMARY KEY (id, terminal_at),
            CONSTRAINT delivery_events_terminal_ordinal_check CHECK (ordinal >= 0),
            CONSTRAINT delivery_events_terminal_state_code_check
                CHECK (state_code IN (10, 11)),
            CONSTRAINT delivery_events_terminal_envelope_bytes_check
                CHECK (envelope_bytes BETWEEN 1 AND 1048576),
            CONSTRAINT delivery_events_terminal_failure_code_check CHECK (
                failure_code IS NULL
                OR char_length(failure_code) BETWEEN 1 AND 128
            ),
            CONSTRAINT delivery_events_terminal_failure_detail_check CHECK (
                failure_detail IS NULL OR char_length(failure_detail) <= 4096
            )
        ) PARTITION BY RANGE (terminal_at)
        """
    )
    op.execute(
        """
        CREATE INDEX delivery_events_terminal_event_idx
            ON delivery_events_terminal (event_id, terminal_at DESC)
        """
    )

    # --- partition children / inherited parent baseline indexes ---
    op.execute(_HORIZON_SQL)


def downgrade() -> None:
    """Downgrade schema."""
    # Children before parents (dynamic YYYYMMDD names from the same UTC horizon).
    op.execute(_DROP_CHILDREN_SQL)

    op.execute("DROP TABLE IF EXISTS delivery_events_terminal")
    op.execute("DROP TABLE IF EXISTS tasks_terminal")
    op.execute("DROP TABLE IF EXISTS task_attempts")
    op.execute("DROP TABLE IF EXISTS admin_audit_log")

    op.execute("DROP TABLE IF EXISTS partition_maintenance_status")
    op.execute("DROP TABLE IF EXISTS queue_counters")
    op.execute("DROP TABLE IF EXISTS completion_effects")
    op.execute("DROP TABLE IF EXISTS admin_replay")
    op.execute("DROP TABLE IF EXISTS complete_replay")
    op.execute("DROP TABLE IF EXISTS claim_registry")
    op.execute("DROP TABLE IF EXISTS enqueue_dedup")
    op.execute("DROP TABLE IF EXISTS delivery_events_active")
    op.execute("DROP TABLE IF EXISTS task_payloads_active")
    op.execute("DROP TABLE IF EXISTS tasks_active")
    op.execute(
        "ALTER TABLE queues DROP CONSTRAINT IF EXISTS queues_active_policy_version_id_fkey"
    )
    op.execute("DROP TABLE IF EXISTS queue_policy_versions")
    op.execute("DROP TABLE IF EXISTS queues")
