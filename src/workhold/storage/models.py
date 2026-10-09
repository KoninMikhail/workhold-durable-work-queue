"""SQLAlchemy metadata for the Phase 3.1 physical storage catalog.

Models only — no queue behavior. All tables inherit ``workhold.db.Base``.
"""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID as UuidType

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Index,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    desc,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, BYTEA, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from workhold.db import Base

_TS = DateTime(timezone=True)
_STMT_TS = text("statement_timestamp()")
_EMPTY_UUID_ARRAY = text("'{}'::uuid[]")
_EMPTY_JSONB = text("'{}'::jsonb")


class Queue(Base):
    __tablename__ = "queues"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    queue_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    state_code: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    config_version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    active_policy_version_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey(
            "queue_policy_versions.id",
            name="queues_active_policy_version_id_fkey",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)
    updated_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        UniqueConstraint("queue_id", name="queues_queue_id_key"),
        UniqueConstraint("name", name="queues_name_key"),
        CheckConstraint(
            "char_length(name) BETWEEN 1 AND 128 AND name = lower(name) "
            "AND name ~ '^[a-z0-9][a-z0-9._-]*$'",
            name="queues_name_format_check",
        ),
        CheckConstraint("state_code IN (1, 2, 3)", name="queues_state_code_check"),
        CheckConstraint("config_version >= 1", name="queues_config_version_check"),
    )


class QueuePolicyVersion(Base):
    __tablename__ = "queue_policy_versions"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    queue_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("queues.id", name="queue_policy_versions_queue_id_fkey", ondelete="RESTRICT"),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    backoff_strategy_code: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("1")
    )
    retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        UniqueConstraint("queue_id", "version", name="queue_policy_versions_queue_id_version_key"),
        CheckConstraint("version >= 1", name="queue_policy_versions_version_check"),
        CheckConstraint("max_attempts >= 1", name="queue_policy_versions_max_attempts_check"),
        CheckConstraint(
            "backoff_strategy_code = 1", name="queue_policy_versions_backoff_strategy_code_check"
        ),
        CheckConstraint(
            "retry_delay_seconds BETWEEN 0 AND 86400",
            name="queue_policy_versions_retry_delay_seconds_check",
        ),
    )


class TaskActive(Base):
    __tablename__ = "tasks_active"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    queue_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("queues.id", name="tasks_active_queue_id_fkey", ondelete="RESTRICT"),
        nullable=False,
    )
    producer_id: Mapped[str] = mapped_column(Text, nullable=False)
    state_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    available_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    retry_policy_version_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "queue_policy_versions.id",
            name="tasks_active_retry_policy_version_id_fkey",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    generation: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    current_claim_id: Mapped[UuidType | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_task_id: Mapped[UuidType | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    spawn_ordinal: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)
    updated_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        UniqueConstraint("task_id", name="tasks_active_task_id_key"),
        CheckConstraint(
            "char_length(producer_id) BETWEEN 1 AND 128",
            name="tasks_active_producer_id_check",
        ),
        CheckConstraint("state_code IN (1, 2, 3)", name="tasks_active_state_code_check"),
        CheckConstraint(
            "priority BETWEEN -32768 AND 32767",
            name="tasks_active_priority_check",
        ),
        CheckConstraint("generation >= 0", name="tasks_active_generation_check"),
        CheckConstraint(
            "worker_id IS NULL OR char_length(worker_id) BETWEEN 1 AND 128",
            name="tasks_active_worker_id_check",
        ),
        CheckConstraint(
            "spawn_ordinal IS NULL OR spawn_ordinal >= 0",
            name="tasks_active_spawn_ordinal_check",
        ),
        CheckConstraint(
            "("
            "current_claim_id IS NULL AND claimed_at IS NULL "
            "AND lease_expires_at IS NULL AND worker_id IS NULL"
            ") OR ("
            "current_claim_id IS NOT NULL AND claimed_at IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND worker_id IS NOT NULL"
            ")",
            name="tasks_active_claim_fields_nullability_check",
        ),
        CheckConstraint(
            "("
            "source_task_id IS NULL AND spawn_ordinal IS NULL"
            ") OR ("
            "source_task_id IS NOT NULL AND spawn_ordinal IS NOT NULL"
            ")",
            name="tasks_active_spawn_lineage_nullability_check",
        ),
        Index(
            "tasks_active_claim_idx",
            "queue_id",
            "state_code",
            desc("priority"),
            "available_at",
            "id",
        ),
        Index(
            "tasks_active_spawn_lineage_uidx",
            "source_task_id",
            "spawn_ordinal",
            unique=True,
            postgresql_where=text("source_task_id IS NOT NULL"),
        ),
    )


class TaskPayloadActive(Base):
    __tablename__ = "task_payloads_active"

    task_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("tasks_active.id", name="task_payloads_active_task_id_fkey", ondelete="CASCADE"),
        primary_key=True,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    payload_bytes: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "payload_bytes BETWEEN 1 AND 1048576",
            name="task_payloads_active_payload_bytes_check",
        ),
    )


class DeliveryEventActive(Base):
    """Pending/publishing Delivery Outbox work (unpartitioned; Phase 5).

    Phase 3.1 physical names: ``ordinal`` (completion_ordinal), ``generation``
    (relay_generation), ``current_claim_id`` (relay_claim_token), ``claimed_at`` /
    ``lease_expires_at`` (relay_* times). Phase 5 adds ``relay_principal_id``,
    ``delivery_attempt``, and ``last_failure_code``.
    """

    __tablename__ = "delivery_events_active"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    state_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    envelope: Mapped[dict] = mapped_column(JSONB, nullable=False)
    envelope_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    available_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    current_claim_id: Mapped[UuidType | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    relay_principal_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_attempt: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)
    updated_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        UniqueConstraint("event_id", name="delivery_events_active_event_id_key"),
        UniqueConstraint(
            "source_task_id",
            "ordinal",
            name="delivery_events_active_source_task_ordinal_key",
        ),
        CheckConstraint("ordinal >= 0", name="delivery_events_active_ordinal_check"),
        CheckConstraint("state_code IN (1, 2)", name="delivery_events_active_state_code_check"),
        CheckConstraint(
            "envelope_bytes BETWEEN 1 AND 1048576",
            name="delivery_events_active_envelope_bytes_check",
        ),
        CheckConstraint("generation >= 0", name="delivery_events_active_generation_check"),
        CheckConstraint(
            "delivery_attempt >= 0",
            name="delivery_events_active_delivery_attempt_check",
        ),
        CheckConstraint(
            "relay_principal_id IS NULL OR "
            "char_length(relay_principal_id) BETWEEN 1 AND 128",
            name="delivery_events_active_relay_principal_id_check",
        ),
        CheckConstraint(
            "last_failure_code IS NULL OR "
            "char_length(last_failure_code) BETWEEN 1 AND 128",
            name="delivery_events_active_last_failure_code_check",
        ),
        CheckConstraint(
            "("
            # Pending: no live claim authority. generation may be > 0 after
            # retry/backoff so reclaim keeps monotonic fencing (05-03).
            "state_code = 1 AND current_claim_id IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL "
            "AND relay_principal_id IS NULL"
            ") OR ("
            "state_code = 2 AND generation >= 1 AND current_claim_id IS NOT NULL "
            "AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND relay_principal_id IS NOT NULL"
            ")",
            name="delivery_events_active_state_claim_fence_check",
        ),
    )


class EnqueueDedup(Base):
    __tablename__ = "enqueue_dedup"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    producer_id: Mapped[str] = mapped_column(Text, nullable=False)
    queue_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("queues.id", name="enqueue_dedup_queue_id_fkey", ondelete="RESTRICT"),
        nullable=False,
    )
    key_hash: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    request_fingerprint: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)
    expires_at: Mapped[datetime] = mapped_column(_TS, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "producer_id",
            "queue_id",
            "key_hash",
            name="enqueue_dedup_producer_id_queue_id_key_hash_key",
        ),
        CheckConstraint(
            "char_length(producer_id) BETWEEN 1 AND 128",
            name="enqueue_dedup_producer_id_check",
        ),
        CheckConstraint("octet_length(key_hash) = 32", name="enqueue_dedup_key_hash_check"),
        CheckConstraint(
            "octet_length(request_fingerprint) = 32",
            name="enqueue_dedup_request_fingerprint_check",
        ),
        CheckConstraint(
            "expires_at >= created_at + interval '30 days' "
            "AND expires_at <= created_at + interval '365 days'",
            name="enqueue_dedup_expires_at_check",
        ),
        Index("enqueue_dedup_expires_at_idx", "expires_at"),
    )


class ClaimRegistry(Base):
    __tablename__ = "claim_registry"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    claim_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    claim_token: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    claimed_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False)

    __table_args__ = (
        UniqueConstraint("claim_id", name="claim_registry_claim_id_key"),
        UniqueConstraint("claim_token", name="claim_registry_claim_token_key"),
        CheckConstraint("generation >= 1", name="claim_registry_generation_check"),
        CheckConstraint(
            "lease_expires_at > claimed_at",
            name="claim_registry_lease_expires_at_check",
        ),
    )


class CompleteReplay(Base):
    __tablename__ = "complete_replay"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    claim_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    operation_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    request_fingerprint: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    result_state_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    available_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    terminal_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    spawned_task_ids: Mapped[list[UuidType]] = mapped_column(
        ARRAY(UUID(as_uuid=True)), nullable=False, server_default=_EMPTY_UUID_ARRAY
    )
    event_ids: Mapped[list[UuidType]] = mapped_column(
        ARRAY(UUID(as_uuid=True)), nullable=False, server_default=_EMPTY_UUID_ARRAY
    )
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(_TS, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "claim_id", "operation_code", name="complete_replay_claim_id_operation_code_key"
        ),
        CheckConstraint(
            "operation_code IN (1, 2, 3)", name="complete_replay_operation_code_check"
        ),
        CheckConstraint(
            "octet_length(request_fingerprint) = 32",
            name="complete_replay_request_fingerprint_check",
        ),
        CheckConstraint(
            "result_state_code IN (3, 10, 11, 12)",
            name="complete_replay_result_state_code_check",
        ),
        CheckConstraint(
            "expires_at >= created_at + interval '1 day' "
            "AND expires_at <= created_at + interval '30 days'",
            name="complete_replay_expires_at_check",
        ),
        CheckConstraint(
            "("
            "result_state_code = 3 AND available_at IS NOT NULL AND terminal_at IS NULL"
            ") OR ("
            "result_state_code IN (10, 11, 12) AND terminal_at IS NOT NULL "
            "AND available_at IS NULL"
            ")",
            name="complete_replay_result_shape_check",
        ),
        Index("complete_replay_expires_at_idx", "expires_at"),
    )


class AdminReplay(Base):
    __tablename__ = "admin_replay"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    admin_principal_id: Mapped[str] = mapped_column(Text, nullable=False)
    operation_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    key_hash: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    request_fingerprint: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    http_status: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    response_body: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(_TS, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "admin_principal_id",
            "operation_code",
            "key_hash",
            name="admin_replay_principal_operation_key_hash_key",
        ),
        CheckConstraint(
            "char_length(admin_principal_id) BETWEEN 1 AND 128",
            name="admin_replay_admin_principal_id_check",
        ),
        CheckConstraint(
            "operation_code IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)",
            name="admin_replay_operation_code_check",
        ),
        CheckConstraint("octet_length(key_hash) = 32", name="admin_replay_key_hash_check"),
        CheckConstraint(
            "octet_length(request_fingerprint) = 32",
            name="admin_replay_request_fingerprint_check",
        ),
        CheckConstraint(
            "http_status BETWEEN 200 AND 299", name="admin_replay_http_status_check"
        ),
        CheckConstraint(
            "expires_at >= created_at + interval '7 days' "
            "AND expires_at <= created_at + interval '90 days'",
            name="admin_replay_expires_at_check",
        ),
        Index("admin_replay_expires_at_idx", "expires_at"),
    )


class CompletionEffect(Base):
    __tablename__ = "completion_effects"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    source_claim_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    effect_kind_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    resource_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        UniqueConstraint(
            "source_claim_id",
            "effect_kind_code",
            "ordinal",
            name="completion_effects_source_claim_kind_ordinal_key",
        ),
        UniqueConstraint("resource_id", name="completion_effects_resource_id_key"),
        CheckConstraint(
            "effect_kind_code IN (1, 2)", name="completion_effects_effect_kind_code_check"
        ),
        CheckConstraint("ordinal >= 0", name="completion_effects_ordinal_check"),
    )


class QueueCounter(Base):
    __tablename__ = "queue_counters"

    queue_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("queues.id", name="queue_counters_queue_id_fkey", ondelete="CASCADE"),
        primary_key=True,
    )
    delayed_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    ready_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    leased_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    as_of: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        CheckConstraint("delayed_count >= 0", name="queue_counters_delayed_count_check"),
        CheckConstraint("ready_count >= 0", name="queue_counters_ready_count_check"),
        CheckConstraint("leased_count >= 0", name="queue_counters_leased_count_check"),
    )


class PartitionMaintenanceStatus(Base):
    __tablename__ = "partition_maintenance_status"

    singleton_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    last_started_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    last_succeeded_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    premade_through: Mapped[date | None] = mapped_column(Date, nullable=True)
    retained_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=_STMT_TS)

    __table_args__ = (
        CheckConstraint("singleton_id = 1", name="partition_maintenance_status_singleton_id_check"),
        CheckConstraint(
            "last_error_code IS NULL OR char_length(last_error_code) BETWEEN 1 AND 128",
            name="partition_maintenance_status_last_error_code_check",
        ),
        CheckConstraint(
            "last_error_detail IS NULL OR char_length(last_error_detail) <= 4096",
            name="partition_maintenance_status_last_error_detail_check",
        ),
    )


class AdminAuditLog(Base):
    __tablename__ = "admin_audit_log"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    audit_at: Mapped[datetime] = mapped_column(_TS, primary_key=True, nullable=False)
    queue_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    actor_id: Mapped[str] = mapped_column(Text, nullable=False)
    operation_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    previous_config_version: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    new_config_version: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    request_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_EMPTY_JSONB)

    __table_args__ = (
        CheckConstraint(
            "char_length(actor_id) BETWEEN 1 AND 128",
            name="admin_audit_log_actor_id_check",
        ),
        CheckConstraint(
            "operation_code BETWEEN 1 AND 15",
            name="admin_audit_log_operation_code_check",
        ),
        Index("admin_audit_log_queue_audit_idx", "queue_id", desc("audit_at"), "id"),
        {"postgresql_partition_by": "RANGE (audit_at)"},
    )


class TaskAttempt(Base):
    __tablename__ = "task_attempts"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    claim_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    claimed_at: Mapped[datetime] = mapped_column(_TS, primary_key=True, nullable=False)
    worker_id: Mapped[str] = mapped_column(Text, nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    outcome_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint("generation >= 1", name="task_attempts_generation_check"),
        CheckConstraint(
            "char_length(worker_id) BETWEEN 1 AND 128",
            name="task_attempts_worker_id_check",
        ),
        CheckConstraint(
            "outcome_code IN (1, 2, 3, 4, 5, 6)", name="task_attempts_outcome_code_check"
        ),
        CheckConstraint(
            "failure_code IS NULL OR char_length(failure_code) BETWEEN 1 AND 128",
            name="task_attempts_failure_code_check",
        ),
        CheckConstraint(
            "failure_detail IS NULL OR char_length(failure_detail) <= 4096",
            name="task_attempts_failure_detail_check",
        ),
        Index("task_attempts_task_claimed_idx", "task_id", desc("claimed_at"), "id"),
        {"postgresql_partition_by": "RANGE (claimed_at)"},
    )


class TaskTerminal(Base):
    __tablename__ = "tasks_terminal"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    queue_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    producer_id: Mapped[str] = mapped_column(Text, nullable=False)
    state_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    available_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    retry_policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    payload_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    terminal_at: Mapped[datetime] = mapped_column(_TS, primary_key=True, nullable=False)
    failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_task_id: Mapped[UuidType | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    spawn_ordinal: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "char_length(producer_id) BETWEEN 1 AND 128",
            name="tasks_terminal_producer_id_check",
        ),
        CheckConstraint(
            "state_code IN (10, 11, 12)", name="tasks_terminal_state_code_check"
        ),
        CheckConstraint(
            "priority BETWEEN -32768 AND 32767",
            name="tasks_terminal_priority_check",
        ),
        CheckConstraint(
            "retry_policy_version >= 1", name="tasks_terminal_retry_policy_version_check"
        ),
        CheckConstraint(
            "payload_bytes BETWEEN 1 AND 1048576",
            name="tasks_terminal_payload_bytes_check",
        ),
        CheckConstraint(
            "failure_code IS NULL OR char_length(failure_code) BETWEEN 1 AND 128",
            name="tasks_terminal_failure_code_check",
        ),
        CheckConstraint(
            "failure_detail IS NULL OR char_length(failure_detail) <= 4096",
            name="tasks_terminal_failure_detail_check",
        ),
        CheckConstraint(
            "spawn_ordinal IS NULL OR spawn_ordinal >= 0",
            name="tasks_terminal_spawn_ordinal_check",
        ),
        CheckConstraint(
            "("
            "source_task_id IS NULL AND spawn_ordinal IS NULL"
            ") OR ("
            "source_task_id IS NOT NULL AND spawn_ordinal IS NOT NULL"
            ")",
            name="tasks_terminal_spawn_lineage_nullability_check",
        ),
        Index("tasks_terminal_task_terminal_idx", "task_id", desc("terminal_at")),
        Index(
            "tasks_terminal_spawn_lineage_idx",
            "source_task_id",
            "spawn_ordinal",
            "terminal_at",
            postgresql_where=text("source_task_id IS NOT NULL"),
        ),
        {"postgresql_partition_by": "RANGE (terminal_at)"},
    )


class DeliveryEventTerminal(Base):
    """Published/dead-lettered Delivery Outbox history (daily UTC RANGE; Phase 5).

    ``state_code`` encodes terminal_outcome (10=published, 11=dead_lettered).
    No live relay claim authority columns.
    """

    __tablename__ = "delivery_events_terminal"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_task_id: Mapped[UuidType] = mapped_column(UUID(as_uuid=True), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    state_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    envelope: Mapped[dict] = mapped_column(JSONB, nullable=False)
    envelope_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    terminal_at: Mapped[datetime] = mapped_column(_TS, primary_key=True, nullable=False)
    failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_attempt: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )

    __table_args__ = (
        CheckConstraint("ordinal >= 0", name="delivery_events_terminal_ordinal_check"),
        CheckConstraint(
            "state_code IN (10, 11)", name="delivery_events_terminal_state_code_check"
        ),
        CheckConstraint(
            "envelope_bytes BETWEEN 1 AND 1048576",
            name="delivery_events_terminal_envelope_bytes_check",
        ),
        CheckConstraint(
            "failure_code IS NULL OR char_length(failure_code) BETWEEN 1 AND 128",
            name="delivery_events_terminal_failure_code_check",
        ),
        CheckConstraint(
            "failure_detail IS NULL OR char_length(failure_detail) <= 4096",
            name="delivery_events_terminal_failure_detail_check",
        ),
        CheckConstraint(
            "delivery_attempt >= 0",
            name="delivery_events_terminal_delivery_attempt_check",
        ),
        Index(
            "delivery_events_terminal_event_idx",
            "event_id",
            desc("terminal_at"),
        ),
        {"postgresql_partition_by": "RANGE (terminal_at)"},
    )


class BreakGlassElevation(Base):
    """Durable temporary replay-rate elevation (break-glass raiseReplayLimit)."""

    __tablename__ = "break_glass_elevations"

    queue_name: Mapped[str] = mapped_column(Text, primary_key=True)
    factor: Mapped[float] = mapped_column(Float, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(_TS, nullable=False)
    actor_id: Mapped[str] = mapped_column(Text, nullable=False)
    incident_ref_hash: Mapped[str] = mapped_column(Text, nullable=False)
    raised_at: Mapped[datetime] = mapped_column(_TS, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "char_length(queue_name) BETWEEN 1 AND 128",
            name="break_glass_elevations_queue_name_check",
        ),
        CheckConstraint(
            "factor >= 1.0 AND factor <= 10.0",
            name="break_glass_elevations_factor_check",
        ),
        CheckConstraint(
            "char_length(actor_id) BETWEEN 1 AND 128",
            name="break_glass_elevations_actor_id_check",
        ),
        CheckConstraint(
            "char_length(incident_ref_hash) = 16",
            name="break_glass_elevations_incident_ref_hash_check",
        ),
        CheckConstraint(
            "expires_at > raised_at",
            name="break_glass_elevations_expires_after_raised_check",
        ),
        Index("break_glass_elevations_expires_at_idx", "expires_at"),
    )
