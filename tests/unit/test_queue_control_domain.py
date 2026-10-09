"""Binary domain-contract tests for named-queue control (WORK-12, CTRL-01/02/07/09)."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from workhold.domain import queue_control as qc

# Every cell from docs/04-architecture/runtime-semantics.md operation matrix.
_OPERATION_STATE_MATRIX: tuple[
    tuple[qc.QueueOperation, qc.QueueState, qc.OperationGateOutcome], ...
] = (
    (qc.QueueOperation.EXTERNAL_ENQUEUE, qc.QueueState.ACTIVE, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.EXTERNAL_ENQUEUE, qc.QueueState.PAUSED, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.EXTERNAL_ENQUEUE, qc.QueueState.DRAINING, qc.OperationGateOutcome.REJECTED),
    (qc.QueueOperation.INTERNAL_SPAWN, qc.QueueState.ACTIVE, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.INTERNAL_SPAWN, qc.QueueState.PAUSED, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.INTERNAL_SPAWN, qc.QueueState.DRAINING, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.CLAIM, qc.QueueState.ACTIVE, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.CLAIM, qc.QueueState.PAUSED, qc.OperationGateOutcome.PAUSED_EMPTY),
    (qc.QueueOperation.CLAIM, qc.QueueState.DRAINING, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.LEASE_MUTATION, qc.QueueState.ACTIVE, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.LEASE_MUTATION, qc.QueueState.PAUSED, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.LEASE_MUTATION, qc.QueueState.DRAINING, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.CANCEL, qc.QueueState.ACTIVE, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.CANCEL, qc.QueueState.PAUSED, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.CANCEL, qc.QueueState.DRAINING, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.DELIVERY_RELAY, qc.QueueState.ACTIVE, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.DELIVERY_RELAY, qc.QueueState.PAUSED, qc.OperationGateOutcome.ALLOWED),
    (qc.QueueOperation.DELIVERY_RELAY, qc.QueueState.DRAINING, qc.OperationGateOutcome.ALLOWED),
)

_DEPLOYMENT_ONLY_FIELD_FRAGMENTS: frozenset[str] = frozenset(
    {
        "ddl",
        "partition",
        "postgres",
        "database_url",
        "pool",
        "listener",
        "tls",
        "payload_max",
        "request_max",
        "hard_security",
        "max_connections",
        "credential",
    }
)


def test_queue_state_accepts_only_active_paused_draining() -> None:
    assert {s.value for s in qc.QueueState} == {"active", "paused", "draining"}
    assert qc.QueueState("active") is qc.QueueState.ACTIVE
    assert qc.QueueState("paused") is qc.QueueState.PAUSED
    assert qc.QueueState("draining") is qc.QueueState.DRAINING
    with pytest.raises(qc.DomainValidationError) as exc:
        qc.parse_queue_state("stopped")
    assert exc.value.code == "validation_failed"


@pytest.mark.parametrize(("operation", "state", "expected"), _OPERATION_STATE_MATRIX)
def test_operation_gate_matrix(
    operation: qc.QueueOperation,
    state: qc.QueueState,
    expected: qc.OperationGateOutcome,
) -> None:
    assert qc.evaluate_operation_gate(state, operation) is expected


def test_paused_claim_is_successful_empty_not_exception() -> None:
    outcome = qc.evaluate_operation_gate(qc.QueueState.PAUSED, qc.QueueOperation.CLAIM)
    assert outcome is qc.OperationGateOutcome.PAUSED_EMPTY
    assert outcome is not qc.OperationGateOutcome.REJECTED


def test_disabled_retry_means_one_processing_attempt() -> None:
    policy = qc.validate_retry_policy_draft(
        enabled=False,
        max_attempts=1,
        backoff_strategy="fixed",
        retry_delay_seconds=0,
        deployment_retry_delay_ceiling_seconds=86400,
    )
    assert policy.enabled is False
    assert policy.allowed_processing_attempts == 1


def test_disabled_retry_ignores_higher_max_attempts_for_allowed_count() -> None:
    policy = qc.validate_retry_policy_draft(
        enabled=False,
        max_attempts=5,
        backoff_strategy="fixed",
        retry_delay_seconds=10,
        deployment_retry_delay_ceiling_seconds=86400,
    )
    assert policy.max_attempts == 5
    assert policy.allowed_processing_attempts == 1


def test_enabled_retry_requires_max_attempts_at_least_one() -> None:
    policy = qc.validate_retry_policy_draft(
        enabled=True,
        max_attempts=3,
        backoff_strategy="fixed",
        retry_delay_seconds=30,
        deployment_retry_delay_ceiling_seconds=86400,
    )
    assert policy.allowed_processing_attempts == 3

    with pytest.raises(qc.DomainValidationError) as exc:
        qc.validate_retry_policy_draft(
            enabled=True,
            max_attempts=0,
            backoff_strategy="fixed",
            retry_delay_seconds=0,
            deployment_retry_delay_ceiling_seconds=86400,
        )
    assert exc.value.code == "validation_failed"


def test_only_fixed_backoff_accepted() -> None:
    with pytest.raises(qc.DomainValidationError) as exc:
        qc.validate_retry_policy_draft(
            enabled=True,
            max_attempts=1,
            backoff_strategy="exponential",
            retry_delay_seconds=1,
            deployment_retry_delay_ceiling_seconds=86400,
        )
    assert exc.value.code == "validation_failed"


def test_retry_delay_bounded_by_deployment_ceiling() -> None:
    ok = qc.validate_retry_policy_draft(
        enabled=True,
        max_attempts=1,
        backoff_strategy="fixed",
        retry_delay_seconds=60,
        deployment_retry_delay_ceiling_seconds=60,
    )
    assert ok.retry_delay_seconds == 60

    with pytest.raises(qc.DomainValidationError) as exc:
        qc.validate_retry_policy_draft(
            enabled=True,
            max_attempts=1,
            backoff_strategy="fixed",
            retry_delay_seconds=61,
            deployment_retry_delay_ceiling_seconds=60,
        )
    assert exc.value.code == "validation_failed"

    with pytest.raises(qc.DomainValidationError) as exc_neg:
        qc.validate_retry_policy_draft(
            enabled=True,
            max_attempts=1,
            backoff_strategy="fixed",
            retry_delay_seconds=-1,
            deployment_retry_delay_ceiling_seconds=86400,
        )
    assert exc_neg.value.code == "validation_failed"


def test_retry_delay_rejects_above_contract_absolute_max() -> None:
    with pytest.raises(qc.DomainValidationError) as exc:
        qc.validate_retry_policy_draft(
            enabled=True,
            max_attempts=1,
            backoff_strategy="fixed",
            retry_delay_seconds=86401,
            deployment_retry_delay_ceiling_seconds=100_000,
        )
    assert exc.value.code == "validation_failed"


def test_config_version_requires_minimum_one() -> None:
    assert qc.parse_config_version(1).value == 1
    assert qc.parse_config_version(99).value == 99
    with pytest.raises(qc.DomainValidationError) as exc:
        qc.parse_config_version(0)
    assert exc.value.code == "validation_failed"
    with pytest.raises(qc.DomainValidationError) as exc_neg:
        qc.parse_config_version(-3)
    assert exc_neg.value.code == "validation_failed"


def test_policy_version_identity_requires_minimum_one() -> None:
    assert qc.parse_policy_version(1).value == 1
    with pytest.raises(qc.DomainValidationError) as exc:
        qc.parse_policy_version(0)
    assert exc.value.code == "validation_failed"


def test_expected_config_version_mismatch_is_conflict() -> None:
    current = qc.parse_config_version(2)
    expected = qc.parse_config_version(1)
    with pytest.raises(qc.DomainValidationError) as exc:
        qc.assert_expected_config_version(current=current, expected=expected)
    assert exc.value.code == "config_version_conflict"
    qc.assert_expected_config_version(current=current, expected=qc.parse_config_version(2))


def test_runtime_mutation_models_exclude_deployment_settings() -> None:
    metadata = qc.AdminRequestMetadata(
        actor_id="admin-1",
        request_id="00000000-0000-4000-8000-000000000001",
        idempotency_key="idem-1",
    )
    policy = qc.validate_retry_policy_draft(
        enabled=True,
        max_attempts=2,
        backoff_strategy="fixed",
        retry_delay_seconds=5,
        deployment_retry_delay_ceiling_seconds=86400,
    )
    mutations: list[Any] = [
        qc.SetQueueStateMutation(
            expected_config_version=qc.parse_config_version(1),
            state=qc.QueueState.PAUSED,
            metadata=metadata,
        ),
        qc.CreatePolicyMutation(policy=policy, metadata=metadata),
        qc.ActivatePolicyMutation(
            expected_config_version=qc.parse_config_version(1),
            policy_version=qc.parse_policy_version(1),
            metadata=metadata,
        ),
        qc.CreateQueueMutation(
            name="orders",
            initial_policy=policy,
            metadata=metadata,
        ),
    ]
    for mutation in mutations:
        assert dataclasses.is_dataclass(mutation)
        assert type(mutation).__dataclass_params__.frozen  # type: ignore[attr-defined]
        field_names = {f.name for f in dataclasses.fields(mutation)}
        joined = " ".join(sorted(field_names)).lower()
        for fragment in _DEPLOYMENT_ONLY_FIELD_FRAGMENTS:
            assert fragment not in joined
            assert fragment not in field_names


def test_admin_request_metadata_bounds() -> None:
    ok = qc.AdminRequestMetadata(
        actor_id="a",
        request_id="00000000-0000-4000-8000-000000000002",
        idempotency_key="k",
    )
    assert ok.actor_id == "a"

    with pytest.raises(qc.DomainValidationError) as exc:
        qc.AdminRequestMetadata(
            actor_id="",
            request_id="00000000-0000-4000-8000-000000000002",
            idempotency_key="k",
        )
    assert exc.value.code == "validation_failed"


def test_create_queue_name_follows_openapi_pattern() -> None:
    metadata = qc.AdminRequestMetadata(
        actor_id="admin-1",
        request_id="00000000-0000-4000-8000-000000000003",
        idempotency_key="idem-q",
    )
    policy = qc.validate_retry_policy_draft(
        enabled=True,
        max_attempts=1,
        backoff_strategy="fixed",
        retry_delay_seconds=0,
        deployment_retry_delay_ceiling_seconds=86400,
    )
    created = qc.CreateQueueMutation(
        name="billing.jobs",
        initial_policy=policy,
        metadata=metadata,
    )
    assert created.name == "billing.jobs"

    with pytest.raises(qc.DomainValidationError) as exc:
        qc.CreateQueueMutation(
            name="BadName",
            initial_policy=policy,
            metadata=metadata,
        )
    assert exc.value.code == "validation_failed"
