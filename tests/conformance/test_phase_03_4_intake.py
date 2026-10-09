"""Phase 3.4 durable producer intake — black-box acceptance evidence map.

Maps WORK-02, WORK-10, WORK-13, OPS-04 (enqueue), API-02, API-03 (enqueue),
QUAL-02 (enqueue/state races), and all eight Roadmap success criteria to named
assertions that invoke shared conformance proofs (and reference concurrency /
uncertain-commit symbols exercised by the plan verify command).
"""

from __future__ import annotations

import inspect
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

import tests.concurrency.test_enqueue_state_races as race_mod
import tests.conformance.test_enqueue_http as enqueue_http
import tests.conformance.test_enqueue_uncertain_commit as uncertain
import tests.conformance.test_resolve_submission_http as resolve_http
from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.intake.depth import DepthCeilings
from queue_service.intake.service import EnqueueService
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import BearerCredentialAuthenticator

REPO_ROOT = Path(__file__).resolve().parents[2]

PHASE_03_4_EVIDENCE: dict[str, tuple[str, str]] = {
    "WORK-02": (
        "tests/conformance/test_enqueue_http.py",
        "test_authorized_enqueue_success_and_matching_replay",
    ),
    "WORK-10": (
        "tests/conformance/test_enqueue_http.py",
        "test_negative_enqueue_cases",
    ),
    "WORK-13": (
        "tests/concurrency/test_enqueue_state_races.py",
        "test_enqueue_wins_over_policy_activation_snapshots_old_version",
    ),
    "OPS-04": (
        "tests/conformance/test_enqueue_http.py",
        "test_negative_enqueue_cases",
    ),
    "API-02": (
        "tests/conformance/test_enqueue_http.py",
        "test_negative_enqueue_cases",
    ),
    "API-03": (
        "tests/conformance/test_enqueue_http.py",
        "test_changed_fingerprint_conflict",
    ),
    "QUAL-02": (
        "tests/concurrency/test_enqueue_state_races.py",
        "test_drain_wins_serialization_rejects_new_enqueue_without_task",
    ),
    "ROADMAP-SC1": (
        "tests/conformance/test_enqueue_uncertain_commit.py",
        "test_failure_before_commit_leaves_no_task_then_retry_creates_one",
    ),
    "ROADMAP-SC2": (
        "tests/conformance/test_enqueue_http.py",
        "test_authorized_enqueue_success_and_matching_replay",
    ),
    "ROADMAP-SC3": (
        "tests/conformance/test_enqueue_http.py",
        "test_negative_enqueue_cases",
    ),
    "ROADMAP-SC4": (
        "tests/concurrency/test_enqueue_state_races.py",
        "test_enqueue_wins_over_policy_activation_snapshots_old_version",
    ),
    "ROADMAP-SC5": (
        "tests/conformance/test_enqueue_http.py",
        "test_negative_enqueue_cases",
    ),
    "ROADMAP-SC6": (
        "tests/conformance/test_enqueue_http.py",
        "test_negative_enqueue_cases",
    ),
    "ROADMAP-SC7": (
        "tests/conformance/test_resolve_submission_http.py",
        "test_owner_resolve_by_queue_and_key_matches_enqueued_task",
    ),
    "ROADMAP-SC8": (
        "tests/concurrency/test_enqueue_state_races.py",
        "test_matching_replay_succeeds_across_state_flip",
    ),
    "UNCERTAIN-AFTER-COMMIT": (
        "tests/conformance/test_enqueue_uncertain_commit.py",
        "test_failure_after_commit_before_response_retry_returns_same_task",
    ),
}

_MODULE_BY_PATH = {
    "tests/conformance/test_enqueue_http.py": enqueue_http,
    "tests/conformance/test_resolve_submission_http.py": resolve_http,
    "tests/conformance/test_enqueue_uncertain_commit.py": uncertain,
    "tests/concurrency/test_enqueue_state_races.py": race_mod,
}

pytest_plugins = ["tests.integration.conftest"]


@pytest.mark.parametrize("req_id,path_symbol", sorted(PHASE_03_4_EVIDENCE.items()))
def test_phase_03_4_evidence_symbol_exists(
    req_id: str, path_symbol: tuple[str, str]
) -> None:
    """Every requirement/roadmap slice has a named callable evidence reference."""
    rel_path, symbol = path_symbol
    assert (REPO_ROOT / rel_path).is_file(), f"{req_id}: missing {rel_path}"
    fn = getattr(_MODULE_BY_PATH[rel_path], symbol)
    assert callable(fn), f"{req_id}: {symbol} is not callable"
    assert inspect.isfunction(fn)


@pytest.fixture
def queue_name() -> str:
    return f"orders.phase34.{uuid.uuid4().hex[:12]}"


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for phase 3.4 intake suite")
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture
def enqueue_app(
    session_factory: sessionmaker[Session], queue_name: str
) -> Any:
    authorizer = Authorizer(
        queue_scopes={
            enqueue_http.PRODUCER_PRINCIPAL: frozenset(
                {queue_name, enqueue_http.BASE_QUEUE_NAME}
            ),
            enqueue_http.PRODUCER_OTHER_PRINCIPAL: frozenset({enqueue_http.OTHER_QUEUE}),
            enqueue_http.WORKER_PRINCIPAL: frozenset(
                {queue_name, enqueue_http.BASE_QUEUE_NAME}
            ),
            enqueue_http.ADMIN_PRINCIPAL: frozenset(
                {queue_name, enqueue_http.BASE_QUEUE_NAME}
            ),
        }
    )
    service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=DepthCeilings(
            queue_active_depth=100,
            instance_active_depth=500,
            retry_after_ms=250,
        ),
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(
            enqueue_http._bindings()
        ),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18093),
        session_factory=session_factory,
        enqueue_service=service,
    )


@pytest.fixture
def resolve_app(
    session_factory: sessionmaker[Session], queue_name: str
) -> Any:
    authorizer = Authorizer(
        queue_scopes={
            resolve_http.PRODUCER_PRINCIPAL: frozenset(
                {queue_name, resolve_http.BASE_QUEUE_NAME}
            ),
            resolve_http.PRODUCER_OTHER_PRINCIPAL: frozenset(
                {queue_name, resolve_http.OTHER_QUEUE}
            ),
            resolve_http.PRODUCER_UNSCOPED_PRINCIPAL: frozenset({resolve_http.OTHER_QUEUE}),
            resolve_http.WORKER_PRINCIPAL: frozenset(
                {queue_name, resolve_http.BASE_QUEUE_NAME}
            ),
            resolve_http.ADMIN_PRINCIPAL: frozenset(
                {queue_name, resolve_http.BASE_QUEUE_NAME}
            ),
        }
    )
    service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=DepthCeilings(
            queue_active_depth=100,
            instance_active_depth=500,
            retry_after_ms=250,
        ),
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(
            resolve_http._bindings()
        ),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18094),
        session_factory=session_factory,
        enqueue_service=service,
    )


def test_work_02_and_roadmap_sc2_matching_replay(
    enqueue_app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """WORK-02 / Roadmap SC-2: durable matching replay via shared HTTP proof."""
    enqueue_http.test_authorized_enqueue_success_and_matching_replay(
        enqueue_app, session_factory, queue_name, caplog
    )


def test_api_03_fingerprint_conflict(
    enqueue_app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    """API-03 enqueue slice via shared HTTP proof."""
    enqueue_http.test_changed_fingerprint_conflict(
        enqueue_app, session_factory, queue_name
    )


@pytest.mark.parametrize(
    ("case", "expected_status", "expected_code", "retryable", "expect_retry_after"),
    [
        ("invalid_priority_out_of_range", 400, "validation_failed", False, False),
        ("future_available_at", 400, "validation_failed", False, False),
        ("draining", 409, "queue_draining", True, True),
        ("oversized_payload", 413, "payload_too_large", False, False),
    ],
)
def test_work_10_ops_04_api_02_roadmap_sc3_sc5_sc6(
    enqueue_app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    case: str,
    expected_status: int,
    expected_code: str,
    retryable: bool,
    expect_retry_after: bool,
) -> None:
    """WORK-10 / OPS-04 / API-02 / Roadmap SC-3, SC-5, SC-6 via shared negatives."""
    enqueue_http.test_negative_enqueue_cases(
        enqueue_app,
        session_factory,
        queue_name,
        case,
        expected_status,
        expected_code,
        retryable,
        expect_retry_after,
    )


def test_roadmap_sc7_owner_resolve(
    resolve_app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    """Roadmap SC-7 via shared resolveSubmission HTTP proof."""
    resolve_http.test_owner_resolve_by_queue_and_key_matches_enqueued_task(
        resolve_app, session_factory, queue_name
    )


def test_qual_02_work_13_roadmap_sc4_sc8_symbols_and_callable() -> None:
    """QUAL-02 / WORK-13 / Roadmap SC-4 / SC-8: concurrency evidence callables."""
    assert callable(
        race_mod.test_drain_wins_serialization_rejects_new_enqueue_without_task
    )
    assert callable(race_mod.test_matching_replay_succeeds_across_state_flip)
    assert callable(
        race_mod.test_enqueue_wins_over_policy_activation_snapshots_old_version
    )


def test_uncertain_commit_roadmap_sc1_symbols_and_callable() -> None:
    """Roadmap SC-1 + after-commit window: uncertain-commit evidence callables."""
    assert callable(
        uncertain.test_failure_before_commit_leaves_no_task_then_retry_creates_one
    )
    assert callable(
        uncertain.test_failure_after_commit_before_response_retry_returns_same_task
    )
