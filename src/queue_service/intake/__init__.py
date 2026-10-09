"""Producer intake command boundary (normalize + fingerprint before persistence)."""

from queue_service.intake.admission import (
    DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS,
    DEFAULT_PAYLOAD_MAX_BYTES,
    DEFAULT_REQUEST_MAX_BYTES,
    EnqueueAdmissionLimits,
    validate_producer_enqueue_admission,
)
from queue_service.intake.contracts import (
    FINGERPRINT_SIZE_BYTES,
    EnqueueCommand,
    IntakeValidationError,
    normalize_enqueue_command,
)
from queue_service.intake.depth import (
    DEFAULT_INSTANCE_ACTIVE_DEPTH,
    DEFAULT_QUEUE_ACTIVE_DEPTH,
    DepthCeilings,
    DepthReservation,
    reserve_active_depth,
)
from queue_service.intake.repository import (
    DedupScope,
    EnqueuePersistenceResult,
    EnqueueRepository,
)
from queue_service.intake.service import EnqueueFaultHooks, EnqueueService

__all__ = [
    "DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS",
    "DEFAULT_INSTANCE_ACTIVE_DEPTH",
    "DEFAULT_PAYLOAD_MAX_BYTES",
    "DEFAULT_QUEUE_ACTIVE_DEPTH",
    "DEFAULT_REQUEST_MAX_BYTES",
    "DedupScope",
    "DepthCeilings",
    "DepthReservation",
    "EnqueueAdmissionLimits",
    "EnqueueCommand",
    "EnqueueFaultHooks",
    "EnqueuePersistenceResult",
    "EnqueueRepository",
    "EnqueueService",
    "FINGERPRINT_SIZE_BYTES",
    "IntakeValidationError",
    "normalize_enqueue_command",
    "reserve_active_depth",
    "validate_producer_enqueue_admission",
]
