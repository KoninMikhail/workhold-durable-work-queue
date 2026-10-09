"""Public consumer client distribution.

Thin role surface over shared ``_queue_service_client_core`` primitives.
Operation ownership: getCapabilities, claimTasks, heartbeatClaim, completeClaim,
failClaim, acknowledgeClaimCancellation
(see ``packages/client-operation-ownership.json``).
"""

from __future__ import annotations

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.codecs import (
    PayloadDecodeError,
    PayloadDecoder,
    PayloadEncoder,
    TypedClaimView,
    TypedTaskView,
    decode_payload,
    encode_payload,
)
from _queue_service_client_core.errors import (
    AuthenticationError,
    ErrorCode,
    LeaseLostError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
    RequestCancelledError,
    TerminalConflictError,
    TimeoutError,
    TransportError,
)
from _queue_service_client_core.models import (
    AckCancelResult,
    ClaimSummary,
    CompleteResult,
    FailResult,
    HeartbeatResult,
    ProtocolErrorBody,
    Task,
    TaskState,
)
from _queue_service_client_core.transport import HttpJsonTransport, encode_path_segment
from queue_service_consumer.client import Claim, ConsumerClient
from queue_service_consumer.supervisor import (
    DEFAULT_WAIT_SECONDS,
    HEARTBEAT_JITTER_RATIO,
    CancellationToken,
    ConsumerSupervisor,
    HandlerErrorEvent,
    LeaseLostEvent,
)

__version__ = "1.0.0"


__all__ = [
    "AckCancelResult",
    "AuthenticationError",
    "CancellationToken",
    "Capabilities",
    "Claim",
    "ClaimSummary",
    "CompleteResult",
    "ConsumerClient",
    "ConsumerSupervisor",
    "DEFAULT_WAIT_SECONDS",
    "ErrorCode",
    "FailResult",
    "HandlerErrorEvent",
    "HEARTBEAT_JITTER_RATIO",
    "HeartbeatResult",
    "HttpJsonTransport",
    "LeaseLostError",
    "LeaseLostEvent",
    "MalformedResponseError",
    "PayloadDecodeError",
    "PayloadDecoder",
    "PayloadEncoder",
    "ProtocolError",
    "ProtocolErrorBody",
    "QueueClientError",
    "RequestCancelledError",
    "Task",
    "TaskState",
    "TerminalConflictError",
    "TimeoutError",
    "TransportError",
    "TypedClaimView",
    "TypedTaskView",
    "decode_payload",
    "encode_path_segment",
    "encode_payload",
    "__version__",
]
