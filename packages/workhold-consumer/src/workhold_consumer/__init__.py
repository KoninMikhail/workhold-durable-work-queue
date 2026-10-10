"""Public consumer client distribution.

Thin role surface over shared ``_workhold_client_core`` primitives.
Operation ownership: getCapabilities, claimTasks, heartbeatClaim, completeClaim,
failClaim, acknowledgeClaimCancellation
(see ``packages/client-operation-ownership.json``).
"""

from __future__ import annotations

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.codecs import (
    PayloadDecodeError,
    PayloadDecoder,
    PayloadEncoder,
    TypedClaimView,
    TypedTaskView,
    decode_payload,
    encode_payload,
)
from _workhold_client_core.errors import (
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
from _workhold_client_core.models import (
    AckCancelResult,
    ClaimSummary,
    CompleteResult,
    FailResult,
    HeartbeatResult,
    ProtocolErrorBody,
    Task,
    TaskState,
)
from _workhold_client_core.transport import HttpJsonTransport, encode_path_segment
from workhold_consumer.client import Claim, ConsumerClient
from workhold_consumer.supervisor import (
    DEFAULT_WAIT_SECONDS,
    HEARTBEAT_JITTER_RATIO,
    CancellationToken,
    ConsumerSupervisor,
    HandlerErrorEvent,
    LeaseLostEvent,
)

__version__ = "1.0.4"  # x-release-please-version


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
