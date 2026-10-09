"""Private shared Queue client primitives (not a public role API surface)."""

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
    measure_json_bytes,
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
    raise_for_protocol_status,
)
from _workhold_client_core.models import (
    AckCancelResult,
    CancelResponse,
    ClaimSummary,
    CompleteResult,
    EnqueueResponse,
    FailResult,
    HeartbeatResult,
    ProtocolErrorBody,
    ResolveSubmissionResponse,
    Task,
    TaskState,
)
from _workhold_client_core.priority import (
    PRIORITY_DEFAULT,
    PRIORITY_MAX,
    PRIORITY_MIN,
    validate_priority,
)
from _workhold_client_core.redaction import (
    REDACTED,
    redact_headers,
    redact_text,
    sanitize_for_diagnostics,
)
from _workhold_client_core.config import ClientConfig
from _workhold_client_core.requests import PreparedRequest, prepare_json_request
from _workhold_client_core.transport import (
    AsyncTransport,
    HttpJsonTransport,
    SyncTransport,
    TransportResponse,
    encode_path_segment,
)

__version__ = "1.0.2"

__all__ = [
    "AckCancelResult",
    "AsyncTransport",
    "AuthenticationError",
    "CancelResponse",
    "Capabilities",
    "ClientConfig",
    "ClaimSummary",
    "CompleteResult",
    "EnqueueResponse",
    "ErrorCode",
    "FailResult",
    "HeartbeatResult",
    "HttpJsonTransport",
    "LeaseLostError",
    "PayloadDecodeError",
    "PayloadDecoder",
    "PayloadEncoder",
    "PreparedRequest",
    "MalformedResponseError",
    "PRIORITY_DEFAULT",
    "PRIORITY_MAX",
    "PRIORITY_MIN",
    "ProtocolError",
    "ProtocolErrorBody",
    "QueueClientError",
    "RequestCancelledError",
    "SyncTransport",
    "REDACTED",
    "ResolveSubmissionResponse",
    "Task",
    "TaskState",
    "TerminalConflictError",
    "TimeoutError",
    "TransportError",
    "TransportResponse",
    "TypedClaimView",
    "TypedTaskView",
    "decode_payload",
    "encode_path_segment",
    "encode_payload",
    "measure_json_bytes",
    "prepare_json_request",
    "raise_for_protocol_status",
    "redact_headers",
    "redact_text",
    "sanitize_for_diagnostics",
    "validate_priority",
    "__version__",
]
