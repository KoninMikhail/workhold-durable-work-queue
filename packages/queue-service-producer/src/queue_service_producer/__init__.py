"""Public producer client distribution.

Thin role surface over shared ``_queue_service_client_core`` primitives.
Operation ownership: getCapabilities, enqueueTask, resolveSubmission, getTask,
cancelTask (see ``packages/client-operation-ownership.json``).
"""

from __future__ import annotations

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.codecs import (
    PayloadDecodeError,
    PayloadDecoder,
    PayloadEncoder,
    TypedTaskView,
    decode_payload,
    encode_payload,
)
from queue_service_producer.async_client import AsyncProducerClient
from _queue_service_client_core.errors import (
    AuthenticationError,
    ErrorCode,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
    TimeoutError,
    TransportError,
)
from _queue_service_client_core.models import (
    CancelResponse,
    EnqueueResponse,
    ProtocolErrorBody,
    ResolveSubmissionResponse,
    Task,
    TaskState,
)
from _queue_service_client_core.priority import (
    PRIORITY_DEFAULT,
    PRIORITY_MAX,
    PRIORITY_MIN,
    validate_priority,
)
from _queue_service_client_core.transport import HttpJsonTransport, encode_path_segment
from queue_service_producer.client import ProducerClient

__version__ = "1.0.0"

__all__ = [
    "AsyncProducerClient",
    "AuthenticationError",
    "CancelResponse",
    "Capabilities",
    "EnqueueResponse",
    "ErrorCode",
    "HttpJsonTransport",
    "MalformedResponseError",
    "PRIORITY_DEFAULT",
    "PRIORITY_MAX",
    "PRIORITY_MIN",
    "PayloadDecodeError",
    "PayloadDecoder",
    "PayloadEncoder",
    "ProducerClient",
    "ProtocolError",
    "ProtocolErrorBody",
    "QueueClientError",
    "ResolveSubmissionResponse",
    "Task",
    "TaskState",
    "TimeoutError",
    "TransportError",
    "TypedTaskView",
    "decode_payload",
    "encode_path_segment",
    "encode_payload",
    "validate_priority",
    "__version__",
]
