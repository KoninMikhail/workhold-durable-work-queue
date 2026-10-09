"""Security primitives: principals, authentication, authorization, and data handling."""

from queue_service.security.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    AuthorizationResult,
    Authorizer,
    Operation,
    QUEUE_SCOPED_OPERATIONS,
    ROLE_OPERATION_GRANTS,
)
from queue_service.security.credentials import (
    AuthenticationResult,
    BearerCredentialAuthenticator,
    CredentialBinding,
    IdentityAuthenticator,
    Unauthenticated,
)
from queue_service.security.payload_policy import (
    PayloadHandlingPolicy,
    PayloadIndexingRejected,
    PayloadRetentionPolicy,
    PayloadTooLarge,
    PayloadView,
)
from queue_service.security.principals import Principal, ServiceRole
from queue_service.security.redaction import (
    DIAGNOSTIC_ALLOWLIST,
    REDACTED,
    TRUNCATED,
    DiagnosticSanitizerError,
    sanitize_for_diagnostics,
)

__all__ = [
    "AuthenticationResult",
    "AuthorizationContext",
    "AuthorizationDenied",
    "AuthorizationResult",
    "Authorizer",
    "BearerCredentialAuthenticator",
    "CredentialBinding",
    "DIAGNOSTIC_ALLOWLIST",
    "DiagnosticSanitizerError",
    "IdentityAuthenticator",
    "Operation",
    "PayloadHandlingPolicy",
    "PayloadIndexingRejected",
    "PayloadRetentionPolicy",
    "PayloadTooLarge",
    "PayloadView",
    "Principal",
    "QUEUE_SCOPED_OPERATIONS",
    "REDACTED",
    "ROLE_OPERATION_GRANTS",
    "ServiceRole",
    "TRUNCATED",
    "Unauthenticated",
    "sanitize_for_diagnostics",
]
