# Architecture Decision Records

[Documentation](../../README.md) › [Architecture](../README.md) › **ADR**

Why repository-level decisions were accepted. Start with the [reading path](../../00-onboarding/01-reading-path.md) if you have not yet followed the product path.

| ADR | Title | Status |
| --- | --- | --- |
| [001-durable-work-queue-core](001-durable-work-queue-core.md) | Durable Work Queue is the product core | Accepted |
| [002-named-queues-per-instance](002-named-queues-per-instance.md) | Named queues inside a per-application instance | Accepted |
| [003-separate-spawns-and-events](003-separate-spawns-and-events.md) | Separate spawned tasks from delivery events | Accepted |
| [004-app-local-outbox-bridge](004-app-local-outbox-bridge.md) | App-local outbox bridge for business databases | Accepted |
| [005-fenced-leases](005-fenced-leases.md) | Fenced leases for multi-replica processing | Accepted |
| [006-hot-cold-partitioning](006-hot-cold-partitioning.md) | Partition cold history, not active queue state | Accepted |
| [007-postgresql-type-policy](007-postgresql-type-policy.md) | PostgreSQL type policy | Accepted |
| [008-queue-retry-policy](008-queue-retry-policy.md) | Retry policy belongs to the named queue | Accepted |
| [009-runtime-control-plane](009-runtime-control-plane.md) | Persisted runtime control plane | Accepted |
| [010-queue-runtime-states](010-queue-runtime-states.md) | Three queue runtime states | Accepted |
| [011-no-business-result-store](011-no-business-result-store.md) | workhold does not store business results | Accepted |
| [012-protocol-first-clients](012-protocol-first-clients.md) | Protocol-first, batch-ready clients | Accepted |
| [013-separate-security-planes](013-separate-security-planes.md) | Separate producer, worker and admin security planes | Accepted |
| [014-layered-admission-control](014-layered-admission-control.md) | Layered admission control | Accepted |
| [015-single-image-multi-role](015-single-image-multi-role.md) | One image with explicit process roles | Accepted |
| [016-http-openapi-claim-security](016-http-openapi-claim-security.md) | HTTP/JSON OpenAPI and split claim credentials | Accepted |
| [017-correctness-registry-retention](017-correctness-registry-retention.md) | Correctness registry retention defaults | Accepted |
| [018-http-first-delivery-relay](018-http-first-delivery-relay.md) | Pluggable Delivery Relay with HTTP first | Accepted |
| [019-initial-production-gate](019-initial-production-gate.md) | Initial production performance gate | Accepted |
| [020-python-sdk-packaging](020-python-sdk-packaging.md) | Python SDK is a separate distribution in this repository | Superseded by [029](029-role-split-python-clients.md) |
| [021-cloudevents-envelope](021-cloudevents-envelope.md) | CloudEvents 1.0 delivery envelope | Accepted |
| [022-physical-contract-baseline](022-physical-contract-baseline.md) | Phase 3.1 physical API/storage baseline | Accepted |
| [023-qualified-kernel-storage-profile](023-qualified-kernel-storage-profile.md) | Qualified kernel storage profile (Phase 3.9 QUAL-03) | Accepted |
| [024-postgresql-18-6-runtime-engine](024-postgresql-18-6-runtime-engine.md) | PostgreSQL 18.6 is the only runtime engine | Accepted |
| [025-container-file-secrets](025-container-file-secrets.md) | Container secrets via `*_FILE` and entrypoint | Accepted |
| [026-optional-glitchtip-sentry-dsn](026-optional-glitchtip-sentry-dsn.md) | Optional GlitchTip via `SENTRY_DSN` presence | Accepted |
| [027-stdlib-http-asgi-runtime](027-stdlib-http-asgi-runtime.md) | Stdlib HTTP + pure ASGI API runtime | Accepted |
| [028-catalog-apply-process-role](028-catalog-apply-process-role.md) | Fifth process role `apply` for catalog ensure-exists | Accepted |
| [029-role-split-python-clients](029-role-split-python-clients.md) | Role-split producer/consumer/admin Python clients | Accepted; distribution names superseded by [030](030-workhold-distribution-names.md) |
| [030-workhold-distribution-names](030-workhold-distribution-names.md) | Public distributions use the workhold name base | Accepted |

Index, HASH, and payload selections from Phase 3.9 QUAL-03 are recorded in ADR 023.
The engine pin, file secrets, GlitchTip, and stdlib HTTP are ADR 024–027.
The catalog apply process role is ADR 028.
Python client role split is ADR 029; distribution names are ADR 030.
Post-benchmark production SLOs remain follow-up work under
[12-physical-contract-benchmarks.md](../12-physical-contract-benchmarks.md).
