# Operations

[Documentation](../README.md) › **Operations**

Production deployment, security, observability, and recovery procedures.

Start with the [reading path](../00-onboarding/01-reading-path.md).

| # | Document | Purpose |
| --- | --- | --- |
| 1 | [01-security.md](01-security.md) | Trust boundary, identities, RBAC, tokens, payload handling, and file-secret (`*_FILE`) injection |
| 2 | [02-deployment.md](02-deployment.md) | Process roles, image entrypoint, secret mounts, migrations, readiness, connection budget, and backup |
| 3 | [03-observability.md](03-observability.md) | Metrics, logs, traces, SLIs, and alerts |
| 4 | [04-application-outbox-bridge.md](04-application-outbox-bridge.md) | Bridge lag, health, SLIs, labels, and alert principles |
| 5 | [05-runbooks.md](05-runbooks.md) | Detection, containment, PITR, and recovery procedures by alert |
| 6 | [06-chaos-testing.md](06-chaos-testing.md) | Kernel chaos-scenario matrix, fault injection, and QUAL-04 commands |
| 7 | [07-admin-tools.md](07-admin-tools.md) | Safe, dangerous, and break-glass operations |
| 8 | [08-release-qualification.md](08-release-qualification.md) | Kernel qualification record (synthetic CI evidence) |
| 9 | [09-client-release-qualification.md](09-client-release-qualification.md) | Role-client set qualification record |
| 10 | [10-release.md](10-release.md) | GitHub release-please cycle, image, and client publish |

Exact deployment manifests and numeric SLOs follow implementation benchmarks.

---

← [Contents](../README.md) · [Security](01-security.md) →
