# Security policy

## Supported versions

| Version | Security fixes |
| --- | --- |
| 1.0.x (latest tagged release) | Yes |
| `main` before a tag | Development line |

## Reporting a vulnerability

Report privately. Do not open a public GitHub issue, pull request, or
discussion for a suspected vulnerability.

1. Open a [private security advisory](https://github.com/KoninMikhail/workhold-durable-work-queue/security/advisories/new).
2. If that form is unavailable, email [dev.konin@gmail.com](mailto:dev.konin@gmail.com).

Include:

- the affected component: runtime image, `workhold`, or a client package
  (`workhold-client-core`, `workhold-producer`, `workhold-consumer`,
  `workhold-admin`);
- the version, image tag, or commit;
- what an attacker can do, and whether a credential is required;
- a minimal reproduction with secrets removed.

Do not include live tokens, DSNs, claim tokens, or customer data.

The operational trust model (principals, claim tokens, file secrets) is
[docs/05-operations/01-security.md](docs/05-operations/01-security.md). That
page is not the reporting channel.
