# 032. Client-side content key

**Status:** Accepted  
**Date:** 2026-11-23  
**Scope:** Python producer and consumer clients (sync and async) and the task payload

## Context

Workhold treats the task payload as application-owned opaque JSON and leaves
storage encryption to the deployment. Some applications also want the business
content of a task to be unreadable to the queue operator and to the database. A
server-held key would give the queue the power to read that content, and a
randomized ciphertext would change the body of every enqueue retry.

## Decision

- The producer and consumer clients accept one optional `content_key` constructor
  argument. The application passes the same key to every client that must write or
  read protected payloads. Omitting it or passing `None` leaves the payload as
  plaintext JSON.
- The content key stays in the client; the server does not decrypt, never receives
  the key, and stores and returns the task payload as opaque JSON.
- The key is a 32-byte secret, passed as `bytes` or as standard base64 text. The SDK
  does not read it from the environment and does not derive it from a passphrase.
- `content_key` is not an authentication secret. It does not grant a role and does
  not replace the bearer token. Producer, worker, and admin credentials stay distinct
  as in [ADR 013](013-separate-security-planes.md), which is unchanged.
- The HTTP and storage contracts do not change.

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| Server-side decryption | The server would have to hold the key and would stop treating the payload as opaque |
| Random nonce | A new ciphertext on every retry changes the request body and breaks enqueue idempotency |
| Passphrase KDF | The shared secret is the 32-byte key itself; a KDF adds parameters and a second secret format |

## Consequences

**Positive:** The queue operator and the database see only ciphertext for a keyed
client. An unkeyed client behaves exactly as before.

**Negative / trade-offs:**

- The nonce is deterministic: the first 12 bytes of an HMAC over the canonical
  plaintext, so the same plaintext and key give the same envelope. Two different
  plaintexts could in principle share a nonce.
- The wire value is an object with three fields. A plaintext payload that has exactly
  that shape is indistinguishable from an envelope.
- The server cannot read the business content of an encrypted payload.
- Losing the key makes stored payloads unrecoverable. There is no key rotation.

## References

- [ADR 013](013-separate-security-planes.md) (separate security planes)
- [Client SDK guide](../../02-guides/06-client-sdk-ergonomics.md)
- [Security contract](../../05-operations/01-security.md)
