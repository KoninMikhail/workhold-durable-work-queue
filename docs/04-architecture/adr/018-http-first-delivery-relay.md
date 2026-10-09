# 018. Pluggable Delivery Relay with HTTP first

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Delivery Outbox transport

## Context

Parser v1 shows that callback delivery is needed, and future applications may
need RabbitMQ or Kafka. A mandatory single broker would couple the Work Queue
to infrastructure that the core task path does not need.

## Decision

The Delivery Relay uses a transport-adapter boundary. The first supported
adapter is an HTTP webhook. It publishes a stable event ID and envelope, enforces
destination allowlists and timeouts, classifies transient and permanent responses,
applies exponential delivery backoff and a circuit breaker, and dead-letters
exhausted events.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Mandatory RabbitMQ | Adds a broker dependency to every per-app deployment |
| Mandatory Kafka | Incompatible operational weight and the callback use case |
| Transport remains undefined | Blocks Phase 5 implementation and conformance |
| Transport-specific outbox schema | Blocks additive adapters |

## Consequences

**Positive:** A concrete first delivery path without coupling Work Queue
persistence to a broker.

**Negative / trade-offs:** HTTP destination security, SSRF protection, and response
classification become product responsibilities.

**Follow-up:** Phase 5 defines the transport-neutral event envelope and
HTTP adapter configuration; broker adapters can follow without schema changes.

## References

- [Separate spawns/events ADR](003-separate-spawns-and-events.md)
- [Security](../../05-operations/01-security.md)
