# ADR 0002: Prefer At-Least-Once Delivery with Idempotent Effects

## Status

Accepted

## Context

Distributed event systems can duplicate messages through client retries,
producer retries, consumer rebalances, and manual replays.

## Decision

Design for at-least-once delivery and make duplicate effects explicit:

- API clients provide an idempotency key.
- The ingestion service reserves the key before publishing.
- Workers keep a processed-event record per consumer group.
- Poison messages move to a DLQ instead of blocking a partition forever.

## Consequences

This model is easier to reason about than claiming exactly-once behavior across
HTTP, Kafka, Redis, PostgreSQL, and worker side effects. Application handlers
must be written so replaying an event does not corrupt state.

