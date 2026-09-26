# ADR 0001: Use a Kafka-Compatible Event Stream

## Status

Accepted

## Context

The platform needs to absorb bursts at the API edge while allowing independent
worker groups to process events at their own pace. Per-aggregate ordering matters
for order lifecycle events.

## Decision

Use a Kafka-compatible stream API. The local stack uses Redpanda because it keeps
the Docker Compose footprint small while preserving Kafka producer and consumer
concepts.

## Consequences

- Partition keys become a first-class design decision.
- Consumer lag is the main scaling signal for workers.
- Exactly-once business effects still require idempotent consumers and database
  transactions; the stream alone does not solve that problem.

