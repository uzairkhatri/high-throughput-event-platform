# Reliability and Operations

## Failure Handling

| Failure | Expected control |
| --- | --- |
| Client retries the same request | Idempotency key returns the original accepted event |
| Worker receives the same event twice | Consumer-side processed-event table prevents duplicate projection writes |
| Transient dependency failure | Bounded exponential backoff with jitter |
| Poison message | Dead-letter topic with the original envelope, failure type, message, and attempt count |
| Stream lag increases | Scale workers by consumer group lag and partition count |
| Hot aggregate | Investigate business-level sharding or command-side throttling |

## SLO Candidates

The repository does not publish benchmark claims. Teams adopting this pattern
should define environment-specific SLOs after running load tests against their
own infrastructure:

- ingestion availability;
- accepted-event latency at the API edge;
- stream publish error rate;
- consumer lag by topic and partition;
- DLQ rate by event type;
- projection freshness.

## Benchmarking

Use `docker compose --profile benchmark run --rm k6` to execute the pinned smoke
profile. Follow [the benchmark protocol](benchmarks.md) before publishing results.

