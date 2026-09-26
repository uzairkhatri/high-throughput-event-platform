# Reliability and Operations

## Failure Handling

| Failure | Expected control |
| --- | --- |
| Client retries the same request | Idempotency key returns the original accepted event |
| Worker receives the same event twice | Consumer-side processed-event table prevents duplicate projection writes |
| Transient dependency failure | Bounded exponential backoff with jitter |
| Poison message | Dead-letter topic/table with error metadata |
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

Use `load-tests/k6-ingestion.js` as a smoke profile. Store measured reports in
`reports/` and publish the hardware, image tags, partition count, worker count,
payload shape, and database configuration with any numbers.

