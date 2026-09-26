# GitHub Actions Smoke Run: 2026-09-26

This report records one reproducible CI smoke run. It is evidence that the checked-in
profile executes successfully; it is not a production sizing or capacity claim.

## Source

- Commit: `2e0459b69bb66e72bb03fa8a80eff45ffa2cf1d3`
- Workflow run: [CI #7](https://github.com/uzairkhatri/high-throughput-event-platform/actions/runs/36246081076)
- Raw artifact: `k6-summary` (artifact ID `10907109026`, retained by GitHub Actions until its configured expiry)
- Profile: 10 constant virtual users for 30 seconds with a 100 ms sleep per iteration
- Request: one unique `order.created` event per iteration

## Environment

| Property | Recorded value |
| --- | --- |
| Runner | GitHub-hosted Linux x64 runner on Azure |
| Kernel | Linux `6.17.0-1022-azure` |
| CPU | 4 vCPU, AMD EPYC 7763 64-Core Processor |
| Memory | 15 GiB visible to the runner |
| Docker Engine | 28.0.4 |
| Docker Compose | 2.38.2 |
| API replicas | 1 |
| Worker replicas | 1 |
| Event partitions | 12 |
| Redpanda | `v24.2.4` |
| PostgreSQL | `16-alpine` |
| Redis | `7-alpine` |
| OpenTelemetry Collector | `0.109.0` |
| k6 | `0.54.0` |

## Result

| Metric | Measured value |
| --- | ---: |
| Completed requests | 2,654 |
| Request rate | 88.16 requests/second |
| Failed requests | 0.00% |
| Check pass rate | 100.00% |
| Mean request duration | 12.65 ms |
| Median request duration | 12.27 ms |
| p90 request duration | 14.14 ms |
| p95 request duration | 15.51 ms |
| Maximum request duration | 59.84 ms |

## Interpretation

The threshold checks passed: request failure rate remained below 1% and p95 request
duration remained below 500 ms. These measurements cover the API acceptance path,
including Redis idempotency and acknowledged Kafka publication.

This run did not record consumer lag, projection freshness, database saturation, or
steady-state behavior beyond 30 seconds. It therefore must not be presented as the
platform's end-to-end throughput limit. A capacity study should increase duration and
arrival rate, capture lag per partition, and verify PostgreSQL projection convergence.
