# Benchmark Protocol

This repository does not treat a laptop smoke test as a universal capacity claim.
The included k6 profile is a repeatable way to characterize one declared environment.

## Procedure

1. Record CPU model, assigned Docker CPU and memory, operating system, Docker version, and image tags.
2. Start a clean stack with `docker compose down -v` followed by `docker compose up -d --build api worker`.
3. Confirm the API is healthy and both Kafka topics have 12 partitions.
4. Run `docker compose --profile benchmark run --rm k6`.
5. Preserve `benchmark-results/summary.json` and record API/worker replica counts.
6. Report request rate, failure rate, p50/p95/p99 latency, consumer lag, and DLQ count together.

## Publication Rule

Only commit a dated report when the raw summary and environment metadata are available.
Do not generalize a development-machine result into a production throughput guarantee.
