# High-Throughput Event Platform

**Reference architecture for event ingestion, Kafka-style streaming, idempotent workers, retries, DLQ handling, and observable operations.**

This repository demonstrates distributed-systems design choices that often sit underneath production AI, fintech, marketplace, and workflow products: durable ingestion, partitioned streams, at-least-once processing, backpressure-aware workers, and operational evidence. Its Docker path runs the complete flow with Redpanda, Redis, PostgreSQL, and OpenTelemetry.

It is a **reference architecture, not a benchmark claim**. The code and configuration are intentionally inspectable; any throughput numbers should be generated in your own environment with the included load-test harness and published with hardware, partition, payload, and worker details.

## What You Can Inspect

| Engineering question | Where it is shown |
| --- | --- |
| How are duplicate client retries handled? | Atomic Redis `SET NX` reservations with owner-safe rollback |
| How is stream ordering controlled? | `tenant_id:aggregate_id` partition keys in `app/common/partitioning.py` |
| What happens when a worker sees a bad event? | Bounded retries and Kafka DLQ records in `app/workers/processor.py` |
| How are retries bounded? | Exponential backoff policy with jitter in `app/common/retry.py` |
| How are replayed events handled? | Transactional `processed_events` insert and projection update in PostgreSQL |
| Where do PostgreSQL, Redis, and Kafka fit? | Running adapters, `docker-compose.yml`, schema, and architecture docs |
| How would this run on Kubernetes? | API and worker manifests in `deploy/kubernetes/` |
| How is observability wired? | FastAPI instrumentation and OpenTelemetry collector config |
| How can load be tested honestly? | k6 smoke profile in `load-tests/k6-ingestion.js` |

## Architecture

```mermaid
flowchart LR
    Client[API clients] --> Gateway[FastAPI ingestion gateway]
    Gateway --> Idem[Redis idempotency keys]
    Gateway --> Events[(Kafka-compatible stream)]
    Events --> WorkerA[Order projection worker]
    Events --> WorkerB[Other consumer groups]
    WorkerA --> Postgres[(PostgreSQL projections)]
    WorkerA --> DLQ[(Dead-letter topic)]
    Gateway --> OTel[OpenTelemetry collector]
    WorkerA --> OTel
```

## Repository Structure

```text
app/
  api/                  FastAPI ingestion gateway
  common/               event envelope, idempotency, partitioning, retry, telemetry
  db/schema.sql          PostgreSQL tables for events, projections, processed markers, DLQ
  workers/              example projection worker and DLQ behavior
deploy/
  kubernetes/           API and worker deployment manifests
  otel/                 OpenTelemetry collector config
docs/
  adrs/                 architecture decision records
  architecture.md       boundaries, delivery semantics, partition strategy
  reliability.md        retries, lag, DLQ, SLO candidates
  security.md           production security checklist
load-tests/             k6 ingestion smoke profile
tests/                  unit tests for ingestion, partitioning, idempotency, worker behavior
```

## Run the Complete Stack

```bash
docker compose up -d --build api worker
```

This creates two 12-partition topics, starts the API and projection worker, shares idempotency reservations through Redis, persists projections in PostgreSQL, and exports API and worker traces through the OpenTelemetry collector.

Run the end-to-end proof:

```bash
python -m pip install -e ".[dev,distributed]"
RUN_INTEGRATION_TESTS=1 pytest -q -m integration
```

PowerShell:

```powershell
$env:RUN_INTEGRATION_TESTS = "1"
python -m pytest -q -m integration
```

The integration test verifies topic partition counts, duplicate request suppression, ordered create/cancel projection, and poison-event delivery to the DLQ.

## Run the Lightweight API

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[dev]"
uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

Submit an event:

```bash
curl -X POST http://127.0.0.1:8000/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Trace-Id: demo-trace-1" \
  -d '{
    "tenant_id": "tenant-a",
    "event_type": "order.created",
    "aggregate_id": "order-123",
    "idempotency_key": "demo-request-12345",
    "payload": { "amount": 42 }
  }'
```

The default process uses in-memory adapters so the API can be inspected without infrastructure. Docker Compose explicitly selects the distributed runtime.

## Verify It

```bash
python -m pip install -e ".[dev]"
ruff check .
pytest -q
```

The unit tests prove validation, stable partition keys, reservation rollback, Redis duplicate suppression, bounded retries, worker-side duplicate handling, and DLQ routing. CI also runs the complete containerized integration test.

## Load Testing Without Inflated Claims

With the Docker stack running, execute k6 through its pinned container image:

```bash
docker compose --profile benchmark run --rm k6
```

The raw summary is written to `benchmark-results/summary.json`, which is intentionally ignored by Git until it has been reviewed and documented with its environment metadata.

Do not copy numbers from another machine into this README. If you publish results, include:

- CPU, memory, disk, and network context;
- API replica count and worker count;
- partition count;
- payload size and event mix;
- Kafka/PostgreSQL/Redis deployment details;
- p50/p95/p99 latency, error rate, consumer lag, and DLQ rate.

## Design Notes

- **Delivery model:** at-least-once delivery with Redis idempotency at ingestion and transactional PostgreSQL idempotency at consumers.
- **Partitioning:** `tenant_id:aggregate_id` preserves per-aggregate order while spreading tenants across partitions.
- **Retries:** recoverable failures should use bounded exponential backoff with jitter; poison messages belong in the DLQ.
- **Schema evolution:** event schema versions are part of the envelope; production deployments should add a schema registry and compatibility checks.
- **Observability:** trace IDs flow through the event envelope and OpenTelemetry spans are emitted by the API and worker.

## Production Gaps

Before adopting this pattern for real traffic, add authentication, tenant authorization, schema registry checks, dashboards, alerts, autoscaling from consumer lag, signed images, TLS/mTLS, secret rotation, and operator-approved DLQ replay tooling.

## Explore the Decisions

- [Architecture](docs/architecture.md)
- [Reliability and operations](docs/reliability.md)
- [Benchmark protocol](docs/benchmarks.md)
- [Security considerations](docs/security.md)
- [ADR 0001: Kafka-compatible stream](docs/adrs/0001-use-kafka-compatible-stream.md)
- [ADR 0002: at-least-once delivery with idempotent effects](docs/adrs/0002-idempotency-and-at-least-once-delivery.md)

Built by [Uzair Khatri](https://uzairkhatri.com). Licensed under [MIT](LICENSE).

