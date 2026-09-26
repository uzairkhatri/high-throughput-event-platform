from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request, status
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from app.common.idempotency import IdempotencyStore, InMemoryIdempotencyStore, RedisIdempotencyStore
from app.common.models import AcceptedEvent, EventEnvelope, EventRequest
from app.common.partitioning import partition_key
from app.common.publisher import EventPublisher, InMemoryPublisher, KafkaPublisher
from app.common.settings import settings
from app.common.telemetry import configure_tracing


def build_app(
    publisher: EventPublisher | None = None,
    idempotency_store: IdempotencyStore | None = None,
) -> FastAPI:
    configure_tracing(settings.service_name, settings.otlp_endpoint)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        redis_client = None
        kafka_producer = None
        if publisher is not None or idempotency_store is not None:
            app.state.publisher = publisher or InMemoryPublisher()
            app.state.idempotency_store = idempotency_store or InMemoryIdempotencyStore()
        elif settings.runtime_mode == "distributed":
            from aiokafka import AIOKafkaProducer
            from redis.asyncio import Redis

            redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
            await redis_client.ping()
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                acks="all",
                enable_idempotence=True,
            )
            await kafka_producer.start()
            app.state.publisher = KafkaPublisher(kafka_producer, settings.events_topic)
            app.state.idempotency_store = RedisIdempotencyStore(
                redis_client,
                settings.idempotency_ttl_seconds,
            )
        else:
            app.state.publisher = InMemoryPublisher()
            app.state.idempotency_store = InMemoryIdempotencyStore()

        yield

        if kafka_producer is not None:
            await kafka_producer.stop()
        if redis_client is not None:
            await redis_client.aclose()

    app = FastAPI(
        title="High-Throughput Event Platform",
        version="0.2.0",
        description="Reference ingestion API for partitioned event streams.",
        lifespan=lifespan,
    )
    uses_local_dependencies = (
        publisher is not None
        or idempotency_store is not None
        or settings.runtime_mode != "distributed"
    )
    if uses_local_dependencies:
        app.state.publisher = publisher or InMemoryPublisher()
        app.state.idempotency_store = idempotency_store or InMemoryIdempotencyStore()
    FastAPIInstrumentor.instrument_app(app)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/events", response_model=AcceptedEvent, status_code=status.HTTP_202_ACCEPTED)
    async def ingest_event(
        request: Request,
        body: EventRequest,
        x_trace_id: str | None = Header(default=None),
    ) -> AcceptedEvent:
        if len(body.model_dump_json().encode("utf-8")) > settings.max_payload_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="event payload exceeds configured byte limit",
            )

        trace_id = x_trace_id or str(uuid4())
        envelope = EventEnvelope(
            trace_id=trace_id,
            tenant_id=body.tenant_id,
            event_type=body.event_type,
            aggregate_id=body.aggregate_id,
            idempotency_key=body.idempotency_key,
            payload=body.payload,
        )
        reservation_key = f"{body.tenant_id}:{body.idempotency_key}"
        idempotency_result = await request.app.state.idempotency_store.reserve(
            reservation_key,
            envelope.event_id,
        )
        key = partition_key(body.tenant_id, body.aggregate_id)

        if not idempotency_result.accepted:
            return AcceptedEvent(
                status="duplicate",
                event_id=idempotency_result.event_id,
                partition_key=key,
                duplicate=True,
            )

        try:
            await request.app.state.publisher.publish(envelope, key)
        except Exception as exc:
            await request.app.state.idempotency_store.release(
                reservation_key,
                envelope.event_id,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="event stream is temporarily unavailable",
            ) from exc
        return AcceptedEvent(status="accepted", event_id=envelope.event_id, partition_key=key)

    return app


app = build_app()

