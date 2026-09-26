from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request, status
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from app.common.idempotency import InMemoryIdempotencyStore
from app.common.models import AcceptedEvent, EventEnvelope, EventRequest
from app.common.partitioning import partition_key
from app.common.publisher import EventPublisher, InMemoryPublisher
from app.common.settings import settings
from app.common.telemetry import configure_tracing


def build_app(
    publisher: EventPublisher | None = None,
    idempotency_store: InMemoryIdempotencyStore | None = None,
) -> FastAPI:
    configure_tracing(settings.service_name, settings.otlp_endpoint)
    app = FastAPI(
        title="High-Throughput Event Platform",
        version="0.1.0",
        description="Reference ingestion API for partitioned event streams.",
    )
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
        idempotency_result = request.app.state.idempotency_store.reserve(
            body.idempotency_key,
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

        await request.app.state.publisher.publish(envelope, key)
        return AcceptedEvent(status="accepted", event_id=envelope.event_id, partition_key=key)

    return app


app = build_app()

