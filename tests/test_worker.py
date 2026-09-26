import pytest

from app.common.models import EventEnvelope
from app.workers.order_projection import OrderProjectionWorker


@pytest.mark.asyncio
async def test_worker_projects_order_created_once() -> None:
    worker = OrderProjectionWorker()
    envelope = EventEnvelope(
        trace_id="trace-1",
        tenant_id="tenant-a",
        event_type="order.created",
        aggregate_id="order-123",
        idempotency_key="request-12345",
        payload={"amount": 42},
    )

    assert await worker.handle(envelope) == "processed"
    assert await worker.handle(envelope) == "already_processed"
    assert worker.state.orders["order-123"]["status"] == "created"


@pytest.mark.asyncio
async def test_worker_dead_letters_non_recoverable_payload() -> None:
    worker = OrderProjectionWorker()
    envelope = EventEnvelope(
        trace_id="trace-1",
        tenant_id="tenant-a",
        event_type="order.created",
        aggregate_id="order-123",
        idempotency_key="request-12345",
        payload={"amount": -1},
    )

    assert await worker.handle(envelope) == "dead_lettered"
    assert len(worker.state.dlq) == 1
    assert worker.state.dlq[0].error_type == "NonRecoverableWorkerError"

