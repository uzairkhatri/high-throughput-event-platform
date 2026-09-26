import asyncio
import os
from uuid import uuid4

import httpx
import pytest

from app.common.models import DeadLetterRecord

asyncpg = pytest.importorskip("asyncpg")
AIOKafkaConsumer = pytest.importorskip("aiokafka").AIOKafkaConsumer

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION_TESTS") != "1",
        reason="set RUN_INTEGRATION_TESTS=1 with the Docker Compose stack running",
    ),
]

API_URL = os.getenv("INTEGRATION_API_URL", "http://127.0.0.1:8000")
POSTGRES_DSN = os.getenv(
    "INTEGRATION_POSTGRES_DSN",
    "postgresql://events:events@127.0.0.1:5432/events",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "INTEGRATION_KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:19092",
)


async def wait_for_projection(
    connection: object,
    tenant_id: str,
    order_id: str,
    expected_status: str,
) -> None:
    for _ in range(100):
        status = await connection.fetchval(
            """
            SELECT status FROM order_projection
            WHERE tenant_id = $1 AND order_id = $2
            """,
            tenant_id,
            order_id,
        )
        if status == expected_status:
            return
        await asyncio.sleep(0.2)
    raise AssertionError(f"projection did not reach status={expected_status}")


@pytest.mark.asyncio
async def test_end_to_end_ordering_idempotency_and_dlq() -> None:
    suffix = uuid4().hex
    tenant_id = f"tenant-{suffix[:8]}"
    order_id = f"order-{suffix}"
    consumer = AIOKafkaConsumer(
        "events.orders.dlq.v1",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"integration-dlq-{suffix}",
        auto_offset_reset="latest",
    )
    await consumer.start()
    connection = await asyncpg.connect(POSTGRES_DSN)

    try:
        partitions = await consumer.partitions_for_topic("events.orders.dlq.v1")
        assert partitions is not None
        assert len(partitions) == 12

        async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
            created_payload = {
                "tenant_id": tenant_id,
                "event_type": "order.created",
                "aggregate_id": order_id,
                "idempotency_key": f"create-{suffix}",
                "payload": {"amount": 42},
            }
            created = await client.post("/v1/events", json=created_payload)
            duplicate = await client.post("/v1/events", json=created_payload)
            assert created.status_code == 202
            assert duplicate.status_code == 202
            assert duplicate.json()["duplicate"] is True
            assert duplicate.json()["event_id"] == created.json()["event_id"]
            await wait_for_projection(connection, tenant_id, order_id, "created")

            cancelled = await client.post(
                "/v1/events",
                json={
                    **created_payload,
                    "event_type": "order.cancelled",
                    "idempotency_key": f"cancel-{suffix}",
                    "payload": {"reason": "customer_request"},
                },
            )
            assert cancelled.status_code == 202
            await wait_for_projection(connection, tenant_id, order_id, "cancelled")

            invalid = await client.post(
                "/v1/events",
                json={
                    **created_payload,
                    "aggregate_id": f"invalid-{order_id}",
                    "idempotency_key": f"invalid-{suffix}",
                    "payload": {"amount": -1},
                },
            )
            assert invalid.status_code == 202

        message = await asyncio.wait_for(consumer.getone(), timeout=20)
        record = DeadLetterRecord.model_validate_json(message.value)
        assert str(record.envelope.event_id) == invalid.json()["event_id"]
        assert record.error_type == "NonRecoverableWorkerError"
        assert record.attempts == 1
    finally:
        await connection.close()
        await consumer.stop()
