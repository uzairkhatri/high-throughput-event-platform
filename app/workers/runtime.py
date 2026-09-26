import asyncio
import logging

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from opentelemetry import trace

from app.common.database import PostgresOrderRepository
from app.common.models import EventEnvelope
from app.common.retry import RetryPolicy
from app.common.settings import settings
from app.common.telemetry import configure_tracing
from app.workers.processor import DeadLetterPublisher, DistributedOrderProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run() -> None:
    configure_tracing("order-projection-worker", settings.otlp_endpoint)
    tracer = trace.get_tracer(__name__)
    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=10)
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    consumer = AIOKafkaConsumer(
        settings.events_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await producer.start()
    await consumer.start()
    processor = DistributedOrderProcessor(
        repository=PostgresOrderRepository(pool, settings.consumer_group),
        dead_letter_publisher=DeadLetterPublisher(producer, settings.dlq_topic),
        retry_policy=RetryPolicy(max_attempts=settings.worker_max_attempts),
    )

    try:
        async for message in consumer:
            try:
                envelope = EventEnvelope.model_validate_json(message.value)
                with tracer.start_as_current_span("process-order-event") as span:
                    span.set_attribute("messaging.kafka.partition", message.partition)
                    span.set_attribute("messaging.kafka.offset", message.offset)
                    span.set_attribute("event.id", str(envelope.event_id))
                    result = await processor.process(envelope)
                    span.set_attribute("event.processing_result", result)
                await consumer.commit()
            except Exception:
                logger.exception(
                    "message processing failed; stopping before a later offset can be committed",
                    extra={"partition": message.partition, "offset": message.offset},
                )
                raise
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
