import asyncio
from collections.abc import Awaitable, Callable

from app.common.database import OrderRepository
from app.common.models import DeadLetterRecord, EventEnvelope
from app.common.retry import RetryPolicy
from app.workers.order_projection import NonRecoverableWorkerError, RecoverableWorkerError


class DeadLetterPublisher:
    def __init__(self, producer: object, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, record: DeadLetterRecord) -> None:
        key = f"{record.envelope.tenant_id}:{record.envelope.aggregate_id}".encode()
        await self._producer.send_and_wait(
            self._topic,
            record.model_dump_json().encode(),
            key=key,
        )


class DistributedOrderProcessor:
    def __init__(
        self,
        repository: OrderRepository,
        dead_letter_publisher: DeadLetterPublisher,
        retry_policy: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._repository = repository
        self._dead_letter_publisher = dead_letter_publisher
        self._retry_policy = retry_policy
        self._sleep = sleep

    async def process(self, envelope: EventEnvelope) -> str:
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                return await self._repository.apply(envelope)
            except NonRecoverableWorkerError as exc:
                await self._dead_letter(envelope, exc, attempt)
                return "dead_lettered"
            except RecoverableWorkerError as exc:
                if attempt == self._retry_policy.max_attempts:
                    await self._dead_letter(envelope, exc, attempt)
                    return "dead_lettered"
                await self._sleep(self._retry_policy.delay_for_attempt(attempt))

        raise RuntimeError("retry loop exhausted without a terminal result")

    async def _dead_letter(
        self,
        envelope: EventEnvelope,
        error: Exception,
        attempts: int,
    ) -> None:
        await self._dead_letter_publisher.publish(
            DeadLetterRecord(
                envelope=envelope,
                error_type=type(error).__name__,
                error_message=str(error),
                attempts=attempts,
            )
        )

