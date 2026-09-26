import pytest

from app.common.models import EventEnvelope
from app.common.retry import RetryPolicy
from app.workers.order_projection import NonRecoverableWorkerError, RecoverableWorkerError
from app.workers.processor import DistributedOrderProcessor


class StubRepository:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    async def apply(self, envelope: EventEnvelope) -> str:
        outcome = self.outcomes[min(self.calls, len(self.outcomes) - 1)]
        self.calls += 1
        if isinstance(outcome, Exception):
            raise outcome
        return str(outcome)


class RecordingDlq:
    def __init__(self) -> None:
        self.records = []

    async def publish(self, record: object) -> None:
        self.records.append(record)


async def no_sleep(delay: float) -> None:
    return None


def envelope() -> EventEnvelope:
    return EventEnvelope(
        trace_id="trace-1",
        tenant_id="tenant-a",
        event_type="order.created",
        aggregate_id="order-123",
        idempotency_key="request-12345",
        payload={"amount": 42},
    )


@pytest.mark.asyncio
async def test_recoverable_failure_is_retried_before_success() -> None:
    repository = StubRepository([RecoverableWorkerError("wait"), "processed"])
    dlq = RecordingDlq()
    processor = DistributedOrderProcessor(
        repository,
        dlq,
        RetryPolicy(max_attempts=3, jitter_ratio=0),
        sleep=no_sleep,
    )

    assert await processor.process(envelope()) == "processed"
    assert repository.calls == 2
    assert dlq.records == []


@pytest.mark.asyncio
async def test_retry_exhaustion_publishes_dlq_record() -> None:
    repository = StubRepository([RecoverableWorkerError("still missing")])
    dlq = RecordingDlq()
    processor = DistributedOrderProcessor(
        repository,
        dlq,
        RetryPolicy(max_attempts=3, jitter_ratio=0),
        sleep=no_sleep,
    )

    assert await processor.process(envelope()) == "dead_lettered"
    assert repository.calls == 3
    assert dlq.records[0].attempts == 3


@pytest.mark.asyncio
async def test_non_recoverable_failure_goes_directly_to_dlq() -> None:
    repository = StubRepository([NonRecoverableWorkerError("bad payload")])
    dlq = RecordingDlq()
    processor = DistributedOrderProcessor(
        repository,
        dlq,
        RetryPolicy(max_attempts=5),
        sleep=no_sleep,
    )

    assert await processor.process(envelope()) == "dead_lettered"
    assert repository.calls == 1
    assert dlq.records[0].error_type == "NonRecoverableWorkerError"
