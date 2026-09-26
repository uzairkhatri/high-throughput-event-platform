from typing import Protocol

from app.common.models import EventEnvelope


class EventPublisher(Protocol):
    async def publish(self, envelope: EventEnvelope, key: str) -> None:
        """Publish an event envelope using the supplied partition key."""


class InMemoryPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, EventEnvelope]] = []

    async def publish(self, envelope: EventEnvelope, key: str) -> None:
        self.published.append((key, envelope))


class KafkaPublisher:
    """Kafka API publisher.

    This class is intentionally small so the architecture boundary is easy to
    inspect. Install the `kafka` extra before using it in a real process.
    """

    def __init__(self, producer: object, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, envelope: EventEnvelope, key: str) -> None:
        payload = envelope.model_dump_json().encode("utf-8")
        key_bytes = key.encode("utf-8")
        await self._producer.send_and_wait(self._topic, payload, key=key_bytes)

