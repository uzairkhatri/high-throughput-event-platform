from dataclasses import dataclass
from threading import Lock
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class IdempotencyResult:
    accepted: bool
    event_id: UUID


class IdempotencyStore(Protocol):
    async def reserve(self, key: str, event_id: UUID) -> IdempotencyResult:
        """Reserve a key or return the event ID that already owns it."""

    async def release(self, key: str, event_id: UUID) -> None:
        """Release a reservation only when it is still owned by event_id."""


class InMemoryIdempotencyStore:
    """Process-local implementation for tests and lightweight development."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._seen: dict[str, UUID] = {}

    async def reserve(self, key: str, event_id: UUID) -> IdempotencyResult:
        with self._lock:
            existing = self._seen.get(key)
            if existing is not None:
                return IdempotencyResult(accepted=False, event_id=existing)
            self._seen[key] = event_id
            return IdempotencyResult(accepted=True, event_id=event_id)

    async def release(self, key: str, event_id: UUID) -> None:
        with self._lock:
            if self._seen.get(key) == event_id:
                del self._seen[key]


class RedisIdempotencyStore:
    _RELEASE_IF_OWNER = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
      return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    def __init__(self, client: object, ttl_seconds: int, prefix: str = "event-idem") -> None:
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def reserve(self, key: str, event_id: UUID) -> IdempotencyResult:
        redis_key = self._key(key)
        event_id_text = str(event_id)
        accepted = await self._client.set(
            redis_key,
            event_id_text,
            ex=self._ttl_seconds,
            nx=True,
        )
        if accepted:
            return IdempotencyResult(accepted=True, event_id=event_id)

        existing = await self._client.get(redis_key)
        if existing is None:
            return await self.reserve(key, event_id)
        if isinstance(existing, bytes):
            existing = existing.decode("utf-8")
        return IdempotencyResult(accepted=False, event_id=UUID(existing))

    async def release(self, key: str, event_id: UUID) -> None:
        await self._client.eval(
            self._RELEASE_IF_OWNER,
            1,
            self._key(key),
            str(event_id),
        )

