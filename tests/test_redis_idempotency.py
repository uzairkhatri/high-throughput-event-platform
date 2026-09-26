from uuid import uuid4

import pytest

from app.common.idempotency import RedisIdempotencyStore


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str, **options: object) -> bool | None:
        if options.get("nx") and key in self.values:
            return None
        self.values[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def eval(self, script: str, key_count: int, key: str, owner: str) -> int:
        if self.values.get(key) == owner:
            del self.values[key]
            return 1
        return 0


@pytest.mark.asyncio
async def test_redis_reservation_returns_original_event_id() -> None:
    redis = FakeRedis()
    store = RedisIdempotencyStore(redis, ttl_seconds=60)
    first_id = uuid4()

    first = await store.reserve("tenant-a:request-1", first_id)
    duplicate = await store.reserve("tenant-a:request-1", uuid4())

    assert first.accepted is True
    assert duplicate.accepted is False
    assert duplicate.event_id == first_id


@pytest.mark.asyncio
async def test_redis_release_requires_matching_owner() -> None:
    redis = FakeRedis()
    store = RedisIdempotencyStore(redis, ttl_seconds=60)
    event_id = uuid4()
    await store.reserve("tenant-a:request-1", event_id)

    await store.release("tenant-a:request-1", uuid4())
    assert (await store.reserve("tenant-a:request-1", uuid4())).accepted is False

    await store.release("tenant-a:request-1", event_id)
    assert (await store.reserve("tenant-a:request-1", uuid4())).accepted is True

