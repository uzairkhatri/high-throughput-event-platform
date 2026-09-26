from dataclasses import dataclass
from threading import Lock
from uuid import UUID


@dataclass(frozen=True)
class IdempotencyResult:
    accepted: bool
    event_id: UUID


class InMemoryIdempotencyStore:
    """Process-local store for tests and local walkthroughs.

    Production deployments should use Redis SET NX with an expiry so API replicas
    share the same idempotency boundary.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._seen: dict[str, UUID] = {}

    def reserve(self, key: str, event_id: UUID) -> IdempotencyResult:
        with self._lock:
            existing = self._seen.get(key)
            if existing is not None:
                return IdempotencyResult(accepted=False, event_id=existing)
            self._seen[key] = event_id
            return IdempotencyResult(accepted=True, event_id=event_id)

