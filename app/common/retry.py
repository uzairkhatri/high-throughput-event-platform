from collections.abc import Callable
from dataclasses import dataclass
from random import Random


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 0.2
    max_delay_seconds: float = 10.0
    jitter_ratio: float = 0.2

    def delay_for_attempt(
        self,
        attempt: int,
        random_fn: Callable[[], float] | None = None,
    ) -> float:
        if attempt < 1:
            raise ValueError("attempt is one-based")
        random_value = random_fn() if random_fn else Random().random()
        exponential = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (attempt - 1)))
        jitter = exponential * self.jitter_ratio * random_value
        return exponential + jitter

