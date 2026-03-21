"""
Token bucket rate limiter for API calls.
Prevents hitting rate limits on Google Sheets (60 req/min), Perplexity, etc.
"""
import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class TokenBucket:
    """
    Token bucket rate limiter.

    capacity: max tokens (burst size)
    refill_rate: tokens added per second
    """

    capacity: float
    refill_rate: float
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _lock: asyncio.Lock = field(init=False)

    def __post_init__(self):
        self._tokens = self.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until `tokens` are available."""
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self.capacity,
                    self._tokens + elapsed * self.refill_rate,
                )
                self._last_refill = now

                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return

                # Calculate wait time and release lock while waiting
                wait = (tokens - self._tokens) / self.refill_rate
                await asyncio.sleep(wait)


# Shared rate limiters
sheets_limiter = TokenBucket(capacity=50, refill_rate=0.9)   # ~54 req/min (under 60 limit)
linkedin_limiter = TokenBucket(capacity=1, refill_rate=0.2)   # 1 request per 5 seconds
search_limiter = TokenBucket(capacity=5, refill_rate=1.0)     # 5 burst, 1/sec sustained
zerobounce_limiter = TokenBucket(capacity=10, refill_rate=0.5)
