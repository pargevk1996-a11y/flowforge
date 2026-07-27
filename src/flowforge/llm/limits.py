"""Per-provider rate limits.

Budgets bound what a tenant may spend; rate limits bound how fast anyone may call
a provider. Both guard the same step, from opposite sides.

The limiter is a token bucket per provider: ``per_second`` tokens refill
continuously up to ``burst``, and :meth:`acquire` waits for capacity rather than
failing — a workflow that is merely early should be paced, not broken. Only when
the wait would exceed ``max_wait`` does it raise :class:`RateLimitedError`, which
is *retryable*, so the activity's retry policy backs off and tries again.

Waiting happens while holding the bucket's lock, which serialises contenders into
arrival order: no caller can be starved by a livelier neighbour. This is a
single-process limiter (the natural scope of one worker's outbound concurrency);
coordinating a fleet against a shared provider quota belongs in Redis, alongside
the queue and lock adapters.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from flowforge.core.errors import RateLimitedError


class RateLimiter(Protocol):
    async def acquire(self, provider: str, *, tokens: float = 1.0) -> None:
        """Block until ``tokens`` may be spent against ``provider``."""
        ...


@dataclass(frozen=True)
class RateLimit:
    """``per_second`` sustained calls, absorbing bursts of ``burst`` at once."""

    per_second: float
    burst: float | None = None

    @property
    def capacity(self) -> float:
        # One second of sustained rate is the default burst, but never less than a
        # single call — a 0.1/s limit must still admit one call, just slowly.
        return self.burst if self.burst is not None else max(1.0, self.per_second)


class _Bucket:
    def __init__(self, limit: RateLimit, now: float) -> None:
        self.limit = limit
        self.tokens = limit.capacity
        self.updated_at = now
        self.lock = asyncio.Lock()

    def refill(self, now: float) -> None:
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.limit.capacity, self.tokens + elapsed * self.limit.per_second)
        self.updated_at = now


class InMemoryRateLimiter:
    """Token-bucket limiter, one bucket per provider.

    ``clock`` and ``sleep`` are injectable so pacing can be tested without
    spending real seconds.
    """

    def __init__(
        self,
        limits: dict[str, RateLimit] | None = None,
        *,
        default: RateLimit | None = None,
        max_wait: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._limits = dict(limits or {})
        self._default = default
        self._max_wait = max_wait
        self._clock = clock
        self._sleep = sleep
        self._buckets: dict[str, _Bucket] = {}

    def limit_for(self, provider: str) -> RateLimit | None:
        return self._limits.get(provider, self._default)

    async def acquire(self, provider: str, *, tokens: float = 1.0) -> None:
        limit = self.limit_for(provider)
        if limit is None:
            return
        if tokens > limit.capacity:
            raise RateLimitedError(
                f"{provider}: a request of {tokens:g} tokens can never fit a "
                f"bucket of {limit.capacity:g}"
            )

        bucket = self._buckets.get(provider)
        if bucket is None:
            bucket = self._buckets[provider] = _Bucket(limit, self._clock())

        async with bucket.lock:
            bucket.refill(self._clock())
            if bucket.tokens < tokens:
                wait = (tokens - bucket.tokens) / limit.per_second
                if wait > self._max_wait:
                    raise RateLimitedError(
                        f"{provider}: rate limited, {wait:.2f}s of wait exceeds the "
                        f"{self._max_wait:g}s ceiling"
                    )
                await self._sleep(wait)
                bucket.refill(self._clock())
            bucket.tokens -= tokens
