"""Tests for per-provider rate limits.

Time is injected, so pacing is asserted exactly rather than waited for: the fake
clock only advances when the limiter sleeps, which is precisely the behaviour
under test.
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from flowforge import RateLimitedError
from flowforge.llm import InMemoryRateLimiter, LLMStep, RateLimit, ScriptedLLMClient


class Fields(BaseModel):
    vendor: str


class _FakeTime:
    """A monotonic clock that only moves when the limiter waits."""

    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


def _limiter(
    limits: dict[str, RateLimit], *, max_wait: float = 30.0
) -> tuple[InMemoryRateLimiter, _FakeTime]:
    time = _FakeTime()
    limiter = InMemoryRateLimiter(
        limits, max_wait=max_wait, clock=time.clock, sleep=time.sleep
    )
    return limiter, time


async def test_burst_is_free_then_calls_are_paced() -> None:
    limiter, time = _limiter({"openai": RateLimit(per_second=2, burst=2)})

    await limiter.acquire("openai")
    await limiter.acquire("openai")
    assert time.waits == []  # the burst is spent without waiting

    await limiter.acquire("openai")
    await limiter.acquire("openai")
    # Refilling at 2/s, each further call costs half a second.
    assert time.waits == [0.5, 0.5]
    assert time.now == pytest.approx(1.0)


async def test_providers_have_independent_buckets() -> None:
    limiter, time = _limiter(
        {"openai": RateLimit(per_second=1, burst=1), "anthropic": RateLimit(per_second=1, burst=1)}
    )

    await limiter.acquire("openai")
    await limiter.acquire("anthropic")  # a different bucket: still free
    assert time.waits == []

    await limiter.acquire("openai")
    assert time.waits == [1.0]


async def test_unlimited_provider_never_waits() -> None:
    limiter, time = _limiter({"openai": RateLimit(per_second=1, burst=1)})

    for _ in range(5):
        await limiter.acquire("gemini")  # no limit configured, no default
    assert time.waits == []


async def test_default_limit_applies_to_unlisted_providers() -> None:
    time = _FakeTime()
    limiter = InMemoryRateLimiter(
        default=RateLimit(per_second=1, burst=1), clock=time.clock, sleep=time.sleep
    )

    await limiter.acquire("whoever")
    await limiter.acquire("whoever")
    assert time.waits == [1.0]


async def test_wait_beyond_the_ceiling_is_a_retryable_error() -> None:
    limiter, _time = _limiter({"slow": RateLimit(per_second=0.1)}, max_wait=5.0)

    await limiter.acquire("slow")  # burst of 1 is free
    with pytest.raises(RateLimitedError):
        await limiter.acquire("slow")  # would need 10s of refill


async def test_request_larger_than_the_bucket_is_rejected_immediately() -> None:
    limiter, time = _limiter({"openai": RateLimit(per_second=1, burst=2)})

    with pytest.raises(RateLimitedError):
        await limiter.acquire("openai", tokens=5)
    assert time.waits == []  # rejected outright, never waited on


async def test_llm_step_paces_its_schema_retries() -> None:
    # The retry loop is the fastest way to hammer a provider, so it is limited too.
    client = ScriptedLLMClient(['{"vendor": 17}', json.dumps({"vendor": "Acme"})])
    limiter, time = _limiter({"openai": RateLimit(per_second=1, burst=1)})
    step = LLMStep(client, "m", Fields, limiter=limiter)

    result = await step.run("extract")

    assert result == Fields(vendor="Acme")
    assert len(client.calls) == 2
    assert time.waits == [1.0]  # the second attempt waited for capacity
