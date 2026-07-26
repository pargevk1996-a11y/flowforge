"""Tests for the priority queue, the TTL distributed lock, and the worker loop."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from flowforge import (
    Engine,
    InMemoryEventStore,
    InMemoryLockManager,
    InMemoryTaskQueue,
    Registry,
    RunStatus,
    Worker,
    WorkflowContext,
    submit,
)


class Job(BaseModel):
    n: int


class Done(BaseModel):
    n: int


# --------------------------------------------------------------------------
# Priority queue: highest priority first, FIFO within a priority.
# --------------------------------------------------------------------------
async def test_priority_ordering_with_fifo_ties() -> None:
    q = InMemoryTaskQueue()
    await q.enqueue("low", priority=0)
    await q.enqueue("high-1", priority=10)
    await q.enqueue("mid", priority=5)
    await q.enqueue("high-2", priority=10)

    order = []
    while (item := await q.dequeue()) is not None:
        order.append(item.run_id)
    assert order == ["high-1", "high-2", "mid", "low"]


# --------------------------------------------------------------------------
# Distributed lock: single holder, token-scoped release, TTL expiry.
# --------------------------------------------------------------------------
async def test_lock_is_single_holder() -> None:
    locks = InMemoryLockManager()
    token = await locks.acquire("run-1")
    assert token is not None
    assert await locks.acquire("run-1") is None  # already held

    assert await locks.release("run-1", "wrong-token") is False  # not the owner
    assert await locks.release("run-1", token) is True
    assert await locks.acquire("run-1") is not None  # free again


async def test_lock_expires_after_ttl() -> None:
    now = {"t": 1000.0}
    locks = InMemoryLockManager(clock=lambda: now["t"])
    assert await locks.acquire("run-1", ttl=30) is not None
    assert await locks.acquire("run-1", ttl=30) is None

    now["t"] += 31  # lease elapsed
    assert await locks.acquire("run-1", ttl=30) is not None


# --------------------------------------------------------------------------
# Worker loop: submit enqueues, a worker drives to completion.
# --------------------------------------------------------------------------
async def _make() -> tuple[Engine, InMemoryTaskQueue, Worker]:
    async def double(n: int) -> int:
        return n * 2

    async def wf(ctx: WorkflowContext, job: Job) -> Done:
        return Done(n=await ctx.activity(double, job.n))

    store = InMemoryEventStore()
    reg = Registry()
    reg.add(wf, name="doubler")
    engine = Engine(store, reg)
    queue = InMemoryTaskQueue()
    worker = Worker(engine, queue, InMemoryLockManager())
    return engine, queue, worker


async def test_worker_processes_submitted_run() -> None:
    engine, queue, worker = await _make()
    await submit(engine, queue, "r1", "doubler", Job(n=21))

    res = await worker.run_once()
    assert res is not None and res.status is RunStatus.COMPLETED
    assert res.result == Done(n=42)

    assert await worker.run_once() is None  # queue drained


async def test_worker_requeues_when_run_is_locked() -> None:
    engine, queue, worker = await _make()
    locks = InMemoryLockManager()
    worker = Worker(engine, queue, locks)

    await submit(engine, queue, "r1", "doubler", Job(n=1))
    # Someone else holds the run's lock.
    assert await locks.acquire("r1") is not None

    assert await worker.run_once() is None  # could not proceed
    assert await queue.size() == 1  # but the item was put back


async def test_worker_drains_by_priority() -> None:
    engine, queue, worker = await _make()
    await submit(engine, queue, "low", "doubler", Job(n=1), priority=0)
    await submit(engine, queue, "high", "doubler", Job(n=2), priority=10)

    first = await worker.run_once()
    assert first is not None and first.result == Done(n=4)  # high priority ran first


# --------------------------------------------------------------------------
# Redis adapters implement the same protocols (runs only where redis is present).
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_adapters_roundtrip() -> None:
    import os

    pytest.importorskip("redis")
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("REDIS_URL not set")

    from flowforge.queue.redis import RedisLockManager, RedisTaskQueue

    q = RedisTaskQueue.from_url(url, key="flowforge:test:queue")
    await q.enqueue("a", priority=1)
    await q.enqueue("b", priority=5)
    item = await q.dequeue()
    assert item is not None and item.run_id == "b"

    locks = RedisLockManager.from_url(url, prefix="flowforge:test:lock:")
    token = await locks.acquire("x")
    assert token is not None
    assert await locks.acquire("x") is None
    assert await locks.release("x", token) is True
