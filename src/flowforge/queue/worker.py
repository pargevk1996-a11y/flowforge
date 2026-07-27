"""The durable worker loop.

A worker dequeues a run, takes its lock so it is the single writer, drives the
engine one step, and releases. A run that suspends (timer/signal) simply returns
from ``drive``; the timer wheel or a delivered signal re-enqueues it later. If the
lock is already held, the item is requeued and left for another worker.
"""

from __future__ import annotations

import asyncio
from typing import Any

from flowforge.core.engine import Engine, RunResult
from flowforge.queue.base import LockManager, TaskQueue
from flowforge.workflow.definition import WorkflowDef


async def submit(
    engine: Engine,
    queue: TaskQueue,
    run_id: str,
    workflow: str | WorkflowDef[Any, Any],
    workflow_input: Any,
    *,
    priority: int = 0,
    tenant: str = "default",
) -> None:
    """Seed a run and enqueue it for a worker to drive."""
    await engine.create_run(run_id, workflow, workflow_input, tenant=tenant)
    await queue.enqueue(run_id, priority=priority, tenant=tenant)


class Worker:
    def __init__(
        self,
        engine: Engine,
        queue: TaskQueue,
        locks: LockManager,
        *,
        lock_ttl: float = 30.0,
    ) -> None:
        self._engine = engine
        self._queue = queue
        self._locks = locks
        self._lock_ttl = lock_ttl

    async def run_once(self) -> RunResult | None:
        """Process one item. Returns its result, or ``None`` if the queue was
        empty or the run was locked by another worker (and thus requeued)."""
        item = await self._queue.dequeue()
        if item is None:
            return None

        token = await self._locks.acquire(item.run_id, ttl=self._lock_ttl)
        if token is None:
            await self._queue.enqueue(
                item.run_id, priority=item.priority, tenant=item.tenant
            )
            return None

        try:
            return await self._engine.drive(item.run_id)
        finally:
            await self._locks.release(item.run_id, token)

    async def run_forever(
        self, *, poll_interval: float = 0.05, stop: asyncio.Event | None = None
    ) -> None:
        while stop is None or not stop.is_set():
            if await self.run_once() is None:
                await asyncio.sleep(poll_interval)
