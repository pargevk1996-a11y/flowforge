"""The timer wheel: it wakes suspended runs when their timers come due.

Each tick scans the timer store for due timers, appends ``TIMER_FIRED`` to each
run, marks the timer fired, and (if a queue is wired) enqueues the run for a
worker to drive. Marking fired after delivery — which is itself idempotent — means
a crash mid-tick re-delivers harmlessly rather than dropping a wakeup.
"""

from __future__ import annotations

import asyncio

from flowforge.core.engine import Engine
from flowforge.core.events import utcnow
from flowforge.core.supervision import supervise
from flowforge.core.timers import TimerStore
from flowforge.queue.base import TaskQueue
from flowforge.workflow.context import Clock


class TimerWheel:
    def __init__(
        self,
        engine: Engine,
        timers: TimerStore,
        queue: TaskQueue | None = None,
        *,
        clock: Clock = utcnow,
        batch: int = 100,
    ) -> None:
        self._engine = engine
        self._timers = timers
        self._queue = queue
        self._clock = clock
        self._batch = batch

    async def tick(self) -> int:
        """Fire all currently-due timers; return how many fired."""
        now = self._clock()
        fired = 0
        for timer in await self._timers.due(now, limit=self._batch):
            await self._engine.deliver_timer(timer.run_id, timer.command_seq)
            await self._timers.mark_fired(timer.run_id, timer.command_seq)
            if self._queue is not None:
                await self._queue.enqueue(timer.run_id)
            fired += 1
        return fired

    async def run_forever(
        self, *, interval: float = 1.0, stop: asyncio.Event | None = None
    ) -> None:
        async def step() -> None:
            await self.tick()
            await asyncio.sleep(interval)

        await supervise(step, label="timer wheel", stop=stop)
