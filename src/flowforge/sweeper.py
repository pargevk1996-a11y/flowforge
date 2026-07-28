"""The sweeper: it wakes parents whose children finished without saying so.

A child reports back by waking its parent and then writing the outcome into the
parent's log. Both steps are best-effort by construction, and one window cannot be
closed by ordering them differently: a child that commits its own result and then
*dies* — before it gets as far as telling anyone — leaves a parent waiting on a run
that finished minutes ago. Nothing is corrupt; nothing is coming either.

So this is the reaper for that case. Every pass it looks at suspended runs, asks
whether any child they are waiting on has already terminated, and re-queues the
ones that have. It never repairs anything: waking the parent is enough, because a
drive reconciles the outcome from the child's own log — the sweeper only has to
restore the nudge, not the news.

**It is a scan, and it is meant to be.** This covers a crash window, not a hot
path: the ordinary case is handled milliseconds after the child finishes, and a
run reaches the sweeper only when that failed. ``batch`` bounds the work per pass
and the cursor rotates through the suspended set across passes, so a large backlog
costs more passes rather than one enormous one. If suspended runs ever outgrow
this, the answer is an index of unresolved child commands, the way ``timers`` is an
index of pending wakeups — not a bigger scan.
"""

from __future__ import annotations

import asyncio
import logging

from flowforge.core.engine import Engine
from flowforge.core.event_store import EventStore
from flowforge.core.supervision import supervise
from flowforge.core.timeline import RunStatus, pending_children, terminal_event
from flowforge.queue.base import TaskQueue

logger = logging.getLogger(__name__)


class ChildSweeper:
    def __init__(
        self,
        engine: Engine,
        store: EventStore,
        queue: TaskQueue,
        *,
        batch: int = 100,
    ) -> None:
        self._engine = engine
        self._store = store
        self._queue = queue
        self._batch = batch
        self._cursor = 0

    async def sweep(self) -> list[str]:
        """Re-queue every stranded parent found this pass; return their ids."""
        page = await self._store.list_runs(
            status=RunStatus.SUSPENDED, limit=self._batch, offset=self._cursor
        )
        # A short page means the end of the suspended set: start over next pass
        # rather than paging into nothing.
        self._cursor = 0 if len(page.runs) < self._batch else self._cursor + self._batch

        woken: list[str] = []
        for run in page.runs:
            if await self._is_stranded(run.run_id):
                logger.info("waking %s: a child it waits on has already finished", run.run_id)
                await self._queue.enqueue(run.run_id, tenant=run.tenant)
                woken.append(run.run_id)
        return woken

    async def _is_stranded(self, run_id: str) -> bool:
        """Is this run waiting on a child that has already terminated?

        Short-circuits on the first finished child: one is enough to justify a
        drive, and the drive re-derives the rest anyway."""
        history = await self._store.load(run_id)
        if not history or terminal_event(history) is not None:
            return False
        for child in pending_children(history):
            if child.child_run_id is None:
                continue
            if await self._engine.child_outcome(child.child_run_id) is not None:
                return True
        return False

    async def run_forever(
        self, *, interval: float = 60.0, stop: asyncio.Event | None = None
    ) -> None:
        """Sweep on a slow loop. Slow on purpose: this competes with nothing, and
        a parent stranded by a crash is not made worse by waiting a minute."""

        async def step() -> None:
            await self.sweep()
            await asyncio.sleep(interval)

        await supervise(step, label="child sweeper", stop=stop)
