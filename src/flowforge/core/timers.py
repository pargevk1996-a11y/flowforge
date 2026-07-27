"""Durable timers: the index that lets suspended runs wake themselves.

When a workflow sleeps, the run is suspended and a timer is recorded here. A timer
wheel scans for due timers, appends the ``TIMER_FIRED`` event, and re-enqueues the
run. The engine depends only on the :class:`TimerStore` protocol; an in-memory
implementation powers tests and a Postgres one runs in production.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class DueTimer:
    run_id: str
    command_seq: int
    fire_at: datetime


class TimerStore(Protocol):
    async def schedule(self, run_id: str, command_seq: int, fire_at: datetime) -> None:
        """Record a timer. Scheduling the same (run, command) twice is a no-op, so
        a replay after a crash cannot create a duplicate."""
        ...

    async def due(self, now: datetime, *, limit: int = 100) -> list[DueTimer]:
        """Return unfired timers whose ``fire_at`` has passed, oldest first."""
        ...

    async def mark_fired(self, run_id: str, command_seq: int) -> None:
        ...


class InMemoryTimerStore:
    def __init__(self) -> None:
        self._timers: dict[tuple[str, int], tuple[datetime, bool]] = {}

    async def schedule(self, run_id: str, command_seq: int, fire_at: datetime) -> None:
        self._timers.setdefault((run_id, command_seq), (fire_at, False))

    async def due(self, now: datetime, *, limit: int = 100) -> list[DueTimer]:
        due = [
            DueTimer(run_id=run_id, command_seq=cs, fire_at=fire_at)
            for (run_id, cs), (fire_at, fired) in self._timers.items()
            if not fired and fire_at <= now
        ]
        due.sort(key=lambda t: t.fire_at)
        return due[:limit]

    async def mark_fired(self, run_id: str, command_seq: int) -> None:
        key = (run_id, command_seq)
        existing = self._timers.get(key)
        if existing is not None:
            self._timers[key] = (existing[0], True)
