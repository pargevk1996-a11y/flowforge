"""Cron triggers: a schedule, durable state, and a scheduler loop.

:class:`CronSchedule` parses the ordinary five-field expression
(``minute hour day-of-month month day-of-week``) with ``*``, ranges, lists and
steps, plus the usual ``@daily``-style aliases. As in Vixie cron, when *both*
day-of-month and day-of-week are restricted a day matches if **either** does.

The scheduler is durable in the way the rest of the engine is: it remembers the
last tick it fired, so a process that was down for an hour catches up on the ticks
it missed instead of silently skipping them, and each tick is dispatched under a
dedupe key derived from its own timestamp — so catching up twice, or two
schedulers catching up at once, still produces exactly one run per tick.

All arithmetic is UTC, which is what makes "add a day" mean 24 hours here.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Protocol

from flowforge.core.events import utcnow
from flowforge.triggers.base import Trigger, TriggerKind, TriggerRegistry
from flowforge.triggers.dispatch import Delivery, TriggerDispatcher
from flowforge.workflow.context import Clock

_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

_SEARCH_LIMIT = timedelta(days=366 * 4)
"""How far ahead ``next_after`` will look before declaring an expression
unsatisfiable — four years covers every leap-day-only schedule."""


def _parse_field(spec: str, low: int, high: int, *, field: str) -> frozenset[int]:
    values: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, _, raw_step = part.partition("/")
            if not raw_step.isdigit() or int(raw_step) < 1:
                raise ValueError(f"cron {field}: bad step in {spec!r}")
            step = int(raw_step)
        if part in ("*", ""):
            start, end = low, high
        elif "-" in part:
            raw_start, _, raw_end = part.partition("-")
            start, end = _int(raw_start, field, spec), _int(raw_end, field, spec)
        else:
            start = end = _int(part, field, spec)
            end = high if step > 1 else end  # "5/15" means "from 5, every 15"
        if not (low <= start <= high and low <= end <= high and start <= end):
            raise ValueError(f"cron {field}: {part!r} out of range {low}-{high}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


def _int(raw: str, field: str, spec: str) -> int:
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"cron {field}: {spec!r} is not a number") from None


class CronSchedule:
    """A parsed cron expression. Immutable, and cheap to ask repeatedly."""

    def __init__(self, expression: str) -> None:
        expr = _ALIASES.get(expression.strip().lower(), expression).strip()
        fields = expr.split()
        if len(fields) != 5:
            raise ValueError(
                f"cron expression must have 5 fields (or be an alias), got {expression!r}"
            )
        minute, hour, dom, month, dow = fields
        self.expression = expression
        self.minutes = _parse_field(minute, 0, 59, field="minute")
        self.hours = _parse_field(hour, 0, 23, field="hour")
        self.days = _parse_field(dom, 1, 31, field="day-of-month")
        self.months = _parse_field(month, 1, 12, field="month")
        # 7 is Sunday too, so normalise it onto 0 before comparing.
        self.weekdays = frozenset(d % 7 for d in _parse_field(dow, 0, 7, field="day-of-week"))
        self._dom_restricted = dom.strip() != "*"
        self._dow_restricted = dow.strip() != "*"

    def __repr__(self) -> str:
        return f"CronSchedule({self.expression!r})"

    def _day_matches(self, when: datetime) -> bool:
        if when.month not in self.months:
            return False
        by_dom = when.day in self.days
        by_dow = (when.weekday() + 1) % 7 in self.weekdays  # cron: 0 == Sunday
        if self._dom_restricted and self._dow_restricted:
            return by_dom or by_dow
        return by_dom and by_dow

    def matches(self, when: datetime) -> bool:
        return (
            when.minute in self.minutes
            and when.hour in self.hours
            and self._day_matches(when)
        )

    def next_after(self, after: datetime) -> datetime:
        """The first matching minute strictly after ``after``.

        Skips whole days and hours that cannot match rather than stepping minute
        by minute, so a yearly schedule costs a few hundred comparisons, not half
        a million."""
        when = (after + timedelta(minutes=1)).replace(second=0, microsecond=0)
        limit = when + _SEARCH_LIMIT
        while when <= limit:
            if not self._day_matches(when):
                when = (when + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if when.hour not in self.hours:
                when = (when + timedelta(hours=1)).replace(minute=0)
                continue
            if when.minute not in self.minutes:
                when += timedelta(minutes=1)
                continue
            return when
        raise ValueError(f"cron expression never matches: {self.expression!r}")


class CronStateStore(Protocol):
    async def last_fired(self, trigger: str) -> datetime | None:
        """The last tick dispatched for this trigger, or ``None`` if it has never
        been armed."""
        ...

    async def set_last_fired(self, trigger: str, at: datetime) -> None:
        ...


class InMemoryCronStateStore:
    def __init__(self) -> None:
        self._state: dict[str, datetime] = {}

    async def last_fired(self, trigger: str) -> datetime | None:
        return self._state.get(trigger)

    async def set_last_fired(self, trigger: str, at: datetime) -> None:
        self._state[trigger] = at


class CronScheduler:
    """Fires cron triggers whose ticks have come due.

    ``catchup`` bounds how many missed ticks one pass will replay: after a long
    outage a minutely schedule owes thousands of runs, and flooding the queue with
    them is worse than draining them over the next few passes.
    """

    def __init__(
        self,
        dispatcher: TriggerDispatcher,
        triggers: TriggerRegistry,
        state: CronStateStore | None = None,
        *,
        clock: Clock = utcnow,
        catchup: int = 10,
    ) -> None:
        self._dispatcher = dispatcher
        self._triggers = triggers
        self._state = state or InMemoryCronStateStore()
        self._clock = clock
        self._catchup = catchup
        self._schedules: dict[str, CronSchedule] = {}

    def schedule_for(self, trigger: Trigger) -> CronSchedule:
        cached = self._schedules.get(trigger.name)
        if cached is None:
            if trigger.schedule is None:
                raise ValueError(f"cron trigger {trigger.name!r} has no schedule")
            cached = self._schedules[trigger.name] = CronSchedule(trigger.schedule)
        return cached

    async def tick(self) -> list[Delivery]:
        """Dispatch every due tick of every cron trigger; return the deliveries."""
        now = self._clock()
        delivered: list[Delivery] = []
        for trigger in self._triggers.of_kind(TriggerKind.CRON):
            delivered.extend(await self._advance(trigger, now))
        return delivered

    async def _advance(self, trigger: Trigger, now: datetime) -> list[Delivery]:
        schedule = self.schedule_for(trigger)
        cursor = await self._state.last_fired(trigger.name)
        if cursor is None:
            # First sight of this trigger: arm it from now. A schedule that has
            # existed for five minutes does not owe anyone the last five years.
            await self._state.set_last_fired(trigger.name, now)
            return []

        delivered: list[Delivery] = []
        for _ in range(self._catchup):
            due = schedule.next_after(cursor)
            if due > now:
                break
            # The tick's own timestamp is its identity: replaying the catch-up,
            # or running two schedulers, still yields one run per tick.
            delivered.append(
                await self._dispatcher.fire(
                    trigger.name,
                    {"trigger": trigger.name, "fire_at": due.isoformat()},
                    key=due.isoformat(),
                )
            )
            cursor = due
        await self._state.set_last_fired(trigger.name, cursor)
        return delivered

    async def run_forever(
        self, *, interval: float = 1.0, stop: asyncio.Event | None = None
    ) -> None:
        while stop is None or not stop.is_set():
            await self.tick()
            await asyncio.sleep(interval)
