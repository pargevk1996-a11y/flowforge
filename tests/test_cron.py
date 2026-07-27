"""Tests for cron triggers: the expression parser and the scheduler loop.

The scheduler's clock is injected, so "an hour passed" is asserted rather than
waited for. The properties that matter: a schedule fires once per tick, a
scheduler that was down catches up on the ticks it missed, and catching up twice
(or from two schedulers) still starts one run per tick.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from flowforge import InMemoryEventStore, Registry, WorkflowContext
from flowforge.api import build_control_plane
from flowforge.api.controlplane import ControlPlane
from flowforge.triggers import CronSchedule, CronScheduler, cron_trigger


def _at(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


class Tick(BaseModel):
    fire_at: str = ""


class Done(BaseModel):
    fire_at: str


# -- the expression parser --------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "after", "expected"),
    [
        ("* * * * *", _at(2026, 3, 1, 10, 0), _at(2026, 3, 1, 10, 1)),
        ("0 * * * *", _at(2026, 3, 1, 10, 30), _at(2026, 3, 1, 11, 0)),
        ("*/15 * * * *", _at(2026, 3, 1, 10, 3), _at(2026, 3, 1, 10, 15)),
        ("30 2 * * *", _at(2026, 3, 1, 10, 0), _at(2026, 3, 2, 2, 30)),
        ("0 9 1 * *", _at(2026, 3, 1, 10, 0), _at(2026, 4, 1, 9, 0)),
        # Day-of-week 1 is Monday; 2026-03-01 is a Sunday.
        ("0 0 * * 1", _at(2026, 3, 1, 10, 0), _at(2026, 3, 2, 0, 0)),
        # Sunday is both 0 and 7.
        ("0 0 * * 7", _at(2026, 3, 2, 0, 0), _at(2026, 3, 8, 0, 0)),
        ("0 0 29 2 *", _at(2026, 3, 1), _at(2028, 2, 29, 0, 0)),  # leap day
        ("15,45 * * * *", _at(2026, 3, 1, 10, 20), _at(2026, 3, 1, 10, 45)),
        ("0 9-17 * * *", _at(2026, 3, 1, 20, 0), _at(2026, 3, 2, 9, 0)),
        ("@daily", _at(2026, 3, 1, 10, 0), _at(2026, 3, 2, 0, 0)),
        ("@hourly", _at(2026, 3, 1, 10, 30), _at(2026, 3, 1, 11, 0)),
        ("@monthly", _at(2026, 3, 15), _at(2026, 4, 1, 0, 0)),
    ],
)
def test_next_after(expression: str, after: datetime, expected: datetime) -> None:
    assert CronSchedule(expression).next_after(after) == expected


def test_next_after_is_strict() -> None:
    # A schedule that matches *now* still returns the following occurrence, which
    # is what stops the scheduler re-firing the tick it just fired.
    schedule = CronSchedule("0 * * * *")
    assert schedule.next_after(_at(2026, 3, 1, 10, 0)) == _at(2026, 3, 1, 11, 0)


def test_day_of_month_and_weekday_are_ored() -> None:
    # Vixie cron: with both restricted, the 3rd *or* any Monday matches.
    schedule = CronSchedule("0 0 3 * 1")
    assert schedule.matches(_at(2026, 3, 3, 0, 0))  # the 3rd, a Tuesday
    assert schedule.matches(_at(2026, 3, 2, 0, 0))  # a Monday, not the 3rd
    assert not schedule.matches(_at(2026, 3, 4, 0, 0))


def test_step_from_a_start_value() -> None:
    assert CronSchedule("5/20 * * * *").minutes == frozenset({5, 25, 45})


@pytest.mark.parametrize(
    "expression",
    ["* * * *", "60 * * * *", "* 24 * * *", "0 0 0 * *", "x * * * *", "*/0 * * * *"],
)
def test_invalid_expressions_are_rejected(expression: str) -> None:
    with pytest.raises(ValueError):
        CronSchedule(expression)


# -- the scheduler ----------------------------------------------------------


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


def _plane(clock: _Clock, *, schedule: str = "0 * * * *", catchup: int = 10) -> tuple[
    ControlPlane, CronScheduler, list[str]
]:
    fired: list[str] = []

    async def handle(fire_at: str) -> Done:
        fired.append(fire_at)
        return Done(fire_at=fire_at)

    async def wf(ctx: WorkflowContext, inp: Tick) -> Done:
        return await ctx.activity(handle, inp.fire_at, name="handle")

    registry = Registry()
    registry.add(wf, name="sweep")
    cp = build_control_plane(InMemoryEventStore(), registry)
    cp.triggers.add(
        cron_trigger(
            "hourly_sweep",
            "sweep",
            schedule,
            map=lambda event: Tick(fire_at=str(event["fire_at"])),
        )
    )
    scheduler = CronScheduler(
        cp.dispatcher, cp.triggers, clock=clock, catchup=catchup
    )
    return cp, scheduler, fired


async def _drain(cp: ControlPlane) -> None:
    while await cp.worker.run_once() is not None:
        pass


async def test_a_new_schedule_is_armed_not_backfilled() -> None:
    clock = _Clock(_at(2026, 3, 1, 10, 30))
    cp, scheduler, fired = _plane(clock)

    assert await scheduler.tick() == []  # first sight: arm, do not fire history

    clock.advance(timedelta(minutes=31))  # 11:01, so 11:00 is now due
    deliveries = await scheduler.tick()
    await _drain(cp)

    assert [d.started for d in deliveries] == [True]
    assert fired == [_at(2026, 3, 1, 11, 0).isoformat()]


async def test_each_tick_fires_once() -> None:
    clock = _Clock(_at(2026, 3, 1, 10, 30))
    cp, scheduler, fired = _plane(clock)
    await scheduler.tick()  # arm

    clock.advance(timedelta(minutes=31))
    await scheduler.tick()
    await scheduler.tick()  # the loop runs every second; nothing new is due
    await scheduler.tick()
    await _drain(cp)

    assert fired == [_at(2026, 3, 1, 11, 0).isoformat()]


async def test_a_scheduler_that_was_down_catches_up() -> None:
    clock = _Clock(_at(2026, 3, 1, 10, 30))
    cp, scheduler, fired = _plane(clock)
    await scheduler.tick()  # arm

    clock.advance(timedelta(hours=4))  # the process was gone for four hours
    await scheduler.tick()
    await _drain(cp)

    assert fired == [
        _at(2026, 3, 1, hour, 0).isoformat() for hour in (11, 12, 13, 14)
    ]


async def test_catchup_is_bounded_and_resumes_on_later_passes() -> None:
    clock = _Clock(_at(2026, 3, 1, 10, 30))
    cp, scheduler, fired = _plane(clock, catchup=2)
    await scheduler.tick()  # arm

    clock.advance(timedelta(hours=4))
    await scheduler.tick()
    await _drain(cp)
    assert len(fired) == 2  # the backlog is drained, not dumped

    await scheduler.tick()
    await _drain(cp)
    assert len(fired) == 4


async def test_two_schedulers_fire_each_tick_once() -> None:
    """Ticks are deduped by their own timestamp, so a redundant scheduler (or a
    replayed catch-up) cannot double-start a run."""
    clock = _Clock(_at(2026, 3, 1, 10, 30))
    cp, first, fired = _plane(clock)
    second = CronScheduler(cp.dispatcher, cp.triggers, clock=clock)

    await first.tick()
    await second.tick()  # separate cursor: this one arms itself independently
    clock.advance(timedelta(hours=2))

    deliveries = [*await first.tick(), *await second.tick()]
    await _drain(cp)

    started = [d for d in deliveries if d.started]
    assert len(started) == 2  # 11:00 and 12:00
    assert len({d.run_id for d in deliveries}) == 2  # the duplicates point at them
    assert fired == [_at(2026, 3, 1, hour, 0).isoformat() for hour in (11, 12)]


async def test_a_cron_trigger_without_a_schedule_is_rejected() -> None:
    clock = _Clock(_at(2026, 3, 1, 10, 30))
    cp, scheduler, _fired = _plane(clock)
    from flowforge.triggers import Trigger, TriggerKind

    cp.triggers.add(Trigger(name="broken", workflow="sweep", kind=TriggerKind.CRON))
    with pytest.raises(ValueError, match="no schedule"):
        await scheduler.tick()
