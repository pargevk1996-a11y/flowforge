"""Timer wheel: a suspended run wakes itself when its timer comes due, with no
manual intervention — the wheel fires it and a worker drives it to completion."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from flowforge import (
    Engine,
    InMemoryEventStore,
    InMemoryLockManager,
    InMemoryTaskQueue,
    InMemoryTimerStore,
    Registry,
    RunStatus,
    TimerWheel,
    Worker,
    WorkflowContext,
    submit,
)


class Money(BaseModel):
    amount: int


class Receipt(BaseModel):
    txn: str


async def test_wheel_wakes_suspended_run_end_to_end() -> None:
    charged: list[int] = []

    async def charge(amount: int) -> str:
        charged.append(amount)
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        await ctx.sleep(3600)
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    now = {"t": datetime(2026, 1, 1, tzinfo=UTC)}
    clock = lambda: now["t"]  # noqa: E731

    store = InMemoryEventStore()
    timers = InMemoryTimerStore()
    queue = InMemoryTaskQueue()
    reg = Registry()
    engine = Engine(store, reg, clock=clock, timers=timers)
    worker = Worker(engine, queue, InMemoryLockManager())
    wheel = TimerWheel(engine, timers, queue, clock=clock)
    definition = reg.add(wf)

    await submit(engine, queue, "r1", definition, Money(amount=5))

    # Worker drives the run until it suspends on the timer.
    res = await worker.run_once()
    assert res is not None and res.status is RunStatus.SUSPENDED
    assert charged == []

    # Not due yet: the wheel fires nothing and the queue stays empty.
    assert await wheel.tick() == 0
    assert await queue.size() == 0

    # Time passes; the wheel fires the timer and re-enqueues the run.
    now["t"] += timedelta(seconds=3601)
    assert await wheel.tick() == 1
    assert await queue.size() == 1

    # A worker picks the woken run up and finishes it.
    res = await worker.run_once()
    assert res is not None and res.status is RunStatus.COMPLETED
    assert charged == [5]


async def test_wheel_tick_is_idempotent() -> None:
    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        await ctx.sleep(10)
        return Receipt(txn="done")

    now = {"t": datetime(2026, 1, 1, tzinfo=UTC)}
    clock = lambda: now["t"]  # noqa: E731

    timers = InMemoryTimerStore()
    reg = Registry()
    engine = Engine(InMemoryEventStore(), reg, clock=clock, timers=timers)
    wheel = TimerWheel(engine, timers, clock=clock)
    definition = reg.add(wf)

    await engine.start("r1", definition, Money(amount=1))  # suspends, schedules timer

    now["t"] += timedelta(seconds=11)
    assert await wheel.tick() == 1  # fires once
    assert await wheel.tick() == 0  # already fired: nothing to do
