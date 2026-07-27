"""Integration test: durable timers on Postgres, woken by the timer wheel.

Runs only with ``asyncpg`` installed and ``DATABASE_URL`` set; otherwise skipped.
Proves a suspended run's timer survives in the database and the wheel fires it.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from flowforge import Engine, Registry, RunStatus, TimerWheel, WorkflowContext
from flowforge.persistence import (
    PostgresEventStore,
    PostgresTimerStore,
    apply_migrations,
)

pytest.importorskip("asyncpg")
DSN = os.environ.get("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="DATABASE_URL not set")


class Money(BaseModel):
    amount: int


class Receipt(BaseModel):
    txn: str


async def test_durable_timer_fired_by_wheel() -> None:
    charged: list[int] = []

    async def charge(amount: int) -> str:
        charged.append(amount)
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        await ctx.sleep(3600)
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    assert DSN is not None
    await apply_migrations(DSN)
    store = await PostgresEventStore.connect(DSN)
    timers = await PostgresTimerStore.connect(DSN)
    try:
        now = {"t": datetime(2030, 1, 1, tzinfo=UTC)}
        clock = lambda: now["t"]  # noqa: E731

        run_id = uuid.uuid4().hex
        reg = Registry()
        engine = Engine(store, reg, clock=clock, timers=timers)
        wheel = TimerWheel(engine, timers, clock=clock)
        definition = reg.add(wf)

        res = await engine.start(run_id, definition, Money(amount=9))
        assert res.status is RunStatus.SUSPENDED
        assert charged == []

        # Not due yet: re-driving keeps it suspended.
        assert (await engine.drive(run_id)).status is RunStatus.SUSPENDED

        # The timer row lives in Postgres; advance time and let the wheel fire it.
        now["t"] += timedelta(seconds=3601)
        await wheel.tick()

        res = await engine.drive(run_id)
        assert res.status is RunStatus.COMPLETED
        assert charged == [9]
    finally:
        await store.close()
        await timers.close()
