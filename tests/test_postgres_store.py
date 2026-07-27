"""Integration tests for the Postgres event store against a real database.

Runs only when ``asyncpg`` is installed and ``DATABASE_URL`` points at a reachable
Postgres; otherwise skipped. These prove durability for real: resume across a
fresh engine, exactly-once side effects, durable suspend, and optimistic
concurrency enforced by the database.
"""

from __future__ import annotations

import os
import uuid

import pytest
from pydantic import BaseModel

from flowforge import ConcurrencyError, Engine, Registry, RunStatus, WorkflowContext
from flowforge.core.events import Event, EventType
from flowforge.persistence import PostgresEventStore, apply_migrations

pytest.importorskip("asyncpg")
DSN = os.environ.get("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="DATABASE_URL not set")


class Money(BaseModel):
    amount: int


class Receipt(BaseModel):
    txn: str


async def _store() -> PostgresEventStore:
    assert DSN is not None
    await apply_migrations(DSN)
    return await PostgresEventStore.connect(DSN)


async def test_roundtrip_and_resume_across_engines() -> None:
    calls: list[int] = []

    async def charge(amount: int) -> str:
        calls.append(amount)
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    store = await _store()
    try:
        run_id = uuid.uuid4().hex
        reg = Registry()
        definition = reg.add(wf)

        res = await Engine(store, reg).start(run_id, definition, Money(amount=100))
        assert res.status is RunStatus.COMPLETED
        assert calls == [100]

        # A brand-new engine over the same durable store resumes from the log
        # and does not re-run the committed side effect.
        again = await Engine(store, reg).drive(run_id)
        assert again.status is RunStatus.COMPLETED
        assert calls == [100]
    finally:
        await store.close()


async def test_suspend_is_durable_and_resumes() -> None:
    charged: list[int] = []

    async def charge(amount: int) -> str:
        charged.append(amount)
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        await ctx.sleep(3600)
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    store = await _store()
    try:
        run_id = uuid.uuid4().hex
        reg = Registry()
        definition = reg.add(wf)

        res = await Engine(store, reg).start(run_id, definition, Money(amount=7))
        assert res.status is RunStatus.SUSPENDED
        assert charged == []

        # The suspended state lives in Postgres; a fresh engine wakes it.
        res = await Engine(store, reg).fire_timer(run_id)
        assert res.status is RunStatus.COMPLETED
        assert charged == [7]
    finally:
        await store.close()


async def test_optimistic_concurrency_rejects_stale_writer() -> None:
    store = await _store()
    try:
        run_id = uuid.uuid4().hex
        started = Event(
            seq=0, type=EventType.WORKFLOW_STARTED, name="x", payload={"input": {}}
        )
        await store.append(run_id, [started], expected_version=0)

        # The run is now at version 1; appending as if it were still 0 must fail.
        stale = Event(seq=1, type=EventType.WORKFLOW_COMPLETED, payload={})
        with pytest.raises(ConcurrencyError):
            await store.append(run_id, [stale], expected_version=0)
    finally:
        await store.close()
