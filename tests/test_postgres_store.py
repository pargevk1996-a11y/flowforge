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

from flowforge import (
    ConcurrencyError,
    Engine,
    InMemoryLockManager,
    InMemoryTaskQueue,
    Registry,
    RunStatus,
    Worker,
    WorkflowContext,
)
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


async def test_child_fan_out_is_durable_across_engines() -> None:
    """A fan-out over child runs is several logs coordinating through the
    database; a fresh engine must be able to pick it up mid-flight."""
    doubled: list[int] = []

    async def double(ctx: WorkflowContext, inp: Money) -> Receipt:
        doubled.append(inp.amount)
        return Receipt(txn=f"x{inp.amount * 2}")

    async def parent(ctx: WorkflowContext, inp: Money) -> Receipt:
        results: list[Receipt] = await ctx.children(
            "double_pg", [Money(amount=n) for n in range(1, 4)], concurrency=2
        )
        return Receipt(txn=",".join(r.txn for r in results))

    store = await _store()
    try:
        reg = Registry()
        reg.add(double, name="double_pg")
        definition = reg.add(parent, name="parent_pg")
        queue = InMemoryTaskQueue()
        run_id = uuid.uuid4().hex

        res = await Engine(store, reg, queue=queue).start(run_id, definition, Money(amount=0))
        assert res.status is RunStatus.SUSPENDED
        assert await queue.size() == 2  # only the bound, queued for a worker

        # A brand-new engine, a brand-new worker: the fan-out is entirely in the
        # database, so it resumes and finishes.
        engine = Engine(store, reg, queue=queue)
        worker = Worker(engine, queue, InMemoryLockManager())
        while await worker.run_once() is not None:
            pass

        final = await engine.describe(run_id)
        assert final.status is RunStatus.COMPLETED
        assert final.result == {"txn": "x2,x4,x6"}
        assert sorted(doubled) == [1, 2, 3]
    finally:
        await store.close()


async def test_a_page_past_the_end_still_reports_the_true_total() -> None:
    """``COUNT(*) OVER()`` had nothing to count once the offset ran past the last
    row, so paging off the end used to report a total of zero."""
    store = await _store()
    try:
        tenant = f"pager-{uuid.uuid4().hex[:8]}"
        reg = Registry()

        async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
            return Receipt(txn=str(inp.amount))

        definition = reg.add(wf, name=f"pager_{uuid.uuid4().hex[:8]}")
        engine = Engine(store, reg)
        for n in range(3):
            await engine.start(uuid.uuid4().hex, definition, Money(amount=n), tenant=tenant)

        first = await store.list_runs(tenant=tenant, limit=2, offset=0)
        past_end = await store.list_runs(tenant=tenant, limit=2, offset=10)

        assert (len(first.runs), first.total) == (2, 3)
        assert (len(past_end.runs), past_end.total) == (0, 3)
    finally:
        await store.close()


async def test_a_parked_run_is_visible_as_stuck_in_the_run_row() -> None:
    """The run row is what a list view reads, so the projection has to know about
    parking too — otherwise a stuck run hides among the running ones."""
    store = await _store()
    try:
        tenant = f"stuck-{uuid.uuid4().hex[:8]}"
        reg = Registry()

        async def buggy(ctx: WorkflowContext, inp: Money) -> Receipt:
            return Receipt(txn=str(1 // inp.amount))  # amount=0 -> ZeroDivisionError

        definition = reg.add(buggy, name=f"buggy_{uuid.uuid4().hex[:8]}")
        run_id = uuid.uuid4().hex
        engine = Engine(store, reg)

        res = await engine.start(run_id, definition, Money(amount=0), tenant=tenant)
        assert res.status is RunStatus.STUCK

        page = await store.list_runs(tenant=tenant, status="stuck")
        assert [run.run_id for run in page.runs] == [run_id]
        assert (await engine.describe(run_id)).status is RunStatus.STUCK

        # And it is genuinely resumable: the same log, a workflow that works.
        fixed = Registry()

        async def works(ctx: WorkflowContext, inp: Money) -> Receipt:
            return Receipt(txn="fixed")

        fixed.add(works, name=definition.name)
        assert (await Engine(store, fixed).drive(run_id)).status is RunStatus.COMPLETED
        assert (await store.list_runs(tenant=tenant, status="stuck")).total == 0
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
