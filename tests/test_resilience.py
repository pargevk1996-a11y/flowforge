"""Tests for the failure boundaries: what breaks, and what must not break with it.

Three kinds of failure meet three different answers. An activity that exhausted
its retries is a *business* failure: the run fails and compensates. A bug in the
workflow function is a failure of *code*: the run parks, keeps everything it has
done, and waits for a fix. Anything else — a store that cannot be reached — is
infrastructure: the run goes back on the queue.

The property tying them together is that **no failure of a single run may take
down the loop that processes the others**.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from flowforge import (
    Engine,
    InMemoryEventStore,
    InMemoryLockManager,
    InMemoryTaskQueue,
    Registry,
    RunStatus,
    Worker,
    WorkflowContext,
    submit,
)
from flowforge.core.errors import ConcurrencyError, NonRetryableError, RunNotFoundError
from flowforge.core.events import Event, EventType


class In(BaseModel):
    x: int


class Out(BaseModel):
    y: int


class InV2(BaseModel):
    """``In`` after someone added a required field to it."""

    x: int
    required_now: str


async def _echo(x: int) -> int:
    return x


async def _drain(worker: Worker, *, seconds: float = 0.3) -> asyncio.Task[None]:
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run_forever(stop=stop))
    await asyncio.sleep(seconds)
    stop.set()
    return task


# -- a bug in workflow code parks the run --------------------------------------


async def test_a_bug_in_workflow_code_parks_the_run_instead_of_crashing() -> None:
    async def buggy(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x // 0)

    store, reg = InMemoryEventStore(), Registry()
    definition = reg.add(buggy, name="buggy")

    res = await Engine(store, reg).start("r1", definition, In(x=1))

    assert res.status is RunStatus.STUCK
    assert res.error is not None and "ZeroDivisionError" in res.error
    events = await store.load("r1")
    assert events[-1].type is EventType.WORKFLOW_TASK_FAILED
    # Parking is not failing: no terminal event, so nothing downstream treats
    # this run as finished.
    assert not [e for e in events if e.type is EventType.WORKFLOW_FAILED]


async def test_parking_compensates_nothing() -> None:
    """A KeyError is not a reason to void a payment."""
    voided: list[str] = []

    async def pay(ref: str) -> str:
        return ref

    async def refund() -> None:
        voided.append("txn-1")

    async def buggy(ctx: WorkflowContext, inp: In) -> Out:
        await ctx.activity(pay, "txn-1", compensate=refund)
        raise KeyError("a typo in workflow code")

    reg = Registry()
    definition = reg.add(buggy, name="buggy")
    res = await Engine(InMemoryEventStore(), reg).start("r1", definition, In(x=1))

    assert res.status is RunStatus.STUCK
    assert voided == []  # the payment is still there, waiting for the fix


async def test_a_parked_run_resumes_once_the_code_is_fixed() -> None:
    """The parking event carries no command, so replay walks straight past it."""
    calls: list[int] = []

    async def charge(x: int) -> int:
        calls.append(x)
        return x * 2

    async def broken(ctx: WorkflowContext, inp: In) -> Out:
        await ctx.activity(charge, inp.x)
        raise RuntimeError("boom")  # after the side effect, before the result

    async def fixed(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=await ctx.activity(charge, inp.x))

    store = InMemoryEventStore()
    broken_reg = Registry()
    broken_reg.add(broken, name="w")
    assert (await Engine(store, broken_reg).start("r1", "w", In(x=21))).status is RunStatus.STUCK
    assert calls == [21]

    # Deploy the fix; the same log drives to completion without re-charging.
    fixed_reg = Registry()
    fixed_reg.add(fixed, name="w")
    res = await Engine(store, fixed_reg).drive("r1")

    assert res.status is RunStatus.COMPLETED
    assert res.result == Out(y=42)
    assert calls == [21]  # the recorded activity was not re-run


async def test_a_run_seeded_before_a_schema_change_parks_rather_than_crashes() -> None:
    async def v1(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x)

    async def v2(ctx: WorkflowContext, inp: InV2) -> Out:
        return Out(y=inp.x)

    store = InMemoryEventStore()
    old = Registry()
    old.add(v1, name="w")
    await Engine(store, old).create_run("r1", "w", In(x=1))

    new = Registry()
    new.add(v2, name="w")
    res = await Engine(store, new).drive("r1")

    assert res.status is RunStatus.STUCK
    assert res.error is not None and "ValidationError" in res.error


async def test_an_unregistered_workflow_parks_rather_than_crashes() -> None:
    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x)

    store = InMemoryEventStore()
    known = Registry()
    known.add(wf, name="w")
    await Engine(store, known).create_run("r1", "w", In(x=1))

    res = await Engine(store, Registry()).drive("r1")  # a worker without that code

    assert res.status is RunStatus.STUCK
    assert res.error is not None and "WorkflowNotFoundError" in res.error


# -- one bad run does not stop the others --------------------------------------


async def test_a_poisoned_run_does_not_stop_the_worker() -> None:
    handled: list[int] = []

    async def poison(ctx: WorkflowContext, inp: In) -> Out:
        raise RuntimeError("a bug in one workflow")

    async def healthy(ctx: WorkflowContext, inp: In) -> Out:
        handled.append(inp.x)
        return Out(y=inp.x)

    store, reg = InMemoryEventStore(), Registry()
    bad, good = reg.add(poison, name="poison"), reg.add(healthy, name="healthy")
    queue = InMemoryTaskQueue()
    engine = Engine(store, reg, queue=queue)
    worker = Worker(engine, queue, InMemoryLockManager())

    await submit(engine, queue, "r-bad", bad, In(x=1))
    await submit(engine, queue, "r-good", good, In(x=7))
    task = await _drain(worker)

    assert not task.done()  # the loop outlived the bad run
    task.cancel()
    assert handled == [7]  # and got to the good one
    assert (await engine.describe("r-bad")).status is RunStatus.STUCK
    assert (await engine.describe("r-good")).status is RunStatus.COMPLETED
    assert await queue.size() == 0  # a parked run is not left spinning in the queue


async def test_infrastructure_failures_put_the_run_back_on_the_queue() -> None:
    """A dequeue is a claim, not a delivery: losing the item loses the run."""

    class BrokenStore(InMemoryEventStore):
        fail = True

        async def load(self, run_id: str) -> list[Event]:
            if self.fail:
                raise ConnectionError("event store unreachable")
            return await super().load(run_id)

    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x)

    store = BrokenStore()
    reg = Registry()
    definition = reg.add(wf, name="w")
    queue = InMemoryTaskQueue()
    engine = Engine(store, reg, queue=queue)
    worker = Worker(engine, queue, InMemoryLockManager())

    store.fail = False
    await submit(engine, queue, "r1", definition, In(x=1))
    store.fail = True

    with pytest.raises(ConnectionError):
        await worker.run_once()
    assert await queue.size() == 1  # still claimable

    store.fail = False
    assert (await worker.run_once()) is not None
    assert (await engine.describe("r1")).status is RunStatus.COMPLETED


async def test_a_queued_run_with_no_log_is_dropped_not_retried_forever() -> None:
    reg = Registry()
    queue = InMemoryTaskQueue()
    engine = Engine(InMemoryEventStore(), reg, queue=queue)
    worker = Worker(engine, queue, InMemoryLockManager())

    await queue.enqueue("never-existed")

    assert await worker.run_once() is None
    assert await queue.size() == 0  # nothing to put back, so nothing spins

    with pytest.raises(RunNotFoundError):
        await engine.drive("never-existed")


async def test_the_worker_loop_survives_a_store_that_is_down() -> None:
    class DownStore(InMemoryEventStore):
        async def load(self, run_id: str) -> list[Event]:
            raise ConnectionError("event store unreachable")

    reg = Registry()
    queue = InMemoryTaskQueue()
    worker = Worker(Engine(DownStore(), reg, queue=queue), queue, InMemoryLockManager())
    await queue.enqueue("r1")

    task = await _drain(worker, seconds=0.2)

    assert not task.done()  # backing off, not dead
    task.cancel()
    assert await queue.size() == 1  # and the run is still there for when it recovers


async def test_a_lost_race_is_retried_not_parked() -> None:
    """A stale-writer collision is the one ``Exception`` that must *not* park:
    parking would strand a perfectly healthy run on someone else's race."""
    attempts: list[int] = []

    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        attempts.append(len(attempts))
        if len(attempts) == 1:
            # Simulate the timer wheel appending to this run mid-drive: the next
            # append the workflow makes will collide on the version.
            history = await ctx._store.load(ctx.run_id)
            await ctx._store.append(
                ctx.run_id,
                [Event(seq=len(history), type=EventType.TIMER_FIRED, command_seq=99)],
                expected_version=len(history),
            )
        return Out(y=await ctx.activity(_echo, inp.x))

    store, reg = InMemoryEventStore(), Registry()
    definition = reg.add(wf, name="racy")
    queue = InMemoryTaskQueue()
    engine = Engine(store, reg, queue=queue)
    worker = Worker(engine, queue, InMemoryLockManager())
    await submit(engine, queue, "r1", definition, In(x=5))

    with pytest.raises(ConcurrencyError):
        await worker.run_once()
    assert await queue.size() == 1  # back on the queue, not parked
    events = await store.load("r1")
    assert not [e for e in events if e.type is EventType.WORKFLOW_TASK_FAILED]

    # The retry sees the extra event, replays past it, and finishes.
    assert (await worker.run_once()) is not None
    assert (await engine.describe("r1")).status is RunStatus.COMPLETED


async def test_a_failed_activity_is_still_an_ordinary_failure() -> None:
    """The new parking path must not swallow the business-failure path."""
    voided: list[str] = []

    async def pay(ref: str) -> str:
        return ref

    async def refund() -> None:
        voided.append("txn-1")

    async def boom(ref: str) -> str:
        raise NonRetryableError("gateway refused")

    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        await ctx.activity(pay, "txn-1", compensate=refund)
        await ctx.activity(boom, "txn-1")
        return Out(y=inp.x)

    reg = Registry()
    definition = reg.add(wf, name="w")
    res = await Engine(InMemoryEventStore(), reg).start("r1", definition, In(x=1))

    assert res.status is RunStatus.FAILED
    assert voided == ["txn-1"]  # compensated, as a business failure should be
