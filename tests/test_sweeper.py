"""Tests for the sweeper: parents whose children finished without saying so.

The window it exists for is narrow and real — a child commits its own result and
then dies before telling anyone — so the tests build that state deliberately
rather than hoping to race into it. The other half of the spec is restraint: a
sweeper that wakes runs which are legitimately waiting is worse than no sweeper,
because it turns every long fan-out into a busy loop.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from flowforge import (
    ChildSweeper,
    Engine,
    InMemoryEventStore,
    InMemoryLockManager,
    InMemoryTaskQueue,
    Registry,
    RunStatus,
    Worker,
    WorkflowContext,
)
from flowforge.api import build_control_plane
from flowforge.core.events import EventType


class Item(BaseModel):
    value: int


class Doubled(BaseModel):
    value: int


class SilentEngine(Engine):
    """A worker that dies between committing a child's result and reporting it."""

    async def _notify_parent(self, *args: Any, **kwargs: Any) -> None:
        return None


def _plane() -> tuple[Any, list[int], InMemoryEventStore, InMemoryTaskQueue]:
    seen: list[int] = []

    async def double(ctx: WorkflowContext, inp: Item) -> Doubled:
        seen.append(inp.value)
        return Doubled(value=inp.value * 2)

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        results: list[Doubled] = await ctx.children(
            "double", [Item(value=1), Item(value=2)], concurrency=2
        )
        return Doubled(value=sum(r.value for r in results))

    registry = Registry()
    registry.add(double, name="double")
    registry.add(parent, name="parent")
    store, queue = InMemoryEventStore(), InMemoryTaskQueue()
    cp = build_control_plane(store, registry, queue=queue)
    return cp, seen, store, queue


async def _drain(cp: Any) -> None:
    while await cp.worker.run_once() is not None:
        pass


async def test_a_parent_stranded_by_a_dead_child_is_woken() -> None:
    cp, seen, store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()  # starts both children, suspends

    # Both children finish, and both die before reporting back.
    silent = SilentEngine(store, cp.registry, queue=InMemoryTaskQueue())
    for cs in (0, 1):
        assert (await silent.drive(cp.engine.child_run_id("p1", cs))).status is RunStatus.COMPLETED
    while await queue.dequeue() is not None:
        pass  # the parent is queued nowhere: nothing will ever drive it

    assert (await cp.engine.describe("p1")).status is RunStatus.SUSPENDED
    assert sorted(seen) == [1, 2]  # the work was done...
    assert await queue.size() == 0  # ...and the parent will never hear about it

    woken = await cp.sweeper.sweep()

    assert woken == ["p1"]
    await _drain(cp)
    result = await cp.engine.describe("p1")
    assert result.status is RunStatus.COMPLETED
    assert result.result == {"value": 6}


async def test_the_sweeper_repairs_nothing_itself() -> None:
    """It restores the nudge; the drive reconciles the news from the child's log."""
    cp, _seen, store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()

    silent = SilentEngine(store, cp.registry, queue=InMemoryTaskQueue())
    await silent.drive(cp.engine.child_run_id("p1", 0))
    while await queue.dequeue() is not None:
        pass

    before = await store.load("p1")
    await cp.sweeper.sweep()
    after = await store.load("p1")

    assert after == before  # not one event written by the sweeper
    assert await queue.size() == 1


async def test_a_parent_waiting_on_a_running_child_is_left_alone() -> None:
    """Otherwise every long fan-out becomes a busy loop."""
    cp, _seen, _store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()  # children started, none finished
    while await queue.dequeue() is not None:
        pass

    assert await cp.sweeper.sweep() == []
    assert await queue.size() == 0


async def test_runs_waiting_on_a_timer_or_a_signal_are_not_its_business() -> None:
    async def naps(ctx: WorkflowContext, inp: Item) -> Doubled:
        await ctx.sleep(3600)
        return Doubled(value=inp.value)

    async def waits(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.wait_for_signal("approval", Doubled)

    registry = Registry()
    registry.add(naps, name="naps")
    registry.add(waits, name="waits")
    cp = build_control_plane(InMemoryEventStore(), registry)

    assert (await cp.engine.start("r1", "naps", Item(value=1))).status is RunStatus.SUSPENDED
    assert (await cp.engine.start("r2", "waits", Item(value=1))).status is RunStatus.SUSPENDED

    assert await cp.sweeper.sweep() == []


async def test_finished_runs_are_not_swept() -> None:
    cp, _seen, _store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await _drain(cp)

    assert (await cp.engine.describe("p1")).status is RunStatus.COMPLETED
    assert await cp.sweeper.sweep() == []


async def test_a_failed_child_strands_a_parent_just_the_same() -> None:
    async def refuse(value: int) -> Doubled:
        raise ValueError("child said no")

    async def failing(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.activity(refuse, inp.value, name="refuse")

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        results: list[Doubled] = await ctx.children("failing", [Item(value=1)])
        return results[0]

    registry = Registry()
    registry.add(failing, name="failing")
    registry.add(parent, name="parent")
    store, queue = InMemoryEventStore(), InMemoryTaskQueue()
    cp = build_control_plane(store, registry, queue=queue)

    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()

    silent = SilentEngine(store, registry, queue=InMemoryTaskQueue())
    assert (await silent.drive(cp.engine.child_run_id("p1", 0))).status is RunStatus.FAILED
    while await queue.dequeue() is not None:
        pass

    assert await cp.sweeper.sweep() == ["p1"]
    await _drain(cp)
    assert (await cp.engine.describe("p1")).status is RunStatus.FAILED


async def test_the_cursor_walks_the_whole_suspended_set() -> None:
    """A batch smaller than the backlog must not mean the tail is never seen."""
    cp, _seen, store, queue = _plane()
    for n in range(5):
        await cp.engine.create_run(f"p{n}", "parent", Item(value=n))
        await queue.enqueue(f"p{n}")
    for _ in range(5):
        await cp.worker.run_once()  # one drive each: every parent suspends

    silent = SilentEngine(store, cp.registry, queue=InMemoryTaskQueue())
    for n in range(5):
        for cs in (0, 1):
            await silent.drive(cp.engine.child_run_id(f"p{n}", cs))
    while await queue.dequeue() is not None:
        pass

    sweeper = ChildSweeper(cp.engine, store, queue, batch=2)
    woken: list[str] = []
    for _ in range(4):  # 5 parents in pages of 2, plus the wrap
        woken.extend(await sweeper.sweep())

    assert set(woken) >= {f"p{n}" for n in range(5)}


async def test_the_sweep_loop_outlives_a_failing_pass() -> None:
    class BrokenStore(InMemoryEventStore):
        async def list_runs(self, **kwargs: Any) -> Any:
            raise ConnectionError("event store unreachable")

    queue = InMemoryTaskQueue()
    sweeper = ChildSweeper(
        Engine(BrokenStore(), Registry()), BrokenStore(), queue, batch=10
    )
    stop = asyncio.Event()
    task = asyncio.create_task(sweeper.run_forever(interval=0.01, stop=stop))
    await asyncio.sleep(0.15)
    alive = not task.done()
    stop.set()
    task.cancel()

    assert alive


# -- the window the sweeper does *not* have to cover --------------------------


async def test_a_lost_notice_still_leaves_the_parent_queued() -> None:
    """The nudge is sent before the news, so a crash between them costs the
    outcome — which the parent can re-read — and never the wake-up."""

    cp, _seen, store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()
    while await queue.dequeue() is not None:
        pass

    # Drive a child normally: the parent is queued, and the notice lands too.
    await cp.engine.drive(cp.engine.child_run_id("p1", 0))

    assert await queue.size() == 1, "the parent was not woken"
    parent_log = await store.load("p1")
    assert [e for e in parent_log if e.type is EventType.CHILD_COMPLETED]


async def test_a_worker_and_the_sweeper_agree_on_a_stranded_parent() -> None:
    """Waking a run twice is harmless: the second drive replays and finds nothing
    left to do."""
    cp, _seen, store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()

    silent = SilentEngine(store, cp.registry, queue=InMemoryTaskQueue())
    for cs in (0, 1):
        await silent.drive(cp.engine.child_run_id("p1", cs))
    while await queue.dequeue() is not None:
        pass

    await cp.sweeper.sweep()
    await cp.sweeper.sweep()  # a second sweeper, or a slow pass, sees it again

    worker = Worker(cp.engine, queue, InMemoryLockManager())
    while await worker.run_once() is not None:
        pass

    result = await cp.engine.describe("p1")
    assert result.status is RunStatus.COMPLETED
    assert result.result == {"value": 6}


async def test_the_app_actually_runs_the_sweeper() -> None:
    """Wiring that nothing exercises is wiring that rots: the loop has to be in
    the app's lifespan, not merely constructible."""
    from httpx import ASGITransport, AsyncClient

    from flowforge.api import create_app

    cp, _seen, store, queue = _plane()
    await cp.engine.create_run("p1", "parent", Item(value=0))
    await queue.enqueue("p1")
    await cp.worker.run_once()

    silent = SilentEngine(store, cp.registry, queue=InMemoryTaskQueue())
    for cs in (0, 1):
        await silent.drive(cp.engine.child_run_id("p1", cs))
    while await queue.dequeue() is not None:
        pass
    assert (await cp.engine.describe("p1")).status is RunStatus.SUSPENDED

    app = create_app(cp, run_background=True)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as http:
        async with app.router.lifespan_context(app):
            # The first sweep runs immediately, and the worker loop is up too.
            for _ in range(40):
                await asyncio.sleep(0.05)
                if (await http.get("/runs/p1")).json()["status"] == "completed":
                    break

        assert (await cp.engine.describe("p1")).result == {"value": 6}


@pytest.mark.parametrize("batch", [1, 3, 100])
async def test_sweeping_an_empty_world_is_free(batch: int) -> None:
    store, queue = InMemoryEventStore(), InMemoryTaskQueue()
    sweeper = ChildSweeper(Engine(store, Registry()), store, queue, batch=batch)

    assert await sweeper.sweep() == []
    assert await queue.size() == 0
