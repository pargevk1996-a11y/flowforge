"""Tests for sub-workflows: ``ctx.child`` and ``ctx.children``.

A child is a real run, so the properties to prove are about two logs and not one:
the parent suspends rather than blocks, it is woken when the child finishes, it
never starts the same child twice, a failed child unwinds the parent through its
compensations, and the concurrency bound on a fan-out is derived from the log —
so it survives a restart instead of living in a semaphore.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from flowforge import (
    ChildFailedError,
    Engine,
    InMemoryEventStore,
    InMemoryTaskQueue,
    Registry,
    RunStatus,
    WorkflowContext,
)
from flowforge.api import build_control_plane
from flowforge.core.errors import NonRetryableError
from flowforge.core.events import EventType


class Item(BaseModel):
    value: int


class Doubled(BaseModel):
    value: int


class Batch(BaseModel):
    values: list[int]


class Totals(BaseModel):
    values: list[int]


def _registry(child_fn: object = None) -> tuple[Registry, list[int]]:
    seen: list[int] = []

    async def double(ctx: WorkflowContext, inp: Item) -> Doubled:
        seen.append(inp.value)
        return Doubled(value=inp.value * 2)

    reg = Registry()
    reg.add(double, name="double")
    return reg, seen


async def test_parent_suspends_until_its_child_finishes() -> None:
    reg, seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.child("double", inp)

    definition = reg.add(parent, name="parent")
    store = InMemoryEventStore()
    queue = InMemoryTaskQueue()
    engine = Engine(store, reg, queue=queue)

    first = await engine.start("p1", definition, Item(value=21))
    assert first.status is RunStatus.SUSPENDED  # the parent is not blocking a worker
    assert seen == []

    # The child was seeded and queued under a derived id.
    child_id = engine.child_run_id("p1", 0)
    assert (await engine.describe(child_id)).status is RunStatus.RUNNING

    assert (await engine.drive(child_id)).status is RunStatus.COMPLETED
    assert seen == [21]

    # Finishing the child reported back into the parent's log and re-queued it.
    assert (await engine.drive("p1")).result == Doubled(value=42)


async def test_replaying_a_parent_never_starts_a_second_child() -> None:
    reg, seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.child("double", inp)

    definition = reg.add(parent, name="parent")
    engine = Engine(InMemoryEventStore(), reg, queue=InMemoryTaskQueue())

    await engine.start("p1", definition, Item(value=3))
    for _ in range(3):
        assert (await engine.drive("p1")).status is RunStatus.SUSPENDED

    await engine.drive(engine.child_run_id("p1", 0))
    assert seen == [3]  # one child, however many times the parent replayed


async def test_a_failed_child_unwinds_the_parent() -> None:
    undone: list[str] = []

    async def refuse(value: int) -> Doubled:
        raise NonRetryableError("child said no")

    async def explode(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.activity(refuse, inp.value, name="refuse")

    async def book(ref: str) -> str:
        return ref

    async def unbook() -> None:
        undone.append("txn-1")

    reg = Registry()
    reg.add(explode, name="explode")

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        await ctx.activity(book, "txn-1", compensate=unbook)
        return await ctx.child("explode", inp)

    definition = reg.add(parent, name="parent")
    engine = Engine(InMemoryEventStore(), reg, queue=InMemoryTaskQueue())

    await engine.start("p1", definition, Item(value=1))
    # The booking took command 0, so the child is command 1.
    child = await engine.drive(engine.child_run_id("p1", 1))
    assert child.status is RunStatus.FAILED

    parent_result = await engine.drive("p1")
    assert parent_result.status is RunStatus.FAILED
    assert parent_result.error is not None and "child said no" in parent_result.error
    assert undone == ["txn-1"]  # the parent still owed its own rollback


async def test_child_failure_surfaces_as_child_failed_error() -> None:
    async def refuse(value: int) -> Doubled:
        raise NonRetryableError("nope")

    async def explode(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.activity(refuse, inp.value, name="refuse")

    reg = Registry()
    reg.add(explode, name="explode")
    captured: list[ChildFailedError] = []

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        try:
            return await ctx.child("explode", inp)
        except ChildFailedError as exc:
            captured.append(exc)
            raise

    definition = reg.add(parent, name="parent")
    engine = Engine(InMemoryEventStore(), reg, queue=InMemoryTaskQueue())
    await engine.start("p1", definition, Item(value=1))
    await engine.drive(engine.child_run_id("p1", 0))
    await engine.drive("p1")

    assert captured and captured[0].child_run_id == engine.child_run_id("p1", 0)


class _SilentEngine(Engine):
    """An engine whose runs die between finishing and telling their parent."""

    async def _notify_parent(self, *args: Any, **kwargs: Any) -> None:
        return None


async def test_a_lost_completion_notice_is_reconciled_from_the_childs_log() -> None:
    """A child reports into its parent's log and then enqueues it. If it dies in
    between, the parent must not wait forever on a child that already finished."""
    reg, _seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.child("double", inp)

    definition = reg.add(parent, name="parent")
    store = InMemoryEventStore()
    engine = Engine(store, reg, queue=InMemoryTaskQueue())

    await engine.start("p1", definition, Item(value=5))
    child_id = engine.child_run_id("p1", 0)
    # The child completes, but its notice never reaches the parent.
    silent = _SilentEngine(store, reg, queue=InMemoryTaskQueue())
    assert (await silent.drive(child_id)).status is RunStatus.COMPLETED
    events = await store.load("p1")
    assert not [e for e in events if e.type is EventType.CHILD_COMPLETED]

    # Driving the parent reconciles: it asks the child's own log and records it.
    assert (await engine.drive("p1")).result == Doubled(value=10)


async def test_children_fan_out_returns_results_in_input_order() -> None:
    reg, seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Batch) -> Totals:
        results: list[Doubled] = await ctx.children(
            "double", [Item(value=v) for v in inp.values], concurrency=2
        )
        return Totals(values=[r.value for r in results])

    definition = reg.add(parent, name="parent")
    cp = build_control_plane(InMemoryEventStore(), reg)

    await cp.engine.create_run("p1", definition, Batch(values=[1, 2, 3, 4, 5]))
    await cp.queue.enqueue("p1")
    while await cp.worker.run_once() is not None:
        pass

    result = await cp.engine.describe("p1")
    assert result.status is RunStatus.COMPLETED
    assert result.result == {"values": [2, 4, 6, 8, 10]}
    assert sorted(seen) == [1, 2, 3, 4, 5]


async def test_children_bound_is_durable_not_in_memory() -> None:
    """The bound is recomputed from the log on every drive, so it holds across
    the suspends that a fan-out over child runs is made of."""
    reg, _seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Batch) -> Totals:
        results: list[Doubled] = await ctx.children(
            "double", [Item(value=v) for v in inp.values], concurrency=2
        )
        return Totals(values=[r.value for r in results])

    definition = reg.add(parent, name="parent")
    store = InMemoryEventStore()
    engine = Engine(store, reg, queue=InMemoryTaskQueue())

    await engine.start("p1", definition, Batch(values=[1, 2, 3, 4, 5]))

    started = [e for e in await store.load("p1") if e.type is EventType.CHILD_STARTED]
    assert len(started) == 2  # only the bound, not all five

    # Finish one child; the next drive tops the fan-out back up to two in flight.
    await engine.drive(engine.child_run_id("p1", 0))
    await engine.drive("p1")
    events = await store.load("p1")
    assert len([e for e in events if e.type is EventType.CHILD_STARTED]) == 3
    assert len([e for e in events if e.type is EventType.CHILD_COMPLETED]) == 1


async def test_children_report_suspended_while_waiting() -> None:
    reg, _seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Batch) -> Totals:
        results: list[Doubled] = await ctx.children("double", [Item(value=v) for v in inp.values])
        return Totals(values=[r.value for r in results])

    definition = reg.add(parent, name="parent")
    engine = Engine(InMemoryEventStore(), reg, queue=InMemoryTaskQueue())

    await engine.start("p1", definition, Batch(values=[1, 2]))
    assert (await engine.describe("p1")).status is RunStatus.SUSPENDED


async def test_a_failing_child_fails_the_whole_fan_out() -> None:
    async def check(value: int) -> Doubled:
        if value == 2:
            raise NonRetryableError("item two is poison")
        return Doubled(value=value * 2)

    async def maybe(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.activity(check, inp.value, name="check")

    reg = Registry()
    reg.add(maybe, name="maybe")

    async def parent(ctx: WorkflowContext, inp: Batch) -> Totals:
        results: list[Doubled] = await ctx.children(
            "maybe", [Item(value=v) for v in inp.values], concurrency=3
        )
        return Totals(values=[r.value for r in results])

    definition = reg.add(parent, name="parent")
    cp = build_control_plane(InMemoryEventStore(), reg)

    await cp.engine.create_run("p1", definition, Batch(values=[1, 2, 3]))
    await cp.queue.enqueue("p1")
    while await cp.worker.run_once() is not None:
        pass

    result = await cp.engine.describe("p1")
    assert result.status is RunStatus.FAILED
    assert result.error is not None and "item two is poison" in result.error


async def test_children_inherit_the_parents_tenant() -> None:
    reg, _seen = _registry()

    async def parent(ctx: WorkflowContext, inp: Item) -> Doubled:
        return await ctx.child("double", inp)

    definition = reg.add(parent, name="parent")
    store = InMemoryEventStore()
    engine = Engine(store, reg, queue=InMemoryTaskQueue())

    await engine.start("p1", definition, Item(value=1), tenant="acme")

    child_history = await store.load(engine.child_run_id("p1", 0))
    assert child_history[0].payload["tenant"] == "acme"  # billed to the same tenant
    assert child_history[0].payload["parent"] == {"run_id": "p1", "command_seq": 0}


async def test_children_need_an_engine() -> None:
    """A context built without a launcher says so, rather than failing obscurely."""
    ctx = WorkflowContext("r1", [], InMemoryEventStore())
    with pytest.raises(RuntimeError, match="child launcher"):
        await ctx.child("double", Item(value=1))
