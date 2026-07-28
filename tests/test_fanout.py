"""Tests for fan-out inside one run: ``ctx.map`` and ``ctx.map_llm``.

The properties: results come back in item order however they finish; command
numbering is deterministic, so a crash halfway resumes without repeating the
items already done; the concurrency bound actually bounds; a failing item does not
abandon its in-flight siblings; and a fanned-out LLM step is billed and gated per
call like any other.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from pydantic import BaseModel

from flowforge import (
    Budget,
    BudgetGuard,
    Engine,
    InMemoryCostLedger,
    InMemoryEventStore,
    Registry,
    RunStatus,
    WorkflowContext,
)
from flowforge.core.errors import NonRetryableError
from flowforge.core.events import EventType
from flowforge.llm import LLMStep, ModelPrice, Pricing, ScriptedLLMClient

_PER_CALL = 0.02  # ScriptedLLMClient's 10+10 tokens at $1/1k in and out


class Doc(BaseModel):
    paragraphs: list[str]


class Risks(BaseModel):
    findings: list[str]


class Risk(BaseModel):
    level: str


class _Tracker:
    """Records overlap so the concurrency bound can be asserted, not assumed."""

    def __init__(self) -> None:
        self.in_flight = 0
        self.peak = 0
        self.order: list[str] = []

    async def work(self, item: str) -> str:
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        self.order.append(item)
        try:
            await asyncio.sleep(0)  # yield, so overlap is real
            return item.upper()
        finally:
            self.in_flight -= 1


async def test_map_returns_results_in_item_order() -> None:
    tracker = _Tracker()

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(tracker.work, inp.paragraphs, concurrency=4))

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=["a", "b", "c", "d", "e"])
    )

    assert res.status is RunStatus.COMPLETED
    assert res.result == Risks(findings=["A", "B", "C", "D", "E"])


async def test_map_bounds_concurrency() -> None:
    tracker = _Tracker()

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(tracker.work, inp.paragraphs, concurrency=2))

    reg = Registry()
    definition = reg.add(wf)
    await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=[str(i) for i in range(10)])
    )

    assert tracker.peak == 2  # never more than two at once
    assert len(tracker.order) == 10


async def test_map_numbering_is_deterministic_across_a_replay() -> None:
    """Each item keeps its own command number, so a re-drive repeats nothing."""
    calls: list[str] = []

    async def work(item: str) -> str:
        calls.append(item)
        return item.upper()

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(work, inp.paragraphs, concurrency=3))

    store = InMemoryEventStore()
    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(store, reg)

    first = await engine.start("r1", definition, Doc(paragraphs=["a", "b", "c"]))
    assert sorted(calls) == ["a", "b", "c"]

    again = await engine.drive("r1")
    assert again.status is RunStatus.COMPLETED
    # A re-drive of a finished run reports the recorded result straight from the
    # log, so it comes back as the JSON that was committed.
    assert again.result == first.result.model_dump()
    assert sorted(calls) == ["a", "b", "c"]  # nothing ran twice

    # One pair of events per item, each under its own command number.
    events = await store.load("r1")
    completed = [e for e in events if e.type is EventType.ACTIVITY_COMPLETED]
    assert len(completed) == 3
    assert len({e.command_seq for e in completed}) == 3


async def test_map_resumes_a_partially_finished_fan_out() -> None:
    attempts: list[str] = []

    async def flaky(item: str) -> str:
        attempts.append(item)
        if item == "b" and attempts.count("b") == 1:
            raise RuntimeError("transient")
        return item.upper()

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(
            findings=await ctx.map(flaky, inp.paragraphs, concurrency=3, name="scan")
        )

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=["a", "b", "c"])
    )

    assert res.status is RunStatus.COMPLETED
    assert res.result == Risks(findings=["A", "B", "C"])
    assert attempts.count("a") == 1  # only the failing item was retried
    assert attempts.count("b") == 2


async def test_a_failing_item_lets_its_siblings_finish_first() -> None:
    finished: list[str] = []

    async def work(item: str) -> str:
        if item == "a":
            raise NonRetryableError("bad paragraph")
        await asyncio.sleep(0)
        finished.append(item)
        return item.upper()

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(work, inp.paragraphs, concurrency=3))

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=["a", "b", "c"])
    )

    assert res.status is RunStatus.FAILED
    assert res.error is not None and "bad paragraph" in res.error
    # The siblings were not cancelled: their side effects happened and were recorded.
    assert finished == ["b", "c"]


async def test_the_earliest_failure_by_item_order_is_the_one_raised() -> None:
    async def work(item: str) -> str:
        raise NonRetryableError(f"broken:{item}")

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(work, inp.paragraphs, concurrency=3))

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=["a", "b", "c"])
    )

    assert res.error is not None and "broken:a" in res.error


async def test_map_compensations_unwind_in_item_order() -> None:
    voided: list[str] = []

    async def work(item: str) -> str:
        return item.upper()

    async def undo(item: str) -> None:
        voided.append(item)

    async def boom() -> None:
        raise NonRetryableError("later step failed")

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        found = await ctx.map(work, inp.paragraphs, concurrency=3, compensate=undo)
        await ctx.activity(boom)
        return Risks(findings=found)

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=["a", "b", "c"])
    )

    assert res.status is RunStatus.FAILED
    assert voided == ["c", "b", "a"]  # reverse of item order, not of finish order


async def test_map_rejects_a_meaningless_bound() -> None:
    """A nonsensical bound is a bug in workflow code: the run parks, it does not
    take the worker down with it."""

    async def work(item: str) -> str:
        return item

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(work, inp.paragraphs, concurrency=0))

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start(
        "r1", definition, Doc(paragraphs=["a"])
    )
    assert res.status is RunStatus.STUCK
    assert res.error is not None and "concurrency" in res.error


async def test_map_of_an_empty_list_does_nothing() -> None:
    async def work(item: str) -> str:
        raise AssertionError("must not be called")

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        return Risks(findings=await ctx.map(work, inp.paragraphs))

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg).start("r1", definition, Doc(paragraphs=[]))
    assert res.result == Risks(findings=[])


# -- fanning out an LLM step ------------------------------------------------


def _step(*responses: str) -> tuple[LLMStep[Risk], ScriptedLLMClient]:
    client = ScriptedLLMClient(list(responses))
    step = LLMStep(
        client,
        "m",
        Risk,
        pricing=Pricing({"m": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
        name="risk",
    )
    return step, client


async def test_map_llm_bills_every_call_to_the_tenant() -> None:
    step, client = _step(*[json.dumps({"level": "low"})] * 3)
    ledger = InMemoryCostLedger()
    guard = BudgetGuard(ledger)

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        risks = await ctx.map_llm(step, inp.paragraphs, concurrency=2)
        return Risks(findings=[r.level for r in risks])

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg, budget=guard).start(
        "r1", definition, Doc(paragraphs=["p1", "p2", "p3"]), tenant="acme"
    )

    assert res.status is RunStatus.COMPLETED
    assert len(client.calls) == 3
    assert len(ledger.entries) == 3
    assert await guard.spent("acme") == pytest.approx(3 * _PER_CALL)
    # Each charge is attributed to its own item's command, not lumped together.
    assert len({entry.command_seq for _at, entry in ledger.entries}) == 3


async def test_map_llm_stops_at_the_budget_instead_of_burning_it() -> None:
    """A fan-out is the fastest way to spend a month's allowance in a second."""
    step, client = _step(*[json.dumps({"level": "low"})] * 20)
    guard = BudgetGuard(InMemoryCostLedger(), default=Budget(limit_usd=2 * _PER_CALL))

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        risks = await ctx.map_llm(step, inp.paragraphs, concurrency=1)
        return Risks(findings=[r.level for r in risks])

    reg = Registry()
    definition = reg.add(wf)
    res = await Engine(InMemoryEventStore(), reg, budget=guard).start(
        "r1", definition, Doc(paragraphs=[f"p{i}" for i in range(20)]), tenant="acme"
    )

    assert res.status is RunStatus.FAILED
    assert res.error is not None and "budget" in res.error
    assert len(client.calls) == 2  # the third call was refused, not made


async def test_map_llm_replay_calls_no_model() -> None:
    step, client = _step(*[json.dumps({"level": "low"})] * 3)

    async def wf(ctx: WorkflowContext, inp: Doc) -> Risks:
        risks = await ctx.map_llm(step, inp.paragraphs, concurrency=3)
        return Risks(findings=[r.level for r in risks])

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg)

    await engine.start("r1", definition, Doc(paragraphs=["p1", "p2", "p3"]))
    assert len(client.calls) == 3

    again = await engine.drive("r1")
    assert again.status is RunStatus.COMPLETED
    assert len(client.calls) == 3  # replay is free
