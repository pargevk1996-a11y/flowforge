"""Tests for tracing — one span tree per run, across processes and time.

A run is not a call stack: it is driven, suspends, and is driven again somewhere
else, and it branches into child runs driven elsewhere again. The property under
test is that all of that lands in **one trace** anyway, because the trace context
travels in the event log rather than in a process's memory.

The spans are collected by the real OpenTelemetry SDK into memory, so what is
asserted is what an exporter would actually ship.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import BaseModel

from flowforge import (
    BudgetGuard,
    Engine,
    InMemoryCostLedger,
    InMemoryEventStore,
    InMemoryTaskQueue,
    Registry,
    RunStatus,
    WorkflowContext,
)
from flowforge.api import build_control_plane
from flowforge.config import Settings
from flowforge.core.errors import NonRetryableError
from flowforge.core.tracing import NO_TRACING, trace_id_of
from flowforge.llm import LLMStep, ModelPrice, Pricing, ScriptedLLMClient
from flowforge.otel import OtelTracer, configure_tracing
from flowforge.triggers import webhook_trigger

pytest.importorskip("opentelemetry.sdk")


class In(BaseModel):
    x: int


class Out(BaseModel):
    y: int


class Grade(BaseModel):
    level: str


class Recorder:
    """A tracer wired to the real SDK, collecting spans in memory."""

    def __init__(self) -> None:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        self.exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        # A private provider, not the global one: tests must not depend on each
        # other through process-wide state.
        self.tracer = OtelTracer(provider.get_tracer("flowforge.test"))

    @property
    def spans(self) -> list[Any]:
        return list(self.exporter.get_finished_spans())

    def named(self, name: str) -> list[Any]:
        return [s for s in self.spans if s.name == name]

    def one(self, name: str) -> Any:
        found = self.named(name)
        assert len(found) == 1, f"expected exactly one {name!r}, got {len(found)}"
        return found[0]

    def trace_ids(self) -> set[int]:
        return {s.context.trace_id for s in self.spans}

    def parent_of(self, span: Any) -> Any | None:
        if span.parent is None:
            return None
        return next((s for s in self.spans if s.context.span_id == span.parent.span_id), None)


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


async def test_a_run_driven_twice_is_still_one_trace(recorder: Recorder) -> None:
    """The second drive happens after a suspend — in production, on another
    worker, hours later. It must not start a trace of its own."""

    async def pay(x: int) -> int:
        return x

    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        await ctx.sleep(3600)
        return Out(y=await ctx.activity(pay, inp.x))

    reg = Registry()
    definition = reg.add(wf, name="slow")
    store = InMemoryEventStore()

    first = Engine(store, reg, tracer=recorder.tracer)
    assert (await first.start("r1", definition, In(x=1))).status is RunStatus.SUSPENDED

    # A brand-new engine, as a different worker process would be.
    second = Engine(store, reg, tracer=recorder.tracer)
    assert (await second.fire_timer("r1")).status is RunStatus.COMPLETED

    assert len(recorder.trace_ids()) == 1, "the two drives ended up in separate traces"
    assert len(recorder.named("drive slow")) == 2
    # Both drives hang off the anchor recorded in the log, not off each other.
    anchor = recorder.one("run slow")
    for drive in recorder.named("drive slow"):
        assert drive.parent is not None
        assert drive.parent.span_id == anchor.context.span_id


async def test_the_run_carries_its_trace_in_the_log(recorder: Recorder) -> None:
    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x)

    reg = Registry()
    definition = reg.add(wf, name="w")
    store = InMemoryEventStore()
    await Engine(store, reg, tracer=recorder.tracer).start("r1", definition, In(x=1))

    started = (await store.load("r1"))[0]
    traceparent = started.payload["traceparent"]

    anchor = recorder.one("run w")
    assert trace_id_of(traceparent) == format(anchor.context.trace_id, "032x")


async def test_activities_are_spans_but_replays_are_not(recorder: Recorder) -> None:
    """Otherwise every drive would re-emit the whole history of the run."""
    calls: list[int] = []

    async def charge(x: int) -> int:
        calls.append(x)
        return x

    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=await ctx.activity(charge, inp.x, name="charge"))

    reg = Registry()
    definition = reg.add(wf, name="w")
    store = InMemoryEventStore()
    engine = Engine(store, reg, tracer=recorder.tracer)

    await engine.start("r1", definition, In(x=1))
    await engine.drive("r1")  # a replay of a finished run
    await engine.drive("r1")

    assert calls == [1]
    assert len(recorder.named("activity charge")) == 1


async def test_an_llm_step_is_marked_as_one(recorder: Recorder) -> None:
    client = ScriptedLLMClient([json.dumps({"level": "low"})])
    step = LLMStep(
        client,
        "m",
        Grade,
        pricing=Pricing({"m": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
        name="grade",
    )

    async def wf(ctx: WorkflowContext, inp: In) -> Grade:
        return await ctx.llm(step, "clause")

    reg = Registry()
    definition = reg.add(wf, name="w")
    engine = Engine(
        InMemoryEventStore(),
        reg,
        budget=BudgetGuard(InMemoryCostLedger()),
        tracer=recorder.tracer,
    )
    await engine.start("r1", definition, In(x=1), tenant="acme")

    span = recorder.one("llm grade")
    assert span.attributes["flowforge.step.kind"] == "llm"
    assert span.attributes["flowforge.command_seq"] == 0


async def test_a_fan_out_over_child_runs_is_one_tree(recorder: Recorder) -> None:
    """The money shot: a parent, its child commands, and the children's own
    drives — on whatever worker — all in a single trace."""

    async def double(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x * 2)

    async def parent(ctx: WorkflowContext, inp: In) -> Out:
        results: list[Out] = await ctx.children(
            "double", [In(x=1), In(x=2)], concurrency=2
        )
        return Out(y=sum(r.y for r in results))

    reg = Registry()
    reg.add(double, name="double")
    reg.add(parent, name="parent")
    cp = build_control_plane(InMemoryEventStore(), reg, tracer=recorder.tracer)

    await cp.engine.create_run("p1", "parent", In(x=0))
    await cp.queue.enqueue("p1")
    while await cp.worker.run_once() is not None:
        pass

    assert (await cp.engine.describe("p1")).status is RunStatus.COMPLETED
    assert len(recorder.trace_ids()) == 1, "the fan-out split into several traces"

    # Each child's anchor hangs off the parent command that started it.
    child_commands = recorder.named("child double[0]") + recorder.named("child double[1]")
    assert len(child_commands) == 2
    for anchor in recorder.named("run double"):
        assert recorder.parent_of(anchor) in child_commands


async def test_outcomes_are_on_the_drive_span(recorder: Recorder) -> None:
    async def refuse(x: int) -> int:
        raise NonRetryableError("gateway refused")

    async def failing(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=await ctx.activity(refuse, inp.x, name="pay"))

    async def parking(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x // 0)

    async def waiting(ctx: WorkflowContext, inp: In) -> Out:
        await ctx.sleep(60)
        return Out(y=inp.x)

    reg = Registry()
    for fn, name in ((failing, "failing"), (parking, "parking"), (waiting, "waiting")):
        reg.add(fn, name=name)
    engine = Engine(InMemoryEventStore(), reg, tracer=recorder.tracer)

    assert (await engine.start("r1", "failing", In(x=1))).status is RunStatus.FAILED
    assert (await engine.start("r2", "parking", In(x=1))).status is RunStatus.STUCK
    assert (await engine.start("r3", "waiting", In(x=1))).status is RunStatus.SUSPENDED

    outcomes = {
        s.name: s.attributes["flowforge.outcome"]
        for s in recorder.spans
        if s.name.startswith("drive ")
    }
    assert outcomes == {
        "drive failing": "failed",
        "drive parking": "stuck",
        "drive waiting": "suspended",
    }

    from opentelemetry.trace import StatusCode

    # A business failure and a parked run are both errors on the span; a
    # suspension is not — it is the engine working as designed.
    assert recorder.one("drive failing").status.status_code is StatusCode.ERROR
    assert recorder.one("drive parking").status.status_code is StatusCode.ERROR
    assert recorder.one("drive waiting").status.status_code is not StatusCode.ERROR


async def test_a_trigger_delivery_starts_the_trace(recorder: Recorder) -> None:
    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x)

    reg = Registry()
    reg.add(wf, name="w")
    cp = build_control_plane(
        InMemoryEventStore(), reg, queue=InMemoryTaskQueue(), tracer=recorder.tracer
    )
    cp.triggers.add(webhook_trigger("hook", "w", map=lambda event: In(x=int(event["x"]))))

    delivery = await cp.dispatcher.fire("hook", {"x": 3})
    while await cp.worker.run_once() is not None:
        pass

    assert len(recorder.trace_ids()) == 1
    trigger = recorder.one("trigger hook")
    assert trigger.attributes["flowforge.delivery.started"] is True
    assert trigger.attributes["flowforge.run_id"] == delivery.run_id
    # The run the delivery started belongs to the delivery's trace.
    assert recorder.parent_of(recorder.one("run w")) is trigger


async def test_the_timeline_reports_the_trace(recorder: Recorder) -> None:
    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=inp.x)

    reg = Registry()
    definition = reg.add(wf, name="w")
    store = InMemoryEventStore()
    await Engine(store, reg, tracer=recorder.tracer).start("r1", definition, In(x=1))

    from flowforge import build_timeline

    timeline = build_timeline("r1", await store.load("r1"))
    assert timeline.trace_id == format(recorder.one("run w").context.trace_id, "032x")


# -- opting out is free ------------------------------------------------------


async def test_an_uninstrumented_engine_records_nothing(recorder: Recorder) -> None:
    async def wf(ctx: WorkflowContext, inp: In) -> Out:
        return Out(y=await ctx.activity(_double, inp.x))

    reg = Registry()
    definition = reg.add(wf, name="w")
    store = InMemoryEventStore()

    res = await Engine(store, reg).start("r1", definition, In(x=2))  # default tracer

    assert res.status is RunStatus.COMPLETED
    assert recorder.spans == []
    assert "traceparent" not in (await store.load("r1"))[0].payload


async def _double(x: int) -> int:
    return x * 2


def test_tracing_is_off_unless_an_endpoint_is_configured() -> None:
    assert configure_tracing(Settings()).traceparent() is None
    assert NO_TRACING.traceparent() is None


def test_trace_id_of_tolerates_nonsense() -> None:
    assert trace_id_of(None) is None
    assert trace_id_of("") is None
    assert trace_id_of("garbage") is None
    assert trace_id_of("00-" + "a" * 32 + "-" + "b" * 16 + "-01") == "a" * 32
