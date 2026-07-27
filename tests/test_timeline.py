"""Tests for the timeline projection and the debugger endpoints.

The projection turns a stream of low-level events into the steps a human reads.
The property that makes the replay debugger work is that it is a *pure function of
a prefix*: asking for the timeline as of event N gives exactly what the engine
would have replayed from at that point — no snapshots, no special machinery.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from flowforge import (
    Engine,
    InMemoryCostLedger,
    InMemoryEventStore,
    Registry,
    RunStatus,
    WorkflowContext,
    build_timeline,
)
from flowforge.api import build_control_plane, create_app
from flowforge.api.controlplane import ControlPlane
from flowforge.core.errors import NonRetryableError
from flowforge.core.timeline import StepKind, StepStatus
from flowforge.llm import LLMStep, ModelPrice, Pricing, ScriptedLLMClient


class Order(BaseModel):
    sku: str


class Ack(BaseModel):
    ok: bool = True


class Grade(BaseModel):
    level: str


async def test_events_are_folded_into_steps() -> None:
    async def pick(sku: str) -> Ack:
        return Ack()

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        await ctx.activity(pick, inp.sku, name="pick")
        await ctx.sleep(60)
        return await ctx.activity(pick, inp.sku, name="pack")

    store = InMemoryEventStore()
    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(store, reg)
    await engine.start("r1", definition, Order(sku="A-1"), tenant="acme")

    timeline = build_timeline("r1", await store.load("r1"))

    assert timeline.workflow == definition.name
    assert timeline.tenant == "acme"
    assert timeline.status is RunStatus.SUSPENDED
    assert [(s.name, s.kind, s.status) for s in timeline.steps] == [
        ("pick", StepKind.ACTIVITY, StepStatus.COMPLETED),
        ("sleep", StepKind.TIMER, StepStatus.WAITING),
    ]
    # Every event belongs to the step it came from.
    assert timeline.steps[0].event_seqs == [1, 2]
    assert timeline.steps[0].duration_ms is not None


async def test_llm_steps_are_distinguishable_from_ordinary_ones() -> None:
    client = ScriptedLLMClient([json.dumps({"level": "low"})])
    step = LLMStep(
        client,
        "m",
        Grade,
        pricing=Pricing({"m": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
        name="grade",
    )

    async def fetch(sku: str) -> Ack:
        return Ack()

    async def wf(ctx: WorkflowContext, inp: Order) -> Grade:
        await ctx.activity(fetch, inp.sku, name="fetch")
        return await ctx.llm(step, inp.sku)

    store = InMemoryEventStore()
    ledger = InMemoryCostLedger()
    reg = Registry()
    definition = reg.add(wf)
    from flowforge import BudgetGuard

    engine = Engine(store, reg, budget=BudgetGuard(ledger))
    await engine.start("r1", definition, Order(sku="A-1"), tenant="acme")

    costs = await ledger.entries_for_run("r1")
    timeline = build_timeline("r1", await store.load("r1"), costs=costs)

    kinds = {s.name: s.kind for s in timeline.steps}
    assert kinds == {"fetch": StepKind.ACTIVITY, "grade": StepKind.LLM}
    # The cost lands on the step that spent it, and totals up on the run.
    grade = next(s for s in timeline.steps if s.name == "grade")
    assert grade.usd_cost == pytest.approx(0.02)
    assert timeline.usd_cost == pytest.approx(0.02)


async def test_a_failed_run_reports_its_error_and_compensations() -> None:
    async def book(sku: str) -> Ack:
        return Ack()

    async def unbook() -> None:
        return None

    async def boom(sku: str) -> Ack:
        raise NonRetryableError("warehouse on fire")

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        await ctx.activity(book, inp.sku, name="book", compensate=unbook)
        return await ctx.activity(boom, inp.sku, name="ship")

    store = InMemoryEventStore()
    reg = Registry()
    definition = reg.add(wf)
    await Engine(store, reg).start("r1", definition, Order(sku="A-1"))

    timeline = build_timeline("r1", await store.load("r1"))

    assert timeline.status is RunStatus.FAILED
    assert timeline.error is not None and "warehouse on fire" in timeline.error
    ship = next(s for s in timeline.steps if s.name == "ship")
    assert ship.status is StepStatus.FAILED
    assert ship.error is not None and "warehouse on fire" in ship.error
    assert [c.name for c in timeline.compensations] == ["book"]


async def test_the_timeline_of_a_prefix_is_the_past() -> None:
    """Time travel is just a shorter list — this is the replay debugger."""

    async def pick(sku: str) -> Ack:
        return Ack()

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        await ctx.activity(pick, inp.sku, name="pick")
        return await ctx.activity(pick, inp.sku, name="pack")

    store = InMemoryEventStore()
    reg = Registry()
    definition = reg.add(wf)
    await Engine(store, reg).start("r1", definition, Order(sku="A-1"))
    events = await store.load("r1")

    # Right after "pick" was scheduled, it was the only step, and unfinished.
    early = build_timeline("r1", events[:2], truncated_at=1)
    assert early.status is RunStatus.RUNNING
    assert [(s.name, s.status) for s in early.steps] == [("pick", StepStatus.RUNNING)]
    assert early.truncated_at == 1

    # The full log knows how it ended.
    whole = build_timeline("r1", events)
    assert whole.status is RunStatus.COMPLETED
    assert len(whole.steps) == 2


def test_an_unknown_run_has_no_timeline() -> None:
    with pytest.raises(KeyError):
        build_timeline("nope", [])


# -- over HTTP --------------------------------------------------------------


def _plane() -> tuple[ControlPlane, Registry]:
    async def pick(sku: str) -> Ack:
        return Ack()

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        await ctx.activity(pick, inp.sku, name="pick")
        return await ctx.activity(pick, inp.sku, name="pack")

    registry = Registry()
    registry.add(wf, name="fulfil")
    cp = build_control_plane(
        InMemoryEventStore(), registry, ledger=InMemoryCostLedger()
    )
    return cp, registry


def _http(cp: ControlPlane) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=create_app(cp)), base_url="http://t")


async def test_runs_can_be_listed_and_filtered() -> None:
    cp, _reg = _plane()
    async with _http(cp) as http:
        for tenant in ("acme", "acme", "globex"):
            await http.post(
                "/runs",
                json={"workflow": "fulfil", "input": {"sku": "A-1"}, "tenant": tenant},
            )
        while await cp.worker.run_once() is not None:
            pass

        listed = (await http.get("/runs")).json()
        assert listed["total"] == 3
        assert len(listed["runs"]) == 3
        assert {r["status"] for r in listed["runs"]} == {"completed"}

        by_tenant = (await http.get("/runs", params={"tenant": "acme"})).json()
        assert by_tenant["total"] == 2

        paged = (await http.get("/runs", params={"limit": 1, "offset": 2})).json()
        assert len(paged["runs"]) == 1 and paged["total"] == 3

        assert (await http.get("/runs", params={"status": "failed"})).json()["total"] == 0


async def test_timeline_endpoint_returns_steps_and_raw_events() -> None:
    cp, _reg = _plane()
    async with _http(cp) as http:
        run_id = (
            await http.post("/runs", json={"workflow": "fulfil", "input": {"sku": "A-1"}})
        ).json()["run_id"]
        await cp.worker.run_once()

        body = (await http.get(f"/runs/{run_id}/timeline")).json()

    assert body["status"] == "completed"
    assert [s["name"] for s in body["steps"]] == ["pick", "pack"]
    assert len(body["events"]) == body["event_count"]
    assert body["truncated_at"] is None


async def test_timeline_at_replays_to_a_point() -> None:
    cp, _reg = _plane()
    async with _http(cp) as http:
        run_id = (
            await http.post("/runs", json={"workflow": "fulfil", "input": {"sku": "A-1"}})
        ).json()["run_id"]
        await cp.worker.run_once()

        whole = (await http.get(f"/runs/{run_id}/timeline")).json()
        early = (await http.get(f"/runs/{run_id}/timeline", params={"at": 2})).json()

    assert whole["status"] == "completed" and len(whole["steps"]) == 2
    assert early["status"] == "running"  # it had not finished yet
    assert [s["name"] for s in early["steps"]] == ["pick"]
    assert early["truncated_at"] == 2
    assert len(early["events"]) == 3


async def test_the_tree_endpoint_shows_child_runs() -> None:
    async def double(ctx: WorkflowContext, inp: Order) -> Ack:
        return Ack()

    async def parent(ctx: WorkflowContext, inp: Order) -> Ack:
        children: list[Ack] = await ctx.children(
            "double", [Order(sku="a"), Order(sku="b")], concurrency=2
        )
        return children[0]

    registry = Registry()
    registry.add(double, name="double")
    registry.add(parent, name="parent")
    cp = build_control_plane(InMemoryEventStore(), registry)

    async with _http(cp) as http:
        run_id = (
            await http.post("/runs", json={"workflow": "parent", "input": {"sku": "x"}})
        ).json()["run_id"]
        while await cp.worker.run_once() is not None:
            pass

        tree = (await http.get(f"/runs/{run_id}/tree")).json()

    assert tree["workflow"] == "parent"
    assert tree["status"] == "completed"
    assert [c["workflow"] for c in tree["children"]] == ["double", "double"]
    assert [c["command_seq"] for c in tree["children"]] == [0, 1]


async def test_the_built_ui_is_served_beside_the_api(tmp_path: Path) -> None:
    """`flowforge api` serves one origin: the control plane and the debugger."""
    (tmp_path / "index.html").write_text("<!doctype html><title>debugger</title>")
    cp, _reg = _plane()
    app = create_app(cp, ui_dir=tmp_path)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as http:
        page = await http.get("/")
        assert page.status_code == 200
        assert "debugger" in page.text
        # The mount is last, so it does not swallow the API.
        assert (await http.get("/runs")).status_code == 200


async def test_without_a_build_there_is_no_ui_mount(tmp_path: Path) -> None:
    cp, _reg = _plane()
    app = create_app(cp, ui_dir=tmp_path / "never-built")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as http:
        assert (await http.get("/")).status_code == 404
        assert (await http.get("/runs")).status_code == 200


async def test_unknown_runs_are_404_everywhere() -> None:
    cp, _reg = _plane()
    async with _http(cp) as http:
        assert (await http.get("/runs/nope/timeline")).status_code == 404
        assert (await http.get("/runs/nope/tree")).status_code == 404
