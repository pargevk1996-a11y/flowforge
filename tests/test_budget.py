"""Tests for per-tenant cost budgets — the executable spec for cost control.

The properties: every provider call is billed to the tenant recorded on the run;
replay never re-bills; a tenant that runs out of budget cannot start another call
and its run is cancelled *and compensated* rather than left half-done; tenants
cannot spend each other's money; and the window rolls.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
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
from flowforge.api import build_control_plane, create_app
from flowforge.llm import LLMStep, ModelPrice, Pricing, ScriptedLLMClient
from workflows.invoice_to_payment import InvoiceServices, build_invoice_to_payment

# ScriptedLLMClient bills 10 prompt + 10 completion tokens per call, so at these
# prices every call costs exactly $0.02 — small enough to reason about by hand.
_PER_CALL = 0.02


class Doc(BaseModel):
    text: str


class Fields(BaseModel):
    vendor: str


class _FakeClock:
    """A clock the test moves by hand, so window expiry is not a matter of luck."""

    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


def _pricing() -> Pricing:
    return Pricing({"m": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)})


def _step(*responses: str) -> tuple[LLMStep[Fields], ScriptedLLMClient]:
    client = ScriptedLLMClient(list(responses))
    step = LLMStep(client, "m", Fields, pricing=_pricing(), name="extract")
    return step, client


def _ok(vendor: str = "Acme") -> str:
    return json.dumps({"vendor": vendor})


def _guard(
    limit: float | None,
    *,
    clock: _FakeClock | None = None,
    per_tenant: dict[str, Budget] | None = None,
    window: timedelta = timedelta(days=1),
) -> tuple[BudgetGuard, InMemoryCostLedger]:
    clock = clock or _FakeClock()
    ledger = InMemoryCostLedger(clock=clock)
    guard = BudgetGuard(
        ledger,
        default=Budget(limit_usd=limit, window=window) if limit is not None else None,
        per_tenant=per_tenant,
        clock=clock,
    )
    return guard, ledger


async def test_calls_are_billed_to_the_run_tenant() -> None:
    step, _client = _step(_ok())
    guard, ledger = _guard(limit=None)  # accounting, no enforcement

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        return await ctx.llm(step, inp.text)

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    res = await engine.start("r1", definition, Doc(text="bill me"), tenant="acme")
    assert res.status is RunStatus.COMPLETED

    (_at, entry), = ledger.entries
    assert entry.tenant == "acme"
    assert entry.run_id == "r1"
    assert entry.model == "m"
    assert entry.provider == "openai"
    assert entry.command_seq == 0  # the ledger row points at the command that spent
    assert entry.usd == pytest.approx(_PER_CALL)
    assert await guard.spent("acme") == pytest.approx(_PER_CALL)
    assert await guard.remaining("acme") is None  # uncapped tenant


async def test_replay_does_not_bill_twice() -> None:
    step, _client = _step(_ok())
    guard, ledger = _guard(limit=None)

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        return await ctx.llm(step, inp.text)

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    await engine.start("r1", definition, Doc(text="once"), tenant="acme")
    assert len(ledger.entries) == 1

    # Re-driving replays the recorded result: no model call, and no second charge.
    again = await engine.drive("r1")
    assert again.status is RunStatus.COMPLETED
    assert len(ledger.entries) == 1
    assert await guard.spent("acme") == pytest.approx(_PER_CALL)


async def test_schema_retries_are_billed_individually() -> None:
    # First response is invalid, so the step pays for two calls, not one.
    step, _client = _step('{"vendor": 17}', _ok())
    guard, ledger = _guard(limit=None)

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        return await ctx.llm(step, inp.text)

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    res = await engine.start("r1", definition, Doc(text="retry"), tenant="acme")
    assert res.status is RunStatus.COMPLETED
    assert len(ledger.entries) == 2
    assert await guard.spent("acme") == pytest.approx(2 * _PER_CALL)


async def test_exhausted_budget_cancels_the_run_and_compensates() -> None:
    charged: list[str] = []
    refunded: list[str] = []

    async def charge(ref: str) -> str:
        charged.append(ref)
        return ref

    async def refund() -> None:
        refunded.append(charged[-1])

    step, client = _step(_ok(), _ok())
    guard, _ledger = _guard(limit=_PER_CALL)  # room for exactly one call

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        await ctx.activity(charge, "txn-1", compensate=refund)
        await ctx.llm(step, inp.text, name="first")
        return await ctx.llm(step, inp.text, name="second")  # over budget

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    res = await engine.start("r1", definition, Doc(text="spend"), tenant="acme")

    assert res.status is RunStatus.FAILED
    assert res.error is not None and "budget" in res.error
    # The refused call never reached the provider...
    assert len(client.calls) == 1
    # ...and the work already done was rolled back rather than abandoned.
    assert refunded == ["txn-1"]


async def test_tenants_do_not_spend_each_others_budget() -> None:
    step, _client = _step(_ok(), _ok(), _ok())
    guard, _ledger = _guard(limit=_PER_CALL)

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        return await ctx.llm(step, inp.text)

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    # acme spends its whole allowance on one run...
    assert (
        await engine.start("r1", definition, Doc(text="a"), tenant="acme")
    ).status is RunStatus.COMPLETED
    # ...and is then cut off,
    assert (
        await engine.start("r2", definition, Doc(text="a"), tenant="acme")
    ).status is RunStatus.FAILED
    # while globex is untouched by its neighbour.
    assert (
        await engine.start("r3", definition, Doc(text="g"), tenant="globex")
    ).status is RunStatus.COMPLETED
    assert await guard.spent("globex") == pytest.approx(_PER_CALL)


async def test_budget_window_rolls_forward() -> None:
    clock = _FakeClock()
    step, _client = _step(_ok(), _ok())
    guard, _ledger = _guard(limit=_PER_CALL, clock=clock, window=timedelta(hours=1))

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        return await ctx.llm(step, inp.text)

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    assert (
        await engine.start("r1", definition, Doc(text="now"), tenant="acme")
    ).status is RunStatus.COMPLETED
    assert (
        await engine.start("r2", definition, Doc(text="now"), tenant="acme")
    ).status is RunStatus.FAILED  # still inside the window

    clock.advance(timedelta(hours=1, seconds=1))  # the spend ages out
    assert await guard.spent("acme") == 0.0
    assert (
        await engine.start("r3", definition, Doc(text="later"), tenant="acme")
    ).status is RunStatus.COMPLETED


async def test_per_tenant_limit_overrides_the_default() -> None:
    step, _client = _step(*[_ok()] * 4)
    guard, _ledger = _guard(
        limit=_PER_CALL, per_tenant={"whale": Budget(limit_usd=10.0)}
    )

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        await ctx.llm(step, inp.text, name="first")
        return await ctx.llm(step, inp.text, name="second")

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg, budget=guard)

    # Two calls fit the whale's $10, but not the default $0.02.
    assert (
        await engine.start("r1", definition, Doc(text="x"), tenant="whale")
    ).status is RunStatus.COMPLETED
    assert (
        await engine.start("r2", definition, Doc(text="x"), tenant="minnow")
    ).status is RunStatus.FAILED
    assert await guard.remaining("whale") == pytest.approx(10.0 - 2 * _PER_CALL)


async def test_without_a_ledger_nothing_is_metered() -> None:
    step, _client = _step(_ok(), _ok())

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        await ctx.llm(step, inp.text, name="first")
        return await ctx.llm(step, inp.text, name="second")

    reg = Registry()
    definition = reg.add(wf)
    engine = Engine(InMemoryEventStore(), reg)  # no budget guard at all

    res = await engine.start("r1", definition, Doc(text="free"), tenant="acme")
    assert res.status is RunStatus.COMPLETED


# -- through the control plane ---------------------------------------------


def _invoice_json(amount: float) -> str:
    return json.dumps(
        {"vendor": "Acme", "invoice_number": "INV-1", "amount": amount, "currency": "USD"}
    )


async def test_control_plane_reports_spend_and_refuses_exhausted_tenants() -> None:
    services = InvoiceServices()
    client = ScriptedLLMClient([_invoice_json(500), _invoice_json(500)])
    registry = Registry()
    build_invoice_to_payment(
        registry,
        llm_client=client,
        services=services,
        pricing=Pricing({"gpt-4o-mini": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
    )
    cp = build_control_plane(
        InMemoryEventStore(),
        registry,
        ledger=InMemoryCostLedger(),
        budget=Budget(limit_usd=_PER_CALL),
    )
    start = {"workflow": "invoice_to_payment", "input": {"pdf_url": "s3://inv"}}

    transport = ASGITransport(app=create_app(cp))
    async with AsyncClient(transport=transport, base_url="http://t") as http:
        first = await http.post("/runs", json={**start, "tenant": "acme"})
        assert first.status_code == 200
        await cp.worker.run_once()

        spend = (await http.get("/tenants/acme/spend")).json()
        assert spend["spent_usd"] == pytest.approx(_PER_CALL)
        assert spend["limit_usd"] == pytest.approx(_PER_CALL)
        assert spend["remaining_usd"] == pytest.approx(0.0)

        # Admission control: the exhausted tenant cannot even start a run.
        refused = await http.post("/runs", json={**start, "tenant": "acme"})
        assert refused.status_code == 402
        assert "budget" in refused.json()["detail"]

        # A different tenant is unaffected.
        assert (await http.post("/runs", json={**start, "tenant": "globex"})).status_code == 200
