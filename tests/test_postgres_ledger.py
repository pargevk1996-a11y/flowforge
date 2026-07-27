"""Integration tests for the Postgres cost ledger against a real database.

Runs only when ``asyncpg`` is installed and ``DATABASE_URL`` points at a reachable
Postgres; otherwise skipped. These prove the money side for real: charges survive
in ``cost_ledger`` with their precision intact, the rolling-window sum is what the
guard enforces on, the run row carries the tenant it is billed to, and a tenant
that runs out of budget has its run cancelled *and compensated*.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import timedelta

import pytest
from pydantic import BaseModel

from flowforge import (
    Budget,
    BudgetGuard,
    CostEntry,
    Engine,
    Registry,
    RunStatus,
    WorkflowContext,
)
from flowforge.core.events import utcnow
from flowforge.llm import LLMStep, ModelPrice, Pricing, ScriptedLLMClient
from flowforge.persistence import (
    PostgresCostLedger,
    PostgresEventStore,
    apply_migrations,
)

pytest.importorskip("asyncpg")
DSN = os.environ.get("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="DATABASE_URL not set")

_PER_CALL = 0.02  # ScriptedLLMClient's 10+10 tokens at $1/1k in and out


class Doc(BaseModel):
    text: str


class Fields(BaseModel):
    vendor: str


async def _connect() -> tuple[PostgresEventStore, PostgresCostLedger]:
    assert DSN is not None
    await apply_migrations(DSN)
    return await PostgresEventStore.connect(DSN), await PostgresCostLedger.connect(DSN)


def _step(*responses: str) -> LLMStep[Fields]:
    return LLMStep(
        ScriptedLLMClient(list(responses)),
        "m",
        Fields,
        pricing=Pricing({"m": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
        name="extract",
    )


def _ok() -> str:
    return json.dumps({"vendor": "Acme"})


async def _seed_run(store: PostgresEventStore, tenant: str) -> tuple[str, Registry]:
    """A run row must exist before it can be charged — the ledger references it."""

    async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
        return Fields(vendor=inp.text)

    reg = Registry()
    definition = reg.add(wf, name=f"noop_{uuid.uuid4().hex[:8]}")
    run_id = uuid.uuid4().hex
    await Engine(store, reg).create_run(run_id, definition, Doc(text="x"), tenant=tenant)
    return run_id, reg


async def test_charges_persist_and_sum_over_the_window() -> None:
    store, ledger = await _connect()
    try:
        tenant = f"acme-{uuid.uuid4().hex[:8]}"  # isolated from other runs of the suite
        run_id, _reg = await _seed_run(store, tenant)

        await ledger.record(
            CostEntry(
                run_id=run_id,
                tenant=tenant,
                model="gpt-4o-mini",
                usd=0.123456,
                command_seq=1,
                provider="openai",
            )
        )
        await ledger.record(
            CostEntry(run_id=run_id, tenant=tenant, model="gpt-4o-mini", usd=0.1)
        )

        window_start = utcnow() - timedelta(hours=1)
        assert await ledger.spend_since(tenant, window_start) == pytest.approx(0.223456)
        # Nothing was charged to a neighbour, and nothing falls inside a future window.
        assert await ledger.spend_since("someone-else", window_start) == 0.0
        assert await ledger.spend_since(tenant, utcnow() + timedelta(minutes=1)) == 0.0
    finally:
        await store.close()
        await ledger.close()


async def test_run_row_carries_its_tenant() -> None:
    store, ledger = await _connect()
    try:
        tenant = f"globex-{uuid.uuid4().hex[:8]}"
        run_id, _reg = await _seed_run(store, tenant)

        rows = await store._pool.fetch(
            "SELECT tenant_id FROM workflow_runs WHERE run_id = $1", run_id
        )
        assert [row["tenant_id"] for row in rows] == [tenant]
    finally:
        await store.close()
        await ledger.close()


async def test_budget_over_postgres_cancels_and_compensates() -> None:
    store, ledger = await _connect()
    try:
        tenant = f"tight-{uuid.uuid4().hex[:8]}"
        refunded: list[str] = []
        step = _step(_ok(), _ok())

        async def charge(ref: str) -> str:
            return ref

        async def refund() -> None:
            refunded.append("txn-1")

        async def wf(ctx: WorkflowContext, inp: Doc) -> Fields:
            await ctx.activity(charge, "txn-1", compensate=refund)
            await ctx.llm(step, inp.text, name="first")
            return await ctx.llm(step, inp.text, name="second")

        guard = BudgetGuard(ledger, default=Budget(limit_usd=_PER_CALL))
        reg = Registry()
        definition = reg.add(wf, name=f"budgeted_{uuid.uuid4().hex[:8]}")
        engine = Engine(store, reg, budget=guard)

        res = await engine.start(uuid.uuid4().hex, definition, Doc(text="spend"), tenant=tenant)

        assert res.status is RunStatus.FAILED
        assert res.error is not None and "budget" in res.error
        assert refunded == ["txn-1"]  # rolled back, not left half-done
        # Exactly one call was billed; the second was refused before it was made.
        assert await guard.spent(tenant) == pytest.approx(_PER_CALL)
    finally:
        await store.close()
        await ledger.close()
