"""Integration tests for durable trigger state against a real database.

Runs only when ``asyncpg`` is installed and ``DATABASE_URL`` points at a reachable
Postgres; otherwise skipped. The claim is the exactly-once boundary for webhooks
and cron, and a single-process test cannot prove it — these fire concurrent
deliveries at one key and assert the database picks exactly one winner.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from flowforge import InMemoryTaskQueue, Registry, RunStatus, WorkflowContext
from flowforge.api import build_control_plane
from flowforge.persistence import (
    PostgresCronStateStore,
    PostgresDeliveryStore,
    PostgresEventStore,
    apply_migrations,
)
from flowforge.triggers import CronScheduler, cron_trigger, webhook_trigger

pytest.importorskip("asyncpg")
DSN = os.environ.get("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="DATABASE_URL not set")


class Order(BaseModel):
    sku: str


class Ack(BaseModel):
    sku: str


async def _stores() -> tuple[PostgresEventStore, PostgresDeliveryStore, PostgresCronStateStore]:
    assert DSN is not None
    await apply_migrations(DSN)
    return (
        await PostgresEventStore.connect(DSN),
        await PostgresDeliveryStore.connect(DSN),
        await PostgresCronStateStore.connect(DSN),
    )


def _registry(handled: list[str]) -> tuple[Registry, str]:
    """A one-workflow registry; the name is unique so parallel runs of this suite
    never collide in the shared database."""

    async def record(sku: str) -> Ack:
        handled.append(sku)
        return Ack(sku=sku)

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        return await ctx.activity(record, inp.sku, name="record")

    registry = Registry()
    name = f"order_{uuid.uuid4().hex[:8]}"
    registry.add(wf, name=name)
    return registry, name


async def test_concurrent_deliveries_of_one_event_start_one_run() -> None:
    store, deliveries, cron_state = await _stores()
    try:
        handled: list[str] = []
        registry, workflow = _registry(handled)
        cp = build_control_plane(
            store, registry, queue=InMemoryTaskQueue(), deliveries=deliveries
        )
        cp.triggers.add(
            webhook_trigger(
                "order_placed",
                workflow,
                map=lambda event: Order(sku=str(event["sku"])),
                dedupe=lambda event: str(event["order_id"]),
            )
        )
        event = {"sku": "A-1", "order_id": uuid.uuid4().hex}

        # Ten workers receive the same webhook retry at the same moment.
        results = await asyncio.gather(
            *(cp.dispatcher.fire("order_placed", dict(event)) for _ in range(10))
        )

        assert sum(1 for r in results if r.started) == 1
        assert len({r.run_id for r in results}) == 1  # all agree on the winner

        while await cp.worker.run_once() is not None:
            pass
        assert handled == ["A-1"]  # one run, one side effect
        assert (await cp.engine.describe(results[0].run_id)).status is RunStatus.COMPLETED
    finally:
        await store.close()
        await deliveries.close()
        await cron_state.close()


async def test_claim_survives_a_reconnect() -> None:
    store, deliveries, cron_state = await _stores()
    try:
        trigger = f"t-{uuid.uuid4().hex[:8]}"
        run_id, won = await deliveries.claim(trigger, "key-1", "run-a")
        assert (run_id, won) == ("run-a", True)

        # A different process, a different pool, the same durable claim.
        assert DSN is not None
        other = await PostgresDeliveryStore.connect(DSN)
        try:
            assert await other.claim(trigger, "key-1", "run-b") == ("run-a", False)
            assert await other.claimed_run(trigger, "key-1") == "run-a"
            assert await other.claimed_run(trigger, "never") is None
        finally:
            await other.close()
    finally:
        await store.close()
        await deliveries.close()
        await cron_state.close()


async def test_cron_cursor_is_durable_and_never_moves_backwards() -> None:
    store, deliveries, cron_state = await _stores()
    try:
        trigger = f"sweep-{uuid.uuid4().hex[:8]}"
        assert await cron_state.last_fired(trigger) is None

        noon = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
        await cron_state.set_last_fired(trigger, noon)
        assert await cron_state.last_fired(trigger) == noon

        # A scheduler running behind its peer must not rewind the fleet.
        await cron_state.set_last_fired(trigger, noon - timedelta(hours=3))
        assert await cron_state.last_fired(trigger) == noon

        await cron_state.set_last_fired(trigger, noon + timedelta(hours=1))
        assert await cron_state.last_fired(trigger) == noon + timedelta(hours=1)
    finally:
        await store.close()
        await deliveries.close()
        await cron_state.close()


async def test_cron_catchup_across_a_restart_fires_each_tick_once() -> None:
    store, deliveries, cron_state = await _stores()
    try:
        handled: list[str] = []
        registry, workflow = _registry(handled)
        cp = build_control_plane(
            store,
            registry,
            queue=InMemoryTaskQueue(),
            deliveries=deliveries,
            cron_state=cron_state,
        )
        name = f"sweep-{uuid.uuid4().hex[:8]}"
        cp.triggers.add(
            cron_trigger(
                name,
                workflow,
                "0 * * * *",
                map=lambda event: Order(sku=str(event["fire_at"])),
            )
        )

        now = datetime(2026, 3, 1, 10, 30, tzinfo=UTC)
        scheduler = CronScheduler(cp.dispatcher, cp.triggers, cron_state, clock=lambda: now)
        assert await scheduler.tick() == []  # armed

        # Two hours pass, and the process restarts: a brand-new scheduler reads
        # the cursor out of Postgres and owes the ticks it was down for.
        now = datetime(2026, 3, 1, 12, 30, tzinfo=UTC)
        restarted = CronScheduler(cp.dispatcher, cp.triggers, cron_state, clock=lambda: now)
        first_pass = await restarted.tick()
        assert [d.started for d in first_pass] == [True, True]

        # A third scheduler catching up over the same state adds nothing.
        again = CronScheduler(cp.dispatcher, cp.triggers, cron_state, clock=lambda: now)
        assert await again.tick() == []

        while await cp.worker.run_once() is not None:
            pass
        assert handled == [
            datetime(2026, 3, 1, hour, 0, tzinfo=UTC).isoformat() for hour in (11, 12)
        ]
    finally:
        await store.close()
        await deliveries.close()
        await cron_state.close()
