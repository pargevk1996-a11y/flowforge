"""Behavioural tests for the durable execution core.

These cover the properties that separate a real durable engine from a demo:
exactly-once side effects across replay, resume after a mid-run crash,
suspend/resume via timers and signals, typed retry, and saga compensation.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from flowforge import (
    Engine,
    InMemoryEventStore,
    NonRetryableError,
    Registry,
    RetryPolicy,
    RunStatus,
    WorkflowContext,
)


class Money(BaseModel):
    amount: int


class Receipt(BaseModel):
    txn: str


@pytest.fixture
def store() -> InMemoryEventStore:
    return InMemoryEventStore()


# --------------------------------------------------------------------------
# Idempotency: a completed activity is never re-executed on a later drive.
# --------------------------------------------------------------------------
async def test_activity_runs_exactly_once_across_redrive(store: InMemoryEventStore) -> None:
    calls: list[int] = []

    async def charge(amount: int) -> str:
        calls.append(amount)
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    res = await engine.start("r1", definition, Money(amount=100))
    assert res.status is RunStatus.COMPLETED
    assert res.result == Receipt(txn="txn-100")

    # Re-driving a finished run must not re-run the side effect.
    again = await engine.drive("r1")
    assert again.status is RunStatus.COMPLETED
    assert calls == [100]


# --------------------------------------------------------------------------
# Crash / resume: kill mid-run, resume without repeating committed side effects.
# --------------------------------------------------------------------------
async def test_resume_after_crash_does_not_repeat_committed_steps(
    store: InMemoryEventStore,
) -> None:
    a_calls: list[int] = []
    b_calls: list[int] = []
    crash = {"armed": True}

    async def step_a(x: int) -> int:
        a_calls.append(x)
        return x + 1

    async def step_b(x: int) -> int:
        if crash["armed"]:
            crash["armed"] = False
            raise KeyboardInterrupt("simulated kill -9 after step_a committed")
        b_calls.append(x)
        return x * 10

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        a = await ctx.activity(step_a, inp.amount)
        b = await ctx.activity(step_b, a)
        return Receipt(txn=str(b))

    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    # First drive crashes inside step_b (BaseException escapes the engine).
    with pytest.raises(KeyboardInterrupt):
        await engine.start("r1", definition, Money(amount=4))

    # Resume: step_a is replayed from the log (not re-run); step_b runs to success.
    res = await engine.drive("r1")
    assert res.status is RunStatus.COMPLETED
    assert res.result == Receipt(txn="50")
    assert a_calls == [4]  # committed step ran exactly once
    assert b_calls == [5]  # crashed step ran once on resume


# --------------------------------------------------------------------------
# Durable sleep: run suspends, a timer wakes it.
# --------------------------------------------------------------------------
async def test_sleep_suspends_then_timer_resumes(store: InMemoryEventStore) -> None:
    charged: list[int] = []

    async def charge(amount: int) -> str:
        charged.append(amount)
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        await ctx.sleep(3600)
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    res = await engine.start("r1", definition, Money(amount=7))
    assert res.status is RunStatus.SUSPENDED
    assert charged == []  # nothing past the sleep has run

    res = await engine.fire_timer("r1")
    assert res.status is RunStatus.COMPLETED
    assert charged == [7]


# --------------------------------------------------------------------------
# Human-in-the-loop: run waits for a typed signal and continues on delivery.
# --------------------------------------------------------------------------
class Approval(BaseModel):
    approved: bool
    approver: str


async def test_wait_for_signal_human_in_the_loop(store: InMemoryEventStore) -> None:
    async def charge(amount: int) -> str:
        return f"txn-{amount}"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        decision = await ctx.wait_for_signal("cfo_approval", Approval)
        if not decision.approved:
            return Receipt(txn="rejected")
        return Receipt(txn=await ctx.activity(charge, inp.amount))

    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    res = await engine.start("r1", definition, Money(amount=20_000))
    assert res.status is RunStatus.SUSPENDED

    res = await engine.send_signal(
        "r1", "cfo_approval", Approval(approved=True, approver="cfo@acme").model_dump()
    )
    assert res.status is RunStatus.COMPLETED
    assert res.result == Receipt(txn="txn-20000")


# --------------------------------------------------------------------------
# Typed retry: transient failures are retried; the activity eventually succeeds.
# --------------------------------------------------------------------------
async def test_transient_failure_is_retried(store: InMemoryEventStore) -> None:
    attempts = {"n": 0}

    async def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("provider hiccup")
        return "ok"

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        policy = RetryPolicy(max_attempts=5, initial_backoff=0.0)
        return Receipt(txn=await ctx.activity(flaky, retry=policy))

    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    res = await engine.start("r1", definition, Money(amount=1))
    assert res.status is RunStatus.COMPLETED
    assert attempts["n"] == 3


# --------------------------------------------------------------------------
# Saga: an unrecoverable step triggers compensations in reverse order.
# --------------------------------------------------------------------------
async def test_saga_compensations_run_in_reverse(store: InMemoryEventStore) -> None:
    undo: list[str] = []

    async def reserve() -> str:
        return "reserved"

    async def undo_reserve() -> None:
        undo.append("undo_reserve")

    async def debit() -> str:
        return "debited"

    async def undo_debit() -> None:
        undo.append("undo_debit")

    async def create_order() -> str:
        raise NonRetryableError("accounting system rejected the order")

    async def wf(ctx: WorkflowContext, inp: Money) -> Receipt:
        await ctx.activity(reserve, compensate=undo_reserve)
        await ctx.activity(debit, compensate=undo_debit)
        await ctx.activity(create_order)  # fails -> rollback
        return Receipt(txn="unreachable")

    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    res = await engine.start("r1", definition, Money(amount=500))
    assert res.status is RunStatus.FAILED
    assert res.error is not None and "create_order" in res.error
    assert undo == ["undo_debit", "undo_reserve"]
