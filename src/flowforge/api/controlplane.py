"""Assembles the runtime pieces a control plane serves over: engine, store,
registry, queue, worker, a budget guard, and (optionally) a timer wheel."""

from __future__ import annotations

from dataclasses import dataclass

from flowforge.core.budget import Budget, BudgetGuard, CostLedger
from flowforge.core.engine import Engine
from flowforge.core.event_store import EventStore
from flowforge.core.timers import TimerStore
from flowforge.queue.base import LockManager, TaskQueue
from flowforge.queue.memory import InMemoryLockManager, InMemoryTaskQueue
from flowforge.queue.worker import Worker
from flowforge.wheel import TimerWheel
from flowforge.workflow.definition import Registry


@dataclass
class ControlPlane:
    engine: Engine
    store: EventStore
    registry: Registry
    queue: TaskQueue
    worker: Worker
    wheel: TimerWheel | None = None
    budget: BudgetGuard | None = None


def build_control_plane(
    store: EventStore,
    registry: Registry,
    *,
    timers: TimerStore | None = None,
    queue: TaskQueue | None = None,
    locks: LockManager | None = None,
    ledger: CostLedger | None = None,
    budget: Budget | None = None,
    tenant_budgets: dict[str, Budget] | None = None,
) -> ControlPlane:
    """Wire the runtime. A ``ledger`` turns on cost accounting; a ``budget`` (or
    ``tenant_budgets``) additionally turns on enforcement."""
    queue = queue or InMemoryTaskQueue()
    locks = locks or InMemoryLockManager()
    guard = (
        BudgetGuard(ledger, default=budget, per_tenant=tenant_budgets)
        if ledger is not None
        else None
    )
    engine = Engine(store, registry, timers=timers, budget=guard)
    worker = Worker(engine, queue, locks)
    wheel = TimerWheel(engine, timers, queue) if timers is not None else None
    return ControlPlane(
        engine=engine,
        store=store,
        registry=registry,
        queue=queue,
        worker=worker,
        wheel=wheel,
        budget=guard,
    )
