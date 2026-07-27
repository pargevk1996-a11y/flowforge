"""Assembles the runtime pieces a control plane serves over: engine, store,
registry, queue, worker, budget guard, trigger dispatcher, and the two background
loops (timer wheel, cron scheduler)."""

from __future__ import annotations

from dataclasses import dataclass

from flowforge.core.budget import Budget, BudgetGuard, CostLedger
from flowforge.core.engine import Engine
from flowforge.core.event_store import EventStore
from flowforge.core.timers import TimerStore
from flowforge.queue.base import LockManager, TaskQueue
from flowforge.queue.memory import InMemoryLockManager, InMemoryTaskQueue
from flowforge.queue.worker import Worker
from flowforge.triggers.base import TriggerRegistry
from flowforge.triggers.cron import CronScheduler, CronStateStore
from flowforge.triggers.deliveries import DeliveryStore, InMemoryDeliveryStore
from flowforge.triggers.dispatch import TriggerDispatcher
from flowforge.wheel import TimerWheel
from flowforge.workflow.definition import Registry


@dataclass
class ControlPlane:
    engine: Engine
    store: EventStore
    registry: Registry
    queue: TaskQueue
    worker: Worker
    triggers: TriggerRegistry
    dispatcher: TriggerDispatcher
    cron: CronScheduler
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
    triggers: TriggerRegistry | None = None,
    deliveries: DeliveryStore | None = None,
    cron_state: CronStateStore | None = None,
) -> ControlPlane:
    """Wire the runtime. A ``ledger`` turns on cost accounting; a ``budget`` (or
    ``tenant_budgets``) additionally turns on enforcement.

    The trigger registry is built empty and filled by the caller, so a trigger
    registered after start-up still schedules and dispatches — the cron loop reads
    the registry on every tick rather than snapshotting it here."""
    queue = queue or InMemoryTaskQueue()
    locks = locks or InMemoryLockManager()
    triggers = triggers or TriggerRegistry()
    guard = (
        BudgetGuard(ledger, default=budget, per_tenant=tenant_budgets)
        if ledger is not None
        else None
    )
    engine = Engine(store, registry, timers=timers, budget=guard, queue=queue)
    worker = Worker(engine, queue, locks)
    wheel = TimerWheel(engine, timers, queue) if timers is not None else None
    dispatcher = TriggerDispatcher(
        engine,
        queue,
        registry,
        triggers,
        deliveries=deliveries or InMemoryDeliveryStore(),
        budget=guard,
    )
    return ControlPlane(
        engine=engine,
        store=store,
        registry=registry,
        queue=queue,
        worker=worker,
        triggers=triggers,
        dispatcher=dispatcher,
        cron=CronScheduler(dispatcher, triggers, cron_state),
        wheel=wheel,
        budget=guard,
    )
