"""Assembles the runtime pieces a control plane serves over: engine, store,
registry, queue, worker, and (optionally) a timer wheel."""

from __future__ import annotations

from dataclasses import dataclass

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


def build_control_plane(
    store: EventStore,
    registry: Registry,
    *,
    timers: TimerStore | None = None,
    queue: TaskQueue | None = None,
    locks: LockManager | None = None,
) -> ControlPlane:
    queue = queue or InMemoryTaskQueue()
    locks = locks or InMemoryLockManager()
    engine = Engine(store, registry, timers=timers)
    worker = Worker(engine, queue, locks)
    wheel = TimerWheel(engine, timers, queue) if timers is not None else None
    return ControlPlane(
        engine=engine,
        store=store,
        registry=registry,
        queue=queue,
        worker=worker,
        wheel=wheel,
    )
