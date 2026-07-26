"""Work queue, distributed locks, and the durable worker loop.

Only the protocols and in-memory implementations are exported here; the Redis
adapters live in :mod:`flowforge.queue.redis` and are imported explicitly by
callers that have ``redis`` installed, so importing this package never requires it.
"""

from __future__ import annotations

from flowforge.queue.base import LockManager, QueueItem, TaskQueue
from flowforge.queue.memory import InMemoryLockManager, InMemoryTaskQueue
from flowforge.queue.worker import Worker, submit

__all__ = [
    "InMemoryLockManager",
    "InMemoryTaskQueue",
    "LockManager",
    "QueueItem",
    "TaskQueue",
    "Worker",
    "submit",
]
