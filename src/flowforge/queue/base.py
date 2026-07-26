"""Queue and lock abstractions the worker depends on.

The worker is written against these protocols only, so the in-memory
implementations (tests, single-process runs) and the Redis-backed ones
(production) are interchangeable. The lock gives a run a single writer at a time,
which is what makes optimistic concurrency on the event store a rare fallback
rather than the hot path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class QueueItem:
    run_id: str
    priority: int = 0
    tenant: str = "default"


class TaskQueue(Protocol):
    async def enqueue(
        self, run_id: str, *, priority: int = 0, tenant: str = "default"
    ) -> None:
        """Enqueue a run. Higher ``priority`` is dequeued first; ties are FIFO."""
        ...

    async def dequeue(self) -> QueueItem | None:
        """Pop the most urgent ready item, or ``None`` if the queue is empty."""
        ...

    async def size(self) -> int:
        ...


class LockManager(Protocol):
    async def acquire(self, key: str, *, ttl: float = 30.0) -> str | None:
        """Acquire the lock for ``key``; return an opaque token, or ``None`` if it
        is already held. The lock auto-expires after ``ttl`` seconds so a crashed
        worker cannot wedge a run forever."""
        ...

    async def release(self, key: str, token: str) -> bool:
        """Release the lock iff ``token`` still owns it. Returns whether it did."""
        ...
