"""In-memory queue and lock — for tests and single-process runs."""

from __future__ import annotations

import heapq
import time
from collections.abc import Callable
from uuid import uuid4

from flowforge.queue.base import QueueItem


class InMemoryTaskQueue:
    """Priority queue with FIFO tie-breaking (a stable min-heap)."""

    def __init__(self) -> None:
        # (-priority, insertion_seq, run_id, tenant): highest priority, then oldest.
        self._heap: list[tuple[int, int, str, str]] = []
        self._seq = 0

    async def enqueue(
        self, run_id: str, *, priority: int = 0, tenant: str = "default"
    ) -> None:
        heapq.heappush(self._heap, (-priority, self._seq, run_id, tenant))
        self._seq += 1

    async def dequeue(self) -> QueueItem | None:
        if not self._heap:
            return None
        neg_priority, _, run_id, tenant = heapq.heappop(self._heap)
        return QueueItem(run_id=run_id, priority=-neg_priority, tenant=tenant)

    async def size(self) -> int:
        return len(self._heap)


class InMemoryLockManager:
    """TTL lock. ``clock`` is injectable so tests can exercise expiry."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._locks: dict[str, tuple[str, float]] = {}
        self._clock = clock

    async def acquire(self, key: str, *, ttl: float = 30.0) -> str | None:
        now = self._clock()
        held = self._locks.get(key)
        if held is not None and held[1] > now:
            return None
        token = uuid4().hex
        self._locks[key] = (token, now + ttl)
        return token

    async def release(self, key: str, token: str) -> bool:
        held = self._locks.get(key)
        if held is not None and held[0] == token:
            del self._locks[key]
            return True
        return False
