"""Redis-backed queue and lock (production implementations of the protocols).

``redis`` is imported lazily so the package works in-memory without it installed.
The queue is a sorted set scored so that higher priority pops first and ties are
FIFO; the lock is ``SET NX PX`` with a compare-and-delete release, the standard
safe single-holder pattern.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from flowforge.queue.base import QueueItem

_PRIORITY_SCALE = 1_000_000_000_000
_MEMBER_SEP = "\x00"

# Delete the key only if we still own it (compare-and-delete).
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


def _open(url: str) -> Any:
    import redis.asyncio as aioredis

    return aioredis.from_url(url, decode_responses=True)


class RedisTaskQueue:
    def __init__(self, client: Any, *, key: str = "flowforge:queue") -> None:
        self._redis = client
        self._key = key
        self._seq_key = f"{key}:seq"

    @classmethod
    def from_url(cls, url: str, *, key: str = "flowforge:queue") -> RedisTaskQueue:
        return cls(_open(url), key=key)

    async def enqueue(
        self, run_id: str, *, priority: int = 0, tenant: str = "default"
    ) -> None:
        seq = int(await self._redis.incr(self._seq_key))
        # Smaller score = popped first: negate priority, add seq for FIFO ties.
        score = -priority * _PRIORITY_SCALE + seq
        member = _MEMBER_SEP.join((run_id, tenant, str(priority)))
        await self._redis.zadd(self._key, {member: score})

    async def dequeue(self) -> QueueItem | None:
        popped = await self._redis.zpopmin(self._key, 1)
        if not popped:
            return None
        member, _score = popped[0]
        run_id, tenant, priority = str(member).split(_MEMBER_SEP)
        return QueueItem(run_id=run_id, priority=int(priority), tenant=tenant)

    async def size(self) -> int:
        return int(await self._redis.zcard(self._key))


class RedisLockManager:
    def __init__(self, client: Any, *, prefix: str = "flowforge:lock:") -> None:
        self._redis = client
        self._prefix = prefix

    @classmethod
    def from_url(cls, url: str, *, prefix: str = "flowforge:lock:") -> RedisLockManager:
        return cls(_open(url), prefix=prefix)

    async def acquire(self, key: str, *, ttl: float = 30.0) -> str | None:
        token = uuid4().hex
        ok = await self._redis.set(
            self._prefix + key, token, nx=True, px=int(ttl * 1000)
        )
        return token if ok else None

    async def release(self, key: str, token: str) -> bool:
        result = await self._redis.eval(_RELEASE_LUA, 1, self._prefix + key, token)
        return bool(result)
