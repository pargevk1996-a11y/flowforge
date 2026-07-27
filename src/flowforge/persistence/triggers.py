"""Postgres-backed trigger state: delivery claims and cron cursors.

``claim`` is a single ``INSERT ... ON CONFLICT DO NOTHING RETURNING``: the winner
gets its row back, the loser gets nothing and then reads the winner's run id.
There is no check-then-act window for a racing delivery to slip through, which is
the entire point of doing this in the database rather than in the process.

``asyncpg`` is imported lazily.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class PostgresDeliveryStore:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def connect(
        cls, dsn: str, *, min_size: int = 1, max_size: int = 5
    ) -> PostgresDeliveryStore:
        import asyncpg

        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def claim(self, trigger: str, key: str, run_id: str) -> tuple[str, bool]:
        won = await self._pool.fetchval(
            "INSERT INTO trigger_deliveries (trigger_name, dedupe_key, run_id) "
            "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING run_id",
            trigger,
            key,
            run_id,
        )
        if won is not None:
            return str(won), True
        existing = await self.claimed_run(trigger, key)
        # The row can only be missing if someone deleted it between the two
        # statements; treat that as "still ours" rather than dropping the event.
        return (existing, False) if existing is not None else (run_id, True)

    async def claimed_run(self, trigger: str, key: str) -> str | None:
        run_id = await self._pool.fetchval(
            "SELECT run_id FROM trigger_deliveries "
            "WHERE trigger_name = $1 AND dedupe_key = $2",
            trigger,
            key,
        )
        return str(run_id) if run_id is not None else None


class PostgresCronStateStore:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def connect(
        cls, dsn: str, *, min_size: int = 1, max_size: int = 5
    ) -> PostgresCronStateStore:
        import asyncpg

        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def last_fired(self, trigger: str) -> datetime | None:
        at: datetime | None = await self._pool.fetchval(
            "SELECT last_fired FROM cron_state WHERE trigger_name = $1", trigger
        )
        return at

    async def set_last_fired(self, trigger: str, at: datetime) -> None:
        # Never move a cursor backwards: a scheduler running behind its peer must
        # not make the fleet replay ticks the peer has already dispatched.
        await self._pool.execute(
            "INSERT INTO cron_state (trigger_name, last_fired) VALUES ($1, $2) "
            "ON CONFLICT (trigger_name) DO UPDATE "
            "SET last_fired = GREATEST(cron_state.last_fired, EXCLUDED.last_fired), "
            "    updated_at = now()",
            trigger,
            at,
        )
