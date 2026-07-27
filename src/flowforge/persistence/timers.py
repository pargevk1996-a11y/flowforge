"""Postgres-backed timer store (the ``timers`` table).

``schedule`` uses ``ON CONFLICT DO NOTHING`` so replay never duplicates a timer,
and ``due`` uses ``FOR UPDATE SKIP LOCKED`` so multiple timer wheels can share the
load without firing the same timer twice. ``asyncpg`` is imported lazily.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from flowforge.core.timers import DueTimer


class PostgresTimerStore:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def connect(
        cls, dsn: str, *, min_size: int = 1, max_size: int = 5
    ) -> PostgresTimerStore:
        import asyncpg

        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def schedule(self, run_id: str, command_seq: int, fire_at: datetime) -> None:
        await self._pool.execute(
            "INSERT INTO timers (run_id, command_seq, fire_at) VALUES ($1, $2, $3) "
            "ON CONFLICT (run_id, command_seq) DO NOTHING",
            run_id,
            command_seq,
            fire_at,
        )

    async def due(self, now: datetime, *, limit: int = 100) -> list[DueTimer]:
        rows = await self._pool.fetch(
            "SELECT run_id, command_seq, fire_at FROM timers "
            "WHERE NOT fired AND fire_at <= $1 ORDER BY fire_at LIMIT $2 "
            "FOR UPDATE SKIP LOCKED",
            now,
            limit,
        )
        return [
            DueTimer(
                run_id=row["run_id"],
                command_seq=row["command_seq"],
                fire_at=row["fire_at"],
            )
            for row in rows
        ]

    async def mark_fired(self, run_id: str, command_seq: int) -> None:
        await self._pool.execute(
            "UPDATE timers SET fired = true WHERE run_id = $1 AND command_seq = $2",
            run_id,
            command_seq,
        )
