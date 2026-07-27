"""Postgres-backed event store — the production implementation of ``EventStore``.

The event log is the source of truth; ``(run_id, seq)`` is the primary key, and
each append runs in a transaction that takes ``FOR UPDATE`` on the run row and
checks the version. That makes a stale writer fail with :class:`ConcurrencyError`
rather than corrupt the log — the same optimistic-concurrency contract the
in-memory store offers, enforced by the database. ``asyncpg`` is imported lazily
so the package does not require it unless this store is used.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from flowforge.core.errors import ConcurrencyError
from flowforge.core.events import TERMINAL_EVENTS, Event, EventType


async def _init_connection(conn: Any) -> None:
    # Let jsonb round-trip as native dicts instead of raw strings.
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


def _status_for(last: Event) -> str:
    if last.type == EventType.WORKFLOW_COMPLETED:
        return "completed"
    if last.type == EventType.WORKFLOW_FAILED:
        return "failed"
    return "running"


class PostgresEventStore:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def connect(
        cls, dsn: str, *, min_size: int = 1, max_size: int = 10
    ) -> PostgresEventStore:
        import asyncpg

        pool = await asyncpg.create_pool(
            dsn, min_size=min_size, max_size=max_size, init=_init_connection
        )
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def load(self, run_id: str) -> list[Event]:
        rows = await self._pool.fetch(
            "SELECT seq, type, command_seq, name, payload, created_at "
            "FROM events WHERE run_id = $1 ORDER BY seq",
            run_id,
        )
        return [
            Event(
                seq=row["seq"],
                type=EventType(row["type"]),
                command_seq=row["command_seq"],
                name=row["name"],
                payload=row["payload"] or {},
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def append(
        self, run_id: str, events: Sequence[Event], expected_version: int
    ) -> None:
        if not events:
            return
        async with self._pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT version FROM workflow_runs WHERE run_id = $1 FOR UPDATE",
                run_id,
            )
            current: int = row["version"] if row is not None else 0
            if current != expected_version:
                raise ConcurrencyError(
                    f"run {run_id!r}: expected version {expected_version}, got {current}"
                )
            if row is None:
                # First append is WORKFLOW_STARTED, which carries the tenant the
                # run's costs are billed to; project it onto the run row so cost
                # reporting can join without replaying the log.
                await conn.execute(
                    "INSERT INTO workflow_runs (run_id, workflow_name, tenant_id) "
                    "VALUES ($1, $2, $3)",
                    run_id,
                    events[0].name or "",
                    str(events[0].payload.get("tenant") or "default"),
                )
            await conn.executemany(
                "INSERT INTO events "
                "(run_id, seq, type, command_seq, name, payload, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                [
                    (
                        run_id,
                        e.seq,
                        str(e.type),
                        e.command_seq,
                        e.name,
                        e.payload,
                        e.created_at,
                    )
                    for e in events
                ],
            )
            await conn.execute(
                "UPDATE workflow_runs SET version = $2, status = $3, updated_at = now() "
                "WHERE run_id = $1",
                run_id,
                current + len(events),
                _status_for(events[-1]) if events[-1].type in TERMINAL_EVENTS else "running",
            )
