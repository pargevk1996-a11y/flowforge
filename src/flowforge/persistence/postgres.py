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
from flowforge.core.event_store import RunPage, RunSummary
from flowforge.core.events import Event, EventType
from flowforge.core.timeline import RunStatus


async def _init_connection(conn: Any) -> None:
    # Let jsonb round-trip as native dicts instead of raw strings.
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


_SUSPENDING = frozenset(
    {EventType.TIMER_STARTED, EventType.WAIT_STARTED, EventType.CHILD_STARTED}
)

_RUN_FILTERS = (
    "WHERE ($1::text IS NULL OR status = $1) "
    "  AND ($2::text IS NULL OR tenant_id = $2) "
    "  AND ($3::text IS NULL OR workflow_name = $3)"
)


def _status_for(last: Event) -> str:
    """The status to project onto the run row from the event just appended.

    A projection for browsing, not the authority: a parent waiting on three
    children reads as ``running`` for the moment between one child reporting back
    and the parent being driven again. ``Engine.describe`` derives the exact
    status from the log, and the timeline endpoint reports that."""
    if last.type == EventType.WORKFLOW_COMPLETED:
        return "completed"
    if last.type == EventType.WORKFLOW_FAILED:
        return "failed"
    if last.type == EventType.WORKFLOW_TASK_FAILED:
        return "stuck"
    if last.type in _SUSPENDING:
        return "suspended"
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
                _status_for(events[-1]),
            )

    async def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        tenant: str | None = None,
        workflow: str | None = None,
    ) -> RunPage:
        # Counted separately, not with COUNT(*) OVER(): a window function has no
        # rows to count once the offset runs past the last one, so a page beyond
        # the end would report a total of zero and strand whoever is paging.
        async with self._pool.acquire() as conn:
            total = await conn.fetchval(
                "SELECT count(*) FROM workflow_runs " + _RUN_FILTERS,
                status,
                tenant,
                workflow,
            )
            rows = await conn.fetch(
                "SELECT run_id, workflow_name, tenant_id, status, version, created_at, "
                "       updated_at FROM workflow_runs " + _RUN_FILTERS +
                " ORDER BY created_at DESC, run_id DESC LIMIT $4 OFFSET $5",
                status,
                tenant,
                workflow,
                limit,
                offset,
            )
        return RunPage(
            runs=[
                RunSummary(
                    run_id=row["run_id"],
                    workflow=row["workflow_name"],
                    tenant=row["tenant_id"],
                    status=RunStatus(row["status"]),
                    version=row["version"],
                    started_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ],
            total=int(total or 0),
            limit=limit,
            offset=offset,
        )
