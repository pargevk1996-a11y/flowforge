"""Postgres-backed cost ledger (the ``cost_ledger`` table).

Append-only, one row per billable call, indexed by ``(tenant_id, created_at)`` so
the rolling-window sum a budget check needs is an index scan rather than a table
scan. Amounts are ``NUMERIC`` in the database — money is never stored as a float —
and converted at the boundary. ``asyncpg`` is imported lazily.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from flowforge.core.budget import CostEntry


class PostgresCostLedger:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def connect(
        cls, dsn: str, *, min_size: int = 1, max_size: int = 5
    ) -> PostgresCostLedger:
        import asyncpg

        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def record(self, entry: CostEntry) -> None:
        await self._pool.execute(
            "INSERT INTO cost_ledger "
            "(run_id, tenant_id, command_seq, provider, model, usd_cost) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            entry.run_id,
            entry.tenant,
            entry.command_seq,
            entry.provider,
            entry.model,
            Decimal(str(entry.usd)),
        )

    async def entries_for_run(self, run_id: str) -> list[CostEntry]:
        rows = await self._pool.fetch(
            "SELECT run_id, tenant_id, command_seq, provider, model, usd_cost "
            "FROM cost_ledger WHERE run_id = $1 ORDER BY id",
            run_id,
        )
        return [
            CostEntry(
                run_id=row["run_id"],
                tenant=row["tenant_id"],
                model=row["model"] or "",
                usd=float(row["usd_cost"]),
                command_seq=row["command_seq"],
                provider=row["provider"],
            )
            for row in rows
        ]

    async def spend_since(self, tenant: str, since: datetime) -> float:
        total = await self._pool.fetchval(
            "SELECT COALESCE(SUM(usd_cost), 0) FROM cost_ledger "
            "WHERE tenant_id = $1 AND created_at >= $2",
            tenant,
            since,
        )
        return float(total)
