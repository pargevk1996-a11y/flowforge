"""Append-only event store with optimistic concurrency.

The engine depends only on the :class:`EventStore` protocol; the in-memory
implementation here powers tests and local runs, and a Postgres-backed adapter
(``persistence/``) implements the same protocol for production. ``expected_version``
is the run's current log length — appending with a stale version raises
:class:`ConcurrencyError`, enforcing single-writer semantics.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from flowforge.core.errors import ConcurrencyError
from flowforge.core.events import Event
from flowforge.core.timeline import RunStatus, derive_status


class RunSummary(BaseModel):
    """A run as a list sees it — enough to browse by, not to debug from."""

    run_id: str
    workflow: str
    tenant: str
    status: RunStatus
    version: int
    started_at: datetime
    updated_at: datetime


class RunPage(BaseModel):
    runs: list[RunSummary] = []
    total: int = 0
    limit: int = 50
    offset: int = 0


@runtime_checkable
class EventStore(Protocol):
    async def load(self, run_id: str) -> list[Event]:
        """Return the full, ordered event log for a run (empty if unknown)."""
        ...

    async def append(
        self, run_id: str, events: Sequence[Event], expected_version: int
    ) -> None:
        """Atomically append ``events`` iff the current log length equals
        ``expected_version``; otherwise raise :class:`ConcurrencyError`."""
        ...

    async def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        tenant: str | None = None,
        workflow: str | None = None,
    ) -> RunPage:
        """Browse runs, newest first. A read model for the UI, not a hot path —
        the engine never lists runs, it is always given one."""
        ...


class InMemoryEventStore:
    """Non-durable event store for tests and single-process local runs."""

    def __init__(self) -> None:
        self._logs: dict[str, list[Event]] = {}

    async def load(self, run_id: str) -> list[Event]:
        return list(self._logs.get(run_id, []))

    async def append(
        self, run_id: str, events: Sequence[Event], expected_version: int
    ) -> None:
        log = self._logs.setdefault(run_id, [])
        if len(log) != expected_version:
            raise ConcurrencyError(
                f"run {run_id!r}: expected version {expected_version}, got {len(log)}"
            )
        log.extend(events)

    async def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        tenant: str | None = None,
        workflow: str | None = None,
    ) -> RunPage:
        summaries = [
            self._summarise(run_id, log) for run_id, log in self._logs.items() if log
        ]
        matching = [
            run
            for run in summaries
            if (status is None or run.status == status)
            and (tenant is None or run.tenant == tenant)
            and (workflow is None or run.workflow == workflow)
        ]
        matching.sort(key=lambda run: (run.started_at, run.run_id), reverse=True)
        return RunPage(
            runs=matching[offset : offset + limit],
            total=len(matching),
            limit=limit,
            offset=offset,
        )

    @staticmethod
    def _summarise(run_id: str, log: list[Event]) -> RunSummary:
        started = log[0]
        return RunSummary(
            run_id=run_id,
            workflow=started.name or "",
            tenant=str(started.payload.get("tenant") or "default"),
            status=derive_status(log),
            version=len(log),
            started_at=started.created_at,
            updated_at=log[-1].created_at,
        )
