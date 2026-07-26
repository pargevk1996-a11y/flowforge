"""Append-only event store with optimistic concurrency.

The engine depends only on the :class:`EventStore` protocol; the in-memory
implementation here powers tests and local runs, and a Postgres-backed adapter
(``persistence/``) implements the same protocol for production. ``expected_version``
is the run's current log length — appending with a stale version raises
:class:`ConcurrencyError`, enforcing single-writer semantics.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from flowforge.core.errors import ConcurrencyError
from flowforge.core.events import Event


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
