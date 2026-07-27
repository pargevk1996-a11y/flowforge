"""Delivery claims: the record of which external events have already started a run.

One row per ``(trigger, dedupe_key)``, written *before* the run is created. The
claim is the exactly-once boundary between an at-least-once world (webhooks, cron
after a restart) and the log, and it only works if it is atomic: two concurrent
deliveries of the same invoice must produce one winner and one loser, never two
runs. Hence :meth:`claim` returns both the winning run id and whether *this*
caller won, rather than a bare boolean anyone could race around.

Claiming first means a crash between the claim and the run's creation leaves a
key pointing at a run that does not exist; the dispatcher recognises that case and
finishes what the crashed delivery started, rather than swallowing the event.
"""

from __future__ import annotations

from typing import Protocol


class DeliveryStore(Protocol):
    async def claim(self, trigger: str, key: str, run_id: str) -> tuple[str, bool]:
        """Bind ``(trigger, key)`` to ``run_id`` if it is unbound.

        Returns the run id that owns the key — ``run_id`` if this caller won it,
        the earlier one otherwise — and whether this caller was the winner."""
        ...

    async def claimed_run(self, trigger: str, key: str) -> str | None:
        """The run id bound to the key, if any."""
        ...


class InMemoryDeliveryStore:
    def __init__(self) -> None:
        self._claims: dict[tuple[str, str], str] = {}

    async def claim(self, trigger: str, key: str, run_id: str) -> tuple[str, bool]:
        # setdefault is the single-process equivalent of INSERT .. ON CONFLICT
        # DO NOTHING: it decides a winner without a check-then-act gap.
        winner = self._claims.setdefault((trigger, key), run_id)
        return winner, winner == run_id

    async def claimed_run(self, trigger: str, key: str) -> str | None:
        return self._claims.get((trigger, key))
