"""The dispatcher: one path from "an event happened" to "a run is queued".

Every trigger kind funnels through :meth:`TriggerDispatcher.fire`, which claims
the event's identity, maps it to the workflow's typed input, and submits the run.
The ordering is deliberate:

1. **admission control** — an over-budget tenant is refused *before* the claim, so
   a refused delivery is not remembered as delivered and can be retried once the
   budget rolls over;
2. **claim** — a duplicate delivery returns the original run instead of starting a
   second one;
3. **submit** — seeded and enqueued, exactly like a run started over ``POST /runs``.

A crash between (2) and (3) leaves a claimed key with no run behind it. The retry
that follows finds the claim, sees no run, and completes the submission under the
*claimed* id — so an event is never lost to a half-finished delivery, and the
optimistic-concurrency check on the event log settles any race that produces.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from pydantic import TypeAdapter

from flowforge.core.budget import BudgetGuard
from flowforge.core.engine import Engine
from flowforge.core.errors import ConcurrencyError
from flowforge.queue.base import TaskQueue
from flowforge.queue.worker import submit
from flowforge.triggers.base import Event, TriggerRegistry
from flowforge.triggers.deliveries import DeliveryStore
from flowforge.workflow.definition import Registry


@dataclass(frozen=True)
class Delivery:
    trigger: str
    run_id: str
    started: bool
    """``False`` when the event had already been delivered — the run id then
    points at the run the *first* delivery started."""


class TriggerDispatcher:
    def __init__(
        self,
        engine: Engine,
        queue: TaskQueue,
        workflows: Registry,
        triggers: TriggerRegistry,
        *,
        deliveries: DeliveryStore | None = None,
        budget: BudgetGuard | None = None,
    ) -> None:
        self._engine = engine
        self._queue = queue
        self._workflows = workflows
        self._triggers = triggers
        self._deliveries = deliveries
        self._budget = budget

    async def fire(self, name: str, event: Event, *, key: str | None = None) -> Delivery:
        """Deliver ``event`` to the trigger ``name``.

        ``key`` overrides the trigger's own dedupe function — the caller often
        knows a better identity than the body does (a provider's delivery id from
        a header, a cron tick's timestamp)."""
        trigger = self._triggers.get(name)
        wf = self._workflows.get(trigger.workflow)

        if self._budget is not None:
            await self._budget.ensure_within(trigger.tenant)

        dedupe_key = key if key is not None else trigger.dedupe_key(event)
        run_id, mine = await self._claim(name, dedupe_key)
        if not mine and await self._run_exists(run_id):
            return Delivery(trigger=name, run_id=run_id, started=False)

        # Map before submitting: a mapping error must not leave a seeded run
        # behind, only a claim that the sender's next retry can complete.
        workflow_input = TypeAdapter(wf.input_type).validate_python(trigger.map(event))
        try:
            await submit(
                self._engine,
                self._queue,
                run_id,
                wf,
                workflow_input,
                priority=trigger.priority,
                tenant=trigger.tenant,
            )
        except ConcurrencyError:
            # Another delivery of the same event got there first.
            return Delivery(trigger=name, run_id=run_id, started=False)
        return Delivery(trigger=name, run_id=run_id, started=True)

    # -- internals ----------------------------------------------------------

    async def _claim(self, name: str, key: str | None) -> tuple[str, bool]:
        run_id = uuid4().hex
        if key is None or self._deliveries is None:
            return run_id, True
        return await self._deliveries.claim(name, key, run_id)

    async def _run_exists(self, run_id: str) -> bool:
        try:
            await self._engine.describe(run_id)
        except KeyError:
            return False
        return True
