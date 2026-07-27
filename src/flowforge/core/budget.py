"""Per-tenant cost budgets: the ledger, the limit, and the guard that enforces it.

Every billable call is priced and appended to a **cost ledger**. Before a run is
allowed to make the next one, the guard sums that tenant's spend over a rolling
window and refuses once the limit is reached — a :class:`BudgetExceededError`,
which is non-retryable, so the run fails through the ordinary saga path and its
completed steps are compensated. That is what "cancel on exceed" means here: a
runaway retry loop or a stuck workflow cannot burn an unbounded amount of money,
and it cannot leave a half-finished payment behind either.

The check is *pre-flight*: a call is refused when the budget is already exhausted,
never mid-flight. Token counts are not knowable in advance, so the last call is
allowed to overshoot the limit slightly; the guarantee is that no *new* call
starts once the tenant is over.

The engine depends only on the :class:`CostLedger` protocol — in-memory for tests,
Postgres (the ``cost_ledger`` table) in production.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from flowforge.core.errors import BudgetExceededError
from flowforge.core.events import utcnow

Clock = Callable[[], datetime]

DEFAULT_WINDOW = timedelta(days=1)
"""Rolling window used for reporting when a tenant has no explicit budget."""


@dataclass(frozen=True)
class CostEntry:
    """One billable event: what it cost, and which run/command incurred it."""

    run_id: str
    tenant: str
    model: str
    usd: float
    command_seq: int | None = None
    provider: str | None = None


class CostLedger(Protocol):
    async def record(self, entry: CostEntry) -> None:
        """Append a charge. Append-only: the ledger is an audit trail, and a
        crash between the provider call and this write is the only way a real
        dollar goes unrecorded (never the reverse)."""
        ...

    async def spend_since(self, tenant: str, since: datetime) -> float:
        """Total USD charged to ``tenant`` at or after ``since``."""
        ...


class InMemoryCostLedger:
    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = clock or utcnow
        self.entries: list[tuple[datetime, CostEntry]] = []

    async def record(self, entry: CostEntry) -> None:
        self.entries.append((self._clock(), entry))

    async def spend_since(self, tenant: str, since: datetime) -> float:
        return float(
            sum(e.usd for at, e in self.entries if e.tenant == tenant and at >= since)
        )


@dataclass(frozen=True)
class Budget:
    """A spend cap over a rolling window — ``$50/day`` by default shape."""

    limit_usd: float
    window: timedelta = DEFAULT_WINDOW


class BudgetGuard:
    """Binds a ledger to per-tenant limits and answers "may this tenant spend?".

    ``default`` applies to every tenant without an entry in ``per_tenant``; a
    guard with neither is pure accounting — it records spend and enforces nothing.
    """

    def __init__(
        self,
        ledger: CostLedger,
        *,
        default: Budget | None = None,
        per_tenant: dict[str, Budget] | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._ledger = ledger
        self._default = default
        self._per_tenant = dict(per_tenant or {})
        self._clock: Clock = clock or utcnow

    def budget_for(self, tenant: str) -> Budget | None:
        return self._per_tenant.get(tenant, self._default)

    def set_budget(self, tenant: str, budget: Budget) -> None:
        self._per_tenant[tenant] = budget

    async def spent(self, tenant: str) -> float:
        """Spend inside the tenant's current window."""
        budget = self.budget_for(tenant)
        window = budget.window if budget is not None else DEFAULT_WINDOW
        return await self._ledger.spend_since(tenant, self._clock() - window)

    async def remaining(self, tenant: str) -> float | None:
        """Headroom left, or ``None`` when the tenant is uncapped."""
        budget = self.budget_for(tenant)
        if budget is None:
            return None
        return max(0.0, budget.limit_usd - await self.spent(tenant))

    async def ensure_within(self, tenant: str) -> None:
        """Raise :class:`BudgetExceededError` if the tenant is out of budget."""
        budget = self.budget_for(tenant)
        if budget is None:
            return
        spent = await self.spent(tenant)
        if spent >= budget.limit_usd:
            hours = budget.window.total_seconds() / 3600
            raise BudgetExceededError(
                f"tenant {tenant!r} spent ${spent:.4f} of its ${budget.limit_usd:.2f} "
                f"budget in the last {hours:g}h"
            )

    async def charge(self, entry: CostEntry) -> None:
        await self._ledger.record(entry)

    def meter(
        self, run_id: str, tenant: str, command_seq: int | None = None
    ) -> CostMeter:
        """A view of this guard bound to one run's command."""
        return CostMeter(self, run_id=run_id, tenant=tenant, command_seq=command_seq)


class CostMeter:
    """A :class:`BudgetGuard` bound to a single run and command.

    Handed to a step so it can gate and bill its own provider calls without
    knowing anything about tenants, ledgers, or where the run came from.
    """

    def __init__(
        self,
        guard: BudgetGuard,
        *,
        run_id: str,
        tenant: str,
        command_seq: int | None = None,
    ) -> None:
        self._guard = guard
        self.run_id = run_id
        self.tenant = tenant
        self.command_seq = command_seq

    async def check(self) -> None:
        """Refuse to start a call the tenant can no longer pay for."""
        await self._guard.ensure_within(self.tenant)

    async def charge(self, model: str, usd: float, *, provider: str | None = None) -> None:
        await self._guard.charge(
            CostEntry(
                run_id=self.run_id,
                tenant=self.tenant,
                model=model,
                usd=usd,
                command_seq=self.command_seq,
                provider=provider,
            )
        )


class MeteredStep[TOutput](Protocol):
    """The structural contract :meth:`WorkflowContext.llm` drives.

    Kept here, in core, so the workflow API can run a metered step without the
    engine depending on the LLM package. :class:`~flowforge.llm.step.LLMStep`
    satisfies it.
    """

    name: str
    output_type: type[TOutput]

    async def run(self, content: str, /, *, meter: CostMeter | None = None) -> TOutput:
        ...
