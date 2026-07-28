"""Pricing for LLM steps — the numbers per-tenant budgets are enforced on.

:class:`Pricing` turns token usage into dollars; :class:`CostTracker` is a
process-local running total, useful for a single embed or a test assertion. The
durable side — writing each charge to the ``cost_ledger`` and refusing calls once
a tenant is over its limit — lives in :mod:`flowforge.core.budget`.
"""

from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel

from flowforge.core.errors import NonRetryableError
from flowforge.llm.client import LLMUsage


class UnknownModelPriceError(NonRetryableError):
    """A priced setup was asked for a model it has no price for.

    Non-retryable: the price will not appear on its own. Loud on purpose — the
    alternative is charging zero, which silently switches off every budget that
    depends on this number."""


class ModelPrice(BaseModel):
    input_per_1k: float
    output_per_1k: float


class Pricing:
    """Token usage to dollars.

    An **empty** price list means "not pricing anything": every call costs zero,
    which is the right answer when nobody asked for costing. A **non-empty** one
    that lacks the model asked for is a misconfiguration — one typo in a model
    name would otherwise make every call free and quietly disable the tenant's
    budget — so it raises. Charge nothing for a model on purpose by pricing it at
    zero explicitly."""

    def __init__(self, prices: dict[str, ModelPrice] | None = None) -> None:
        self._prices = dict(prices or {})

    def set(self, model: str, price: ModelPrice) -> None:
        self._prices[model] = price

    @property
    def prices_anything(self) -> bool:
        return bool(self._prices)

    def ensure_priced(self, model: str) -> None:
        """Check a model before spending on it, rather than after."""
        if self.prices_anything and model not in self._prices:
            known = ", ".join(sorted(self._prices)) or "(none)"
            raise UnknownModelPriceError(
                f"no price for model {model!r}; priced models are: {known}"
            )

    def cost(self, model: str, usage: LLMUsage) -> float:
        price = self._prices.get(model)
        if price is None:
            self.ensure_priced(model)
            return 0.0
        return (
            usage.prompt_tokens / 1000 * price.input_per_1k
            + usage.completion_tokens / 1000 * price.output_per_1k
        )


class CostTracker:
    """Running total of LLM spend, overall and per model."""

    def __init__(self) -> None:
        self.total_usd: float = 0.0
        self.calls: int = 0
        self.per_model: dict[str, float] = defaultdict(float)

    def add(self, model: str, usd: float) -> None:
        self.total_usd += usd
        self.calls += 1
        self.per_model[model] += usd
