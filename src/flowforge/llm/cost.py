"""Pricing for LLM steps — the numbers per-tenant budgets are enforced on.

:class:`Pricing` turns token usage into dollars; :class:`CostTracker` is a
process-local running total, useful for a single embed or a test assertion. The
durable side — writing each charge to the ``cost_ledger`` and refusing calls once
a tenant is over its limit — lives in :mod:`flowforge.core.budget`.
"""

from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel

from flowforge.llm.client import LLMUsage


class ModelPrice(BaseModel):
    input_per_1k: float
    output_per_1k: float


class Pricing:
    def __init__(self, prices: dict[str, ModelPrice] | None = None) -> None:
        self._prices = dict(prices or {})

    def set(self, model: str, price: ModelPrice) -> None:
        self._prices[model] = price

    def cost(self, model: str, usage: LLMUsage) -> float:
        price = self._prices.get(model)
        if price is None:
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
