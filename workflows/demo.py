"""A control plane with the reference workflows wired up, for `flowforge api`.

A debugger needs something to debug. This assembles both reference workflows —
invoice-to-payment and contract-review — with their triggers, a cost ledger and a
budget, so the whole engine is explorable from one command without a database, a
queue or an API key.

The LLM is a **canned client**, not a real one: it answers from the prompt with a
plausible, deterministic payload. That keeps the demo free and repeatable, and it
is honest about what it is — plug a real
:class:`~flowforge.llm.client.LLMClient` in and nothing else changes.
"""

from __future__ import annotations

import json
import re
from typing import Any

from flowforge import Budget, InMemoryCostLedger, InMemoryEventStore, InMemoryTimerStore, Registry
from flowforge.api.controlplane import ControlPlane, build_control_plane
from flowforge.config import Settings
from flowforge.llm import (
    InMemoryRateLimiter,
    LLMMessage,
    LLMResponse,
    LLMUsage,
    ModelPrice,
    Pricing,
    RateLimit,
)
from flowforge.otel import configure_tracing
from workflows.contract_review import ContractServices, build_contract_review
from workflows.invoice_to_payment import (
    InvoiceServices,
    build_invoice_to_payment,
    register_invoice_triggers,
)

DEMO_MODEL = "gpt-4o-mini"
_RISK_WORDS = ("indemnify", "without limit", "unlimited", "penalty", "terminate immediately")


class CannedLLMClient:
    """Deterministic stand-in for a provider: it reads the schema it is asked for
    and answers in kind. Enough to make the timeline show real LLM steps, real
    token counts and real costs."""

    def __init__(self) -> None:
        self.calls: list[list[LLMMessage]] = []

    async def complete(
        self,
        *,
        model: str,
        messages: list[LLMMessage],
        response_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        self.calls.append(list(messages))
        prompt = "\n".join(message.content for message in messages)
        fields = set((response_schema or {}).get("properties", {}))
        content = (
            self._invoice(prompt)
            if {"invoice_number", "vendor"} <= fields
            else self._risk(prompt)
            if "level" in fields
            else "{}"
        )
        return LLMResponse(
            content=content,
            usage=LLMUsage(prompt_tokens=len(prompt) // 4, completion_tokens=len(content) // 4),
        )

    @staticmethod
    def _invoice(prompt: str) -> str:
        # The digit anchor matters: `[\d,]+` alone happily matches the bare comma
        # in "vendor, invoice_number, amount, currency" and yields an empty float.
        amount = re.search(r"(?:total|amount)\D{0,12}(\d[\d,]*(?:\.\d+)?)", prompt, re.I)
        number = re.search(r"\b(INV-[\w-]+)\b", prompt)
        vendor = re.search(r"Vendor:\s*(.+)", prompt)
        return json.dumps(
            {
                "vendor": (vendor.group(1).strip() if vendor else "Acme"),
                "invoice_number": (number.group(1) if number else "INV-1"),
                "amount": float(amount.group(1).replace(",", "")) if amount else 500.0,
                "currency": "USD",
            }
        )

    @staticmethod
    def _risk(prompt: str) -> str:
        hit = next((word for word in _RISK_WORDS if word in prompt.lower()), None)
        return json.dumps(
            {
                "level": "high" if hit else "low",
                "issue": f"clause mentions {hit!r}" if hit else "no unusual exposure",
            }
        )


def build_demo_control_plane(settings: Settings | None = None) -> ControlPlane:
    """Everything in memory: start it, poke it, restart it, nothing persists.

    Reads ``TENANT_BUDGET_USD_PER_DAY``, ``LLM_RATE_LIMIT_PER_SECOND`` and
    ``OTEL_EXPORTER_OTLP_ENDPOINT`` from the environment, so the knobs
    ``.env.example`` advertises actually do something here — a documented setting
    that nothing reads is worse than no setting."""
    settings = settings or Settings.from_env()
    registry = Registry()
    client = CannedLLMClient()
    pricing = Pricing({DEMO_MODEL: ModelPrice(input_per_1k=0.15, output_per_1k=0.6)})
    limiter = InMemoryRateLimiter(
        default=settings.rate_limit() or RateLimit(per_second=20, burst=20)
    )

    build_invoice_to_payment(
        registry,
        llm_client=client,
        services=InvoiceServices(),
        pricing=pricing,
        limiter=limiter,
    )
    build_contract_review(
        registry,
        llm_client=client,
        services=ContractServices(),
        pricing=pricing,
        limiter=limiter,
        concurrency=3,
    )

    cp = build_control_plane(
        InMemoryEventStore(),
        registry,
        timers=InMemoryTimerStore(),
        ledger=InMemoryCostLedger(),
        budget=settings.default_budget() or Budget(limit_usd=25.0),
        tracer=configure_tracing(settings),
    )
    register_invoice_triggers(cp.triggers)
    return cp
