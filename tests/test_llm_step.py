"""Tests for the typed LLM step: structured output, schema-violation retry with
feedback into the prompt, cost accounting, and durability through the engine."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from flowforge import Engine, InMemoryEventStore, Registry, RunStatus, WorkflowContext
from flowforge.llm import (
    CostTracker,
    LLMStep,
    ModelPrice,
    Pricing,
    SchemaViolationError,
    ScriptedLLMClient,
    UnknownModelPriceError,
)


class Invoice(BaseModel):
    vendor: str
    amount: int


class ExtractRequest(BaseModel):
    text: str


def _pricing() -> Pricing:
    return Pricing({"test-model": ModelPrice(input_per_1k=1.0, output_per_1k=2.0)})


async def test_valid_output_first_try() -> None:
    client = ScriptedLLMClient(['{"vendor": "Acme", "amount": 4200}'])
    step = LLMStep(client, "test-model", Invoice, system="Extract the invoice.")
    result = await step.run("INVOICE Acme total 4200")
    assert result == Invoice(vendor="Acme", amount=4200)
    assert len(client.calls) == 1


async def test_schema_violation_retries_with_feedback() -> None:
    # First response has amount as a non-numeric string -> validation fails,
    # the error is fed back, second response is valid.
    client = ScriptedLLMClient(
        ['{"vendor": "Acme", "amount": "lots"}', '{"vendor": "Acme", "amount": 4200}']
    )
    cost = CostTracker()
    step = LLMStep(client, "test-model", Invoice, pricing=_pricing(), cost=cost)

    result = await step.run("extract")
    assert result == Invoice(vendor="Acme", amount=4200)
    assert len(client.calls) == 2

    # The retry actually carried the validation error back into the conversation.
    retry_prompt = client.calls[1][-1].content
    assert "amount" in retry_prompt and "did not validate" in retry_prompt

    # Both calls were billed.
    assert cost.calls == 2
    assert cost.total_usd > 0


async def test_gives_up_after_budget() -> None:
    client = ScriptedLLMClient(['{"vendor": "Acme"}'] * 3)  # always missing 'amount'
    step = LLMStep(client, "test-model", Invoice, max_schema_retries=3)
    with pytest.raises(SchemaViolationError):
        await step.run("extract")
    assert len(client.calls) == 3


async def test_an_unpriced_model_never_silently_costs_nothing() -> None:
    """A typo in a model name must not disable every budget that depends on it."""
    client = ScriptedLLMClient(['{"vendor": "Acme", "amount": 1}'] * 3)
    step = LLMStep(client, "test-modle", Invoice, pricing=_pricing())  # typo

    with pytest.raises(UnknownModelPriceError, match="test-modle"):
        await step.run("extract")
    assert client.calls == []  # refused before spending, not after


async def test_a_step_with_no_price_list_is_simply_not_priced() -> None:
    """Not asking for costing is different from asking and getting it wrong."""
    client = ScriptedLLMClient(['{"vendor": "Acme", "amount": 1}'])
    cost = CostTracker()
    step = LLMStep(client, "whatever", Invoice, cost=cost)

    assert await step.run("extract") == Invoice(vendor="Acme", amount=1)
    assert cost.total_usd == 0.0


async def test_llm_step_is_durable_through_engine() -> None:
    client = ScriptedLLMClient(
        ['{"vendor": "Acme", "amount": "nope"}', '{"vendor": "Acme", "amount": 4200}']
    )
    cost = CostTracker()
    step = LLMStep(client, "test-model", Invoice, pricing=_pricing(), cost=cost)

    async def wf(ctx: WorkflowContext, inp: ExtractRequest) -> Invoice:
        return await ctx.activity(
            step.run, inp.text, name=step.name, result_type=Invoice
        )

    store = InMemoryEventStore()
    reg = Registry()
    engine = Engine(store, reg)
    definition = reg.add(wf)

    res = await engine.start("r1", definition, ExtractRequest(text="INVOICE Acme 4200"))
    assert res.status is RunStatus.COMPLETED
    assert res.result == Invoice(vendor="Acme", amount=4200)
    assert cost.calls == 2  # one schema retry

    # Re-driving replays the recorded result: the model is NOT called again and
    # the schema-retry cost is not paid twice.
    again = await engine.drive("r1")
    assert again.status is RunStatus.COMPLETED
    assert cost.calls == 2  # unchanged: no new model calls on replay
    assert len(client.calls) == 2
