"""Typed LLM step: structured output with schema-violation retry.

An ``LLMStep[TOutput]`` calls the model, validates the response against a Pydantic
schema, and on a violation feeds the exact error back into the conversation
("you returned X, but field Y must be an int") and tries again — up to a bound.
This is ``Retry = typed``, not ``while True``.

It is meant to be run through ``ctx.llm(step, content)`` so the *validated* result
is recorded once in the event log: replay returns it without ever calling the
model again, and the schema-retry cost is never paid twice. That path also hands
the step a :class:`~flowforge.core.budget.CostMeter` bound to the run's tenant, so
every provider call is checked against the tenant's budget before it is made and
written to the cost ledger after — including the schema retries, which are real
money and are billed as such.
"""

from __future__ import annotations

from pydantic import BaseModel, ValidationError

from flowforge.core.budget import CostMeter
from flowforge.core.errors import NonRetryableError
from flowforge.llm.client import LLMClient, LLMMessage
from flowforge.llm.cost import CostTracker, Pricing
from flowforge.llm.limits import RateLimiter


class SchemaViolationError(NonRetryableError):
    """The model never produced output matching the schema within the retry budget.

    Non-retryable at the activity level: re-invoking the same prompt will not
    help, so this goes to compensation rather than the transient-retry path.
    """


def _format_errors(exc: ValidationError) -> str:
    lines = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "(root)"
        lines.append(f"- field '{loc}': {err['msg']}")
    return "\n".join(lines)


class LLMStep[TOutput: BaseModel]:
    def __init__(
        self,
        client: LLMClient,
        model: str,
        output_type: type[TOutput],
        *,
        system: str | None = None,
        max_schema_retries: int = 3,
        pricing: Pricing | None = None,
        cost: CostTracker | None = None,
        limiter: RateLimiter | None = None,
        provider: str = "openai",
        name: str | None = None,
    ) -> None:
        self.client = client
        self.model = model
        self.output_type = output_type
        self.system = system
        self.max_schema_retries = max_schema_retries
        self.pricing = pricing or Pricing()
        self.cost = cost
        self.limiter = limiter
        self.provider = provider
        self.name = name or f"llm:{output_type.__name__}"

    async def run(self, user_content: str, /, *, meter: CostMeter | None = None) -> TOutput:
        schema = self.output_type.model_json_schema()
        messages: list[LLMMessage] = []
        if self.system is not None:
            messages.append(LLMMessage(role="system", content=self.system))
        messages.append(
            LLMMessage(
                role="user",
                content=(
                    f"{user_content}\n\n"
                    f"Respond with ONLY a JSON object matching this schema:\n{schema}"
                ),
            )
        )

        last_error: ValidationError | None = None
        for _ in range(self.max_schema_retries):
            # Gate every attempt, not just the first: a schema-retry loop is the
            # most likely way for a run to spend money it no longer has.
            if meter is not None:
                await meter.check()
            if self.limiter is not None:
                await self.limiter.acquire(self.provider)

            response = await self.client.complete(
                model=self.model, messages=messages, response_schema=schema
            )
            usd = self.pricing.cost(self.model, response.usage)
            if self.cost is not None:
                self.cost.add(self.model, usd)
            if meter is not None:
                await meter.charge(self.model, usd, provider=self.provider)
            try:
                return self.output_type.model_validate_json(response.content)
            except ValidationError as exc:
                last_error = exc
                messages.append(LLMMessage(role="assistant", content=response.content))
                messages.append(
                    LLMMessage(
                        role="user",
                        content=(
                            "That response did not validate. Fix these problems and "
                            f"return ONLY valid JSON:\n{_format_errors(exc)}"
                        ),
                    )
                )

        raise SchemaViolationError(
            f"{self.name}: no schema-valid output after "
            f"{self.max_schema_retries} attempts: {last_error}"
        )
