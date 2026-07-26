"""Typed LLM step: structured output with schema-violation retry.

An ``LLMStep[TOutput]`` calls the model, validates the response against a Pydantic
schema, and on a violation feeds the exact error back into the conversation
("you returned X, but field Y must be an int") and tries again — up to a bound.
This is ``Retry = typed``, not ``while True``.

It is meant to be run through ``ctx.activity(step.run, content, result_type=...,
name=...)`` so the *validated* result is recorded once in the event log: replay
returns it without ever calling the model again, and the schema-retry cost is
never paid twice.
"""

from __future__ import annotations

from pydantic import BaseModel, ValidationError

from flowforge.core.errors import NonRetryableError
from flowforge.llm.client import LLMClient, LLMMessage
from flowforge.llm.cost import CostTracker, Pricing


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
        name: str | None = None,
    ) -> None:
        self.client = client
        self.model = model
        self.output_type = output_type
        self.system = system
        self.max_schema_retries = max_schema_retries
        self.pricing = pricing or Pricing()
        self.cost = cost
        self.name = name or f"llm:{output_type.__name__}"

    async def run(self, user_content: str) -> TOutput:
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
            response = await self.client.complete(
                model=self.model, messages=messages, response_schema=schema
            )
            if self.cost is not None:
                self.cost.add(self.model, self.pricing.cost(self.model, response.usage))
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
