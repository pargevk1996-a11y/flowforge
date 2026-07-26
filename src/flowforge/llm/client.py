"""Provider-agnostic LLM client abstraction.

The engine depends only on the :class:`LLMClient` protocol, so a real provider
(OpenAI/instructor, etc.) and the deterministic test client below are
interchangeable. Keeping the boundary this thin is what lets LLM steps be tested
in-process, with no network and no keys.
"""

from __future__ import annotations

from typing import Any, Literal, Protocol

from pydantic import BaseModel

Role = Literal["system", "user", "assistant"]


class LLMMessage(BaseModel):
    role: Role
    content: str


class LLMUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0


class LLMResponse(BaseModel):
    content: str
    usage: LLMUsage = LLMUsage()


class LLMClient(Protocol):
    async def complete(
        self,
        *,
        model: str,
        messages: list[LLMMessage],
        response_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        """Return a single completion. ``response_schema`` is the JSON schema the
        caller wants the content to satisfy (advisory; validation is enforced by
        the caller, which retries with feedback on a violation)."""
        ...


class ScriptedLLMClient:
    """Deterministic client for tests: returns queued responses in order and
    records every call so assertions can inspect the retry-with-feedback loop."""

    _DEFAULT_USAGE = LLMUsage(prompt_tokens=10, completion_tokens=10)

    def __init__(self, responses: list[str | LLMResponse]) -> None:
        self._responses: list[LLMResponse] = [
            r
            if isinstance(r, LLMResponse)
            else LLMResponse(content=r, usage=self._DEFAULT_USAGE)
            for r in responses
        ]
        self.calls: list[list[LLMMessage]] = []

    async def complete(
        self,
        *,
        model: str,
        messages: list[LLMMessage],
        response_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        self.calls.append(list(messages))
        if not self._responses:
            raise RuntimeError("ScriptedLLMClient ran out of scripted responses")
        return self._responses.pop(0)
