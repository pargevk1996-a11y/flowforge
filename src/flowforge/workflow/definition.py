"""Workflow definitions and the registry that resolves them by name.

A workflow is a deterministic ``async def (ctx, input) -> output``. The input and
output types are read from the signature so the engine can (de)serialize the
run's input and result through the event log with their Pydantic contracts intact.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, get_type_hints

from flowforge.core.errors import WorkflowNotFoundError
from flowforge.workflow.context import WorkflowContext

type WorkflowFn[TInput, TOutput] = Callable[
    [WorkflowContext, TInput], Awaitable[TOutput]
]


@dataclass(frozen=True)
class WorkflowDef[TInput, TOutput]:
    name: str
    fn: WorkflowFn[TInput, TOutput]
    input_type: type[TInput]
    output_type: type[TOutput]


def _extract_io_types(fn: Callable[..., Any]) -> tuple[type[Any], type[Any]]:
    params = list(inspect.signature(fn).parameters)
    if len(params) != 2:
        raise TypeError(
            f"workflow function must take exactly (ctx, input); got {params!r}"
        )
    hints = get_type_hints(fn)
    input_type: type[Any] = hints.get(params[1], object)
    output_type: type[Any] = hints.get("return", object)
    return input_type, output_type


def define[TInput, TOutput](
    fn: WorkflowFn[TInput, TOutput], *, name: str | None = None
) -> WorkflowDef[TInput, TOutput]:
    input_type, output_type = _extract_io_types(fn)
    return WorkflowDef(
        name=name or fn.__name__,
        fn=fn,
        input_type=input_type,
        output_type=output_type,
    )


class Registry:
    def __init__(self) -> None:
        self._defs: dict[str, WorkflowDef[Any, Any]] = {}

    def register(self, definition: WorkflowDef[Any, Any]) -> WorkflowDef[Any, Any]:
        if definition.name in self._defs:
            raise ValueError(f"workflow {definition.name!r} already registered")
        self._defs[definition.name] = definition
        return definition

    def add[TInput, TOutput](
        self, fn: WorkflowFn[TInput, TOutput], *, name: str | None = None
    ) -> WorkflowDef[TInput, TOutput]:
        definition = define(fn, name=name)
        self.register(definition)
        return definition

    def get(self, name: str) -> WorkflowDef[Any, Any]:
        try:
            return self._defs[name]
        except KeyError:
            raise WorkflowNotFoundError(name) from None
