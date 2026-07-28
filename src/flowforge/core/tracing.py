"""Tracing, for a unit of work that outlives the process that started it.

A run is not a call stack. It is driven, suspends, and is driven again — minutes or
days later, on a different worker — and it branches into child runs that are driven
somewhere else again. Instrumenting each drive on its own would produce a hundred
disconnected traces of the same invoice.

So the trace context travels the only way anything travels in this engine: **in the
event log**. The run's root span context is written into ``WORKFLOW_STARTED`` as a
W3C ``traceparent``, and every later drive — in whatever process, at whatever hour
— starts its span against that recorded parent. The result is one span tree per
run, spanning workers and time, which is what you want when the question is "where
did this invoice spend six hours?".

The engine depends only on the :class:`Tracer` protocol here, and defaults to
:data:`NO_TRACING`, which costs a ``nullcontext``. The OpenTelemetry
implementation lives in :mod:`flowforge.otel` and imports the SDK lazily, so the
engine still needs nothing but Pydantic.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol

type AttributeValue = str | bool | int | float
type Attributes = Mapping[str, AttributeValue]


class Span(Protocol):
    def set_attribute(self, key: str, value: AttributeValue) -> None:
        ...

    def record_error(self, exc: BaseException) -> None:
        """Mark this span as failed.

        Needed explicitly because the engine *handles* most of its failures — a
        parked run and a compensated saga both leave their span normally, and a
        span that exits normally is a span that looks fine."""
        ...


class Tracer(Protocol):
    def span(
        self,
        name: str,
        *,
        parent: str | None = None,
        attributes: Attributes | None = None,
    ) -> AbstractContextManager[Span]:
        """Open a span, as a child of ``parent`` (a W3C traceparent) when given,
        and of whatever span is currently in scope otherwise."""
        ...

    def traceparent(self) -> str | None:
        """The W3C traceparent of the span in scope, for handing across a boundary
        the process cannot follow — the event log, or a child run."""
        ...


class _NoOpSpan:
    def set_attribute(self, key: str, value: AttributeValue) -> None:
        return None

    def record_error(self, exc: BaseException) -> None:
        return None


class NoOpTracer:
    """The default. Not instrumented, and not paying for the option."""

    _span = _NoOpSpan()

    def span(
        self,
        name: str,
        *,
        parent: str | None = None,
        attributes: Attributes | None = None,
    ) -> AbstractContextManager[Span]:
        return nullcontext(self._span)

    def traceparent(self) -> str | None:
        return None


NO_TRACING: Tracer = NoOpTracer()


def trace_id_of(traceparent: str | None) -> str | None:
    """The trace id inside a ``traceparent``, for showing next to a run.

    ``00-<32 hex trace id>-<16 hex span id>-<flags>``. Parsed rather than
    validated: a header this engine wrote is either well-formed or absent."""
    if not traceparent:
        return None
    parts = traceparent.split("-")
    return parts[1] if len(parts) == 4 and len(parts[1]) == 32 else None
