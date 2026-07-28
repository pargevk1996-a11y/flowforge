"""OpenTelemetry adapter for :class:`~flowforge.core.tracing.Tracer`.

Propagation goes through the standard W3C ``traceparent`` and OpenTelemetry's own
propagator rather than hand-rolled hex: a run's recorded parent is extracted into a
non-recording remote span, exactly as if it had arrived over HTTP — which, from the
point of view of a worker picking up a six-hour-old run, it may as well have.

``opentelemetry`` is imported lazily so the ``otel`` extra stays optional.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from flowforge.core.tracing import Attributes, AttributeValue, NoOpTracer, Span, Tracer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from flowforge.config import Settings

TRACER_NAME = "flowforge"


class _OtelSpan:
    def __init__(self, span: Any) -> None:
        self._span = span

    def set_attribute(self, key: str, value: AttributeValue) -> None:
        self._span.set_attribute(key, value)

    def record_error(self, exc: BaseException) -> None:
        from opentelemetry.trace import Status, StatusCode

        self._span.record_exception(exc)
        self._span.set_status(Status(StatusCode.ERROR, f"{type(exc).__name__}: {exc}"))


class OtelTracer:
    """Wraps an OpenTelemetry tracer in the engine's narrower contract."""

    def __init__(self, tracer: Any | None = None) -> None:
        from opentelemetry import trace
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )

        self._tracer = tracer if tracer is not None else trace.get_tracer(TRACER_NAME)
        self._propagator = TraceContextTextMapPropagator()

    @contextmanager
    def span(
        self,
        name: str,
        *,
        parent: str | None = None,
        attributes: Attributes | None = None,
    ) -> Iterator[Span]:
        context = self._propagator.extract({"traceparent": parent}) if parent else None
        with self._tracer.start_as_current_span(
            name, context=context, attributes=dict(attributes or {})
        ) as span:
            yield _OtelSpan(span)

    def traceparent(self) -> str | None:
        carrier: dict[str, str] = {}
        self._propagator.inject(carrier)
        return carrier.get("traceparent")


def configure_tracing(settings: Settings) -> Tracer:
    """Build a tracer from configuration.

    Without ``OTEL_EXPORTER_OTLP_ENDPOINT`` this returns the no-op tracer: tracing
    is opt-in, and an engine nobody asked to instrument should not be starting
    exporters. With it, the SDK is configured here rather than expected from the
    ambient environment, so ``flowforge api`` traces out of the box."""
    if not settings.otel_endpoint:
        return NoOpTracer()

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create({"service.name": settings.otel_service_name})
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{settings.otel_endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(provider)
    return OtelTracer()
