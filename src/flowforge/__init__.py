"""flowforge — durable workflow engine for AI-driven business automation."""

from __future__ import annotations

from flowforge.core.engine import Engine, RunResult, RunStatus
from flowforge.core.errors import (
    ActivityFailedError,
    ConcurrencyError,
    FlowforgeError,
    NonRetryableError,
    RetryableError,
    Suspended,
    WorkflowNotFoundError,
)
from flowforge.core.event_store import EventStore, InMemoryEventStore
from flowforge.core.retry import RetryPolicy
from flowforge.workflow.context import WorkflowContext
from flowforge.workflow.definition import Registry, WorkflowDef, define

__all__ = [
    "ActivityFailedError",
    "ConcurrencyError",
    "Engine",
    "EventStore",
    "FlowforgeError",
    "InMemoryEventStore",
    "NonRetryableError",
    "Registry",
    "RetryPolicy",
    "RetryableError",
    "RunResult",
    "RunStatus",
    "Suspended",
    "WorkflowContext",
    "WorkflowDef",
    "WorkflowNotFoundError",
    "define",
]

__version__ = "0.1.0"
