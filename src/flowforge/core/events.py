"""The event log — the single source of truth for every run.

A run's state is not stored; it is *derived* by replaying these events. Every
event is immutable (``frozen``) and append-only. ``command_seq`` links the events
produced by one workflow command (an activity, a timer, a wait): commands are
numbered deterministically by replay order, which is what lets a re-run recognise
work that has already happened and skip its side effects.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


class EventType(StrEnum):
    WORKFLOW_STARTED = "workflow_started"
    ACTIVITY_SCHEDULED = "activity_scheduled"
    ACTIVITY_COMPLETED = "activity_completed"
    ACTIVITY_FAILED = "activity_failed"
    TIMER_STARTED = "timer_started"
    TIMER_FIRED = "timer_fired"
    WAIT_STARTED = "wait_started"
    SIGNAL_RECEIVED = "signal_received"
    CHILD_STARTED = "child_started"
    CHILD_COMPLETED = "child_completed"
    CHILD_FAILED = "child_failed"
    COMPENSATION_COMPLETED = "compensation_completed"
    WORKFLOW_TASK_FAILED = "workflow_task_failed"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"


TERMINAL_EVENTS = frozenset({EventType.WORKFLOW_COMPLETED, EventType.WORKFLOW_FAILED})


class Event(BaseModel):
    """One immutable entry in a run's event log."""

    model_config = ConfigDict(frozen=True)

    seq: int
    """0-based position of this event within the run's log."""

    type: EventType
    command_seq: int | None = None
    """Links events belonging to the same workflow command; ``None`` for
    lifecycle/compensation events that do not originate from a command."""

    name: str | None = None
    """Activity / signal / timer name, for readability and signal routing."""

    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utcnow)
