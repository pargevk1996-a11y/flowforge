"""The read model: an event log turned into something a human can debug.

The log is the truth, but it is a stream of low-level facts — *scheduled*,
*completed*, *fired*, *received*. What anyone debugging a run actually wants is
the **step**: this activity, this LLM call, this approval, what it returned, how
long it took, what it cost. That is a projection, and it belongs here rather than
in the HTTP layer, because it is engine knowledge: only the engine knows that a
``command_seq`` is what ties four events into one thing that happened.

Because the projection is a pure function of a prefix of the log, truncating the
input is time travel: ``build_timeline(run_id, events[: at + 1])`` is exactly the
state the engine would have replayed from at that point. That is the whole trick
behind the replay debugger — no special machinery, just a shorter list.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

from flowforge.core.budget import CostEntry
from flowforge.core.children import ParentRef
from flowforge.core.errors import RunNotFoundError
from flowforge.core.events import TERMINAL_EVENTS, Event, EventType
from flowforge.core.tracing import trace_id_of


class RunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SUSPENDED = "suspended"
    STUCK = "stuck"
    """The last drive attempt raised in workflow code rather than in an activity.

    Not a terminal state: nothing was compensated and nothing was lost. A bug in
    a workflow function is a bug in *code*, so the answer is to deploy a fix and
    drive the run again — replay picks it up exactly where it stopped, because
    the parking event carries no command and is inert on the way through."""


class StepKind(StrEnum):
    ACTIVITY = "activity"
    LLM = "llm"
    TIMER = "timer"
    SIGNAL = "signal"
    CHILD = "child"


class StepStatus(StrEnum):
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"


# Which event opens a step, and what kind of step it opens.
_OPENERS: dict[EventType, StepKind] = {
    EventType.ACTIVITY_SCHEDULED: StepKind.ACTIVITY,
    EventType.TIMER_STARTED: StepKind.TIMER,
    EventType.WAIT_STARTED: StepKind.SIGNAL,
    EventType.CHILD_STARTED: StepKind.CHILD,
}

# Which event closes it, and whether that counts as success.
_CLOSERS: dict[EventType, bool] = {
    EventType.ACTIVITY_COMPLETED: True,
    EventType.ACTIVITY_FAILED: False,
    EventType.TIMER_FIRED: True,
    EventType.SIGNAL_RECEIVED: True,
    EventType.CHILD_COMPLETED: True,
    EventType.CHILD_FAILED: False,
}

# An unfinished step is "running" if something is working on it, and "waiting" if
# the run is parked until the outside world does something.
_OPEN_STATUS: dict[StepKind, StepStatus] = {
    StepKind.ACTIVITY: StepStatus.RUNNING,
    StepKind.LLM: StepStatus.RUNNING,
    StepKind.TIMER: StepStatus.WAITING,
    StepKind.SIGNAL: StepStatus.WAITING,
    StepKind.CHILD: StepStatus.WAITING,
}

_WAITING_KINDS = (StepKind.TIMER, StepKind.SIGNAL, StepKind.CHILD)


class Step(BaseModel):
    """One workflow command, with every event that belongs to it folded in."""

    command_seq: int
    kind: StepKind
    name: str
    status: StepStatus
    started_at: datetime
    ended_at: datetime | None = None
    duration_ms: float | None = None
    result: Any = None
    error: str | None = None
    child_run_id: str | None = None
    usd_cost: float = 0.0
    event_seqs: list[int] = []


class Compensation(BaseModel):
    """A saga rollback that ran. Not a command — it has no command_seq — so it is
    reported alongside the steps rather than inside them."""

    name: str
    at: datetime
    event_seq: int


class Timeline(BaseModel):
    run_id: str
    workflow: str
    tenant: str
    status: RunStatus
    started_at: datetime
    ended_at: datetime | None = None
    duration_ms: float | None = None
    steps: list[Step] = []
    compensations: list[Compensation] = []
    parent: dict[str, Any] | None = None
    result: Any = None
    error: str | None = None
    usd_cost: float = 0.0
    event_count: int = 0
    trace_id: str | None = None
    """The trace this run belongs to, for jumping straight to it in a viewer."""

    truncated_at: int | None = None
    """Set when this is a view of a prefix of the log rather than all of it."""


def parked_event(events: Sequence[Event]) -> Event | None:
    """The trailing :attr:`~EventType.WORKFLOW_TASK_FAILED`, if the run is parked.

    Trailing specifically: once anything else lands — a timer fires, a child
    reports, a signal arrives — the run has new input and deserves another
    attempt, so it is no longer parked on the old failure."""
    last = events[-1] if events else None
    return last if last is not None and last.type is EventType.WORKFLOW_TASK_FAILED else None


def terminal_event(events: Sequence[Event]) -> Event | None:
    """The run's terminal event, wherever it sits in the log.

    Not ``events[-1]``: a child that finishes just after its parent did appends to
    the parent's log, so the terminal event is not always last."""
    return next((e for e in events if e.type in TERMINAL_EVENTS), None)


def derive_status(events: Sequence[Event]) -> RunStatus:
    """The one definition of a run's status, shared by the engine and the UI."""
    terminal = terminal_event(events)
    if terminal is not None:
        return (
            RunStatus.COMPLETED
            if terminal.type == EventType.WORKFLOW_COMPLETED
            else RunStatus.FAILED
        )
    if parked_event(events) is not None:
        return RunStatus.STUCK
    if any(step.kind in _WAITING_KINDS for step in _open_steps(events)):
        return RunStatus.SUSPENDED
    return RunStatus.RUNNING


def _open_steps(events: Sequence[Event]) -> Iterable[Step]:
    for step in build_steps(events):
        if step.status in (StepStatus.RUNNING, StepStatus.WAITING):
            yield step


def build_steps(
    events: Sequence[Event], costs: Sequence[CostEntry] = ()
) -> list[Step]:
    """Fold the log into one entry per command, in command order."""
    spend: dict[int, float] = {}
    for entry in costs:
        if entry.command_seq is not None:
            spend[entry.command_seq] = spend.get(entry.command_seq, 0.0) + entry.usd

    opened: dict[int, Event] = {}
    closed: dict[int, Event] = {}
    seqs: dict[int, list[int]] = {}
    kinds: dict[int, StepKind] = {}

    for event in events:
        cs = event.command_seq
        if cs is None:
            continue
        seqs.setdefault(cs, []).append(event.seq)
        kind = _OPENERS.get(event.type)
        if kind is not None:
            opened.setdefault(cs, event)
            # ctx.llm records what it is on the scheduling event, so the timeline
            # can tell a model call from any other activity.
            if event.payload.get("kind") == StepKind.LLM:
                kind = StepKind.LLM
            kinds.setdefault(cs, kind)
        elif event.type in _CLOSERS:
            closed.setdefault(cs, event)

    steps: list[Step] = []
    for cs in sorted(opened):
        start = opened[cs]
        kind = kinds[cs]
        end = closed.get(cs)
        steps.append(
            Step(
                command_seq=cs,
                kind=kind,
                name=start.name or f"command {cs}",
                status=_step_status(kind, end),
                started_at=start.created_at,
                ended_at=end.created_at if end is not None else None,
                duration_ms=_duration_ms(start.created_at, end),
                result=_step_result(start, end),
                error=_step_error(end),
                child_run_id=_child_run_id(start, end),
                usd_cost=spend.get(cs, 0.0),
                event_seqs=seqs.get(cs, []),
            )
        )
    return steps


def pending_children(events: Sequence[Event]) -> list[Step]:
    """Child commands this run started and has not heard back from.

    Reuses the projection rather than re-reading the log: an unfinished child is
    exactly an open step of kind ``child``."""
    return [
        step
        for step in build_steps(events)
        if step.kind is StepKind.CHILD and step.status is StepStatus.WAITING
    ]


def build_timeline(
    run_id: str,
    events: Sequence[Event],
    *,
    costs: Sequence[CostEntry] = (),
    truncated_at: int | None = None,
) -> Timeline:
    """Project a run's log (or a prefix of it) into the debugger's read model."""
    if not events:
        raise RunNotFoundError(run_id)

    started = events[0]
    terminal = terminal_event(events)
    parked = parked_event(events)
    steps = build_steps(events, costs)
    return Timeline(
        run_id=run_id,
        workflow=started.name or "",
        tenant=str(started.payload.get("tenant") or "default"),
        status=derive_status(events),
        started_at=started.created_at,
        ended_at=terminal.created_at if terminal is not None else None,
        duration_ms=_duration_ms(started.created_at, terminal),
        steps=steps,
        compensations=[
            Compensation(name=e.name or "", at=e.created_at, event_seq=e.seq)
            for e in events
            if e.type == EventType.COMPENSATION_COMPLETED
        ],
        parent=_parent_payload(started),
        result=terminal.payload.get("result") if terminal is not None else None,
        error=_run_error(terminal, parked),
        usd_cost=round(sum(step.usd_cost for step in steps), 6),
        event_count=len(events),
        trace_id=trace_id_of(started.payload.get("traceparent")),
        truncated_at=truncated_at,
    )


# -- internals --------------------------------------------------------------


def _step_status(kind: StepKind, end: Event | None) -> StepStatus:
    if end is None:
        return _OPEN_STATUS[kind]
    return StepStatus.COMPLETED if _CLOSERS[end.type] else StepStatus.FAILED


def _duration_ms(start: datetime, end: Event | None) -> float | None:
    if end is None:
        return None
    return round((end.created_at - start).total_seconds() * 1000, 3)


def _step_result(start: Event, end: Event | None) -> Any:
    if end is None:
        # A timer that has not fired still has something worth showing: when it will.
        return start.payload.get("fire_at")
    for key in ("result", "data", "fire_at"):
        if key in end.payload:
            return end.payload[key]
    return None


def _run_error(terminal: Event | None, parked: Event | None) -> str | None:
    """Why the run is not going anywhere — whether it failed or is parked."""
    if terminal is not None and terminal.type == EventType.WORKFLOW_FAILED:
        return str(terminal.payload.get("error"))
    if parked is not None:
        return str(parked.payload.get("error"))
    return None


def _step_error(end: Event | None) -> str | None:
    if end is None or "error" not in end.payload:
        return None
    return str(end.payload["error"])


def _child_run_id(start: Event, end: Event | None) -> str | None:
    for event in (end, start):
        if event is not None and "child_run_id" in event.payload:
            return str(event.payload["child_run_id"])
    return None


def _parent_payload(started: Event) -> dict[str, Any] | None:
    parent = ParentRef.from_payload(started.payload.get("parent"))
    return parent.as_payload() if parent is not None else None
