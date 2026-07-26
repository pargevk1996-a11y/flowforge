"""The durable execution engine: it drives a run forward by one attempt.

``drive`` loads the run's history, replays the workflow function, and commits any
new events. It returns when the run completes, fails (after running saga
compensations), or suspends. ``start`` seeds a run; ``fire_timer`` and
``send_signal`` deliver the external events that wake a suspended run — each just
appends the awaited event and re-drives.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import TypeAdapter

from flowforge.core.errors import ActivityFailedError, Suspended
from flowforge.core.event_store import EventStore
from flowforge.core.events import TERMINAL_EVENTS, Event, EventType
from flowforge.workflow.context import Clock, WorkflowContext
from flowforge.workflow.definition import Registry, WorkflowDef


class RunStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    SUSPENDED = "suspended"


@dataclass
class RunResult:
    status: RunStatus
    result: Any = None
    error: str | None = None


class Engine:
    def __init__(
        self, store: EventStore, registry: Registry, clock: Clock | None = None
    ) -> None:
        self._store = store
        self._registry = registry
        self._clock = clock

    async def start(
        self, run_id: str, workflow: str | WorkflowDef[Any, Any], workflow_input: Any
    ) -> RunResult:
        wf = self._resolve(workflow)
        payload = {
            "input": TypeAdapter(wf.input_type).dump_python(workflow_input, mode="json")
        }
        started = Event(seq=0, type=EventType.WORKFLOW_STARTED, name=wf.name, payload=payload)
        await self._store.append(run_id, [started], expected_version=0)
        return await self.drive(run_id)

    async def drive(self, run_id: str) -> RunResult:
        history = await self._store.load(run_id)
        if not history:
            raise ValueError(f"run {run_id!r} does not exist")

        last = history[-1]
        if last.type in TERMINAL_EVENTS:
            return self._terminal_result(last)

        wf = self._registry.get(history[0].name or "")
        workflow_input = TypeAdapter(wf.input_type).validate_python(
            history[0].payload["input"]
        )
        ctx = WorkflowContext(run_id, history, self._store, clock=self._clock)

        try:
            result = await wf.fn(ctx, workflow_input)
        except Suspended:
            return RunResult(RunStatus.SUSPENDED)
        except ActivityFailedError as exc:
            await ctx._run_compensations()
            await ctx._append(EventType.WORKFLOW_FAILED, payload={"error": str(exc)})
            return RunResult(RunStatus.FAILED, error=str(exc))

        await ctx._append(
            EventType.WORKFLOW_COMPLETED,
            payload={"result": TypeAdapter(wf.output_type).dump_python(result, mode="json")},
        )
        return RunResult(RunStatus.COMPLETED, result=result)

    async def fire_timer(self, run_id: str) -> RunResult:
        """Deliver the oldest pending timer, then advance the run."""
        history = await self._store.load(run_id)
        cs = _pending_command(history, EventType.TIMER_STARTED, EventType.TIMER_FIRED)
        if cs is None:
            raise ValueError(f"run {run_id!r} has no pending timer")
        await self._deliver(run_id, history, EventType.TIMER_FIRED, command_seq=cs)
        return await self.drive(run_id)

    async def send_signal(self, run_id: str, name: str, data: Any = None) -> RunResult:
        """Deliver an external signal (e.g. a human approval), then advance."""
        history = await self._store.load(run_id)
        cs = _pending_command(
            history, EventType.WAIT_STARTED, EventType.SIGNAL_RECEIVED, name=name
        )
        if cs is None:
            raise ValueError(f"run {run_id!r} is not waiting for signal {name!r}")
        await self._deliver(
            run_id, history, EventType.SIGNAL_RECEIVED, command_seq=cs, name=name,
            payload={"data": data},
        )
        return await self.drive(run_id)

    # -- internals ----------------------------------------------------------

    async def _deliver(
        self,
        run_id: str,
        history: list[Event],
        type: EventType,
        *,
        command_seq: int,
        name: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event = Event(
            seq=len(history),
            type=type,
            command_seq=command_seq,
            name=name,
            payload=payload or {},
        )
        await self._store.append(run_id, [event], expected_version=len(history))

    def _resolve(self, workflow: str | WorkflowDef[Any, Any]) -> WorkflowDef[Any, Any]:
        if isinstance(workflow, str):
            return self._registry.get(workflow)
        return workflow

    def _terminal_result(self, event: Event) -> RunResult:
        if event.type == EventType.WORKFLOW_COMPLETED:
            return RunResult(RunStatus.COMPLETED, result=event.payload.get("result"))
        return RunResult(RunStatus.FAILED, error=event.payload.get("error"))


def _pending_command(
    history: list[Event],
    started_type: EventType,
    resolved_type: EventType,
    *,
    name: str | None = None,
) -> int | None:
    """The command_seq of the oldest ``started_type`` not yet ``resolved_type``."""
    resolved = {e.command_seq for e in history if e.type == resolved_type}
    for event in history:
        if (
            event.type == started_type
            and event.command_seq not in resolved
            and (name is None or event.name == name)
        ):
            return event.command_seq
    return None
