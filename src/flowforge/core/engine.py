"""The durable execution engine: it drives a run forward by one attempt.

``drive`` loads the run's history, replays the workflow function, and commits any
new events. It returns when the run completes, fails (after running saga
compensations), or suspends. ``start`` seeds a run; ``fire_timer`` and
``send_signal`` deliver the external events that wake a suspended run — each just
appends the awaited event and re-drives.

The engine is also the :class:`~flowforge.core.children.ChildLauncher`: it seeds
child runs on a parent's behalf and, when a run finishes, reports the outcome back
into its parent's log and re-enqueues it. A child is an ordinary run in every
other respect, which is why fan-out needs no scheduler of its own.
"""

from __future__ import annotations

import logging
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter

from flowforge.core.budget import BudgetGuard
from flowforge.core.children import ChildOutcome, ParentRef
from flowforge.core.errors import (
    ActivityFailedError,
    ConcurrencyError,
    RunNotFoundError,
    Suspended,
)
from flowforge.core.event_store import EventStore
from flowforge.core.events import Event, EventType
from flowforge.core.timeline import RunStatus, derive_status, parked_event, terminal_event
from flowforge.core.timers import TimerStore
from flowforge.workflow.context import DEFAULT_TENANT, Clock, WorkflowContext
from flowforge.workflow.definition import Registry, WorkflowDef

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # the queue package imports the engine, so keep the edge one-way
    from flowforge.queue.base import TaskQueue


@dataclass
class RunResult:
    status: RunStatus
    result: Any = None
    error: str | None = None


class Engine:
    def __init__(
        self,
        store: EventStore,
        registry: Registry,
        clock: Clock | None = None,
        timers: TimerStore | None = None,
        budget: BudgetGuard | None = None,
        queue: TaskQueue | None = None,
    ) -> None:
        self._store = store
        self._registry = registry
        self._clock = clock
        self._timers = timers
        self._budget = budget
        self._queue = queue

    async def create_run(
        self,
        run_id: str,
        workflow: str | WorkflowDef[Any, Any],
        workflow_input: Any,
        *,
        tenant: str = DEFAULT_TENANT,
        parent: ParentRef | None = None,
    ) -> WorkflowDef[Any, Any]:
        """Seed a run (append ``WORKFLOW_STARTED``) without driving it. Used by
        the worker path, which enqueues the run for a worker to drive.

        The tenant is written into the first event because it decides who is
        billed for the run's LLM calls — it must survive a crash and be identical
        on every replay, which only the log guarantees."""
        wf = self.resolve(workflow)
        payload: dict[str, Any] = {
            "input": TypeAdapter(wf.input_type).dump_python(workflow_input, mode="json"),
            "tenant": tenant,
        }
        if parent is not None:
            payload["parent"] = parent.as_payload()
        started = Event(seq=0, type=EventType.WORKFLOW_STARTED, name=wf.name, payload=payload)
        await self._store.append(run_id, [started], expected_version=0)
        return wf

    async def start(
        self,
        run_id: str,
        workflow: str | WorkflowDef[Any, Any],
        workflow_input: Any,
        *,
        tenant: str = DEFAULT_TENANT,
    ) -> RunResult:
        """Seed and drive a run inline (convenient for tests and simple embeds)."""
        await self.create_run(run_id, workflow, workflow_input, tenant=tenant)
        return await self.drive(run_id)

    async def drive(self, run_id: str) -> RunResult:
        """Advance a run by one attempt.

        Three ways this ends badly, and they are not the same thing:
        an activity that exhausted its retries is a *business* failure — the run
        fails and compensates; a bug in the workflow function itself is a failure
        of *code* — the run is parked (:attr:`RunStatus.STUCK`) with the error
        recorded and nothing rolled back, because compensating a payment over a
        ``KeyError`` destroys more than it saves; and a store that cannot be
        reached is neither, so it propagates to the caller to retry."""
        history = await self._store.load(run_id)
        if not history:
            raise RunNotFoundError(f"run {run_id!r} does not exist")

        terminal = terminal_event(history)
        if terminal is not None:
            return self._terminal_result(terminal)

        ctx = WorkflowContext(
            run_id,
            history,
            self._store,
            clock=self._clock,
            timers=self._timers,
            budget=self._budget,
            children=self,
        )

        try:
            # Resolution and input validation sit inside the guard on purpose:
            # an unregistered workflow and a run seeded before its input schema
            # changed are both "deploy a fix and drive it again", not crashes.
            wf = self._registry.get(history[0].name or "")
            workflow_input = TypeAdapter(wf.input_type).validate_python(
                history[0].payload["input"]
            )
            result = await wf.fn(ctx, workflow_input)
        except Suspended:
            return RunResult(RunStatus.SUSPENDED)
        except ActivityFailedError as exc:
            await ctx._run_compensations()
            await ctx._append(EventType.WORKFLOW_FAILED, payload={"error": str(exc)})
            await self._notify_parent(history[0], run_id, error=str(exc))
            return RunResult(RunStatus.FAILED, error=str(exc))
        except ConcurrencyError:
            # Another writer advanced this run underneath us — a race, not a bug.
            # Parking it would strand a healthy run; reload and drive it again.
            raise
        except Exception as exc:
            return await self._park(ctx, exc)

        payload = TypeAdapter(wf.output_type).dump_python(result, mode="json")
        await ctx._append(EventType.WORKFLOW_COMPLETED, payload={"result": payload})
        await self._notify_parent(history[0], run_id, result=payload)
        return RunResult(RunStatus.COMPLETED, result=result)

    async def _park(self, ctx: WorkflowContext, exc: Exception) -> RunResult:
        """Record that a drive attempt broke on workflow code, and stop.

        The event carries no ``command_seq``, so replay walks straight past it:
        parking annotates the log without becoming part of the run's state. Fix
        the code, drive the run again, and it continues from where it was."""
        message = f"{type(exc).__name__}: {exc}"
        logger.exception("run %s is stuck: %s", ctx.run_id, message)
        await ctx._append(
            EventType.WORKFLOW_TASK_FAILED,
            payload={"error": message, "type": type(exc).__name__},
        )
        return RunResult(RunStatus.STUCK, error=message)

    async def fire_timer(self, run_id: str) -> RunResult:
        """Deliver the oldest pending timer, then advance the run."""
        history = await self._store.load(run_id)
        cs = _pending_command(history, EventType.TIMER_STARTED, EventType.TIMER_FIRED)
        if cs is None:
            raise ValueError(f"run {run_id!r} has no pending timer")
        await self._deliver(run_id, history, EventType.TIMER_FIRED, command_seq=cs)
        return await self.drive(run_id)

    async def deliver_timer(self, run_id: str, command_seq: int) -> None:
        """Record that a specific timer fired, without driving the run.

        Used by the timer wheel, which enqueues the run for a worker to drive.
        Idempotent: a timer already delivered is a no-op."""
        history = await self._store.load(run_id)
        already = any(
            e.type == EventType.TIMER_FIRED and e.command_seq == command_seq
            for e in history
        )
        if already:
            return
        await self._deliver(run_id, history, EventType.TIMER_FIRED, command_seq=command_seq)

    async def send_signal(self, run_id: str, name: str, data: Any = None) -> RunResult:
        """Deliver an external signal (e.g. a human approval), then advance."""
        await self.deliver_signal(run_id, name, data)
        return await self.drive(run_id)

    async def deliver_signal(self, run_id: str, name: str, data: Any = None) -> None:
        """Record an external signal without driving the run (worker model)."""
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

    async def describe(self, run_id: str) -> RunResult:
        """Report a run's current status from the log, without driving it."""
        history = await self._store.load(run_id)
        if not history:
            raise RunNotFoundError(run_id)
        terminal = terminal_event(history)
        if terminal is not None:
            return self._terminal_result(terminal)
        parked = parked_event(history)
        if parked is not None:
            return RunResult(RunStatus.STUCK, error=str(parked.payload.get("error")))
        return RunResult(derive_status(history))

    # -- child workflows (the ChildLauncher protocol) ------------------------

    def resolve(self, workflow: str | WorkflowDef[Any, Any]) -> WorkflowDef[Any, Any]:
        if isinstance(workflow, str):
            return self._registry.get(workflow)
        return workflow

    def child_run_id(self, parent_run_id: str, command_seq: int) -> str:
        """Derived, not random: replaying the parent must recognise the child it
        already started rather than start a second one."""
        return f"{parent_run_id}.{command_seq}"

    async def start_child(
        self,
        parent: ParentRef,
        workflow: str | WorkflowDef[Any, Any],
        workflow_input: Any,
        *,
        tenant: str = DEFAULT_TENANT,
    ) -> str:
        run_id = self.child_run_id(parent.run_id, parent.command_seq)
        # A ConcurrencyError here means an earlier attempt seeded this exact run
        # and crashed before recording CHILD_STARTED. It is the run we wanted.
        with suppress(ConcurrencyError):
            await self.create_run(
                run_id, workflow, workflow_input, tenant=tenant, parent=parent
            )
        if self._queue is not None:
            await self._queue.enqueue(run_id, tenant=tenant)
        return run_id

    async def child_outcome(self, run_id: str) -> ChildOutcome | None:
        history = await self._store.load(run_id)
        if not history:
            return None
        terminal = terminal_event(history)
        if terminal is None:
            return None
        completed = terminal.type == EventType.WORKFLOW_COMPLETED
        return ChildOutcome(
            run_id=run_id,
            completed=completed,
            result=terminal.payload.get("result") if completed else None,
            error=None if completed else str(terminal.payload.get("error")),
        )

    # -- internals ----------------------------------------------------------

    async def _notify_parent(
        self,
        started: Event,
        child_run_id: str,
        *,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        """Report a finished child into its parent's log and wake the parent.

        Best-effort by design: if the parent has already terminated, or another
        writer is mid-append, the notice is dropped — the parent reconciles from
        the child's own log the next time it runs, so nothing is lost, only
        delayed. The one gap left is a crash between this append and the enqueue
        below, which leaves the parent holding the result but not queued to act
        on it; a sweeper for stranded parents is the next brick."""
        parent = ParentRef.from_payload(started.payload.get("parent"))
        if parent is None:
            return
        history = await self._store.load(parent.run_id)
        if not history or terminal_event(history) is not None:
            return
        if any(
            e.command_seq == parent.command_seq
            and e.type in (EventType.CHILD_COMPLETED, EventType.CHILD_FAILED)
            for e in history
        ):
            return

        completed = error is None
        event = Event(
            seq=len(history),
            type=EventType.CHILD_COMPLETED if completed else EventType.CHILD_FAILED,
            command_seq=parent.command_seq,
            name=started.name,
            payload=(
                {"child_run_id": child_run_id, "result": result}
                if completed
                else {"child_run_id": child_run_id, "error": error}
            ),
        )
        try:
            await self._store.append(parent.run_id, [event], expected_version=len(history))
        except ConcurrencyError:
            return
        if self._queue is not None:
            await self._queue.enqueue(parent.run_id)

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

    def _terminal_result(self, event: Event) -> RunResult:
        if event.type == EventType.WORKFLOW_COMPLETED:
            return RunResult(RunStatus.COMPLETED, result=event.payload.get("result"))
        return RunResult(RunStatus.FAILED, error=event.payload.get("error"))


def _pending_command(
    history: list[Event],
    started_type: EventType,
    *resolved_types: EventType,
    name: str | None = None,
) -> int | None:
    """The command_seq of the oldest ``started_type`` not yet resolved."""
    resolved = {e.command_seq for e in history if e.type in resolved_types}
    for event in history:
        if (
            event.type == started_type
            and event.command_seq not in resolved
            and (name is None or event.name == name)
        ):
            return event.command_seq
    return None
