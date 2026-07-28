"""The API a workflow uses to interact with the durable world.

Every ``await ctx.*`` is a *command*, numbered deterministically by replay order.
On each drive the engine replays the workflow function from the top: a command
whose result is already in the log returns that result without re-executing its
side effect (this is both resume-after-crash and idempotency), while a command
with no recorded result executes for real and commits its outcome. ``sleep`` and
``wait_for_signal`` record what they are waiting for and raise :class:`Suspended`,
freeing the worker until a timer fires or a signal arrives.

Concurrency does not break that. ``map``, ``map_llm`` and ``children`` fan out,
but they hand out their command numbers *up front, in item order*, before anything
is awaited — so numbering is a property of the workflow's shape, not of who
finished first. What varies between runs is the order events land in the log,
which nothing reads for meaning: results are always looked up by command number.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, cast, get_type_hints

from pydantic import TypeAdapter

from flowforge.core.budget import BudgetGuard, CostMeter, MeteredStep
from flowforge.core.children import ChildLauncher, ParentRef
from flowforge.core.errors import (
    ActivityFailedError,
    ChildFailedError,
    NonRetryableError,
    Suspended,
)
from flowforge.core.event_store import EventStore
from flowforge.core.events import Event, EventType, utcnow
from flowforge.core.retry import RetryPolicy
from flowforge.core.timers import TimerStore
from flowforge.core.tracing import NO_TRACING, Tracer

if TYPE_CHECKING:  # definition.py imports this module, so keep the edge one-way
    from flowforge.workflow.definition import WorkflowDef

Clock = Callable[[], datetime]

DEFAULT_TENANT = "default"

_CHILD_RESOLVED = (EventType.CHILD_COMPLETED, EventType.CHILD_FAILED)


def _return_adapter(fn: Callable[..., Any]) -> TypeAdapter[Any]:
    """Build a (de)serializer from a function's declared return type so activity
    results survive a round-trip through the JSON event log with their type."""
    hints = get_type_hints(fn)
    return TypeAdapter(hints.get("return", Any))


def _tenant_of(history: list[Event]) -> str:
    """The tenant recorded on ``WORKFLOW_STARTED``.

    Tenancy is part of the run's identity, so it is written into the log at start
    and read back on every replay — never carried in from the queue item, which
    would make cost attribution depend on how the run happened to be delivered."""
    if not history:
        return DEFAULT_TENANT
    tenant = history[0].payload.get("tenant")
    return str(tenant) if tenant else DEFAULT_TENANT


@dataclass
class _Compensation:
    name: str
    fn: Callable[[], Awaitable[None]]


class WorkflowContext:
    def __init__(
        self,
        run_id: str,
        history: list[Event],
        store: EventStore,
        clock: Clock | None = None,
        timers: TimerStore | None = None,
        budget: BudgetGuard | None = None,
        children: ChildLauncher | None = None,
        tracer: Tracer = NO_TRACING,
    ) -> None:
        self.run_id = run_id
        self.tenant = _tenant_of(history)
        self._store = store
        self._timers = timers
        self._budget = budget
        self._children = children
        self._tracer = tracer
        # Fan-out means several commands append at once; the log is single-writer,
        # so serialise the read-modify-write of the version here rather than let
        # concurrent branches collide on it.
        self._append_lock = asyncio.Lock()
        self._history = list(history)
        self._version = len(history)
        self._command_seq = 0
        self._clock: Clock = clock or utcnow
        self._compensations: list[_Compensation] = []
        self._by_command: dict[int, list[Event]] = {}
        for event in history:
            if event.command_seq is not None:
                self._by_command.setdefault(event.command_seq, []).append(event)

    # -- public workflow API ------------------------------------------------

    def now(self) -> datetime:
        """Wall-clock time. Deterministic decisions should derive from recorded
        activity results or timers rather than this."""
        return self._clock()

    async def activity[T](
        self,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        name: str | None = None,
        retry: RetryPolicy | None = None,
        compensate: Callable[[], Awaitable[None]] | None = None,
        result_type: type[T] | None = None,
        kind: str = "activity",
    ) -> T:
        """Run a side-effecting step durably, with typed retry and an optional
        compensation for the saga rollback.

        ``result_type`` overrides the return-annotation used to (de)serialize the
        result through the event log — needed when the callable's return type is
        generic (e.g. an :class:`~flowforge.llm.step.LLMStep`)."""
        return await self._activity_at(
            self._next_command_seq(),
            fn,
            args,
            label=name or fn.__name__,
            retry=retry,
            compensate=compensate,
            adapter=self._adapter(fn, result_type),
            kind=kind,
        )

    async def _activity_at[T](
        self,
        cs: int,
        fn: Callable[..., Awaitable[T]],
        args: tuple[Any, ...],
        *,
        label: str,
        retry: RetryPolicy | None,
        compensate: Callable[[], Awaitable[None]] | None,
        adapter: TypeAdapter[Any],
        kind: str = "activity",
    ) -> T:
        """One activity under an already-allocated command number.

        Split out from :meth:`activity` because a fan-out has to allocate all of
        its numbers before it awaits anything."""
        completed = self._find(cs, EventType.ACTIVITY_COMPLETED)
        if completed is not None:
            # Already done on a previous drive: return the recorded result and
            # re-register the compensation so the saga list is complete on replay.
            self._register_compensation(label, compensate)
            return cast(T, adapter.validate_python(completed.payload["result"]))

        failed = self._find(cs, EventType.ACTIVITY_FAILED)
        if failed is not None:
            raise ActivityFailedError(label, str(failed.payload.get("error")))

        # The kind is recorded, not inferred: a timeline that cannot tell a model
        # call from an ordinary step cannot show what a run spent its money on.
        # Spans cover execution only. A replayed command did no work on this
        # drive, and emitting a span for it would fill the trace with copies of
        # everything the run has ever done, once per attempt.
        with self._tracer.span(
            f"{kind} {label}",
            attributes={"flowforge.command_seq": cs, "flowforge.step.kind": kind},
        ):
            await self._append(
                EventType.ACTIVITY_SCHEDULED,
                command_seq=cs,
                name=label,
                payload={"kind": kind},
            )
            return await self._execute(
                fn, args, cs, label, retry or RetryPolicy(), compensate, adapter
            )

    async def llm[TOutput](
        self,
        step: MeteredStep[TOutput],
        content: str,
        *,
        name: str | None = None,
        retry: RetryPolicy | None = None,
    ) -> TOutput:
        """Run a typed LLM step as a durable activity, metered against the run's
        tenant budget.

        The step is a shared, run-agnostic object; what this adds is the binding to
        *this* run — a meter carrying the tenant and command_seq, so every provider
        call the step makes (including its schema retries) is checked against the
        budget before and written to the cost ledger after. Exceeding the budget
        raises :class:`~flowforge.core.errors.BudgetExceededError`, which is
        non-retryable and therefore cancels the run through the saga path.

        Like any activity, the *validated* result is recorded once: replay returns
        it without calling the model — or spending a cent — again."""
        return await self.activity(
            partial(step.run, meter=self._meter(self._command_seq)),
            content,
            name=name or step.name,
            retry=retry,
            result_type=step.output_type,
            kind="llm",
        )

    async def map[TItem, TOut](
        self,
        fn: Callable[[TItem], Awaitable[TOut]],
        items: Sequence[TItem],
        *,
        concurrency: int = 5,
        name: str | None = None,
        retry: RetryPolicy | None = None,
        result_type: type[TOut] | None = None,
        compensate: Callable[[TItem], Awaitable[None]] | None = None,
    ) -> list[TOut]:
        """Run one activity per item, at most ``concurrency`` at a time, and
        return the results **in item order**.

        Each item gets its own command number and its own pair of events, so a
        crash halfway through resumes with the finished items already done and
        only the rest to run. Failures do not cancel their siblings: everything
        in flight is allowed to finish and record itself first — abandoning a
        side effect that already happened is how a fan-out loses money — and then
        the earliest failure, by item order, is raised."""
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        label = name or fn.__name__
        adapter = self._adapter(fn, result_type)
        # Allocate every number before the first await: this is what makes the
        # numbering depend on the item order rather than on the event loop.
        seqs = [self._next_command_seq() for _ in items]

        jobs = [
            partial(
                self._activity_at,
                cs,
                fn,
                (item,),
                label=f"{label}[{index}]",
                retry=retry,
                compensate=None,
                adapter=adapter,
            )
            for index, (cs, item) in enumerate(zip(seqs, items, strict=True))
        ]
        outcomes = await self._fan_out(jobs, concurrency=concurrency)

        # Register compensations in item order, so the saga unwinds the fan-out
        # in a defined order rather than in whatever order the items finished.
        for index, (item, outcome) in enumerate(zip(items, outcomes, strict=True)):
            if not isinstance(outcome, BaseException) and compensate is not None:
                self._register_compensation(f"{label}[{index}]", partial(compensate, item))
        return cast(list[TOut], self._first_failure_or_results(outcomes))

    async def map_llm[TOut](
        self,
        step: MeteredStep[TOut],
        contents: Sequence[str],
        *,
        concurrency: int = 5,
        name: str | None = None,
        retry: RetryPolicy | None = None,
    ) -> list[TOut]:
        """Fan a typed LLM step out over many inputs, bounded and metered.

        The budget is what makes the bound matter: forty paragraphs are forty
        billable calls, each checked against the tenant's remaining budget before
        it is made, so a fan-out is the one place a runaway workflow could spend a
        month's allowance in a second — and cannot."""
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        label = name or step.name
        adapter = TypeAdapter(step.output_type)
        seqs = [self._next_command_seq() for _ in contents]

        jobs = [
            partial(
                self._activity_at,
                cs,
                partial(step.run, meter=self._meter(cs)),
                (content,),
                label=f"{label}[{index}]",
                retry=retry,
                compensate=None,
                adapter=adapter,
                kind="llm",
            )
            for index, (cs, content) in enumerate(zip(seqs, contents, strict=True))
        ]
        outcomes = await self._fan_out(jobs, concurrency=concurrency)
        return cast(list[TOut], self._first_failure_or_results(outcomes))

    async def child[TOut](
        self,
        workflow: str | WorkflowDef[Any, TOut],
        workflow_input: Any,
        *,
        name: str | None = None,
    ) -> TOut:
        """Run another workflow as a child run and return its result.

        The parent suspends while the child runs, and is woken when the child
        reaches a terminal state. A failed child raises
        :class:`~flowforge.core.errors.ChildFailedError`, which unwinds the parent
        through its own compensations.

        Passing the :class:`~flowforge.workflow.definition.WorkflowDef` rather
        than its name carries the child's result type through to the caller; by
        name, the caller annotates."""
        results = await self._children_at(
            [self._next_command_seq()], workflow, [workflow_input], concurrency=1, name=name
        )
        return cast(TOut, results[0])

    async def children[TOut](
        self,
        workflow: str | WorkflowDef[Any, TOut],
        inputs: Sequence[Any],
        *,
        concurrency: int = 5,
        name: str | None = None,
    ) -> list[TOut]:
        """Fan out over child runs, at most ``concurrency`` in flight, and return
        their results in input order.

        Unlike :meth:`map`, the bound here is *durable*: the parent starts as many
        children as the limit allows, suspends, and starts the next one each time
        a child reports back. A thousand-item fan-out never has a thousand runs in
        flight, and the pacing survives a restart because it is derived from the
        log rather than held in memory."""
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        seqs = [self._next_command_seq() for _ in inputs]
        return cast(
            list[TOut],
            await self._children_at(
                seqs, workflow, inputs, concurrency=concurrency, name=name
            ),
        )

    async def sleep(self, seconds: float, *, name: str = "sleep") -> None:
        """Durably pause the run. The process is freed; a timer re-enqueues it."""
        cs = self._next_command_seq()
        if self._find(cs, EventType.TIMER_FIRED) is not None:
            return
        if self._find(cs, EventType.TIMER_STARTED) is None:
            fire_at = self.now() + timedelta(seconds=seconds)
            # Schedule before recording the event: if we crash between the two,
            # replay re-schedules (a no-op on the timer store) — a wakeup is never
            # lost, only ever duplicated harmlessly.
            if self._timers is not None:
                await self._timers.schedule(self.run_id, cs, fire_at)
            await self._append(
                EventType.TIMER_STARTED,
                command_seq=cs,
                name=name,
                payload={"fire_at": fire_at.isoformat()},
            )
        raise Suspended(f"timer:{cs}")

    async def wait_for_signal[T](self, name: str, data_type: type[T]) -> T:
        """Suspend until an external signal ``name`` is delivered; return its
        typed payload. This is the human-in-the-loop primitive."""
        cs = self._next_command_seq()
        received = self._find(cs, EventType.SIGNAL_RECEIVED)
        if received is not None:
            return TypeAdapter(data_type).validate_python(received.payload.get("data"))
        if self._find(cs, EventType.WAIT_STARTED) is None:
            await self._append(EventType.WAIT_STARTED, command_seq=cs, name=name)
        raise Suspended(f"signal:{name}:{cs}")

    # -- fan-out internals --------------------------------------------------

    async def _fan_out(
        self, jobs: Sequence[Callable[[], Awaitable[Any]]], *, concurrency: int
    ) -> list[Any]:
        """Run ``jobs`` with at most ``concurrency`` in flight, collecting
        failures rather than cancelling the rest."""
        limit = asyncio.Semaphore(concurrency)

        async def guarded(job: Callable[[], Awaitable[Any]]) -> Any:
            async with limit:
                return await job()

        return list(
            await asyncio.gather(*(guarded(job) for job in jobs), return_exceptions=True)
        )

    @staticmethod
    def _first_failure_or_results(outcomes: list[Any]) -> list[Any]:
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        return outcomes

    def _meter(self, command_seq: int) -> CostMeter | None:
        if self._budget is None:
            return None
        return self._budget.meter(self.run_id, self.tenant, command_seq)

    async def _children_at(
        self,
        seqs: list[int],
        workflow: str | WorkflowDef[Any, Any],
        inputs: Sequence[Any],
        *,
        concurrency: int,
        name: str | None,
    ) -> list[Any]:
        """Advance a block of child commands by one drive.

        Every drive re-derives the whole picture from the log: what has come back,
        what is still out there, and what has not been started yet. That is what
        lets the concurrency bound survive a restart — it is recomputed, never
        remembered."""
        if self._children is None:
            raise RuntimeError(
                "child workflows need an engine with a child launcher; "
                "this context was built without one"
            )
        wf = self._children.resolve(workflow)
        adapter = TypeAdapter(wf.output_type)
        label = name or wf.name

        results: list[Any] = [None] * len(seqs)
        unstarted: list[tuple[int, int]] = []
        in_flight = 0
        failure: ChildFailedError | None = None

        for index, cs in enumerate(seqs):
            outcome_event = await self._child_event(cs, f"{label}[{index}]")
            if outcome_event is None:
                if self._find(cs, EventType.CHILD_STARTED) is None:
                    unstarted.append((index, cs))
                else:
                    in_flight += 1
                continue
            if outcome_event.type is EventType.CHILD_COMPLETED:
                results[index] = adapter.validate_python(outcome_event.payload.get("result"))
            elif failure is None:
                # Fail fast on the earliest failure by input order. Siblings still
                # running are independent runs: they finish, compensate their own
                # work, and find a parent that has already moved on.
                failure = ChildFailedError(
                    wf.name,
                    str(outcome_event.payload.get("child_run_id")),
                    str(outcome_event.payload.get("error")),
                )

        if failure is not None:
            raise failure
        if not unstarted and in_flight == 0:
            return results

        for index, cs in unstarted:
            if in_flight >= concurrency:
                break
            with self._tracer.span(
                f"child {label}[{index}]",
                attributes={"flowforge.command_seq": cs, "flowforge.child.workflow": wf.name},
            ):
                # Inside the span, so the child's own root anchor parents onto this
                # command: a thousand-way fan-out stays one trace.
                child_run_id = await self._children.start_child(
                    ParentRef(self.run_id, cs), wf, inputs[index], tenant=self.tenant
                )
                await self._append(
                    EventType.CHILD_STARTED,
                    command_seq=cs,
                    name=f"{label}[{index}]",
                    payload={"child_run_id": child_run_id, "workflow": wf.name},
                )
            in_flight += 1
        raise Suspended(f"children:{label}:{self.run_id}")

    async def _child_event(self, cs: int, label: str) -> Event | None:
        """The recorded outcome of a child command, reconciling if need be.

        A child appends its result to the parent's log and then enqueues it; a
        crash between the two would leave the parent asleep on a child that has
        long finished. So whenever the parent runs and a child is unaccounted for,
        it asks the child's own log directly and records what it finds."""
        for event in self._by_command.get(cs, []):
            if event.type in _CHILD_RESOLVED:
                return event

        started = self._find(cs, EventType.CHILD_STARTED)
        if started is None or self._children is None:
            return None
        outcome = await self._children.child_outcome(str(started.payload["child_run_id"]))
        if outcome is None:
            return None
        if outcome.completed:
            return await self._append(
                EventType.CHILD_COMPLETED,
                command_seq=cs,
                name=label,
                payload={"child_run_id": outcome.run_id, "result": outcome.result},
            )
        return await self._append(
            EventType.CHILD_FAILED,
            command_seq=cs,
            name=label,
            payload={"child_run_id": outcome.run_id, "error": outcome.error},
        )

    # -- engine-facing internals -------------------------------------------

    async def _execute[T](
        self,
        fn: Callable[..., Awaitable[T]],
        args: tuple[Any, ...],
        cs: int,
        label: str,
        retry: RetryPolicy,
        compensate: Callable[[], Awaitable[None]] | None,
        adapter: TypeAdapter[Any],
    ) -> T:
        last_error: Exception | None = None
        for attempt in range(1, retry.max_attempts + 1):
            try:
                result = await fn(*args)
            except NonRetryableError as exc:
                last_error = exc
                break
            except Exception as exc:  # policy decides retry vs. fail
                last_error = exc
                if attempt < retry.max_attempts:
                    await asyncio.sleep(retry.backoff(attempt + 1))
                continue
            else:
                await self._append(
                    EventType.ACTIVITY_COMPLETED,
                    command_seq=cs,
                    name=label,
                    payload={"result": adapter.dump_python(result, mode="json")},
                )
                self._register_compensation(label, compensate)
                return result

        message = str(last_error)
        await self._append(
            EventType.ACTIVITY_FAILED, command_seq=cs, name=label, payload={"error": message}
        )
        raise ActivityFailedError(label, message) from last_error

    async def _run_compensations(self) -> None:
        """Saga rollback: undo completed steps in reverse order."""
        for comp in reversed(self._compensations):
            await comp.fn()
            await self._append(EventType.COMPENSATION_COMPLETED, name=comp.name)

    async def _append(
        self,
        type: EventType,
        *,
        command_seq: int | None = None,
        name: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        async with self._append_lock:
            event = Event(
                seq=self._version,
                type=type,
                command_seq=command_seq,
                name=name,
                payload=payload or {},
            )
            await self._store.append(self.run_id, [event], expected_version=self._version)
            self._version += 1
            self._history.append(event)
            if command_seq is not None:
                self._by_command.setdefault(command_seq, []).append(event)
            return event

    def _next_command_seq(self) -> int:
        cs = self._command_seq
        self._command_seq += 1
        return cs

    def _find(self, command_seq: int, type: EventType) -> Event | None:
        for event in self._by_command.get(command_seq, []):
            if event.type == type:
                return event
        return None

    @staticmethod
    def _adapter(fn: Callable[..., Any], result_type: type[Any] | None) -> TypeAdapter[Any]:
        if result_type is not None:
            return TypeAdapter(result_type)
        return _return_adapter(fn)

    def _register_compensation(
        self, name: str, compensate: Callable[[], Awaitable[None]] | None
    ) -> None:
        if compensate is not None:
            self._compensations.append(_Compensation(name=name, fn=compensate))
