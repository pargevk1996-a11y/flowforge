"""The API a workflow uses to interact with the durable world.

Every ``await ctx.*`` is a *command*, numbered deterministically by replay order.
On each drive the engine replays the workflow function from the top: a command
whose result is already in the log returns that result without re-executing its
side effect (this is both resume-after-crash and idempotency), while a command
with no recorded result executes for real and commits its outcome. ``sleep`` and
``wait_for_signal`` record what they are waiting for and raise :class:`Suspended`,
freeing the worker until a timer fires or a signal arrives.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast, get_type_hints

from pydantic import TypeAdapter

from flowforge.core.errors import ActivityFailedError, NonRetryableError, Suspended
from flowforge.core.event_store import EventStore
from flowforge.core.events import Event, EventType, utcnow
from flowforge.core.retry import RetryPolicy
from flowforge.core.timers import TimerStore

Clock = Callable[[], datetime]


def _return_adapter(fn: Callable[..., Any]) -> TypeAdapter[Any]:
    """Build a (de)serializer from a function's declared return type so activity
    results survive a round-trip through the JSON event log with their type."""
    hints = get_type_hints(fn)
    return TypeAdapter(hints.get("return", Any))


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
    ) -> None:
        self.run_id = run_id
        self._store = store
        self._timers = timers
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
    ) -> T:
        """Run a side-effecting step durably, with typed retry and an optional
        compensation for the saga rollback.

        ``result_type`` overrides the return-annotation used to (de)serialize the
        result through the event log — needed when the callable's return type is
        generic (e.g. an :class:`~flowforge.llm.step.LLMStep`)."""
        cs = self._next_command_seq()
        label = name or fn.__name__
        adapter = self._adapter(fn, result_type)

        completed = self._find(cs, EventType.ACTIVITY_COMPLETED)
        if completed is not None:
            # Already done on a previous drive: return the recorded result and
            # re-register the compensation so the saga list is complete on replay.
            self._register_compensation(label, compensate)
            return cast(T, adapter.validate_python(completed.payload["result"]))

        failed = self._find(cs, EventType.ACTIVITY_FAILED)
        if failed is not None:
            raise ActivityFailedError(label, str(failed.payload.get("error")))

        await self._append(EventType.ACTIVITY_SCHEDULED, command_seq=cs, name=label)
        return await self._execute(fn, args, cs, label, retry or RetryPolicy(), compensate, adapter)

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
