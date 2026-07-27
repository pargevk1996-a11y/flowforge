"""Triggers: the bindings that turn an external event into a run.

A trigger says *which workflow* an event starts, *how* the event becomes that
workflow's typed input, and *what makes two deliveries the same event*. That last
part is the whole difficulty: webhooks are at-least-once by nature (every provider
retries, and a retry looks exactly like a new invoice), and a cron scheduler that
restarts must not re-run the tick it already ran. So a trigger carries a
``dedupe`` function, and the dispatcher claims that key before it starts anything.

The kinds differ only in where the event comes from — an HTTP post, a provider's
inbound-email payload, or a clock — never in what happens afterwards. They all
converge on the same dispatcher, and so on the same durability guarantees.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from flowforge.core.errors import TriggerNotFoundError

type Event = dict[str, Any]
type EventMapper = Callable[[Event], Any]
type DedupeKeyFn = Callable[[Event], str | None]


class TriggerKind(StrEnum):
    WEBHOOK = "webhook"
    EMAIL = "email"
    CRON = "cron"


def _identity(event: Event) -> Any:
    """Default mapping: the event body *is* the workflow input."""
    return event


@dataclass(frozen=True)
class Trigger:
    name: str
    workflow: str
    kind: TriggerKind = TriggerKind.WEBHOOK
    map: EventMapper = _identity
    dedupe: DedupeKeyFn | None = None
    schedule: str | None = None
    """Cron expression; only meaningful for :attr:`TriggerKind.CRON`."""
    tenant: str = "default"
    priority: int = 0

    def dedupe_key(self, event: Event) -> str | None:
        """The identity of this event, or ``None`` if it has none — in which case
        every delivery is taken at face value and starts a run."""
        return self.dedupe(event) if self.dedupe is not None else None


def webhook_trigger(
    name: str,
    workflow: str,
    *,
    map: EventMapper = _identity,
    dedupe: DedupeKeyFn | None = None,
    tenant: str = "default",
    priority: int = 0,
) -> Trigger:
    return Trigger(
        name=name,
        workflow=workflow,
        kind=TriggerKind.WEBHOOK,
        map=map,
        dedupe=dedupe,
        tenant=tenant,
        priority=priority,
    )


def cron_trigger(
    name: str,
    workflow: str,
    schedule: str,
    *,
    map: EventMapper = _identity,
    tenant: str = "default",
    priority: int = 0,
) -> Trigger:
    """A scheduled trigger. The scheduler supplies the tick's ``fire_at`` as the
    dedupe key, so a restart replays the schedule without double-firing it."""
    return Trigger(
        name=name,
        workflow=workflow,
        kind=TriggerKind.CRON,
        map=map,
        schedule=schedule,
        tenant=tenant,
        priority=priority,
    )


@dataclass
class TriggerRegistry:
    _triggers: dict[str, Trigger] = field(default_factory=dict)

    def add(self, trigger: Trigger) -> Trigger:
        if trigger.name in self._triggers:
            raise ValueError(f"trigger {trigger.name!r} already registered")
        self._triggers[trigger.name] = trigger
        return trigger

    def get(self, name: str) -> Trigger:
        try:
            return self._triggers[name]
        except KeyError:
            raise TriggerNotFoundError(name) from None

    def all(self) -> list[Trigger]:
        return list(self._triggers.values())

    def of_kind(self, kind: TriggerKind) -> list[Trigger]:
        return [t for t in self._triggers.values() if t.kind is kind]
