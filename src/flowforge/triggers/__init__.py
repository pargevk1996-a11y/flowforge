"""Triggers: HTTP webhooks, inbound email, and cron schedules that start runs."""

from __future__ import annotations

from flowforge.triggers.base import (
    Event,
    Trigger,
    TriggerKind,
    TriggerRegistry,
    cron_trigger,
    webhook_trigger,
)
from flowforge.triggers.cron import (
    CronSchedule,
    CronScheduler,
    CronStateStore,
    InMemoryCronStateStore,
)
from flowforge.triggers.deliveries import DeliveryStore, InMemoryDeliveryStore
from flowforge.triggers.dispatch import Delivery, TriggerDispatcher
from flowforge.triggers.email import (
    EmailAttachment,
    InboundEmail,
    email_trigger,
    message_id_key,
    parse_inbound,
)

__all__ = [
    "CronSchedule",
    "CronScheduler",
    "CronStateStore",
    "Delivery",
    "DeliveryStore",
    "EmailAttachment",
    "Event",
    "InMemoryCronStateStore",
    "InMemoryDeliveryStore",
    "InboundEmail",
    "Trigger",
    "TriggerDispatcher",
    "TriggerKind",
    "TriggerRegistry",
    "cron_trigger",
    "email_trigger",
    "message_id_key",
    "parse_inbound",
    "webhook_trigger",
]
