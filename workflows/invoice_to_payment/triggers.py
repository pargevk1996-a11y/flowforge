"""How an invoice actually arrives: by email, by webhook, or on a schedule.

The workflow itself stays ignorant of all three — it takes an ``InvoiceInput`` and
nothing else. These bindings are the only place that knows an invoice PDF may come
from a mail attachment or an AP system's callback.
"""

from __future__ import annotations

from typing import Any

from flowforge.triggers import (
    InboundEmail,
    Trigger,
    TriggerRegistry,
    cron_trigger,
    webhook_trigger,
)
from flowforge.triggers.email import email_trigger
from workflows.invoice_to_payment.schemas import InvoiceInput
from workflows.invoice_to_payment.workflow import WORKFLOW_NAME

EMAIL_TRIGGER = "invoice_email"
WEBHOOK_TRIGGER = "invoice_webhook"
SWEEP_TRIGGER = "invoice_sweep"


def _from_email(email: InboundEmail) -> InvoiceInput:
    """The PDF on an invoice email is the run's input."""
    pdf = email.attachment(suffix=".pdf")
    if pdf is None or pdf.url is None:
        raise ValueError(f"email {email.message_id!r} carries no PDF attachment")
    return InvoiceInput(pdf_url=pdf.url)


def _from_webhook(event: dict[str, Any]) -> InvoiceInput:
    return InvoiceInput(pdf_url=str(event["pdf_url"]))


def _invoice_id(event: dict[str, Any]) -> str | None:
    """An AP system's own invoice id, when it sends one — a far better identity
    than anything we could hash out of the body."""
    invoice_id = event.get("invoice_id")
    return str(invoice_id) if invoice_id is not None else None


def register_invoice_triggers(
    triggers: TriggerRegistry,
    *,
    tenant: str = "default",
    sweep_schedule: str = "0 * * * *",
) -> list[Trigger]:
    """Register all three ways an invoice run gets started."""
    return [
        triggers.add(
            email_trigger(EMAIL_TRIGGER, WORKFLOW_NAME, map=_from_email, tenant=tenant)
        ),
        triggers.add(
            webhook_trigger(
                WEBHOOK_TRIGGER,
                WORKFLOW_NAME,
                map=_from_webhook,
                dedupe=_invoice_id,
                tenant=tenant,
            )
        ),
        triggers.add(
            cron_trigger(
                SWEEP_TRIGGER,
                WORKFLOW_NAME,
                sweep_schedule,
                # The tick carries no invoice of its own; the sweep re-reads the
                # inbox drop and lets the workflow's own idempotency sort it out.
                map=lambda event: InvoiceInput(pdf_url="s3://invoices/inbox"),
                tenant=tenant,
            )
        ),
    ]
