"""Inbound email as a trigger.

Nobody delivers mail to a workflow engine over SMTP; the providers that receive it
(SES, Mailgun, Postmark, SendGrid) post a JSON body at a webhook. So an email
trigger is a webhook with a parser: :func:`parse_inbound` normalises the handful
of shapes those providers use into one :class:`InboundEmail`, and
:func:`message_id_key` dedupes on the ``Message-ID`` — the identity the mail
system itself assigns, which is exactly what survives a provider's retry.

Mail that cannot be parsed raises, and the dispatcher never claims the delivery,
so the provider's next attempt gets a fair hearing instead of being swallowed as a
duplicate.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from flowforge.triggers.base import Event, Trigger, TriggerKind

_MESSAGE_ID_KEYS = ("message_id", "messageId", "Message-Id", "Message-ID", "MessageID")
_SENDER_KEYS = ("from", "From", "sender", "FromFull")
_RECIPIENT_KEYS = ("to", "To", "recipient", "ToFull")
_SUBJECT_KEYS = ("subject", "Subject")
_TEXT_KEYS = ("text", "body-plain", "TextBody", "text_body", "plain")
_ATTACHMENT_KEYS = ("attachments", "Attachments")


class EmailAttachment(BaseModel):
    filename: str | None = None
    url: str | None = None
    content_type: str | None = None


class InboundEmail(BaseModel):
    message_id: str
    sender: str
    subject: str = ""
    text: str = ""
    recipient: str | None = None
    attachments: list[EmailAttachment] = []

    def attachment(self, *, suffix: str | None = None) -> EmailAttachment | None:
        """The first attachment, optionally the first whose filename ends in
        ``suffix`` — enough to answer "the PDF on this invoice email"."""
        for item in self.attachments:
            if suffix is None:
                return item
            if item.filename is not None and item.filename.lower().endswith(suffix.lower()):
                return item
        return None


def _first(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _as_address(value: Any) -> str | None:
    """Providers send an address as a string, or as ``{"email": ...}``."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        found = _first(value, ("email", "Email", "address"))
        return str(found) if found is not None else None
    if isinstance(value, list) and value:
        return _as_address(value[0])
    return None


def _as_attachment(raw: Any) -> EmailAttachment | None:
    if not isinstance(raw, dict):
        return None
    return EmailAttachment(
        filename=_first(raw, ("filename", "Name", "name")),
        url=_first(raw, ("url", "Url", "location", "content_url")),
        content_type=_first(raw, ("content_type", "ContentType", "contentType", "type")),
    )


def parse_inbound(payload: dict[str, Any]) -> InboundEmail:
    """Normalise a provider's inbound-email webhook body."""
    message_id = _first(payload, _MESSAGE_ID_KEYS)
    sender = _as_address(_first(payload, _SENDER_KEYS))
    if message_id is None or sender is None:
        raise ValueError("inbound email needs at least a message id and a sender")

    raw_attachments = _first(payload, _ATTACHMENT_KEYS) or []
    attachments = [
        parsed
        for parsed in (_as_attachment(raw) for raw in raw_attachments)
        if parsed is not None
    ]
    return InboundEmail(
        message_id=str(message_id),
        sender=sender,
        recipient=_as_address(_first(payload, _RECIPIENT_KEYS)),
        subject=str(_first(payload, _SUBJECT_KEYS) or ""),
        text=str(_first(payload, _TEXT_KEYS) or ""),
        attachments=attachments,
    )


def message_id_key(payload: dict[str, Any]) -> str | None:
    """Dedupe function for email triggers: the mail's own ``Message-ID``."""
    message_id = _first(payload, _MESSAGE_ID_KEYS)
    return str(message_id) if message_id is not None else None


def email_trigger(
    name: str,
    workflow: str,
    *,
    map: Callable[[InboundEmail], Any],
    tenant: str = "default",
    priority: int = 0,
) -> Trigger:
    """An email trigger. The mapper is handed a parsed :class:`InboundEmail`, not
    the provider's raw body, so a workflow never learns which vendor relays its
    mail."""

    def _map(event: Event) -> Any:
        return map(parse_inbound(event))

    return Trigger(
        name=name,
        workflow=workflow,
        kind=TriggerKind.EMAIL,
        map=_map,
        dedupe=message_id_key,
        tenant=tenant,
        priority=priority,
    )
