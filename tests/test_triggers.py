"""Tests for triggers — the executable spec for "an external event starts a run".

The properties: an event is mapped to the workflow's typed input; a redelivery
starts nothing and points at the original run; an event with no identity is taken
at face value; a delivery that crashed before it created its run is completed by
the retry rather than swallowed; a malformed event is rejected without claiming
anything; and inbound email is normalised across the shapes providers send.
"""

from __future__ import annotations

import json

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from flowforge import (
    Budget,
    InMemoryCostLedger,
    InMemoryEventStore,
    Registry,
    RunStatus,
    WorkflowContext,
)
from flowforge.api import build_control_plane, create_app
from flowforge.api.controlplane import ControlPlane
from flowforge.core.errors import TriggerNotFoundError
from flowforge.llm import ModelPrice, Pricing, ScriptedLLMClient
from flowforge.triggers import (
    InboundEmail,
    TriggerKind,
    TriggerRegistry,
    email_trigger,
    parse_inbound,
    webhook_trigger,
)
from workflows.invoice_to_payment import (
    EMAIL_TRIGGER,
    WEBHOOK_TRIGGER,
    InvoiceServices,
    build_invoice_to_payment,
    register_invoice_triggers,
)


class Order(BaseModel):
    sku: str
    qty: int = 1


class Ack(BaseModel):
    sku: str


def _plane() -> tuple[ControlPlane, list[Order]]:
    handled: list[Order] = []

    async def record(order: Order) -> Ack:
        handled.append(order)
        return Ack(sku=order.sku)

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        return await ctx.activity(record, inp, name="record")

    registry = Registry()
    registry.add(wf, name="order")
    cp = build_control_plane(InMemoryEventStore(), registry)
    cp.triggers.add(
        webhook_trigger(
            "order_placed",
            "order",
            map=lambda event: Order(sku=str(event["sku"]), qty=int(event.get("qty", 1))),
            dedupe=lambda event: event.get("order_id"),
        )
    )
    return cp, handled


async def test_webhook_event_starts_a_run() -> None:
    cp, handled = _plane()

    delivery = await cp.dispatcher.fire("order_placed", {"sku": "A-1", "qty": 3})

    assert delivery.started
    await cp.worker.run_once()
    assert handled == [Order(sku="A-1", qty=3)]
    assert (await cp.engine.describe(delivery.run_id)).status is RunStatus.COMPLETED


async def test_redelivery_of_the_same_event_starts_nothing() -> None:
    cp, handled = _plane()
    event = {"sku": "A-1", "order_id": "ord-7"}

    first = await cp.dispatcher.fire("order_placed", event)
    second = await cp.dispatcher.fire("order_placed", event)  # the provider retries

    assert first.started and not second.started
    assert second.run_id == first.run_id  # the retry is told about the original
    await cp.worker.run_once()
    await cp.worker.run_once()
    assert handled == [Order(sku="A-1")]  # exactly one run, exactly one side effect


async def test_events_without_an_identity_are_taken_at_face_value() -> None:
    cp, _handled = _plane()

    first = await cp.dispatcher.fire("order_placed", {"sku": "A-1"})  # no order_id
    second = await cp.dispatcher.fire("order_placed", {"sku": "A-1"})

    assert first.started and second.started
    assert first.run_id != second.run_id


async def test_explicit_key_overrides_the_triggers_own_dedupe() -> None:
    cp, _handled = _plane()

    first = await cp.dispatcher.fire("order_placed", {"sku": "A-1"}, key="delivery-1")
    second = await cp.dispatcher.fire("order_placed", {"sku": "A-2"}, key="delivery-1")

    assert first.started and not second.started
    assert second.run_id == first.run_id


async def test_a_claim_whose_run_never_started_is_completed_by_the_retry() -> None:
    """A crash between claiming an event and seeding its run must not lose it."""
    cp, handled = _plane()
    event = {"sku": "A-1", "order_id": "ord-9"}

    # Simulate the crashed delivery: the claim exists, the run does not.
    claimed, won = await cp.dispatcher._deliveries.claim(  # type: ignore[union-attr]
        "order_placed", "ord-9", "run-that-never-was"
    )
    assert won

    retry = await cp.dispatcher.fire("order_placed", event)

    assert retry.started  # the retry finished what the crash interrupted
    assert retry.run_id == claimed  # under the id that was already claimed
    await cp.worker.run_once()
    assert handled == [Order(sku="A-1")]


async def test_a_corrected_retry_can_still_use_a_claimed_key() -> None:
    """The claim is taken before the mapping is attempted, so a bad payload does
    leave a claim behind. What matters is that it does not become a tombstone:
    the sender's corrected retry still gets to start its run."""
    cp, _handled = _plane()
    bad = {"order_id": "ord-11"}  # no sku

    with pytest.raises(KeyError):
        await cp.dispatcher.fire("order_placed", bad)

    good = await cp.dispatcher.fire("order_placed", {"sku": "A-1", "order_id": "ord-11"})
    assert good.started


async def test_unknown_trigger_is_an_error() -> None:
    cp, _handled = _plane()
    with pytest.raises(TriggerNotFoundError):
        await cp.dispatcher.fire("nope", {})


# -- inbound email ----------------------------------------------------------


def test_parse_inbound_normalises_provider_shapes() -> None:
    mailgun = parse_inbound(
        {
            "Message-Id": "<m-1@acme.test>",
            "from": "ap@acme.test",
            "recipient": "invoices@flowforge.test",
            "subject": "Invoice INV-1",
            "body-plain": "see attached",
            "attachments": [
                {
                    "filename": "invoice.pdf",
                    "url": "s3://inv/1.pdf",
                    "content_type": "application/pdf",
                }
            ],
        }
    )
    postmark = parse_inbound(
        {
            "MessageID": "m-2",
            "FromFull": {"Email": "ap@acme.test"},
            "ToFull": [{"Email": "invoices@flowforge.test"}],
            "Subject": "Invoice INV-2",
            "TextBody": "see attached",
            "Attachments": [{"Name": "invoice.pdf", "Url": "s3://inv/2.pdf"}],
        }
    )

    assert mailgun.message_id == "<m-1@acme.test>"
    assert postmark.message_id == "m-2"
    for email in (mailgun, postmark):
        assert email.sender == "ap@acme.test"
        assert email.recipient == "invoices@flowforge.test"
        attachment = email.attachment(suffix=".pdf")
        assert attachment is not None and attachment.url is not None


def test_parse_inbound_rejects_mail_without_an_identity() -> None:
    with pytest.raises(ValueError, match="message id"):
        parse_inbound({"from": "ap@acme.test", "subject": "no id"})


async def test_email_trigger_dedupes_on_message_id() -> None:
    seen: list[str] = []

    async def handle(subject: str) -> Ack:
        seen.append(subject)
        return Ack(sku=subject)

    async def wf(ctx: WorkflowContext, inp: Order) -> Ack:
        return await ctx.activity(handle, inp.sku, name="handle")

    registry = Registry()
    registry.add(wf, name="mail")
    cp = build_control_plane(InMemoryEventStore(), registry)
    cp.triggers.add(
        email_trigger(
            "mail_in", "mail", map=lambda email: Order(sku=email.subject)
        )
    )
    payload = {"message_id": "m-1", "from": "a@b.test", "subject": "Invoice INV-1"}

    first = await cp.dispatcher.fire("mail_in", payload)
    second = await cp.dispatcher.fire("mail_in", dict(payload))  # provider retry

    assert first.started and not second.started
    await cp.worker.run_once()
    await cp.worker.run_once()
    assert seen == ["Invoice INV-1"]


def test_inbound_email_picks_the_attachment_by_suffix() -> None:
    email = InboundEmail.model_validate(
        {
            "message_id": "m-1",
            "sender": "a@b.test",
            "attachments": [
                {"filename": "signature.png", "url": "s3://x/sig.png"},
                {"filename": "invoice.PDF", "url": "s3://x/inv.pdf"},
            ],
        }
    )
    picked = email.attachment(suffix=".pdf")
    assert picked is not None and picked.url == "s3://x/inv.pdf"
    assert email.attachment() is not None and email.attachment().url == "s3://x/sig.png"  # type: ignore[union-attr]


# -- over HTTP, on the reference workflow -----------------------------------


def _invoice_plane() -> tuple[ControlPlane, InvoiceServices]:
    services = InvoiceServices()
    client = ScriptedLLMClient(
        [
            json.dumps(
                {"vendor": "Acme", "invoice_number": "INV-1", "amount": 500, "currency": "USD"}
            )
        ]
        * 4
    )
    registry = Registry()
    build_invoice_to_payment(
        registry,
        llm_client=client,
        services=services,
        pricing=Pricing({"gpt-4o-mini": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
    )
    cp = build_control_plane(
        InMemoryEventStore(),
        registry,
        ledger=InMemoryCostLedger(),
        budget=Budget(limit_usd=1.0),
    )
    register_invoice_triggers(cp.triggers, tenant="acme")
    return cp, services


def _http(cp: ControlPlane) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=create_app(cp)), base_url="http://t")


async def test_invoice_email_webhook_runs_the_workflow_end_to_end() -> None:
    cp, services = _invoice_plane()
    mail = {
        "Message-Id": "<inv-1@acme.test>",
        "from": "ap@acme.test",
        "subject": "Invoice INV-1",
        "attachments": [{"filename": "invoice.pdf", "url": "s3://inv/1.pdf"}],
    }

    async with _http(cp) as http:
        first = await http.post(f"/triggers/{EMAIL_TRIGGER}", json=mail)
        assert first.status_code == 200
        assert first.json()["started"] is True

        await cp.worker.run_once()
        run_id = first.json()["run_id"]
        status = (await http.get(f"/runs/{run_id}")).json()
        assert status["status"] == "completed"
        assert status["result"]["status"] == "paid"
        assert list(services.payments) == ["INV-1"]

        # SES retries the same mail: no second payment.
        again = await http.post(f"/triggers/{EMAIL_TRIGGER}", json=mail)
        assert again.json() == {
            "trigger": EMAIL_TRIGGER,
            "run_id": run_id,
            "started": False,
        }
        await cp.worker.run_once()
        assert list(services.payments) == ["INV-1"]


async def test_trigger_http_errors_are_specific() -> None:
    cp, _services = _invoice_plane()
    async with _http(cp) as http:
        assert (await http.post("/triggers/nope", json={})).status_code == 404

        # Mail with no PDF cannot become an InvoiceInput.
        no_pdf = await http.post(
            f"/triggers/{EMAIL_TRIGGER}",
            json={"message_id": "m-2", "from": "ap@acme.test", "attachments": []},
        )
        assert no_pdf.status_code == 422
        assert "no PDF attachment" in no_pdf.json()["detail"]


async def test_idempotency_header_overrides_the_body() -> None:
    cp, _services = _invoice_plane()
    body = {"pdf_url": "s3://inv/9.pdf"}  # no invoice_id, so no identity of its own
    headers = {"X-Idempotency-Key": "delivery-42"}

    async with _http(cp) as http:
        first = await http.post(f"/triggers/{WEBHOOK_TRIGGER}", json=body, headers=headers)
        second = await http.post(f"/triggers/{WEBHOOK_TRIGGER}", json=body, headers=headers)

    assert first.json()["started"] is True
    assert second.json() == {**first.json(), "started": False}


async def test_triggers_are_listed_with_their_kinds() -> None:
    cp, _services = _invoice_plane()
    async with _http(cp) as http:
        listed = (await http.get("/triggers")).json()["triggers"]

    by_name = {t["name"]: t for t in listed}
    assert by_name[EMAIL_TRIGGER]["kind"] == TriggerKind.EMAIL
    assert by_name[WEBHOOK_TRIGGER]["kind"] == TriggerKind.WEBHOOK
    assert by_name["invoice_sweep"]["schedule"] == "0 * * * *"
    assert all(t["tenant"] == "acme" for t in listed)


async def test_over_budget_tenant_is_refused_at_the_trigger() -> None:
    cp, _services = _invoice_plane()
    cp.budget.set_budget("acme", Budget(limit_usd=0.0))  # type: ignore[union-attr]

    async with _http(cp) as http:
        refused = await http.post(
            f"/triggers/{WEBHOOK_TRIGGER}", json={"pdf_url": "s3://inv/1.pdf"}
        )
    assert refused.status_code == 402


def test_registry_rejects_duplicate_names() -> None:
    triggers = TriggerRegistry()
    triggers.add(webhook_trigger("dup", "wf"))
    with pytest.raises(ValueError, match="already registered"):
        triggers.add(webhook_trigger("dup", "wf"))
