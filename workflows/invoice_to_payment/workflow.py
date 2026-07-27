"""The invoice-to-payment workflow, assembled from injected dependencies.

Built by a factory so the LLM client and services can be real in production and
deterministic in tests. Registering it returns a :class:`WorkflowDef` the engine
resolves by name.
"""

from __future__ import annotations

from flowforge import Registry, WorkflowContext, WorkflowDef
from flowforge.llm import CostTracker, LLMClient, LLMStep
from workflows.invoice_to_payment.schemas import (
    CfoDecision,
    ExtractedInvoice,
    InvoiceInput,
    PaymentResult,
)
from workflows.invoice_to_payment.services import InvoiceServices

WORKFLOW_NAME = "invoice_to_payment"

_EXTRACT_SYSTEM = (
    "You are an accounts-payable assistant. Extract the invoice fields "
    "(vendor, invoice_number, amount, currency) as JSON."
)


def build_invoice_to_payment(
    registry: Registry,
    *,
    llm_client: LLMClient,
    services: InvoiceServices,
    model: str = "gpt-4o-mini",
    approval_threshold: float = 10_000.0,
    cost: CostTracker | None = None,
) -> WorkflowDef[InvoiceInput, PaymentResult]:
    extract = LLMStep(
        llm_client,
        model,
        ExtractedInvoice,
        system=_EXTRACT_SYSTEM,
        cost=cost,
        name="extract_invoice",
    )

    async def invoice_to_payment(
        ctx: WorkflowContext, inp: InvoiceInput
    ) -> PaymentResult:
        text = await ctx.activity(services.ocr_pdf, inp.pdf_url, name="ocr_pdf")
        invoice = await ctx.activity(
            extract.run, text, name=extract.name, result_type=ExtractedInvoice
        )
        await ctx.activity(services.match_vendor, invoice.vendor, name="match_vendor")

        if invoice.amount > approval_threshold:
            decision = await ctx.wait_for_signal("cfo_approval", CfoDecision)
            if not decision.approved:
                return PaymentResult(status="rejected", amount=invoice.amount)

        payment = await ctx.activity(
            services.create_payment,
            invoice,
            name="create_payment",
            # Saga rollback keyed by invoice number (known before the payment runs).
            compensate=lambda: services.void_payment(invoice.invoice_number),
        )
        await ctx.activity(
            services.notify,
            f"Paid {invoice.vendor} {invoice.amount} {invoice.currency}",
            name="notify",
        )
        return payment

    return registry.add(invoice_to_payment, name=WORKFLOW_NAME)
