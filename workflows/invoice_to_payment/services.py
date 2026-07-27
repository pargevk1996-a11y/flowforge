"""Side-effecting activities for the workflow, behind an injectable service object.

In-memory stand-ins for OCR, a vendor directory, a payment gateway, and a
notifier — enough to run the workflow end-to-end and to exercise idempotency and
compensation without external systems. ``create_payment`` is idempotent per
invoice number, and ``void_payment`` is its compensation.
"""

from __future__ import annotations

from uuid import uuid4

from flowforge.core.errors import NonRetryableError
from workflows.invoice_to_payment.schemas import (
    ExtractedInvoice,
    PaymentResult,
    VendorMatch,
)


class InvoiceServices:
    def __init__(
        self,
        *,
        known_vendors: set[str] | None = None,
        fail_notify: bool = False,
        ocr_text: str = "INVOICE\nVendor: Acme\nNo: INV-1\nTotal: 500 USD",
    ) -> None:
        self.known_vendors = known_vendors or {"Acme"}
        self.fail_notify = fail_notify
        self.ocr_text = ocr_text
        # invoice_number -> PaymentResult (the durable-ish state of the gateway)
        self.payments: dict[str, PaymentResult] = {}
        self.voided: list[str] = []
        self.notifications: list[str] = []

    async def ocr_pdf(self, pdf_url: str) -> str:
        return self.ocr_text

    async def match_vendor(self, vendor: str) -> VendorMatch:
        if vendor in self.known_vendors:
            return VendorMatch(known=True, vendor_id=f"vendor-{vendor.lower()}")
        return VendorMatch(known=False)

    async def create_payment(self, invoice: ExtractedInvoice) -> PaymentResult:
        # Idempotent: the same invoice never creates two payments.
        existing = self.payments.get(invoice.invoice_number)
        if existing is not None:
            return existing
        result = PaymentResult(
            status="paid", payment_id=f"pay-{uuid4().hex[:8]}", amount=invoice.amount
        )
        self.payments[invoice.invoice_number] = result
        return result

    async def void_payment(self, invoice_number: str) -> None:
        payment = self.payments.pop(invoice_number, None)
        if payment is not None:
            self.voided.append(invoice_number)

    async def notify(self, message: str) -> None:
        if self.fail_notify:
            raise NonRetryableError(f"notifier down: {message}")
        self.notifications.append(message)
