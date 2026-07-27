"""Typed contracts for the invoice-to-payment workflow."""

from __future__ import annotations

from pydantic import BaseModel, Field


class InvoiceInput(BaseModel):
    pdf_url: str
    tenant: str = "default"


class ExtractedInvoice(BaseModel):
    vendor: str
    invoice_number: str
    amount: float = Field(ge=0)
    currency: str = "USD"


class VendorMatch(BaseModel):
    known: bool
    vendor_id: str | None = None


class CfoDecision(BaseModel):
    approved: bool
    approver: str
    note: str = ""


class PaymentResult(BaseModel):
    status: str  # "paid" | "rejected"
    payment_id: str | None = None
    amount: float = 0.0
