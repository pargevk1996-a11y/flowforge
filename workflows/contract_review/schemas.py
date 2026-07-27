"""Typed contracts for the contract-review workflow."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ContractInput(BaseModel):
    contract_url: str
    tenant: str = "default"


class ParagraphRisk(BaseModel):
    level: str = Field(description="low | medium | high")
    issue: str = ""


class RiskReport(BaseModel):
    status: str  # "approved" | "rejected"
    high_risk: int = 0
    findings: list[ParagraphRisk] = []
    report_id: str | None = None


class LegalDecision(BaseModel):
    approved: bool
    reviewer: str
    note: str = ""
