"""Side-effecting activities for contract review, behind an injectable service.

In-memory stand-ins for fetching a contract and filing the finished report —
enough to run the workflow end to end without external systems. Filing is
idempotent per reference, and ``retract_report`` is its compensation.
"""

from __future__ import annotations

from workflows.contract_review.schemas import RiskReport


class ContractServices:
    def __init__(
        self, *, paragraphs: list[str] | None = None, fail_filing: bool = False
    ) -> None:
        # `is None`, not `or`: an empty contract is a legitimate input, not an
        # absent argument.
        self.paragraphs = (
            paragraphs
            if paragraphs is not None
            else [
                "The supplier shall indemnify the customer without limit.",
                "Either party may terminate with 30 days notice.",
                "Payment is due within 90 days of invoice.",
            ]
        )
        self.fail_filing = fail_filing
        self.filed: dict[str, RiskReport] = {}
        self.retracted: list[str] = []

    async def fetch_paragraphs(self, contract_url: str) -> list[str]:
        return list(self.paragraphs)

    async def file_report(self, ref: str, report: RiskReport) -> str:
        if self.fail_filing:
            raise RuntimeError("document store unavailable")
        # Idempotent: filing the same reference twice files one report.
        self.filed.setdefault(ref, report)
        return ref

    async def retract_report(self, ref: str) -> None:
        if self.filed.pop(ref, None) is not None:
            self.retracted.append(ref)
