"""Contract-review reference workflow.

Contract in -> fan out one typed LLM risk check per paragraph (bounded, metered)
-> fan in -> escalate to legal when anything scores high -> file the report, with
a compensation. Exercises fan-out/fan-in, human-in-the-loop, and cost control in
one run.
"""

from __future__ import annotations

from workflows.contract_review.schemas import (
    ContractInput,
    LegalDecision,
    ParagraphRisk,
    RiskReport,
)
from workflows.contract_review.services import ContractServices
from workflows.contract_review.workflow import WORKFLOW_NAME, build_contract_review

__all__ = [
    "WORKFLOW_NAME",
    "ContractInput",
    "ContractServices",
    "LegalDecision",
    "ParagraphRisk",
    "RiskReport",
    "build_contract_review",
]
