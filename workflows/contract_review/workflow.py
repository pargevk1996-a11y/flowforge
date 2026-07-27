"""The contract-review workflow: fan out over paragraphs, fan back in, escalate.

This is the shape a document pipeline actually has. One contract becomes forty
independent LLM judgements, which is exactly where an AI workflow either scales or
falls over — forty calls at once will hit a provider's rate limit and can spend a
tenant's daily budget in a second. ``ctx.map_llm`` bounds the concurrency, and
each call is metered and gated like any other, so the fan-out is the *safe* way to
do this rather than the dangerous one.

The fan-in is ordinary code: risks come back in paragraph order regardless of who
finished first, so the summary is deterministic and the run replays identically.
"""

from __future__ import annotations

from flowforge import Registry, WorkflowContext, WorkflowDef
from flowforge.llm import CostTracker, LLMClient, LLMStep, Pricing, RateLimiter
from workflows.contract_review.schemas import (
    ContractInput,
    LegalDecision,
    ParagraphRisk,
    RiskReport,
)
from workflows.contract_review.services import ContractServices

WORKFLOW_NAME = "contract_review"

_RISK_SYSTEM = (
    "You are a contract reviewer. For the given clause, return the risk level "
    "(low, medium or high) and a one-line description of the issue."
)


def build_contract_review(
    registry: Registry,
    *,
    llm_client: LLMClient,
    services: ContractServices,
    model: str = "gpt-4o-mini",
    concurrency: int = 4,
    escalate_above: int = 0,
    cost: CostTracker | None = None,
    pricing: Pricing | None = None,
    limiter: RateLimiter | None = None,
) -> WorkflowDef[ContractInput, RiskReport]:
    risk = LLMStep(
        llm_client,
        model,
        ParagraphRisk,
        system=_RISK_SYSTEM,
        cost=cost,
        pricing=pricing,
        limiter=limiter,
        name="paragraph_risk",
    )

    async def contract_review(ctx: WorkflowContext, inp: ContractInput) -> RiskReport:
        paragraphs = await ctx.activity(
            services.fetch_paragraphs, inp.contract_url, name="fetch_paragraphs"
        )

        # Fan out: one LLM judgement per paragraph, at most `concurrency` at once.
        findings = await ctx.map_llm(risk, paragraphs, concurrency=concurrency)

        # Fan in: order is by paragraph, not by who answered first.
        high_risk = sum(1 for f in findings if f.level == "high")
        report = RiskReport(status="approved", high_risk=high_risk, findings=findings)

        if high_risk > escalate_above:
            decision = await ctx.wait_for_signal("legal_approval", LegalDecision)
            if not decision.approved:
                return RiskReport(status="rejected", high_risk=high_risk, findings=findings)

        # The filing reference is the run id: known before the activity runs, so
        # the compensation can name what it has to undo.
        report_id = await ctx.activity(
            services.file_report,
            ctx.run_id,
            report,
            name="file_report",
            compensate=lambda: services.retract_report(ctx.run_id),
        )
        return report.model_copy(update={"report_id": report_id})

    return registry.add(contract_review, name=WORKFLOW_NAME)
