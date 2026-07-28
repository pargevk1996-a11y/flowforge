"""End-to-end tests for the contract-review workflow over the control plane.

One contract becomes many independent LLM judgements: the run fans out, bounded
and metered, fans back in in paragraph order, escalates to legal when anything
scores high, and rolls the filing back when a later step fails.
"""

from __future__ import annotations

import json

import pytest
from httpx import ASGITransport, AsyncClient

from flowforge import Budget, InMemoryCostLedger, InMemoryEventStore, Registry
from flowforge.api import build_control_plane, create_app
from flowforge.api.controlplane import ControlPlane
from flowforge.llm import (
    InMemoryRateLimiter,
    LLMResponse,
    LLMUsage,
    ModelPrice,
    Pricing,
    RateLimit,
    ScriptedLLMClient,
)
from workflows.contract_review import ContractServices, build_contract_review

_PER_CALL = 0.02
_START = {"workflow": "contract_review", "input": {"contract_url": "s3://c/1.pdf"}}


def _risk(level: str, issue: str = "") -> str:
    return json.dumps({"level": level, "issue": issue or f"{level} risk"})


def _plane(
    levels: list[str],
    *,
    fail_filing: bool = False,
    concurrency: int = 4,
    budget: float | None = None,
    limiter: InMemoryRateLimiter | None = None,
) -> tuple[ControlPlane, ContractServices, ScriptedLLMClient]:
    services = ContractServices(
        paragraphs=[f"clause {i}" for i in range(len(levels))], fail_filing=fail_filing
    )
    client = ScriptedLLMClient([_risk(level) for level in levels])
    registry = Registry()
    build_contract_review(
        registry,
        llm_client=client,
        services=services,
        concurrency=concurrency,
        pricing=Pricing({"gpt-4o-mini": ModelPrice(input_per_1k=1.0, output_per_1k=1.0)}),
        limiter=limiter,
    )
    cp = build_control_plane(
        InMemoryEventStore(),
        registry,
        ledger=InMemoryCostLedger(),
        budget=Budget(limit_usd=budget) if budget is not None else None,
    )
    return cp, services, client


def _http(cp: ControlPlane) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=create_app(cp)), base_url="http://t")


async def test_low_risk_contract_is_filed_without_escalation() -> None:
    cp, services, client = _plane(["low", "low", "low"])
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json=_START)).json()["run_id"]
        await cp.worker.run_once()

        status = (await http.get(f"/runs/{run_id}")).json()
        assert status["status"] == "completed"
        assert status["result"]["high_risk"] == 0
        assert status["result"]["report_id"] == run_id
        assert len(client.calls) == 3  # one judgement per paragraph
        assert list(services.filed) == [run_id]


async def test_findings_come_back_in_paragraph_order() -> None:
    # The scripted client answers in call order; with a bound of 1 the calls are
    # made in paragraph order, so the mapping is unambiguous.
    cp, _services, _client = _plane(["low", "high", "medium"], concurrency=1)
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json=_START)).json()["run_id"]
        await cp.worker.run_once()  # fans out, then suspends on legal approval

        await http.post(
            f"/runs/{run_id}/signals",
            json={"name": "legal_approval", "data": {"approved": True, "reviewer": "legal@acme"}},
        )
        await cp.worker.run_once()

        result = (await http.get(f"/runs/{run_id}")).json()["result"]
        assert [f["level"] for f in result["findings"]] == ["low", "high", "medium"]
        assert result["high_risk"] == 1


async def test_high_risk_waits_for_legal_and_can_be_rejected() -> None:
    cp, services, _client = _plane(["low", "high"])
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json=_START)).json()["run_id"]
        await cp.worker.run_once()
        assert (await http.get(f"/runs/{run_id}")).json()["status"] == "suspended"
        assert services.filed == {}  # nothing filed before legal saw it

        await http.post(
            f"/runs/{run_id}/signals",
            json={"name": "legal_approval", "data": {"approved": False, "reviewer": "legal@acme"}},
        )
        await cp.worker.run_once()

        result = (await http.get(f"/runs/{run_id}")).json()["result"]
        assert result["status"] == "rejected"
        assert services.filed == {}


async def test_a_failed_filing_leaves_nothing_filed() -> None:
    cp, services, _client = _plane(["low", "low"], fail_filing=True)
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json=_START)).json()["run_id"]
        await cp.worker.run_once()

        assert (await http.get(f"/runs/{run_id}")).json()["status"] == "failed"
        assert services.filed == {}


async def test_every_paragraph_is_billed_to_the_tenant() -> None:
    cp, _services, _client = _plane(["low"] * 5)
    async with _http(cp) as http:
        await http.post("/runs", json={**_START, "tenant": "acme"})
        await cp.worker.run_once()

        spend = (await http.get("/tenants/acme/spend")).json()
        assert spend["spent_usd"] == pytest.approx(5 * _PER_CALL)


async def test_a_fan_out_cannot_outspend_the_budget() -> None:
    """Forty clauses at once is where a workflow would burn a day's allowance."""
    cp, services, client = _plane(["low"] * 40, concurrency=1, budget=3 * _PER_CALL)
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json={**_START, "tenant": "acme"})).json()["run_id"]
        await cp.worker.run_once()

        status = (await http.get(f"/runs/{run_id}")).json()
        assert status["status"] == "failed"
        assert "budget" in status["error"]
        assert len(client.calls) == 3  # stopped at the limit, not after it
        assert services.filed == {}


async def test_the_fan_out_is_paced_by_the_provider_rate_limit() -> None:
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    limiter = InMemoryRateLimiter(
        {"openai": RateLimit(per_second=1, burst=1)},
        clock=lambda: 0.0,  # time never advances, so every refill must be waited for
        sleep=sleep,
    )
    cp, _services, client = _plane(["low"] * 4, concurrency=4, limiter=limiter)

    async with _http(cp) as http:
        await http.post("/runs", json=_START)
        await cp.worker.run_once()

    assert len(client.calls) == 4
    assert len(waits) == 3  # the first call had the burst; the rest queued behind it


async def test_the_timeline_shows_one_command_per_paragraph() -> None:
    cp, _services, _client = _plane(["low", "low", "low"])
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json=_START)).json()["run_id"]
        await cp.worker.run_once()

        events = (await http.get(f"/runs/{run_id}/timeline")).json()["events"]

    completed = [e for e in events if e["type"] == "activity_completed"]
    risks = [e for e in completed if str(e["name"]).startswith("paragraph_risk")]
    assert sorted(e["name"] for e in risks) == [
        "paragraph_risk[0]",
        "paragraph_risk[1]",
        "paragraph_risk[2]",
    ]
    assert len({e["command_seq"] for e in risks}) == 3


async def test_a_contract_with_no_paragraphs_files_an_empty_report() -> None:
    cp, services, client = _plane([])
    async with _http(cp) as http:
        run_id = (await http.post("/runs", json=_START)).json()["run_id"]
        await cp.worker.run_once()

        status = (await http.get(f"/runs/{run_id}")).json()
        assert status["status"] == "completed"
        assert status["result"]["findings"] == []
        assert client.calls == []
        assert list(services.filed) == [run_id]


async def test_usage_is_billed_even_when_the_model_is_verbose() -> None:
    """A per-call price is a function of tokens, so an unusual response still
    lands in the ledger with the right number."""
    services = ContractServices(paragraphs=["clause 0"])
    client = ScriptedLLMClient(
        [
            LLMResponse(
                content=_risk("low"),
                usage=LLMUsage(prompt_tokens=500, completion_tokens=100),
            )
        ]
    )
    registry = Registry()
    build_contract_review(
        registry,
        llm_client=client,
        services=services,
        pricing=Pricing({"gpt-4o-mini": ModelPrice(input_per_1k=1.0, output_per_1k=2.0)}),
    )
    ledger = InMemoryCostLedger()
    cp = build_control_plane(InMemoryEventStore(), registry, ledger=ledger)

    async with _http(cp) as http:
        await http.post("/runs", json={**_START, "tenant": "acme"})
        await cp.worker.run_once()

    (_at, entry), = ledger.entries
    assert entry.usd == pytest.approx(500 / 1000 * 1.0 + 100 / 1000 * 2.0)
