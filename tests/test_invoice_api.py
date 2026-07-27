"""End-to-end tests for the control plane driving the invoice-to-payment workflow.

Exercises the HTTP surface (start / status / timeline / signal) together with the
whole engine: typed LLM extraction, auto-pay under threshold, human approval above
it, rejection, and saga compensation when a later step fails. The worker is driven
explicitly so the assertions are deterministic.
"""

from __future__ import annotations

import json

from httpx import ASGITransport, AsyncClient

from flowforge import InMemoryEventStore, Registry
from flowforge.api import build_control_plane, create_app
from flowforge.api.controlplane import ControlPlane
from flowforge.llm import ScriptedLLMClient
from workflows.invoice_to_payment import InvoiceServices, build_invoice_to_payment

_INPUT = {"workflow": "invoice_to_payment", "input": {"pdf_url": "s3://inv"}}


def _invoice_json(amount: float, *, vendor: str = "Acme", number: str = "INV-1") -> str:
    return json.dumps(
        {"vendor": vendor, "invoice_number": number, "amount": amount, "currency": "USD"}
    )


def _setup(
    amount: float, *, fail_notify: bool = False
) -> tuple[ControlPlane, InvoiceServices]:
    services = InvoiceServices(fail_notify=fail_notify)
    client = ScriptedLLMClient([_invoice_json(amount)])
    registry = Registry()
    build_invoice_to_payment(registry, llm_client=client, services=services)
    return build_control_plane(InMemoryEventStore(), registry), services


def _http(cp: ControlPlane) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=create_app(cp)), base_url="http://t")


async def test_small_invoice_auto_pays() -> None:
    cp, services = _setup(500)
    async with _http(cp) as client:
        run_id = (await client.post("/runs", json=_INPUT)).json()["run_id"]

        await cp.worker.run_once()  # drives to completion (no approval needed)

        status = (await client.get(f"/runs/{run_id}")).json()
        assert status["status"] == "completed"
        assert status["result"]["status"] == "paid"
        assert list(services.payments) == ["INV-1"]
        assert services.notifications  # notified


async def test_large_invoice_waits_for_cfo_then_pays() -> None:
    cp, services = _setup(25_000)
    async with _http(cp) as client:
        run_id = (await client.post("/runs", json=_INPUT)).json()["run_id"]

        await cp.worker.run_once()  # runs up to the CFO approval, then suspends
        assert (await client.get(f"/runs/{run_id}")).json()["status"] == "suspended"
        assert services.payments == {}  # nothing paid before approval

        sig = await client.post(
            f"/runs/{run_id}/signals",
            json={"name": "cfo_approval", "data": {"approved": True, "approver": "cfo@acme"}},
        )
        assert sig.status_code == 200

        await cp.worker.run_once()  # the signal re-enqueued the run; finish it
        status = (await client.get(f"/runs/{run_id}")).json()
        assert status["status"] == "completed"
        assert status["result"]["status"] == "paid"


async def test_large_invoice_rejected() -> None:
    cp, services = _setup(25_000)
    async with _http(cp) as client:
        run_id = (await client.post("/runs", json=_INPUT)).json()["run_id"]
        await cp.worker.run_once()

        await client.post(
            f"/runs/{run_id}/signals",
            json={"name": "cfo_approval", "data": {"approved": False, "approver": "cfo@acme"}},
        )
        await cp.worker.run_once()

        status = (await client.get(f"/runs/{run_id}")).json()
        assert status["status"] == "completed"
        assert status["result"]["status"] == "rejected"
        assert services.payments == {}  # never paid


async def test_saga_voids_payment_when_notify_fails() -> None:
    cp, services = _setup(500, fail_notify=True)
    async with _http(cp) as client:
        run_id = (await client.post("/runs", json=_INPUT)).json()["run_id"]
        await cp.worker.run_once()  # pays, then notify fails -> compensation

        status = (await client.get(f"/runs/{run_id}")).json()
        assert status["status"] == "failed"
        assert services.voided == ["INV-1"]  # payment rolled back
        assert services.payments == {}


async def test_timeline_shows_events() -> None:
    cp, _ = _setup(500)
    async with _http(cp) as client:
        run_id = (await client.post("/runs", json=_INPUT)).json()["run_id"]
        await cp.worker.run_once()

        timeline = (await client.get(f"/runs/{run_id}/timeline")).json()
        names = [e["name"] for e in timeline["events"] if e["name"]]
        assert "extract_invoice" in names
        assert "create_payment" in names
        types = {e["type"] for e in timeline["events"]}
        assert "workflow_completed" in types


async def test_unknown_workflow_is_404() -> None:
    cp, _ = _setup(500)
    async with _http(cp) as client:
        resp = await client.post("/runs", json={"workflow": "nope", "input": {}})
        assert resp.status_code == 404
