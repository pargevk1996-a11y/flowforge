"""FastAPI control plane.

Thin HTTP surface over the engine: start a run, inspect its status, read its full
event timeline, deliver a signal (e.g. a human approval), read a tenant's spend,
and receive external events on a trigger. Runs are enqueued for a worker to drive;
with ``run_background=True`` the app runs its own worker, timer-wheel and cron
loops, so the whole thing is live behind ``flowforge api``.

``POST /triggers/{name}`` is the door webhooks and inbound email come through. It
is deliberately boring about retries: a redelivered event returns ``200`` with the
run id the first delivery started and ``started: false``, because a sender that
gets an error will simply try again, and two runs for one invoice is the failure
mode that actually costs money.

Budgets are enforced twice, on purpose: admission control refuses to *start* a run
for an exhausted tenant (``402``), and the meter inside each LLM step refuses to
make a call once the budget runs out mid-run. The first is politeness, the second
is the actual guarantee.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

from fastapi import Body, FastAPI, Header, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, TypeAdapter

from flowforge.api.controlplane import ControlPlane
from flowforge.core.errors import (
    BudgetExceededError,
    RunNotFoundError,
    TriggerNotFoundError,
    WorkflowNotFoundError,
)
from flowforge.core.timeline import build_timeline
from flowforge.queue.worker import submit


async def _tree(cp: ControlPlane, run_id: str, *, depth: int) -> dict[str, Any] | None:
    """A run and, recursively, the children it started."""
    events = await cp.store.load(run_id)
    if not events:
        return None
    timeline = build_timeline(run_id, events)
    children: list[dict[str, Any]] = []
    if depth > 0:
        for step in timeline.steps:
            if step.child_run_id is None:
                continue
            child = await _tree(cp, step.child_run_id, depth=depth - 1)
            if child is not None:
                children.append({**child, "command_seq": step.command_seq})
    return {
        "run_id": run_id,
        "workflow": timeline.workflow,
        "status": timeline.status,
        "usd_cost": timeline.usd_cost,
        "children": children,
    }


class StartRunRequest(BaseModel):
    workflow: str
    input: dict[str, Any]
    priority: int = 0
    tenant: str = "default"


class SignalRequest(BaseModel):
    name: str
    data: dict[str, Any] | None = None


def create_app(
    cp: ControlPlane,
    *,
    run_background: bool = False,
    ui_dir: Path | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> Any:
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(cp.worker.run_forever(stop=stop)),
            asyncio.create_task(cp.cron.run_forever(stop=stop)),
        ]
        if cp.wheel is not None:
            tasks.append(asyncio.create_task(cp.wheel.run_forever(stop=stop)))
        try:
            yield
        finally:
            stop.set()
            for task in tasks:
                task.cancel()

    app = FastAPI(title="flowforge control plane", lifespan=lifespan if run_background else None)

    @app.post("/runs")
    async def start_run(req: StartRunRequest) -> dict[str, str]:
        try:
            wf = cp.registry.get(req.workflow)
        except WorkflowNotFoundError as exc:
            raise HTTPException(404, f"unknown workflow {req.workflow!r}") from exc
        if cp.budget is not None:
            try:
                await cp.budget.ensure_within(req.tenant)
            except BudgetExceededError as exc:
                raise HTTPException(402, str(exc)) from exc
        workflow_input = TypeAdapter(wf.input_type).validate_python(req.input)
        run_id = uuid4().hex
        await submit(
            cp.engine, cp.queue, run_id, wf, workflow_input,
            priority=req.priority, tenant=req.tenant,
        )
        return {"run_id": run_id, "status": "running"}

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        try:
            info = await cp.engine.describe(run_id)
        except RunNotFoundError as exc:
            raise HTTPException(404, f"unknown run {run_id!r}") from exc
        return {
            "run_id": run_id,
            "status": info.status,
            "result": info.result,
            "error": info.error,
        }

    @app.get("/runs")
    async def list_runs(
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        tenant: str | None = None,
        workflow: str | None = None,
    ) -> dict[str, Any]:
        page = await cp.store.list_runs(
            limit=min(max(limit, 1), 200),
            offset=max(offset, 0),
            status=status,
            tenant=tenant,
            workflow=workflow,
        )
        return page.model_dump(mode="json")

    @app.get("/runs/{run_id}/timeline")
    async def get_timeline(run_id: str, at: int | None = None) -> dict[str, Any]:
        """The run projected into steps, plus the raw log behind them.

        ``at`` truncates the log to that event, which *is* the replay debugger:
        the projection is a pure function of a prefix, so a shorter list is
        exactly what the engine would have seen at that point."""
        events = await cp.store.load(run_id)
        if not events:
            raise HTTPException(404, f"unknown run {run_id!r}")
        if at is not None:
            events = events[: max(at, 0) + 1]
        costs = await cp.ledger.entries_for_run(run_id) if cp.ledger is not None else []
        timeline = build_timeline(run_id, events, costs=costs, truncated_at=at)
        return {
            **timeline.model_dump(mode="json"),
            "events": [e.model_dump(mode="json") for e in events],
        }

    @app.get("/runs/{run_id}/tree")
    async def get_tree(run_id: str, depth: int = 3) -> dict[str, Any]:
        """The run and its sub-workflows, since a fan-out is many logs at once."""
        node = await _tree(cp, run_id, depth=max(min(depth, 8), 0))
        if node is None:
            raise HTTPException(404, f"unknown run {run_id!r}")
        return node

    @app.post("/runs/{run_id}/signals")
    async def send_signal(run_id: str, req: SignalRequest) -> dict[str, bool]:
        try:
            await cp.engine.deliver_signal(run_id, req.name, req.data)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        await cp.queue.enqueue(run_id)
        return {"ok": True}

    @app.get("/triggers")
    async def list_triggers() -> dict[str, Any]:
        return {
            "triggers": [
                {
                    "name": t.name,
                    "kind": t.kind,
                    "workflow": t.workflow,
                    "tenant": t.tenant,
                    "schedule": t.schedule,
                }
                for t in cp.triggers.all()
            ]
        }

    @app.post("/triggers/{name}")
    async def fire_trigger(
        name: str,
        event: Annotated[dict[str, Any] | None, Body()] = None,
        idempotency_key: Annotated[
            str | None, Header(alias="X-Idempotency-Key")
        ] = None,
    ) -> dict[str, Any]:
        try:
            delivery = await cp.dispatcher.fire(name, event or {}, key=idempotency_key)
        except TriggerNotFoundError as exc:
            raise HTTPException(404, f"unknown trigger {name!r}") from exc
        except BudgetExceededError as exc:
            raise HTTPException(402, str(exc)) from exc
        except ValueError as exc:  # includes pydantic's ValidationError
            # The event does not describe a run this workflow can start. 422, not
            # 500: the sender's payload is wrong, and retrying it will not help.
            raise HTTPException(422, f"event does not fit trigger {name!r}: {exc}") from exc
        return {
            "trigger": delivery.trigger,
            "run_id": delivery.run_id,
            "started": delivery.started,
        }

    @app.get("/tenants/{tenant}/spend")
    async def get_spend(tenant: str) -> dict[str, Any]:
        if cp.budget is None:
            raise HTTPException(404, "no cost ledger is configured")
        budget = cp.budget.budget_for(tenant)
        return {
            "tenant": tenant,
            "spent_usd": await cp.budget.spent(tenant),
            "limit_usd": budget.limit_usd if budget is not None else None,
            "remaining_usd": await cp.budget.remaining(tenant),
            "window_seconds": budget.window.total_seconds() if budget is not None else None,
        }

    if ui_dir is not None and (ui_dir / "index.html").exists():
        # Mounted last, at the root, so it catches everything the API did not —
        # and `html=True` serves index.html for the hash routes the UI owns.
        app.mount("/", StaticFiles(directory=ui_dir, html=True), name="ui")

    return app
