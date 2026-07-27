"""FastAPI control plane.

Thin HTTP surface over the engine: start a run, inspect its status, read its full
event timeline, and deliver a signal (e.g. a human approval). Runs are enqueued
for a worker to drive; with ``run_background=True`` the app runs its own worker and
timer-wheel loops, so the whole thing is live behind ``flowforge api``.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, TypeAdapter

from flowforge.api.controlplane import ControlPlane
from flowforge.core.errors import WorkflowNotFoundError
from flowforge.queue.worker import submit


class StartRunRequest(BaseModel):
    workflow: str
    input: dict[str, Any]
    priority: int = 0
    tenant: str = "default"


class SignalRequest(BaseModel):
    name: str
    data: dict[str, Any] | None = None


def create_app(cp: ControlPlane, *, run_background: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> Any:
        stop = asyncio.Event()
        tasks = [asyncio.create_task(cp.worker.run_forever(stop=stop))]
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
        except KeyError as exc:
            raise HTTPException(404, f"unknown run {run_id!r}") from exc
        return {
            "run_id": run_id,
            "status": info.status,
            "result": info.result,
            "error": info.error,
        }

    @app.get("/runs/{run_id}/timeline")
    async def get_timeline(run_id: str) -> dict[str, Any]:
        events = await cp.store.load(run_id)
        if not events:
            raise HTTPException(404, f"unknown run {run_id!r}")
        return {"run_id": run_id, "events": [e.model_dump(mode="json") for e in events]}

    @app.post("/runs/{run_id}/signals")
    async def send_signal(run_id: str, req: SignalRequest) -> dict[str, bool]:
        try:
            await cp.engine.deliver_signal(run_id, req.name, req.data)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        await cp.queue.enqueue(run_id)
        return {"ok": True}

    return app
