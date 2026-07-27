# flowforge

**Durable workflow engine for AI-driven business automation.** Event-sourced
execution, typed LLM steps with structured retry, human-in-the-loop via
suspend/resume, compensating actions, and per-tenant cost budgets. Temporal-style
reliability, built around LLMs as first-class primitives.

flowforge sits between *n8n / Zapier* (pretty, but the LLM is bolted on the side
and fails silently) and *Temporal / Airflow* (reliable, but AI agents are a
foreign body). It is durable execution designed with **LLM steps as first-class
primitives**.

---

## The core idea: deterministic replay

A workflow is an ordinary deterministic `async def`. Every `await ctx.*` is a
*command*, numbered by replay order. On each drive the engine replays the function
from the top:

- a command whose result is already in the event log **returns that result
  without re-executing its side effect** — this is both resume-after-crash and
  idempotency;
- a command with no recorded result **executes for real** and commits its outcome;
- `sleep` / `wait_for_signal` record what they wait for and **suspend** — the
  worker process is freed, and a later timer firing or signal delivery re-enqueues
  the run.

```python
async def invoice_to_payment(ctx: WorkflowContext, inp: Invoice) -> Payment:
    text   = await ctx.activity(ocr_pdf, inp.pdf_url)
    fields = await ctx.activity(extract_fields, text)          # (LLM step: roadmap)
    if fields.amount > 10_000:
        decision = await ctx.wait_for_signal("cfo_approval", Approval)  # suspends
        if not decision.approved:
            return Payment(status="rejected")
    return await ctx.activity(
        create_payment, fields,
        compensate=void_payment,                               # saga rollback
    )
```

| Generic AI project | flowforge |
|---|---|
| "ask a question → get an answer" | a multi-step business process |
| LLM = the product | LLM = one step out of 15 |
| a crash loses the customer | a crash resumes in seconds |
| cost = "we'll look later" | budget per tenant, cancel on exceed |
| human review = a separate button | human review = a language primitive |
| retry = `while True` | retry = typed, feedback into the prompt |

---

## Status

The **durable execution core is implemented and tested**: event-sourced state,
deterministic replay, exactly-once side effects, crash/resume, durable
`sleep`, human-in-the-loop signals, typed retry, and saga compensations — plus a
**typed `LLMStep`** with structured output, schema-violation retry that feeds the
error back into the prompt, and per-call cost tracking. There is a durable worker
loop with a priority queue and distributed locks (in-memory + Redis), and a
**Postgres-backed event store** with a migration runner — the durability tests
run against a real database. All green under `mypy --strict`.

```bash
# Run the Postgres integration tests against a throwaway database:
docker run -d --name ff-pg -e POSTGRES_USER=flowforge -e POSTGRES_PASSWORD=flowforge \
  -e POSTGRES_DB=flowforge -p 5432:5432 postgres:16
DATABASE_URL=postgresql://flowforge:flowforge@localhost:5432/flowforge pytest
```

```bash
make install   # venv + editable install with dev extras
make check     # ruff + mypy --strict + pytest
```

The tests in `tests/test_engine.py` are the executable spec for the properties
above (idempotency across replay, resume after a simulated `kill -9`,
suspend→resume via timer and via signal, retry, reverse-order compensation).

### Roadmap

| Area | State |
|---|---|
| Event-sourced core, replay, saga, suspend/resume | ✅ done |
| Typed `LLMStep[In, Out]` — structured output + schema-violation retry into the prompt + cost tracking | ✅ done |
| Durable worker loop + priority queue + distributed locks (in-memory + Redis adapters) | ✅ done |
| Postgres event store + migration runner + `flowforge migrate` (tested against a real DB) | ✅ done |
| Timer wheel — durable `sleep` wakes runs automatically (in-memory + Postgres, tested on a real DB) | ✅ done |
| Per-tenant cost budgets (persist `cost_ledger`, cancel on exceed) & per-provider rate limits | 🔜 |
| Triggers (HTTP / webhook / cron / email) | 🔜 |
| Sub-workflows, fan-out/fan-in with bounded concurrency | 🔜 |
| FastAPI control plane + React/Vite timeline & replay debugger | 🔜 |

---

## Three reference workflows (target scenarios)

### 1. Invoice-to-payment

```mermaid
flowchart LR
    A[Email / SES inbound] --> B[OCR PDF]
    B --> C[LLM: extract typed fields]
    C --> D[Match vendor DB]
    D --> E{amount > $10k?}
    E -- yes --> F[WaitForApproval: CFO]
    E -- no --> G[Create payment]
    F --> G
    G --> H[Notify Slack]
    G -. compensate .-> V[Void payment]
```

### 2. Support ticket triage

```mermaid
flowchart LR
    A[Zendesk webhook] --> B[LLM classify]
    B --> C[RAG over knowledge base]
    C --> D[Draft reply]
    D --> E{confidence >= 0.8?}
    E -- no --> F[WaitForApproval: agent]
    E -- yes --> G[Send reply]
    F --> G
    G --> H[Sleep 48h]
    H --> I{customer replied?}
    I -- no --> J[Follow-up]
```

### 3. Contract review pipeline

```mermaid
flowchart LR
    A[Contract] --> B[Fan-out by paragraph]
    B --> C1[LLM risk check]
    B --> C2[LLM risk check]
    B --> C3[LLM risk check]
    C1 --> D[Fan-in]
    C2 --> D
    C3 --> D
    D --> E[Summary report]
    E --> F[WaitForApproval: legal]
```

---

## Tech stack

Python 3.12 · `mypy --strict` · Pydantic v2 · PostgreSQL 16 + asyncpg · Redis ·
FastAPI · instructor (structured LLM output) · OpenTelemetry · React + Vite.

No LangChain / CrewAI. No Temporal SDK — this is a purpose-built analogue,
specialised for LLM steps. No Celery — too weak for durable execution.

## License

Apache-2.0
