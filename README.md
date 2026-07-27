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
    fields = await ctx.llm(extract_fields, text)               # typed, billed, capped
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
run against a real database. Every LLM call is billed to its tenant in a durable
**cost ledger**, capped by a rolling per-tenant budget, and paced by a
per-provider rate limit. Runs start from **triggers** — webhook, inbound email, or
cron — with exactly-once delivery claims over at-least-once sources. All green
under `mypy --strict`.

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
| FastAPI control plane (start / status / timeline / signal) + **invoice-to-payment** reference workflow end-to-end | ✅ done |
| Per-tenant cost budgets (durable `cost_ledger`, cancel + compensate on exceed) & per-provider rate limits | ✅ done |
| Triggers — HTTP/webhook, inbound email, cron — with exactly-once delivery claims | ✅ done |
| Sub-workflows, fan-out/fan-in with bounded concurrency | 🔜 next |
| React/Vite timeline & replay debugger UI | 🔜 |

---

## Control plane

A thin FastAPI surface over the engine. Runs are enqueued and driven by a worker;
`invoice-to-payment` is registered and runnable end-to-end.

| Method & path | Purpose |
|---|---|
| `POST /runs` | start a run: `{workflow, input, priority?, tenant?}` → `{run_id}` |
| `GET /runs/{id}` | status: `running` / `suspended` / `completed` / `failed` (+ result/error) |
| `GET /runs/{id}/timeline` | the full event log — every prompt, LLM result, retry, wait, and cost |
| `POST /runs/{id}/signals` | deliver a signal, e.g. the CFO approval that wakes a suspended run |
| `GET /tenants/{tenant}/spend` | spend in the current window, the limit, and what is left |
| `GET /triggers` | the registered triggers, their kinds and schedules |
| `POST /triggers/{name}` | deliver an external event: a webhook body, or a provider's inbound email |

`POST /runs` and `POST /triggers/{name}` answer `402` when the tenant's budget is
already exhausted.

The end-to-end tests in `tests/test_invoice_api.py` drive the real HTTP surface
through auto-pay under the threshold, human approval above it, rejection, and saga
rollback when a downstream step fails.

---

## Triggers

A run has to start somehow, and in a business process it is rarely a human
pressing a button: an invoice arrives as email, an AP system calls a webhook, a
sweep runs hourly. All three land on one dispatcher, so they inherit one set of
guarantees.

| Kind | Arrives as | Deduped on |
|---|---|---|
| `webhook` | `POST /triggers/{name}` with a JSON body | whatever identity the sender provides (`X-Idempotency-Key`, or a field the trigger picks) |
| `email` | the same endpoint, carrying a provider's inbound-email payload | the mail's own `Message-ID` |
| `cron` | a five-field schedule, driven by the scheduler loop | the tick's own timestamp |

**Exactly-once, out of at-least-once.** Every external source retries — that is
what makes them reliable, and it is also what would pay an invoice twice. So the
dispatcher *claims* the event's identity in `trigger_deliveries` before it creates
anything, and a redelivery gets `200` with `started: false` and the run id the
first delivery started. The claim is a single `INSERT ... ON CONFLICT DO NOTHING
RETURNING`, so ten concurrent deliveries of one webhook produce one winner and
nine runners-up — proven against a real database in `tests/test_postgres_triggers.py`.

A crash between claiming and seeding the run would strand the event; the retry
that follows notices the claim has no run behind it and finishes the job under the
claimed id.

**Cron catches up.** The scheduler remembers the last tick it fired, so a process
that was down for four hours replays the four ticks it missed instead of skipping
them — bounded per pass, because a minutely schedule after a long outage owes
thousands of runs and flooding the queue is worse than draining it. Each tick is
dispatched under its own timestamp, so catching up twice, or running two
schedulers, still yields one run per tick.

```python
register_invoice_triggers(cp.triggers, tenant="acme")

# an invoice PDF mailed to the AP inbox, an AP system's callback, an hourly sweep
POST /triggers/invoice_email    {"Message-Id": "...", "attachments": [...]}
POST /triggers/invoice_webhook  {"invoice_id": "INV-1", "pdf_url": "s3://..."}
```

The email mapper is handed a parsed `InboundEmail` rather than a provider's raw
body, so a workflow never learns which vendor relays its mail — `parse_inbound`
normalises the Mailgun/Postmark/SES shapes into one model.

---

## Cost control

An LLM step is the one part of a workflow that spends money while it runs, so it
is the one part that has to be bounded. Two independent limits do that:

**Per-tenant budgets.** Every provider call is priced from token usage and written
to a durable `cost_ledger` row tagged with the run, the command, and the tenant.
Before the *next* call, the guard sums that tenant's spend over a rolling window
(`$/day` by default) and refuses once the limit is reached. The refusal is a
`BudgetExceededError` — non-retryable, so the run fails through the ordinary saga
path and **everything it already did is compensated**. A run cannot be left
half-paid because the money ran out.

The tenant is written into `WORKFLOW_STARTED`, not carried in from the queue, so
who gets billed is identical on every replay and survives a crash. Schema retries
inside a step are billed individually — they are real calls — and each one is
gated, because a retry loop is the fastest way for a run to spend money it no
longer has. Replay bills nothing: a recorded result never re-calls the model.

**Per-provider rate limits.** A token bucket per provider paces outbound calls,
waiting for capacity rather than failing. Only when the wait would exceed a
ceiling does it raise `RateLimitedError` — which is *retryable*, so the activity's
retry policy backs off and tries again.

```python
cp = build_control_plane(
    store, registry,
    ledger=PostgresCostLedger(pool),          # durable accounting
    budget=Budget(limit_usd=50.0),            # $50/day per tenant
    tenant_budgets={"whale": Budget(limit_usd=5_000.0)},
)
```

Configure the defaults with `TENANT_BUDGET_USD_PER_DAY` and
`LLM_RATE_LIMIT_PER_SECOND` (see `.env.example`). A ledger with no budget is pure
accounting: it records everything and enforces nothing.

`tests/test_budget.py` is the executable spec — billing to the right tenant, no
double-billing on replay, cancel-and-compensate on exceed, tenant isolation, and
a rolling window; `tests/test_postgres_ledger.py` proves the same against a real
database.

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
