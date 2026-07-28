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
cron — with exactly-once delivery claims over at-least-once sources, and fan out
over activities, LLM steps or **child runs** with a bound that survives a restart.
A React/Vite **replay debugger** scrubs any run back through its own log, and
**OpenTelemetry tracing** gives each run a single span tree that survives the
process that started it. A sweeper reaps parents whose children died before
reporting back. All green under `mypy --strict` and `tsc --strict`.

The suite skips what it cannot reach, so run it against throwaway infrastructure
to execute every test rather than most of them:

```bash
docker run -d --rm --name ff-pg -e POSTGRES_USER=flowforge \
  -e POSTGRES_PASSWORD=flowforge -e POSTGRES_DB=flowforge -p 5432:5432 postgres:16
docker run -d --rm --name ff-redis -p 6379:6379 redis:7-alpine

DATABASE_URL=postgresql://flowforge:flowforge@localhost:5432/flowforge \
REDIS_URL=redis://localhost:6379/0 pytest        # 196 tests, no skips
```

```bash
make install   # venv + editable install with dev + serve extras
make check     # ruff + mypy --strict + pytest
make ui-install && make ui   # tsc --strict + vitest + vite build
make serve     # the control plane and the debugger on :8000
```

`make serve` runs both reference workflows against a **canned LLM client** — no key,
no database, no queue — so the whole engine is explorable from one command.

The tests in `tests/test_engine.py` are the executable spec for the properties
above (idempotency across replay, resume after a simulated `kill -9`,
suspend→resume via timer and via signal, retry, reverse-order compensation), and
`tests/test_resilience.py` is the spec for the failure boundaries below.

### Three kinds of failure, three answers

Conflating these is how durable engines lose data or lose availability:

| What broke | What happens |
|---|---|
| An activity exhausted its retries | **Business failure** — the run fails and its saga compensates |
| The workflow function itself raised | **Parked** (`stuck`) — the error is recorded, *nothing is rolled back*, and the run resumes from where it stopped once the code is fixed |
| The store is unreachable | **Infrastructure** — the run goes back on the queue and the worker backs off |

Compensating a payment because of a `KeyError` destroys more than it saves, so a
bug in workflow code never triggers a rollback. Resuming is safe because the
parking event carries no command: replay walks straight past it and picks up at
the next unfinished step, repeating no side effect. An unregistered workflow and a
run seeded before its input schema changed park for the same reason.

And no single run's failure may take down the loop that processes the others: the
worker, timer wheel and cron scheduler run through one supervisor that logs a
failed iteration, backs off and carries on — a detached task that dies on its
first exception dies *silently*, and one malformed cron mapper must not stop every
other schedule in the process.

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
| Sub-workflows, fan-out/fan-in with bounded concurrency + **contract-review** reference workflow | ✅ done |
| React/Vite timeline & **replay debugger** UI (`flowforge api --demo`) | ✅ done |
| OpenTelemetry tracing — one span tree per run, across workers and restarts | ✅ done |
| A sweeper for parents whose children finished without reporting back | ✅ done |

---

## Control plane

A thin FastAPI surface over the engine. Runs are enqueued and driven by a worker;
`invoice-to-payment` and `contract-review` are registered and runnable end-to-end.

| Method & path | Purpose |
|---|---|
| `POST /runs` | start a run: `{workflow, input, priority?, tenant?}` → `{run_id}` |
| `GET /runs/{id}` | status: `running` / `suspended` / `completed` / `failed` / `stuck` (+ result/error) |
| `GET /runs/{id}/timeline` | the run folded into **steps** + the raw log; `?at=N` replays it to that point |
| `POST /runs/{id}/signals` | deliver a signal, e.g. the CFO approval that wakes a suspended run |
| `GET /tenants/{tenant}/spend` | spend in the current window, the limit, and what is left |
| `GET /runs` | browse runs: filter by `status`, `tenant`, `workflow`; paginated |
| `GET /runs/{id}/tree` | the run and its sub-workflows, since a fan-out is many logs |
| `GET /triggers` | the registered triggers, their kinds and schedules |
| `POST /triggers/{name}` | deliver an external event: a webhook body, or a provider's inbound email |

`POST /runs` and `POST /triggers/{name}` answer `402` when the tenant's budget is
already exhausted.

The end-to-end tests in `tests/test_invoice_api.py` drive the real HTTP surface
through auto-pay under the threshold, human approval above it, rejection, and saga
rollback when a downstream step fails.

---

## Tracing

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 flowforge api --demo
```

A run is not a call stack. It is driven, suspends, and is driven again — minutes
or days later, on a different worker — and it branches into child runs driven
elsewhere again. Instrument each drive on its own and you get a hundred
disconnected traces of the same invoice.

So the trace context travels the only way anything travels here: **in the event
log**. The run's root span context is written into `WORKFLOW_STARTED` as a W3C
`traceparent`, and every later drive, in whatever process, starts its span against
that recorded parent:

```
run invoice_to_payment                     ← anchor, written into the log
├── drive invoice_to_payment               ← worker A, 09:00
│   ├── activity ocr_pdf
│   ├── llm extract_invoice
│   └── outcome=suspended, waiting_on=signal:cfo_approval:3
└── drive invoice_to_payment               ← worker B, 14:32, after the approval
    ├── activity create_payment
    └── outcome=completed
```

A fan-out stays one tree too: each child command is a span, and the child run's
own anchor is created *inside* it, so a forty-way fan-out is forty subtrees of one
trace rather than forty traces.

Three deliberate choices. The root span is opened and closed immediately — it is
an anchor, not a measurement, because no other process can close a span this one
opened. Replayed commands get no span: a drive re-walks everything the run has
ever done, and spanning that would fill the trace with copies. And a suspension is
*not* an error on the span — it is the engine working as designed — while a
compensated failure and a parked run both are.

`tests/test_tracing.py` asserts against spans collected from the real SDK, so what
is tested is what an exporter would ship.

---

## The replay debugger

```
make ui-install && make ui && make serve    # → http://localhost:8000
```

A run's log is the truth, but it is a stream of low-level facts — *scheduled*,
*completed*, *fired*. The debugger projects it into the **step**: this activity,
this LLM call, this approval; what it returned, how long it took, what it cost.

- **Run list** — filter by status, tenant or workflow; refreshes itself, because
  a worker finishes a run while you are looking at it. `stuck` runs are the ones
  waiting on a code fix.
- **Steps** — one row per command, with the LLM calls marked and a cost bar, so
  the one clause out of forty that spent real money is visible at a glance.
  Failures-first ordering for triage; expand a row for its payloads.
- **Events** — the raw log underneath, with the events of the selected step
  highlighted. When the projection and your expectations disagree, this settles it.
- **Tree** — parent and child runs, since a fan-out is many logs at once.

**Time travel is not a simulation.** The projection is a pure function of a
*prefix* of the log, so dragging the scrubber to event N asks the server for
`build_timeline(run_id, events[:N+1])` — which is exactly what the engine would
have replayed from at that point. There is no snapshot machinery, and there is
nothing to keep in sync: a shorter list is the past.

The UI is React + Vite + TypeScript (`tsc --strict`), no component library, no
state library, ~800 lines. `flowforge api` serves the built app beside the API on
one origin; `npm --prefix ui run dev` proxies to it for hacking on the frontend.

---

## Fan-out, fan-in, and sub-workflows

One contract becomes forty clauses to judge; one batch becomes a thousand records
to process. This is where an AI workflow either scales or falls over — forty
simultaneous calls will trip a provider's rate limit, and can spend a tenant's
daily budget in about a second.

**Deterministic replay survives concurrency** because command numbers are handed
out *up front, in item order*, before anything is awaited. Numbering is a property
of the workflow's shape, not of who finished first. What varies between runs is
only the order events land in the log, which nothing reads for meaning.

| Primitive | Fans out over | Bound |
|---|---|---|
| `ctx.map(fn, items, concurrency=N)` | activities inside this run | a semaphore, N at once |
| `ctx.map_llm(step, contents, concurrency=N)` | typed LLM steps, each metered and gated | as above, and the tenant's budget |
| `ctx.children(workflow, inputs, concurrency=N)` | **child runs** — each its own log, id and worker | derived from the log, so it survives a restart |

```python
async def contract_review(ctx: WorkflowContext, inp: Contract) -> RiskReport:
    paragraphs = await ctx.activity(fetch_paragraphs, inp.url)
    findings   = await ctx.map_llm(risk_step, paragraphs, concurrency=4)  # fan out
    high       = sum(1 for f in findings if f.level == "high")            # fan in
    if high:
        await ctx.wait_for_signal("legal_approval", LegalDecision)
    ...
```

A failing item does **not** cancel its siblings: everything in flight is allowed
to finish and record itself first — abandoning a side effect that already happened
is how a fan-out loses money — and then the earliest failure, by item order, is
raised. Compensations unwind in item order too, not in finish order.

**Sub-workflows are real runs.** `ctx.child(workflow, input)` seeds an independent
run and suspends the parent; the child wakes the parent and writes the outcome into
its log when it terminates. Child ids are derived (`{parent}.{command_seq}`), so
replaying a parent recognises the child it already started instead of starting a
second one.

Reporting back is two steps, and **the wake-up comes first on purpose**: a crash
between them leaves the parent queued without the result, which the next drive
re-reads from the child's own log. Losing the news is survivable; losing the nudge
is not. The window that ordering cannot close — a child that commits its result and
then dies before reporting at all — is what `ChildSweeper` is for: it scans
suspended runs, asks whether a child they wait on has already terminated, and
re-queues the ones that have. It repairs nothing itself; restoring the nudge is
enough, because the drive reconciles the outcome. It is a scan, deliberately: this
covers a crash window, not a hot path.

That is what makes the bound on `ctx.children` *durable*: a thousand-item fan-out
never has a thousand runs in flight, and the pacing is recomputed from the log on
every drive rather than held in a semaphore that a restart would forget.

`tests/test_fanout.py` and `tests/test_children.py` are the executable spec;
`workflows/contract_review/` runs the whole thing end to end.

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
`LLM_RATE_LIMIT_PER_SECOND` (see `.env.example`); `flowforge api --demo` reads
both, and a rate of zero is refused at start-up rather than dividing by zero on
the first call. A ledger with no budget is pure
accounting: it records everything and enforces nothing.

`tests/test_budget.py` is the executable spec — billing to the right tenant, no
double-billing on replay, cancel-and-compensate on exceed, tenant isolation, and
a rolling window; `tests/test_postgres_ledger.py` proves the same against a real
database.

---

## Three reference workflows

Two are implemented end to end (`workflows/`); the third is the target shape for
support triage.

### 1. Invoice-to-payment — implemented

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

### 2. Support ticket triage — target scenario

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

### 3. Contract review pipeline — implemented

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
    F --> G[File report]
    G -. compensate .-> R[Retract report]
```

The fan-out is bounded and metered: `ctx.map_llm(risk_step, paragraphs,
concurrency=4)`, every clause billed to the tenant and refused once the budget is
gone. `tests/test_contract_review.py` drives it over the real HTTP surface.

---

## Tech stack

Python 3.12 · `mypy --strict` · Pydantic v2 · PostgreSQL 16 + asyncpg · Redis ·
FastAPI · React 19 + Vite + TypeScript (`tsc --strict`).

Structured LLM output is done here rather than delegated: the schema goes into the
prompt, the response is validated against the Pydantic model, and a violation is
fed back as a correction — so there is no `instructor` dependency to explain. The
provider SDKs live behind the `llm` extra and one 40-line `LLMClient` protocol;
nothing in the engine imports them.

No LangChain / CrewAI. No Temporal SDK — this is a purpose-built analogue,
specialised for LLM steps. No Celery — too weak for durable execution.

OpenTelemetry lives behind the `otel` extra and a five-method protocol; without it
the engine's tracer is a `nullcontext` and costs nothing.

## License

Apache-2.0
