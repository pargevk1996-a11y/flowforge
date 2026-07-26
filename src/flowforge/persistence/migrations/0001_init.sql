-- flowforge event store schema.
-- The event log is the source of truth; runs/timers/signals are projections and
-- durable scheduling state derived from or coordinating with it.

CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id        TEXT PRIMARY KEY,
    workflow_name TEXT        NOT NULL,
    tenant_id     TEXT        NOT NULL DEFAULT 'default',
    status        TEXT        NOT NULL DEFAULT 'running',  -- running|suspended|completed|failed
    version       INTEGER     NOT NULL DEFAULT 0,          -- == length of the event log
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only log. (run_id, seq) is unique, which is how optimistic concurrency
-- is enforced: a stale writer collides on the next seq and is rejected.
CREATE TABLE IF NOT EXISTS events (
    run_id      TEXT        NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    seq         INTEGER     NOT NULL,
    type        TEXT        NOT NULL,
    command_seq INTEGER,
    name        TEXT,
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

CREATE INDEX IF NOT EXISTS events_by_command
    ON events (run_id, command_seq);

-- Durable timers for sleep()/timeouts. The timer wheel scans due rows, appends
-- the corresponding TIMER_FIRED event, and re-enqueues the run.
CREATE TABLE IF NOT EXISTS timers (
    run_id      TEXT        NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    command_seq INTEGER     NOT NULL,
    fire_at     TIMESTAMPTZ NOT NULL,
    fired       BOOLEAN     NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, command_seq)
);

CREATE INDEX IF NOT EXISTS timers_due
    ON timers (fire_at) WHERE NOT fired;

-- Per-step cost accounting for per-tenant budgets.
CREATE TABLE IF NOT EXISTS cost_ledger (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT        NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    tenant_id   TEXT        NOT NULL,
    command_seq INTEGER,
    provider    TEXT,
    model       TEXT,
    usd_cost    NUMERIC(12, 6) NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cost_by_tenant
    ON cost_ledger (tenant_id, created_at);
