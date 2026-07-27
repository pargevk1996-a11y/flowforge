-- Trigger state: what has already been delivered, and how far each schedule ran.

-- The exactly-once boundary for at-least-once sources. The primary key does the
-- work: two concurrent deliveries of one webhook collide here, and the loser is
-- handed the winner's run_id instead of starting a second run.
-- No foreign key to workflow_runs on purpose — the claim is written *before* the
-- run exists, which is what makes it a claim rather than a record.
CREATE TABLE IF NOT EXISTS trigger_deliveries (
    trigger_name TEXT        NOT NULL,
    dedupe_key   TEXT        NOT NULL,
    run_id       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trigger_name, dedupe_key)
);

CREATE INDEX IF NOT EXISTS trigger_deliveries_by_run
    ON trigger_deliveries (run_id);

-- How far each cron schedule has been advanced. Durable so a scheduler that
-- restarts catches up on the ticks it missed instead of skipping them.
CREATE TABLE IF NOT EXISTS cron_state (
    trigger_name TEXT PRIMARY KEY,
    last_fired   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
