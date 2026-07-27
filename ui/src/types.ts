/**
 * The wire shapes, mirroring `flowforge.core.timeline` and `core.event_store`.
 *
 * Hand-written rather than generated: the surface is small, and a hand-written
 * mirror fails loudly at the type level when the server's read model moves,
 * which is exactly when someone should look.
 */

export type RunStatus = "running" | "suspended" | "completed" | "failed";
export type StepKind = "activity" | "llm" | "timer" | "signal" | "child";
export type StepStatus = "running" | "waiting" | "completed" | "failed";

export interface RunSummary {
  run_id: string;
  workflow: string;
  tenant: string;
  status: RunStatus;
  version: number;
  started_at: string;
  updated_at: string;
}

export interface RunPage {
  runs: RunSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface Step {
  command_seq: number;
  kind: StepKind;
  name: string;
  status: StepStatus;
  started_at: string;
  ended_at: string | null;
  duration_ms: number | null;
  result: unknown;
  error: string | null;
  child_run_id: string | null;
  usd_cost: number;
  event_seqs: number[];
}

export interface Compensation {
  name: string;
  at: string;
  event_seq: number;
}

export interface RunEvent {
  seq: number;
  type: string;
  command_seq: number | null;
  name: string | null;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface Timeline {
  run_id: string;
  workflow: string;
  tenant: string;
  status: RunStatus;
  started_at: string;
  ended_at: string | null;
  duration_ms: number | null;
  steps: Step[];
  compensations: Compensation[];
  parent: { run_id: string; command_seq: number } | null;
  result: unknown;
  error: string | null;
  usd_cost: number;
  event_count: number;
  truncated_at: number | null;
  events: RunEvent[];
}

export interface RunTree {
  run_id: string;
  workflow: string;
  status: RunStatus;
  usd_cost: number;
  command_seq?: number;
  children: RunTree[];
}
