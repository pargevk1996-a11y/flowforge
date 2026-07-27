/**
 * Pure formatting and summarising. No React, no fetch — so it is the part of the
 * UI worth unit-testing, and it is tested.
 */

import type { Step, StepKind, StepStatus, Timeline } from "./types";

/** Durations a human reads at a glance: never more than three significant digits. */
export function formatDuration(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1) return "<1ms";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

/** Money, at the precision LLM calls actually cost. */
export function formatCost(usd: number): string {
  if (usd === 0) return "$0";
  if (usd < 0.01) return `$${usd.toFixed(4)}`;
  return `$${usd.toFixed(2)}`;
}

export function formatTime(iso: string): string {
  const at = new Date(iso);
  return Number.isNaN(at.getTime()) ? iso : at.toISOString().slice(11, 23);
}

export function formatDate(iso: string): string {
  const at = new Date(iso);
  return Number.isNaN(at.getTime()) ? iso : at.toISOString().replace("T", " ").slice(0, 19);
}

export function relativeTime(iso: string, now: number = Date.now()): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const seconds = Math.max(0, Math.round((now - then) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

const KIND_GLYPHS: Record<StepKind, string> = {
  activity: "▸",
  llm: "✦",
  timer: "◷",
  signal: "⏻",
  child: "⑂",
};

export function kindGlyph(kind: StepKind): string {
  return KIND_GLYPHS[kind] ?? "▸";
}

/** Steps that are still open are the answer to "why is this run not finished?". */
export function pendingSteps(steps: Step[]): Step[] {
  return steps.filter((step) => step.status === "running" || step.status === "waiting");
}

export interface RunTotals {
  steps: number;
  llmCalls: number;
  failed: number;
  pending: number;
  usd: number;
  /** Wall-clock, when the run has ended; otherwise the span covered so far. */
  durationMs: number | null;
}

export function summarise(timeline: Timeline): RunTotals {
  return {
    steps: timeline.steps.length,
    llmCalls: timeline.steps.filter((step) => step.kind === "llm").length,
    failed: timeline.steps.filter((step) => step.status === "failed").length,
    pending: pendingSteps(timeline.steps).length,
    usd: timeline.usd_cost,
    durationMs: timeline.duration_ms ?? spanOf(timeline.steps),
  };
}

function spanOf(steps: Step[]): number | null {
  const ends = steps.map((step) => step.ended_at).filter((at): at is string => at !== null);
  const first = steps[0];
  if (first === undefined || ends.length === 0) return null;
  const start = new Date(first.started_at).getTime();
  const last = Math.max(...ends.map((at) => new Date(at).getTime()));
  return Number.isNaN(start) || Number.isNaN(last) ? null : last - start;
}

/**
 * A step's share of the run's spend, for the cost bar. Runs that cost nothing
 * get zeroes rather than NaN — a workflow with no LLM step is the common case,
 * not an error.
 */
export function costShare(steps: Step[]): Map<number, number> {
  const total = steps.reduce((sum, step) => sum + step.usd_cost, 0);
  return new Map(
    steps.map((step) => [step.command_seq, total > 0 ? step.usd_cost / total : 0]),
  );
}

const STATUS_ORDER: Record<StepStatus, number> = {
  failed: 0,
  running: 1,
  waiting: 2,
  completed: 3,
};

/** Failures first when triaging, command order otherwise. */
export function sortSteps(steps: Step[], byStatus: boolean): Step[] {
  const sorted = [...steps];
  sorted.sort((a, b) =>
    byStatus && STATUS_ORDER[a.status] !== STATUS_ORDER[b.status]
      ? STATUS_ORDER[a.status] - STATUS_ORDER[b.status]
      : a.command_seq - b.command_seq,
  );
  return sorted;
}

export function truncate(value: unknown, max = 120): string {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  if (text === undefined) return "—";
  return text.length > max ? `${text.slice(0, max)}…` : text;
}
