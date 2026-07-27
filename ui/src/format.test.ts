import { describe, expect, it } from "vitest";

import { costShare, formatCost, formatDuration, pendingSteps, relativeTime, sortSteps, summarise, truncate } from "./format";
import type { Step, Timeline } from "./types";

function step(overrides: Partial<Step> = {}): Step {
  return {
    command_seq: 0,
    kind: "activity",
    name: "step",
    status: "completed",
    started_at: "2026-03-01T10:00:00Z",
    ended_at: "2026-03-01T10:00:01Z",
    duration_ms: 1000,
    result: null,
    error: null,
    child_run_id: null,
    usd_cost: 0,
    event_seqs: [],
    ...overrides,
  };
}

describe("formatDuration", () => {
  it("keeps every scale readable", () => {
    expect(formatDuration(null)).toBe("—");
    expect(formatDuration(0.4)).toBe("<1ms");
    expect(formatDuration(87)).toBe("87ms");
    expect(formatDuration(1500)).toBe("1.50s");
    expect(formatDuration(42_000)).toBe("42.0s");
    expect(formatDuration(90_000)).toBe("1m 30s");
    expect(formatDuration(3_900_000)).toBe("1h 5m");
  });
});

describe("formatCost", () => {
  it("shows fractions of a cent, which is what a call actually costs", () => {
    expect(formatCost(0)).toBe("$0");
    expect(formatCost(0.0002)).toBe("$0.0002");
    expect(formatCost(1.5)).toBe("$1.50");
  });
});

describe("relativeTime", () => {
  const now = new Date("2026-03-01T12:00:00Z").getTime();

  it("counts backwards in the largest unit that fits", () => {
    expect(relativeTime("2026-03-01T11:59:30Z", now)).toBe("30s ago");
    expect(relativeTime("2026-03-01T11:30:00Z", now)).toBe("30m ago");
    expect(relativeTime("2026-03-01T09:00:00Z", now)).toBe("3h ago");
    expect(relativeTime("2026-02-26T12:00:00Z", now)).toBe("3d ago");
  });

  it("never reports the future as a negative age", () => {
    expect(relativeTime("2026-03-01T12:00:30Z", now)).toBe("0s ago");
  });
});

describe("pendingSteps", () => {
  it("answers why a run has not finished", () => {
    const steps = [
      step({ command_seq: 0 }),
      step({ command_seq: 1, kind: "signal", status: "waiting" }),
      step({ command_seq: 2, status: "running" }),
    ];
    expect(pendingSteps(steps).map((s) => s.command_seq)).toEqual([1, 2]);
  });
});

describe("costShare", () => {
  it("splits the run's spend across its steps", () => {
    const shares = costShare([
      step({ command_seq: 0, usd_cost: 0.75 }),
      step({ command_seq: 1, usd_cost: 0.25 }),
    ]);
    expect(shares.get(0)).toBeCloseTo(0.75);
    expect(shares.get(1)).toBeCloseTo(0.25);
  });

  it("gives a free run zeroes rather than NaN", () => {
    const shares = costShare([step({ command_seq: 0 }), step({ command_seq: 1 })]);
    expect([...shares.values()]).toEqual([0, 0]);
  });
});

describe("sortSteps", () => {
  const steps = [
    step({ command_seq: 0, status: "completed" }),
    step({ command_seq: 1, status: "failed" }),
    step({ command_seq: 2, status: "waiting" }),
  ];

  it("keeps command order by default — that is the order they happened", () => {
    expect(sortSteps(steps, false).map((s) => s.command_seq)).toEqual([0, 1, 2]);
  });

  it("puts failures first when triaging", () => {
    expect(sortSteps(steps, true).map((s) => s.command_seq)).toEqual([1, 2, 0]);
  });

  it("does not mutate its input", () => {
    sortSteps(steps, true);
    expect(steps.map((s) => s.command_seq)).toEqual([0, 1, 2]);
  });
});

describe("summarise", () => {
  const timeline: Timeline = {
    run_id: "r1",
    workflow: "contract_review",
    tenant: "acme",
    status: "suspended",
    started_at: "2026-03-01T10:00:00Z",
    ended_at: null,
    duration_ms: null,
    steps: [
      step({ command_seq: 0, ended_at: "2026-03-01T10:00:02Z" }),
      step({ command_seq: 1, kind: "llm", usd_cost: 0.02 }),
      step({ command_seq: 2, kind: "llm", status: "failed", usd_cost: 0.01 }),
      step({ command_seq: 3, kind: "signal", status: "waiting", ended_at: null }),
    ],
    compensations: [],
    parent: null,
    result: null,
    error: null,
    usd_cost: 0.03,
    event_count: 9,
    truncated_at: null,
    events: [],
  };

  it("counts what a triage view needs", () => {
    const totals = summarise(timeline);
    expect(totals).toMatchObject({ steps: 4, llmCalls: 2, failed: 1, pending: 1, usd: 0.03 });
  });

  it("falls back to the span of finished steps while a run is still open", () => {
    expect(summarise(timeline).durationMs).toBe(2000);
  });
});

describe("truncate", () => {
  it("shortens long values but leaves short ones alone", () => {
    expect(truncate("hello")).toBe("hello");
    expect(truncate({ a: 1 })).toBe('{"a":1}');
    expect(truncate("x".repeat(200), 10)).toBe(`${"x".repeat(10)}…`);
  });

  it("survives values JSON cannot describe", () => {
    expect(truncate(undefined)).toBe("—");
  });
});
