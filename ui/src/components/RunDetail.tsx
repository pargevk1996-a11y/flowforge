import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api";
import { formatCost, formatDate, formatDuration, pendingSteps, summarise } from "../format";
import type { RunTree, Timeline } from "../types";
import { EventTable } from "./EventTable";
import { Json } from "./Json";
import { RunTreeView } from "./RunTreeView";
import { Scrubber } from "./Scrubber";
import { StatusPill } from "./StatusPill";
import { StepTable } from "./StepTable";

type Tab = "steps" | "events" | "tree";

const TERMINAL = new Set(["completed", "failed"]);

interface Props {
  runId: string;
  onOpen: (runId: string) => void;
  onClose: () => void;
}

export function RunDetail({ runId, onOpen, onClose }: Props) {
  const [live, setLive] = useState<Timeline | null>(null);
  const [view, setView] = useState<Timeline | null>(null);
  const [tree, setTree] = useState<RunTree | null>(null);
  const [at, setAt] = useState<number | null>(null);
  const [tab, setTab] = useState<Tab>("steps");
  const [triage, setTriage] = useState(false);
  const [selected, setSelected] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Bumped whenever the view moves to another run, so a response that was
  // already in flight for the previous one cannot land on top of this one.
  const generation = useRef(0);

  // The full timeline is always fetched: it is what the scrubber's range is
  // measured against, and what "go live" returns to.
  const loadLive = useCallback(async () => {
    const mine = generation.current;
    try {
      const timeline = await api.timeline(runId);
      if (generation.current !== mine) return null;
      setLive(timeline);
      setError(null);
      return timeline;
    } catch (cause) {
      if (generation.current !== mine) return null;
      setError(cause instanceof Error ? cause.message : String(cause));
      return null;
    }
  }, [runId]);

  useEffect(() => {
    // Clear the previous run's data, not just the controls: keeping it would
    // render one run's steps under another run's id until the fetch returns —
    // exactly what happens when following a child link out of a fan-out.
    generation.current += 1;
    setLive(null);
    setView(null);
    setError(null);
    setAt(null);
    setSelected(null);
    setTree(null);
    void loadLive();
  }, [loadLive]);

  // Follow a running run, but stop polling the moment it settles — and never
  // poll while the scrubber is parked in the past, which does not change.
  useEffect(() => {
    if (at !== null || (live !== null && TERMINAL.has(live.status))) return;
    const timer = window.setInterval(() => void loadLive(), 2000);
    return () => window.clearInterval(timer);
  }, [at, live, loadLive]);

  useEffect(() => {
    if (at === null) {
      setView(live);
      return;
    }
    let cancelled = false;
    const mine = generation.current;
    void api
      .timeline(runId, at)
      .then((timeline) => {
        if (!cancelled && generation.current === mine) setView(timeline);
      })
      .catch((cause: unknown) => {
        if (!cancelled && generation.current === mine) {
          setError(cause instanceof Error ? cause.message : String(cause));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [runId, at, live]);

  useEffect(() => {
    if (tab !== "tree" || tree !== null) return;
    const mine = generation.current;
    void api
      .tree(runId)
      .then((node) => generation.current === mine && setTree(node))
      .catch(() => undefined);
  }, [tab, tree, runId]);

  if (error !== null && live === null) {
    return (
      <section className="panel">
        <p className="error">Could not load run {runId}: {error}</p>
        <button onClick={onClose} type="button">← all runs</button>
      </section>
    );
  }
  if (view === null || live === null) return <section className="panel muted">loading…</section>;

  const totals = summarise(view);
  const waiting = pendingSteps(view.steps);
  const selectedStep = view.steps.find((step) => step.command_seq === selected) ?? null;

  return (
    <section className="panel">
      <div className="run-head">
        <button className="link" onClick={onClose} type="button">← all runs</button>
        <h1>{view.workflow}</h1>
        <StatusPill status={view.status} />
        {view.truncated_at !== null && <span className="badge-past">replaying the past</span>}
        <span className="spacer" />
        <span className="mono muted">{runId}</span>
      </div>

      <dl className="stats">
        <div><dt>tenant</dt><dd>{view.tenant}</dd></div>
        <div><dt>started</dt><dd className="mono">{formatDate(view.started_at)}</dd></div>
        <div><dt>duration</dt><dd>{formatDuration(totals.durationMs)}</dd></div>
        <div><dt>steps</dt><dd>{totals.steps}</dd></div>
        <div><dt>llm calls</dt><dd>{totals.llmCalls}</dd></div>
        <div><dt>cost</dt><dd>{formatCost(totals.usd)}</dd></div>
        {view.trace_id !== null && (
          <div>
            <dt>trace</dt>
            <dd className="mono clip" title={view.trace_id}>
              {view.trace_id.slice(0, 16)}…
            </dd>
          </div>
        )}
      </dl>

      <Scrubber at={at} onChange={setAt} total={live.event_count} />

      {view.status === "suspended" && waiting.length > 0 && (
        <p className="note">
          Waiting on {waiting.map((step) => `${step.name} (${step.kind})`).join(", ")}.
        </p>
      )}
      {view.status === "stuck" && (
        <p className="note">
          The last attempt broke in workflow code, so the run is parked: nothing was
          rolled back. Fix the code and drive it again — replay resumes from here.
        </p>
      )}
      {view.error !== null && <pre className="error-text banner">{view.error}</pre>}
      {view.parent !== null && (
        <p className="note">
          Child of{" "}
          <button className="link mono" onClick={() => onOpen(view.parent!.run_id)} type="button">
            {view.parent.run_id}
          </button>{" "}
          at command #{view.parent.command_seq}.
        </p>
      )}

      <div className="tabs">
        {(["steps", "events", "tree"] as const).map((name) => (
          <button
            className={tab === name ? "tab tab-on" : "tab"}
            key={name}
            onClick={() => setTab(name)}
            type="button"
          >
            {name}
            {name === "events" && <span className="muted"> {view.events.length}</span>}
          </button>
        ))}
        <span className="spacer" />
        {tab === "steps" && (
          <label className="toggle">
            <input checked={triage} onChange={(e) => setTriage(e.target.checked)} type="checkbox" />
            failures first
          </label>
        )}
      </div>

      {tab === "steps" && (
        <StepTable
          onOpenChild={onOpen}
          onSelect={setSelected}
          replaying={view.truncated_at !== null}
          selected={selected}
          steps={view.steps}
          triage={triage}
        />
      )}
      {tab === "events" && (
        <EventTable events={view.events} highlight={selectedStep?.event_seqs ?? []} />
      )}
      {tab === "tree" &&
        (tree === null ? (
          <p className="muted">loading…</p>
        ) : (
          <RunTreeView node={tree} onOpen={onOpen} />
        ))}

      {view.compensations.length > 0 && (
        <div className="compensations">
          <h2>compensations</h2>
          <p className="muted">
            The saga unwound in reverse order: {view.compensations.map((c) => c.name).join(" ← ")}
          </p>
        </div>
      )}

      {view.result !== null && <Json label="result" value={view.result} />}
    </section>
  );
}
