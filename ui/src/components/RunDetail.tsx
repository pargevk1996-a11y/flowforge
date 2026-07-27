import { useCallback, useEffect, useState } from "react";

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

  // The full timeline is always fetched: it is what the scrubber's range is
  // measured against, and what "go live" returns to.
  const loadLive = useCallback(async () => {
    try {
      const timeline = await api.timeline(runId);
      setLive(timeline);
      setError(null);
      return timeline;
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      return null;
    }
  }, [runId]);

  useEffect(() => {
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
    void api
      .timeline(runId, at)
      .then((timeline) => !cancelled && setView(timeline))
      .catch((cause: unknown) =>
        setError(cause instanceof Error ? cause.message : String(cause)),
      );
    return () => {
      cancelled = true;
    };
  }, [runId, at, live]);

  useEffect(() => {
    if (tab !== "tree" || tree !== null) return;
    void api.tree(runId).then(setTree).catch(() => setTree(null));
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
      </dl>

      <Scrubber at={at} onChange={setAt} total={live.event_count} />

      {view.status === "suspended" && waiting.length > 0 && (
        <p className="note">
          Waiting on {waiting.map((step) => `${step.name} (${step.kind})`).join(", ")}.
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
