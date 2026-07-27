import { useCallback, useEffect, useState } from "react";

import { api, type RunFilters } from "../api";
import { relativeTime } from "../format";
import type { RunPage } from "../types";
import { StatusPill } from "./StatusPill";

const PAGE = 25;
const STATUSES = ["", "running", "suspended", "completed", "failed"] as const;

export function RunList({ onOpen }: { onOpen: (runId: string) => void }) {
  const [page, setPage] = useState<RunPage | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<RunFilters>({});
  const [offset, setOffset] = useState(0);

  const load = useCallback(async () => {
    try {
      setPage(await api.listRuns({ ...filters, limit: PAGE, offset }));
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, [filters, offset]);

  // Runs move on their own — a worker finishes one while you are looking at the
  // list — so the list refreshes itself rather than going quietly stale.
  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 3000);
    return () => window.clearInterval(timer);
  }, [load]);

  const setFilter = (key: keyof RunFilters, value: string) => {
    setOffset(0);
    setFilters((current) => ({ ...current, [key]: value === "" ? undefined : value }));
  };

  return (
    <section className="panel">
      <div className="toolbar">
        <select
          aria-label="status"
          value={filters.status ?? ""}
          onChange={(event) => setFilter("status", event.target.value)}
        >
          {STATUSES.map((status) => (
            <option key={status} value={status}>
              {status === "" ? "all statuses" : status}
            </option>
          ))}
        </select>
        <input
          aria-label="workflow"
          placeholder="workflow"
          value={filters.workflow ?? ""}
          onChange={(event) => setFilter("workflow", event.target.value)}
        />
        <input
          aria-label="tenant"
          placeholder="tenant"
          value={filters.tenant ?? ""}
          onChange={(event) => setFilter("tenant", event.target.value)}
        />
        <span className="spacer" />
        <span className="muted">{page ? `${page.total} runs` : "…"}</span>
      </div>

      {error !== null && <p className="error">Could not reach the control plane: {error}</p>}

      <table className="grid">
        <thead>
          <tr>
            <th>run</th>
            <th>workflow</th>
            <th>tenant</th>
            <th>status</th>
            <th className="num">events</th>
            <th>started</th>
          </tr>
        </thead>
        <tbody>
          {page?.runs.map((run) => (
            <tr key={run.run_id} onClick={() => onOpen(run.run_id)} tabIndex={0}>
              <td className="mono">{run.run_id.slice(0, 12)}</td>
              <td>{run.workflow}</td>
              <td className="muted">{run.tenant}</td>
              <td>
                <StatusPill status={run.status} />
              </td>
              <td className="num">{run.version}</td>
              <td className="muted">{relativeTime(run.started_at)}</td>
            </tr>
          ))}
          {page?.runs.length === 0 && (
            <tr>
              <td colSpan={6} className="empty">
                No runs yet. Start one with <code>POST /runs</code> or a trigger.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {page !== null && page.total > PAGE && (
        <div className="pager">
          <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))} type="button">
            ← newer
          </button>
          <span className="muted">
            {offset + 1}–{Math.min(offset + PAGE, page.total)} of {page.total}
          </span>
          <button
            disabled={offset + PAGE >= page.total}
            onClick={() => setOffset(offset + PAGE)}
            type="button"
          >
            older →
          </button>
        </div>
      )}
    </section>
  );
}
