import { Fragment } from "react";

import { costShare, formatCost, formatDuration, formatTime, kindGlyph, sortSteps, truncate } from "../format";
import type { Step } from "../types";
import { Json } from "./Json";
import { StatusPill } from "./StatusPill";

interface Props {
  steps: Step[];
  selected: number | null;
  triage: boolean;
  onSelect: (commandSeq: number | null) => void;
  onOpenChild: (runId: string) => void;
}

/**
 * The step view: one row per command, in the order they were issued. The cost
 * bar is drawn from each step's share of the run's spend, which is how you find
 * the one clause out of forty that cost real money.
 */
export function StepTable({ steps, selected, triage, onSelect, onOpenChild }: Props) {
  if (steps.length === 0) {
    return <p className="empty">Nothing has happened yet at this point in the log.</p>;
  }
  const shares = costShare(steps);

  return (
    <table className="grid steps">
      <thead>
        <tr>
          <th className="num">#</th>
          <th>step</th>
          <th>status</th>
          <th className="num">took</th>
          <th className="num">cost</th>
          <th>result</th>
        </tr>
      </thead>
      <tbody>
        {sortSteps(steps, triage).map((step) => {
          const isOpen = selected === step.command_seq;
          const childRunId = step.child_run_id;
          return (
            <Fragment key={step.command_seq}>
              <tr
                className={isOpen ? "row-open" : undefined}
                onClick={() => onSelect(isOpen ? null : step.command_seq)}
                tabIndex={0}
              >
                <td className="num muted">{step.command_seq}</td>
                <td>
                  <span className={`glyph glyph-${step.kind}`} title={step.kind}>
                    {kindGlyph(step.kind)}
                  </span>
                  <span className="mono">{step.name}</span>
                </td>
                <td>
                  <StatusPill status={step.status} />
                </td>
                <td className="num muted">{formatDuration(step.duration_ms)}</td>
                <td className="num">
                  {step.usd_cost > 0 ? (
                    <span className="cost">
                      <span className="cost-track">
                        <span
                          className="cost-bar"
                          style={{
                            width: `${Math.round((shares.get(step.command_seq) ?? 0) * 100)}%`,
                          }}
                        />
                      </span>
                      {formatCost(step.usd_cost)}
                    </span>
                  ) : (
                    <span className="muted">—</span>
                  )}
                </td>
                <td className="muted clip">
                  {step.error !== null ? (
                    <span className="error-text">{truncate(step.error, 80)}</span>
                  ) : (
                    truncate(step.result, 80)
                  )}
                </td>
              </tr>
              {isOpen && (
                <tr className="row-detail">
                  <td colSpan={6}>
                    <div className="detail-grid">
                      <dl>
                        <dt>kind</dt>
                        <dd>{step.kind}</dd>
                        <dt>started</dt>
                        <dd className="mono">{formatTime(step.started_at)}</dd>
                        <dt>ended</dt>
                        <dd className="mono">
                          {step.ended_at === null ? "—" : formatTime(step.ended_at)}
                        </dd>
                        <dt>events</dt>
                        <dd className="mono">{step.event_seqs.join(", ") || "—"}</dd>
                        {childRunId !== null && (
                          <>
                            <dt>child run</dt>
                            <dd>
                              <button
                                className="link"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  onOpenChild(childRunId);
                                }}
                                type="button"
                              >
                                {childRunId.slice(0, 16)} →
                              </button>
                            </dd>
                          </>
                        )}
                      </dl>
                      {step.error !== null ? (
                        <pre className="error-text">{step.error}</pre>
                      ) : (
                        <Json value={step.result} label="result" />
                      )}
                    </div>
                  </td>
                </tr>
              )}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
}
