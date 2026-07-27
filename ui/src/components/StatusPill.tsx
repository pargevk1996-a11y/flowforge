import type { RunStatus, StepStatus } from "../types";

/** One vocabulary of colour for run status and step status alike. */
export function StatusPill({ status }: { status: RunStatus | StepStatus }) {
  return <span className={`pill pill-${status}`}>{status}</span>;
}
