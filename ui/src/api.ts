/** The typed client for the control plane. One place that knows about URLs. */

import type { RunPage, RunTree, Timeline } from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function get<T>(path: string, params: Record<string, string | number | undefined> = {}): Promise<T> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") query.set(key, String(value));
  }
  const suffix = query.size > 0 ? `?${query}` : "";
  const response = await fetch(`${path}${suffix}`, { headers: { accept: "application/json" } });
  if (!response.ok) {
    const detail = await response.text().catch(() => response.statusText);
    throw new ApiError(response.status, detail || response.statusText);
  }
  return (await response.json()) as T;
}

export interface RunFilters {
  status?: string;
  tenant?: string;
  workflow?: string;
  limit?: number;
  offset?: number;
}

export const api = {
  listRuns: (filters: RunFilters = {}) => get<RunPage>("/runs", { ...filters }),

  /** `at` is the replay position: the timeline as of that event, and no later. */
  timeline: (runId: string, at?: number) =>
    get<Timeline>(`/runs/${encodeURIComponent(runId)}/timeline`, { at }),

  tree: (runId: string) => get<RunTree>(`/runs/${encodeURIComponent(runId)}/tree`),

  async signal(runId: string, name: string, data: unknown): Promise<void> {
    const response = await fetch(`/runs/${encodeURIComponent(runId)}/signals`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name, data }),
    });
    if (!response.ok) {
      throw new ApiError(response.status, await response.text().catch(() => response.statusText));
    }
  },
};
