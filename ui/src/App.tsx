import { useCallback, useEffect, useState } from "react";

import { RunDetail } from "./components/RunDetail";
import { RunList } from "./components/RunList";

/**
 * Two screens: the runs you have, and the one you are debugging. The selected
 * run lives in the URL hash, so a link to a broken run is a link anyone can open.
 */
function runIdFromHash(): string | null {
  const match = /^#\/runs\/(.+)$/.exec(window.location.hash);
  return match?.[1] ? decodeURIComponent(match[1]) : null;
}

export function App() {
  const [runId, setRunId] = useState<string | null>(runIdFromHash);

  useEffect(() => {
    const sync = () => setRunId(runIdFromHash());
    window.addEventListener("hashchange", sync);
    return () => window.removeEventListener("hashchange", sync);
  }, []);

  const open = useCallback((id: string | null) => {
    window.location.hash = id === null ? "#/" : `#/runs/${encodeURIComponent(id)}`;
  }, []);

  return (
    <div className="app">
      <header className="app-bar">
        <button className="brand" onClick={() => open(null)} type="button">
          flowforge
        </button>
        <span className="app-bar-sub">replay debugger</span>
      </header>
      <main>
        {runId === null ? (
          <RunList onOpen={open} />
        ) : (
          <RunDetail runId={runId} onOpen={open} onClose={() => open(null)} />
        )}
      </main>
    </div>
  );
}
