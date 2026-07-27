interface Props {
  total: number;
  /** `null` means live: the whole log, and following along as it grows. */
  at: number | null;
  onChange: (at: number | null) => void;
}

/**
 * The replay control. Dragging it does not simulate anything — the server
 * projects a *prefix* of the log, so every position is genuinely the state the
 * engine would have replayed from at that point.
 */
export function Scrubber({ total, at, onChange }: Props) {
  const last = Math.max(total - 1, 0);
  const position = at ?? last;
  const live = at === null;

  return (
    <div className="scrubber">
      <button
        className="step-btn"
        disabled={position <= 0}
        onClick={() => onChange(Math.max(0, position - 1))}
        title="previous event"
        type="button"
      >
        ◀
      </button>
      <input
        aria-label="replay position"
        max={last}
        min={0}
        onChange={(event) => onChange(Number(event.target.value))}
        type="range"
        value={position}
      />
      <button
        className="step-btn"
        disabled={position >= last}
        onClick={() => onChange(Math.min(last, position + 1))}
        title="next event"
        type="button"
      >
        ▶
      </button>
      <span className="scrubber-pos mono">
        event {position} / {last}
      </span>
      <button
        className={live ? "live live-on" : "live"}
        onClick={() => onChange(live ? last : null)}
        type="button"
      >
        {live ? "● live" : "go live"}
      </button>
    </div>
  );
}
