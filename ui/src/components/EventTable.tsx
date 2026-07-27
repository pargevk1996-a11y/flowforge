import { Fragment, useState } from "react";

import { formatTime, truncate } from "../format";
import type { RunEvent } from "../types";
import { Json } from "./Json";

/**
 * The raw log. The step view is the story; this is the evidence, and when the
 * projection and your expectations disagree, this is what settles it.
 */
export function EventTable({ events, highlight }: { events: RunEvent[]; highlight: number[] }) {
  const [open, setOpen] = useState<number | null>(null);
  const marked = new Set(highlight);

  return (
    <table className="grid events">
      <thead>
        <tr>
          <th className="num">seq</th>
          <th>type</th>
          <th className="num">cmd</th>
          <th>name</th>
          <th>at</th>
          <th>payload</th>
        </tr>
      </thead>
      <tbody>
        {events.map((event) => (
          <Fragment key={event.seq}>
            <tr
              className={[marked.has(event.seq) ? "row-marked" : "", open === event.seq ? "row-open" : ""]
                .filter(Boolean)
                .join(" ")}
              onClick={() => setOpen(open === event.seq ? null : event.seq)}
              tabIndex={0}
            >
              <td className="num muted">{event.seq}</td>
              <td className="mono">{event.type}</td>
              <td className="num muted">{event.command_seq ?? "—"}</td>
              <td>{event.name ?? <span className="muted">—</span>}</td>
              <td className="mono muted">{formatTime(event.created_at)}</td>
              <td className="muted clip">{truncate(event.payload, 70)}</td>
            </tr>
            {open === event.seq && (
              <tr className="row-detail">
                <td colSpan={6}>
                  <Json value={event.payload} label={`event ${event.seq} payload`} />
                </td>
              </tr>
            )}
          </Fragment>
        ))}
      </tbody>
    </table>
  );
}
