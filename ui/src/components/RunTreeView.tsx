import { formatCost } from "../format";
import type { RunTree } from "../types";
import { StatusPill } from "./StatusPill";

/** A fan-out is many logs at once; this is the shape of the whole thing. */
export function RunTreeView({ node, onOpen, depth = 0 }: {
  node: RunTree;
  onOpen: (runId: string) => void;
  depth?: number;
}) {
  return (
    <div className="tree-node" style={{ marginLeft: depth * 18 }}>
      <div className="tree-row">
        {depth > 0 && <span className="tree-branch">└</span>}
        {node.command_seq !== undefined && <span className="muted mono">#{node.command_seq}</span>}
        <button className="link mono" onClick={() => onOpen(node.run_id)} type="button">
          {node.workflow}
        </button>
        <StatusPill status={node.status} />
        {node.usd_cost > 0 && <span className="muted">{formatCost(node.usd_cost)}</span>}
        <span className="muted mono clip">{node.run_id.slice(0, 20)}</span>
      </div>
      {node.children.map((child) => (
        <RunTreeView depth={depth + 1} key={child.run_id} node={child} onOpen={onOpen} />
      ))}
    </div>
  );
}
