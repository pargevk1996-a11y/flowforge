"""Sub-workflows: the contract between a parent run and its children.

A child is a *real run* — its own event log, its own id, its own row in the
control plane — not a nested function call. That is what makes fan-out durable:
a thousand-item fan-out is a thousand independent runs the queue can spread over
workers, each retrying, suspending and compensating on its own, and a parent that
crashes finds them all exactly where it left them.

The parent does not poll. It records what it started, suspends, and is woken when
a child reaches a terminal state — the same mechanism a timer or a signal uses.
Child run ids are *derived* from the parent's (``{parent}.{command_seq}``) rather
than random, so replaying the parent recognises the child it already started
instead of starting a second one.

The engine implements :class:`ChildLauncher`; the workflow context depends only on
this protocol, which is what keeps the two out of an import cycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # imported for typing only — see the module docstring
    from flowforge.workflow.definition import WorkflowDef


@dataclass(frozen=True)
class ParentRef:
    """Where a child should report back to. Written into its first event."""

    run_id: str
    command_seq: int

    def as_payload(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "command_seq": self.command_seq}

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> ParentRef | None:
        if not payload:
            return None
        return cls(run_id=str(payload["run_id"]), command_seq=int(payload["command_seq"]))


@dataclass(frozen=True)
class ChildOutcome:
    run_id: str
    completed: bool
    result: Any = None
    error: str | None = None


class ChildLauncher(Protocol):
    def resolve(self, workflow: str | WorkflowDef[Any, Any]) -> WorkflowDef[Any, Any]:
        """Look up a workflow definition by name (or pass one through)."""
        ...

    def child_run_id(self, parent_run_id: str, command_seq: int) -> str:
        """The child's id, derived so replay never starts a duplicate."""
        ...

    async def start_child(
        self,
        parent: ParentRef,
        workflow: str | WorkflowDef[Any, Any],
        workflow_input: Any,
        *,
        tenant: str,
    ) -> str:
        """Seed and enqueue the child run; return its id. Idempotent: starting a
        child that already exists is a no-op that returns the same id."""
        ...

    async def child_outcome(self, run_id: str) -> ChildOutcome | None:
        """The child's terminal outcome, or ``None`` while it is still running.

        The parent uses this to reconcile: a completion notice can be lost to a
        crash, and a parent that only ever waited would wait forever."""
        ...
