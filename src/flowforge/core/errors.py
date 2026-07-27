"""Error taxonomy for the durable execution engine.

The distinction between retryable and non-retryable failures is what makes
``Retry = typed`` rather than ``while True``. ``Suspended`` is control flow, not
an error: it unwinds the workflow coroutine so the worker process is freed while
the run waits (on a timer or a human), and is deliberately a ``BaseException`` so
that ordinary ``except Exception`` handlers in workflow code do not swallow it.
"""

from __future__ import annotations


class FlowforgeError(Exception):
    """Base class for all flowforge errors."""


class RetryableError(FlowforgeError):
    """A transient failure; the activity should be retried per its policy."""


class NonRetryableError(FlowforgeError):
    """A permanent failure; do not retry, go straight to compensation."""


class ActivityFailedError(FlowforgeError):
    """An activity exhausted its retries (or failed non-retryably)."""

    def __init__(self, activity: str, message: str) -> None:
        self.activity = activity
        self.message = message
        super().__init__(f"activity {activity!r} failed: {message}")


class BudgetExceededError(NonRetryableError):
    """The tenant's spend limit is exhausted; no further billable call is allowed.

    Deliberately non-retryable: waiting and trying again inside the same run would
    not make the money reappear. The run therefore fails through the normal saga
    path, so whatever it already did is compensated rather than left half-done.
    """


class RateLimitedError(RetryableError):
    """A provider's rate limit could not be satisfied within the allowed wait.

    Retryable: the limiter refills over time, so the activity's retry policy is
    exactly the right place to back off and try again.
    """


class ConcurrencyError(FlowforgeError):
    """Optimistic-concurrency violation while appending to the event log.

    Signals that another writer advanced the run; the caller must reload and
    retry. In production a distributed lock makes this rare, but the event store
    enforces single-writer semantics regardless.
    """


class WorkflowNotFoundError(FlowforgeError):
    """No workflow is registered under the given name."""


class TriggerNotFoundError(FlowforgeError):
    """No trigger is registered under the given name."""


class Suspended(BaseException):
    """Raised to suspend a run until an external event (timer/signal) arrives.

    Not an error: the already-appended events describe what the run is waiting
    for, and a later timer firing or signal delivery re-enqueues the run.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)
