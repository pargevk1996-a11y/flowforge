"""Retry policy for activities.

Short backoffs are handled in-process; genuinely long backoffs should be turned
into durable timers so the worker is not held — that is a planned refinement, and
the policy shape already carries the ceiling that decides it.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RetryPolicy(BaseModel):
    max_attempts: int = Field(default=3, ge=1)
    initial_backoff: float = Field(default=0.1, ge=0)
    multiplier: float = Field(default=2.0, ge=1)
    max_backoff: float = Field(default=30.0, ge=0)

    def backoff(self, attempt: int) -> float:
        """Seconds to wait before ``attempt`` (1-based). Attempt 1 is immediate."""
        if attempt <= 1:
            return 0.0
        delay = self.initial_backoff * (self.multiplier ** (attempt - 2))
        return min(delay, self.max_backoff)
