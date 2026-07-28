"""One rule, stated once: a loop must outlive its iterations.

The worker, the timer wheel and the cron scheduler are all the same shape — do a
unit of work, wait, repeat, forever. Written naively that shape has a fatal
property: the first exception ends the loop, and because these run as detached
tasks nobody is awaiting, it ends *silently*. One malformed cron mapper then
stops every schedule in the process; one unreachable database stops every run.

So iterations are allowed to fail and loops are not. A failed iteration is logged
and followed by a backoff, which is what keeps a persistent fault (a store that is
down) from becoming a hot loop while it stays broken.

``CancelledError`` is a ``BaseException`` and deliberately not caught here:
shutdown must still be able to stop these.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

DEFAULT_ERROR_BACKOFF = 1.0
"""Seconds to wait after a failed iteration, so a persistent fault does not spin."""


async def supervise(
    step: Callable[[], Awaitable[None]],
    *,
    label: str,
    stop: asyncio.Event | None = None,
    error_backoff: float = DEFAULT_ERROR_BACKOFF,
) -> None:
    """Run ``step`` until ``stop`` is set, surviving anything it raises.

    ``step`` owns its own pacing — how long the loop idles between units of work
    is the caller's business; only failure handling is shared."""
    while stop is None or not stop.is_set():
        try:
            await step()
        except Exception:
            logger.exception("%s: iteration failed; the loop continues", label)
            await asyncio.sleep(error_backoff)
