"""Exponential backoff with jitter.

The backoff delay for a given attempt is::

    base_delay = min(backoff_base * backoff_factor ** (attempt - 1), backoff_max)
    delay      = base_delay + U(0, jitter * base_delay)

``jitter`` is a fraction of the base delay (0.0 disables jitter). An injectable
``rng`` makes the jitter deterministic in tests.
"""

from __future__ import annotations

import random
from typing import Callable


def compute_backoff(
    attempt: int,
    *,
    base: float = 0.5,
    factor: float = 2.0,
    maximum: float = 30.0,
    jitter: float = 0.1,
    rng: Callable[[], float] | None = None,
) -> float:
    """Return the delay (seconds) before retry ``attempt`` (1-indexed).

    ``attempt`` is the attempt number that just failed. For the first failure
    (attempt=1) the base delay is ``base``; it grows geometrically thereafter,
    capped at ``maximum``.
    """
    if attempt < 1:
        attempt = 1
    base_delay = min(base * (factor ** (attempt - 1)), maximum)
    if jitter <= 0:
        return base_delay
    r = rng() if rng is not None else random.random()
    return base_delay + r * jitter * base_delay
