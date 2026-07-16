"""A minimal 5-field cron evaluator (a scheduling "plus" feature).

Fields: ``minute hour day-of-month month day-of-week`` (0=Sunday..6=Saturday).
Each field supports ``*``, ``*/n``, ``a-b`` ranges, ``a,b,c`` lists, and plain
integers. This is intentionally small - just enough for periodic scheduling.
"""

from __future__ import annotations

from datetime import datetime, timedelta

_FIELD_RANGES = [
    (0, 59),   # minute
    (0, 23),   # hour
    (1, 31),   # day of month
    (1, 12),   # month
    (0, 6),    # day of week (0=Sunday)
]


def _parse_field(spec: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            step = int(step_s)
        if part in ("*", ""):
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(part)
        values.update(range(start, end + 1, step))
    return {v for v in values if lo <= v <= hi}


def parse_cron(expr: str) -> list[set[int]]:
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"cron expression must have 5 fields, got {len(fields)}: {expr!r}")
    return [_parse_field(f, lo, hi) for f, (lo, hi) in zip(fields, _FIELD_RANGES)]


def cron_matches(expr: str, dt: datetime) -> bool:
    """Return True if ``dt`` (minute resolution) satisfies the cron expression."""
    minute, hour, dom, month, dow = parse_cron(expr)
    # Python: Monday=0..Sunday=6; cron: Sunday=0..Saturday=6.
    cron_dow = (dt.weekday() + 1) % 7
    return (
        dt.minute in minute
        and dt.hour in hour
        and dt.day in dom
        and dt.month in month
        and cron_dow in dow
    )


def next_run(expr: str, after: datetime) -> datetime:
    """Return the next datetime strictly after ``after`` that matches ``expr``.

    Scans minute-by-minute (bounded to ~1 year) - fine for periodic scheduling.
    """
    candidate = (after + timedelta(minutes=1)).replace(second=0, microsecond=0)
    for _ in range(367 * 24 * 60):
        if cron_matches(expr, candidate):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError(f"no cron match found within a year for {expr!r}")
