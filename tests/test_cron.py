"""Minimal cron evaluator."""

from __future__ import annotations

from datetime import datetime

import pytest

from jobq.cron import cron_matches, next_run, parse_cron


def test_parse_fields():
    minute, hour, dom, month, dow = parse_cron("*/15 0 1 * 1-5")
    assert minute == {0, 15, 30, 45}
    assert hour == {0}
    assert dom == {1}
    assert month == set(range(1, 13))
    assert dow == {1, 2, 3, 4, 5}


def test_matches_every_five_minutes():
    expr = "*/5 * * * *"
    assert cron_matches(expr, datetime(2026, 7, 16, 10, 0))
    assert cron_matches(expr, datetime(2026, 7, 16, 10, 5))
    assert not cron_matches(expr, datetime(2026, 7, 16, 10, 3))


def test_next_run_advances():
    after = datetime(2026, 7, 16, 10, 2)
    nxt = next_run("*/5 * * * *", after)
    assert nxt == datetime(2026, 7, 16, 10, 5)


def test_invalid_expression():
    with pytest.raises(ValueError):
        parse_cron("* * *")  # not 5 fields
