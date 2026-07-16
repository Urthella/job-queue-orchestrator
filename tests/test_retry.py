"""Retry / exponential backoff behaviour."""

from __future__ import annotations

import asyncio

import pytest

from jobq.engine import Engine
from jobq.retry import compute_backoff


def test_compute_backoff_geometric_no_jitter():
    # jitter=0 -> deterministic geometric growth, capped at maximum.
    assert compute_backoff(1, base=1.0, factor=2.0, maximum=100, jitter=0.0) == 1.0
    assert compute_backoff(2, base=1.0, factor=2.0, maximum=100, jitter=0.0) == 2.0
    assert compute_backoff(3, base=1.0, factor=2.0, maximum=100, jitter=0.0) == 4.0
    assert compute_backoff(10, base=1.0, factor=2.0, maximum=5.0, jitter=0.0) == 5.0


def test_compute_backoff_jitter_bounds():
    # With rng()->r, delay = base_delay + r*jitter*base_delay, in [base, base*(1+jitter)].
    lo = compute_backoff(1, base=2.0, factor=2.0, jitter=0.5, rng=lambda: 0.0)
    hi = compute_backoff(1, base=2.0, factor=2.0, jitter=0.5, rng=lambda: 1.0)
    # delay = base_delay + r*jitter*base_delay; base_delay=2.0, jitter=0.5
    assert lo == 2.0                       # r=0 -> no added jitter
    assert hi == 2.0 + 1.0 * 0.5 * 2.0     # r=1 -> +1.0 -> 3.0
    assert hi == 3.0


def test_fail_schedules_backoff_then_deadletters(store):
    now = 1000.0
    jid = store.enqueue(
        "x", max_attempts=3, backoff_base=1.0, backoff_factor=2.0, jitter=0.0, now=now
    )

    # attempt 1
    j = store.claim("w", now=now)
    assert j.attempts == 1
    out = store.fail(jid, "boom", now=now)
    assert out.outcome == "retry"
    assert out.next_run_at == now + 1.0            # base * factor**0
    assert store.claim("w", now=now + 0.5) is None  # not due yet

    # attempt 2
    j2 = store.claim("w", now=now + 1.0)
    assert j2.attempts == 2
    out2 = store.fail(jid, "boom", now=now + 1.0)
    assert out2.outcome == "retry"
    assert out2.next_run_at == (now + 1.0) + 2.0    # base * factor**1

    # attempt 3 -> exhausts max_attempts -> dead
    j3 = store.claim("w", now=now + 3.0)
    assert j3.attempts == 3
    out3 = store.fail(jid, "boom", now=now + 3.0)
    assert out3.outcome == "dead"
    assert store.get_job(jid).status == "dead"


@pytest.mark.asyncio
async def test_flaky_job_succeeds_after_retries(store):
    import jobq.tasks  # noqa: F401 - register handlers

    engine = Engine(store, concurrency=2, poll_interval=0.01, heartbeat_interval=0.2)
    await engine.start()
    try:
        jid = store.enqueue(
            "flaky", kwargs={"fail_times": 2}, max_attempts=5, backoff_base=0.05
        )
        # Wait for it to settle.
        for _ in range(400):
            job = store.get_job(jid)
            if job.status in ("succeeded", "dead"):
                break
            await asyncio.sleep(0.02)
        job = store.get_job(jid)
        assert job.status == "succeeded", f"ended {job.status}: {job.error}"
        assert job.attempts == 3, f"expected 3 attempts, got {job.attempts}"
        assert job.result == {"ok": True, "succeeded_on_attempt": 3}

        retry_events = [e for e in engine.pool.events
                        if e["event"] == "retry" and e["job_id"] == jid]
        assert len(retry_events) == 2  # failed twice, retried twice
    finally:
        await engine.stop()
