"""Heartbeat + reaper: crashed/stuck workers' jobs are requeued and recovered."""

from __future__ import annotations

import asyncio

import pytest

from jobq.engine import Engine


def _force_stuck_running(store, job_id, *, worker="ghost", heartbeat_ts=0.0, attempts=1):
    store._conn().execute(
        "UPDATE jobs SET status='running', attempts=?, worker_id=?, "
        "locked_at=?, last_heartbeat=? WHERE id=?",
        (attempts, worker, heartbeat_ts, heartbeat_ts, job_id),
    )


def test_reaper_requeues_stale_and_spares_fresh(store):
    now = 2000.0
    stale = store.enqueue("recover_me", max_attempts=5, now=now)
    _force_stuck_running(store, stale, heartbeat_ts=now - 100, attempts=1)

    # A genuinely-running job with a fresh heartbeat must NOT be reaped.
    fresh = store.enqueue("recover_me", max_attempts=5, now=now)
    store.claim("live-worker", now=now)  # sets last_heartbeat=now on `fresh`

    requeued = store.reap(heartbeat_timeout=5.0, now=now)
    assert requeued == [stale]
    assert store.get_job(stale).status == "queued"
    assert store.get_job(fresh).status == "running"


def test_reaper_deadletters_when_attempts_exhausted(store):
    now = 3000.0
    jid = store.enqueue("x", max_attempts=2, now=now)
    _force_stuck_running(store, jid, heartbeat_ts=now - 100, attempts=2)  # at the cap
    requeued = store.reap(heartbeat_timeout=5.0, now=now)
    assert requeued == []
    assert store.get_job(jid).status == "dead"


@pytest.mark.asyncio
async def test_engine_recovers_crashed_job(store):
    import jobq.tasks  # noqa: F401

    engine = Engine(
        store,
        concurrency=2,
        poll_interval=0.02,
        heartbeat_interval=0.3,
        reap_interval=0.2,
        heartbeat_timeout=1.0,
    )
    await engine.start()
    try:
        # A job that a now-crashed worker claimed and never finished.
        jid = store.enqueue("recover_me", max_attempts=5)
        _force_stuck_running(store, jid, heartbeat_ts=0.0, attempts=1)

        for _ in range(600):
            job = store.get_job(jid)
            if job.status == "succeeded":
                break
            await asyncio.sleep(0.02)

        job = store.get_job(jid)
        assert job.status == "succeeded", f"ended {job.status}"
        assert jid in engine.scheduler.reaped_ids
        # attempt bumped: claim-at-crash (1) + reaper requeue -> re-claim (2)
        assert job.attempts >= 2
    finally:
        await engine.stop()


# --- regression: fenced terminal transitions (reaped straggler must not clobber) ---

def test_stale_complete_does_not_clobber_reclaimed_job(store):
    """A slow-but-alive worker A whose job is reaped and re-claimed by B must not
    corrupt the job's state when A finally reports back."""
    now = 5000.0
    jid = store.enqueue("x", max_attempts=3, now=now)

    a = store.claim("worker-A", now=now)          # A: attempts=1, running, owner=A
    assert a.id == jid and a.worker_id == "worker-A"

    # A's heartbeat goes stale; reaper requeues; B re-claims.
    _force_stuck_running(store, jid, worker="worker-A", heartbeat_ts=now - 100, attempts=1)
    assert store.reap(heartbeat_timeout=5.0, now=now) == [jid]
    b = store.claim("worker-B", now=now)          # B: attempts=2, running, owner=B
    assert b.id == jid and b.worker_id == "worker-B"

    # Stale A.complete() is fenced out (wrong owner) -> dropped, state untouched.
    assert store.complete(jid, {"by": "A"}, now=now, worker_id="worker-A") is False
    job = store.get_job(jid)
    assert job.status == "running" and job.worker_id == "worker-B"

    # Stale A.fail() is fenced out -> "lost", no premature dead-letter / requeue.
    out = store.fail(jid, "late boom", now=now, worker_id="worker-A")
    assert out.outcome == "lost"
    assert store.get_job(jid).status == "running"

    # The rightful owner B still commits normally.
    assert store.complete(jid, {"by": "B"}, now=now, worker_id="worker-B") is True
    job = store.get_job(jid)
    assert job.status == "succeeded" and job.result == {"by": "B"}


def test_fenced_complete_and_fail_apply_for_rightful_owner(store):
    now = 6000.0
    jid = store.enqueue("x", max_attempts=3, now=now)
    j = store.claim("worker-A", now=now)
    # Correct owner completes: applies.
    assert store.complete(jid, {"ok": 1}, now=now, worker_id="worker-A") is True
    assert store.get_job(jid).status == "succeeded"

    jid2 = store.enqueue("y", max_attempts=3, now=now)
    store.claim("worker-A", now=now)
    out = store.fail(jid2, "boom", now=now, worker_id="worker-A")
    assert out.outcome == "retry"
    assert store.get_job(jid2).status == "queued"
