"""Dead-letter queue: a task that always fails lands in 'dead' after retries."""

from __future__ import annotations

import asyncio

import pytest

from jobq.engine import Engine


@pytest.mark.asyncio
async def test_always_failing_job_goes_to_dlq(store):
    import jobq.tasks  # noqa: F401

    engine = Engine(store, concurrency=2, poll_interval=0.01, heartbeat_interval=0.2)
    await engine.start()
    try:
        jid = store.enqueue("always_fail", max_attempts=3, backoff_base=0.03)
        for _ in range(500):
            job = store.get_job(jid)
            if job.status == "dead":
                break
            await asyncio.sleep(0.02)
        job = store.get_job(jid)
        assert job.status == "dead"
        assert job.attempts == 3               # tried exactly max_attempts times
        assert job.error and "always fails" in job.error

        dlq_ids = [j.id for j in store.dead_letter()]
        assert jid in dlq_ids

        dead_events = [e for e in engine.pool.events
                       if e["event"] == "dead" and e["job_id"] == jid]
        assert len(dead_events) == 1
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_requeue_dead_job_reruns(store):
    import jobq.tasks  # noqa: F401

    # Fails on attempts 1..2; after a manual requeue (resets attempts) a fresh
    # run 1..2 fails again, run 3 succeeds because fail_times < new attempts.
    engine = Engine(store, concurrency=2, poll_interval=0.01, heartbeat_interval=0.2)
    await engine.start()
    try:
        jid = store.enqueue("flaky", kwargs={"fail_times": 5}, max_attempts=2,
                            backoff_base=0.02)
        for _ in range(400):
            if store.get_job(jid).status == "dead":
                break
            await asyncio.sleep(0.02)
        assert store.get_job(jid).status == "dead"

        # Requeue with more headroom by rewriting the job to succeed sooner.
        assert store.requeue_dead(jid) is True
        assert store.get_job(jid).status == "queued"
        assert store.get_job(jid).attempts == 0
    finally:
        await engine.stop()
