"""Store round-trip, priority/run_at ordering, and basic lifecycle."""

from __future__ import annotations

from jobq.models import JobStatus


def test_enqueue_get_roundtrip(store):
    jid = store.enqueue("add", args=[1, 2, 3], kwargs={"k": "v"}, priority=5)
    job = store.get_job(jid)
    assert job is not None
    assert job.task == "add"
    assert job.args == [1, 2, 3]
    assert job.kwargs == {"k": "v"}
    assert job.priority == 5
    assert job.status == JobStatus.QUEUED.value
    assert job.attempts == 0


def test_claim_marks_running_and_increments_attempts(store):
    jid = store.enqueue("add", args=[1])
    job = store.claim("w1")
    assert job is not None and job.id == jid
    assert job.status == JobStatus.RUNNING.value
    assert job.attempts == 1
    assert job.worker_id == "w1"
    # nothing else to claim
    assert store.claim("w2") is None


def test_priority_then_runat_ordering(store):
    now = 1000.0
    # Same priority: earlier run_at first.
    low_early = store.enqueue("t", priority=0, run_at=now + 1, now=now)
    low_late = store.enqueue("t", priority=0, run_at=now + 5, now=now)
    # Higher priority should jump ahead of both.
    high = store.enqueue("t", priority=10, run_at=now + 5, now=now)

    order = []
    for _ in range(3):
        j = store.claim("w", now=now + 10)
        order.append(j.id)
    assert order[0] == high            # priority wins
    assert order[1] == low_early       # then earliest run_at
    assert order[2] == low_late


def test_run_at_in_future_not_claimable(store):
    now = 2000.0
    jid = store.enqueue("t", run_at=now + 100, now=now)
    assert store.claim("w", now=now) is None
    assert store.claim("w", now=now + 100).id == jid


def test_complete_stores_result_and_counts(store):
    jid = store.enqueue("add", args=[2, 2])
    store.claim("w")
    store.complete(jid, {"value": 4})
    job = store.get_job(jid)
    assert job.status == JobStatus.SUCCEEDED.value
    assert job.result == {"value": 4}
    counts = store.counts_by_status()
    assert counts["succeeded"] == 1
    assert counts["queued"] == 0


def test_dead_letter_and_manual_requeue(store):
    now = 5000.0
    jid = store.enqueue("x", max_attempts=1, now=now)
    store.claim("w", now=now)                 # attempts -> 1
    out = store.fail(jid, "boom", now=now)    # 1 >= 1 -> dead
    assert out.outcome == "dead"
    assert store.get_job(jid).status == JobStatus.DEAD.value
    assert [j.id for j in store.dead_letter()] == [jid]

    assert store.requeue_dead(jid, now=now) is True
    j = store.get_job(jid)
    assert j.status == JobStatus.QUEUED.value
    assert j.attempts == 0
