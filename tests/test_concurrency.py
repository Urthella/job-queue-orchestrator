"""HEADLINE TEST: race-safe claiming under real concurrent connections.

Spins up N threads (each with its own SQLite connection via the store's
thread-local pool) hammering a queue of M jobs. Every job must be claimed
EXACTLY once - no double-claims, none lost. This is the core guarantee that
makes the SQLite store an honest stand-in for Postgres ``FOR UPDATE SKIP
LOCKED``.

Threads (not cooperative asyncio on one connection) are used deliberately so the
``BEGIN IMMEDIATE`` claim genuinely contends across separate connections.
"""

from __future__ import annotations

import threading
from collections import Counter

import pytest

from jobq.store.sqlite_store import SQLiteJobStore


def _drain_with_threads(store: SQLiteJobStore, n_workers: int) -> list[int]:
    claimed: list[int] = []
    lock = threading.Lock()
    start = threading.Barrier(n_workers)

    def worker(wid: str) -> None:
        local: list[int] = []
        start.wait()  # release all threads simultaneously -> maximise contention
        while True:
            job = store.claim(wid)
            if job is None:
                break
            local.append(job.id)
            # Complete immediately; no requeue, so a drained queue stays drained.
            store.complete(job.id, {"by": wid})
        with lock:
            claimed.extend(local)

    threads = [
        threading.Thread(target=worker, args=(f"w{i}",), name=f"w{i}")
        for i in range(n_workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return claimed


@pytest.mark.parametrize("n_workers,m_jobs", [(8, 500), (16, 300)])
def test_no_double_claim(db_path, n_workers, m_jobs):
    store = SQLiteJobStore(db_path, busy_timeout_ms=10000)
    store.init_schema()
    try:
        expected = set()
        for i in range(m_jobs):
            expected.add(store.enqueue("noop", args=[i]))

        claimed = _drain_with_threads(store, n_workers)

        # 1. No id claimed more than once.
        counts = Counter(claimed)
        duplicates = {jid: c for jid, c in counts.items() if c > 1}
        assert not duplicates, f"DOUBLE-CLAIMED jobs: {duplicates}"

        # 2. Every job claimed exactly once (set equality + exact count).
        assert len(claimed) == m_jobs
        assert set(claimed) == expected

        # 3. DB agrees: all succeeded, none left queued/running, attempts==1.
        c = store.counts_by_status()
        assert c["succeeded"] == m_jobs
        assert c["queued"] == 0 and c["running"] == 0
        for j in store.list_jobs(limit=m_jobs + 10):
            assert j.attempts == 1, f"job {j.id} attempts={j.attempts} (claimed twice?)"
    finally:
        store.close()


def test_repeated_runs_are_stable(db_path):
    """Run the whole scenario several times; races are nondeterministic so a
    single pass can get lucky. A double-claim here means the store is broken."""
    for run in range(5):
        p = f"{db_path}.{run}"
        store = SQLiteJobStore(p, busy_timeout_ms=10000)
        store.init_schema()
        try:
            expected = {store.enqueue("noop", args=[i]) for i in range(200)}
            claimed = _drain_with_threads(store, 12)
            counts = Counter(claimed)
            assert all(c == 1 for c in counts.values()), (
                f"run {run}: double-claim detected: "
                f"{ {k: v for k, v in counts.items() if v > 1} }"
            )
            assert set(claimed) == expected
        finally:
            store.close()
