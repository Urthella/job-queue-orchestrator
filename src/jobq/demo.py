"""End-to-end demo: ``python -m jobq.demo``.

Starts a worker pool + scheduler on a throwaway SQLite database, then:
  * enqueues a batch of ordinary jobs (with priority + a delayed job),
  * enqueues flaky jobs that fail-then-retry (exponential backoff),
  * enqueues always-failing jobs that exhaust retries -> dead-letter queue,
  * submits the diamond DAG (A -> B, A -> C, (B, C) -> D),
  * injects a "crashed worker" (a job stuck in ``running`` with a stale
    heartbeat) so the reaper requeues it and a live worker finishes it,
then prints a summary and exits cleanly.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
from pathlib import Path

from .dag import advance_workflow, load_dag_file
from .engine import Engine
from .models import WorkflowStatus
from .store.sqlite_store import SQLiteJobStore


def _dag_path() -> str:
    return str(Path(__file__).parent / "dags" / "diamond.yaml")


def _simulate_crashed_job(store: SQLiteJobStore, now: float) -> int:
    """Enqueue a job and force it into a stale ``running`` state, as if a worker
    claimed it and then crashed without heartbeating. The reaper should requeue
    it. (SQLite-specific low-level setup, fine for a demo.)"""
    job_id = store.enqueue("recover_me", max_attempts=5)
    stale = now - 3600.0  # one hour ago -> definitely past any timeout
    store._conn().execute(
        "UPDATE jobs SET status='running', attempts=1, worker_id='ghost-crashed', "
        "locked_at=?, last_heartbeat=? WHERE id=?",
        (stale, stale, job_id),
    )
    return job_id


async def _wait_settled(store: SQLiteJobStore, run_id: int, *, timeout: float = 30.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = time.time() + timeout
    while time.time() < deadline:
        counts = await loop.run_in_executor(None, store.counts_by_status)
        run = await loop.run_in_executor(None, store.get_workflow_run, run_id)
        wf_done = run is not None and run["status"] != WorkflowStatus.RUNNING.value
        if counts.get("queued", 0) == 0 and counts.get("running", 0) == 0 and wf_done:
            return
        await asyncio.sleep(0.05)


def _print_summary(store: SQLiteJobStore, engine: Engine, run_id: int, crashed_id: int) -> None:
    from . import tasks as extasks

    print("\n" + "=" * 66)
    print("  DEMO SUMMARY")
    print("=" * 66)

    counts = store.counts_by_status()
    print("\nJobs by final state:")
    for status, n in counts.items():
        print(f"    {status:<11} {n}")

    retries = [e for e in engine.pool.events if e["event"] == "retry"]
    print(f"\nRetries observed (fail -> backoff -> requeue): {len(retries)}")
    seen = {}
    for e in retries:
        seen.setdefault(e["task"], 0)
        seen[e["task"]] += 1
    for task, n in seen.items():
        print(f"    {task}: {n} retry event(s)")

    dlq = store.dead_letter()
    print(f"\nDead-letter queue ({len(dlq)} job(s)):")
    for j in dlq:
        err = (j.error or "").splitlines()[0] if j.error else ""
        print(f"    #{j.id} {j.task}  attempts={j.attempts}/{j.max_attempts}  {err[:44]}")

    print(f"\nReaper requeued {len(engine.scheduler.reaped_ids)} stuck job(s): "
          f"{engine.scheduler.reaped_ids}")
    crashed = store.get_job(crashed_id)
    if crashed:
        print(f"    crashed job #{crashed_id} final status: {crashed.status} "
              f"(attempts={crashed.attempts})")

    run = store.get_workflow_run(run_id)
    steps = store.list_workflow_steps(run_id)
    print(f"\nDiamond DAG run #{run_id}: status={run['status']}")
    for s in steps:
        print(f"    step {s['name']:<2} [{s['status']:<9}] task={s['task']:<12} "
              f"result={s['result']}")
    # Execution order proof (start events, in observed order).
    starts = [name for (evt, name, _t) in extasks.EXECUTION_LOG if evt == "start"]
    print(f"    observed start order: {starts}")
    d_step = next((s for s in steps if s["name"] == "D"), None)
    if d_step:
        print(f"    D result = {d_step['result']}  (expected 45 = (10+5) + (10*3))")

    print("\n" + "=" * 66)


async def main(database_url: str | None = None) -> int:
    from . import tasks as extasks

    extasks.reset_logs()
    tmpdir = tempfile.mkdtemp(prefix="jobq_demo_")
    db_path = os.path.join(tmpdir, "demo.db")
    store = SQLiteJobStore(db_path)
    store.init_schema()
    try:
        engine = Engine(
            store,
            concurrency=4,
            poll_interval=0.02,
            heartbeat_interval=0.5,
            reap_interval=0.4,
            heartbeat_timeout=2.0,
        )
        await engine.start()
        print(f"[demo] engine started (concurrency=4, db={db_path})")

        now = time.time()
        # 1) ordinary jobs, one high-priority, one delayed
        for i in range(5):
            store.enqueue("add", args=[i, i * 2])
        store.enqueue("echo", kwargs={"message": "high priority"}, priority=10)
        store.enqueue("echo", kwargs={"message": "delayed 0.3s"}, run_at=now + 0.3)

        # 2) flaky jobs: fail twice then succeed (backoff retry)
        for _ in range(3):
            store.enqueue("flaky", kwargs={"fail_times": 2}, max_attempts=5, backoff_base=0.1)

        # 3) always-fail jobs -> dead-letter after retries exhausted
        for _ in range(2):
            store.enqueue("always_fail", max_attempts=2, backoff_base=0.05)

        # 4) crashed-worker scenario -> reaper requeue
        crashed_id = _simulate_crashed_job(store, now)

        # 5) diamond DAG
        dag = load_dag_file(_dag_path())
        run_id = engine_submit(store, dag)
        print(f"[demo] submitted diamond DAG as workflow run #{run_id}")

        print("[demo] running... (waiting for queue to drain and DAG to finish)")
        await _wait_settled(store, run_id, timeout=30.0)
        # Final workflow advance to make sure the terminal status is persisted.
        await asyncio.get_running_loop().run_in_executor(
            None, advance_workflow, store, run_id
        )

        await engine.stop()
        _print_summary(store, engine, run_id, crashed_id)
    finally:
        store.close()
        shutil.rmtree(tmpdir, ignore_errors=True)
    return 0


def engine_submit(store: SQLiteJobStore, dag) -> int:
    from .dag import submit_workflow

    return submit_workflow(store, dag)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(asyncio.run(main()))
