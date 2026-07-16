"""Async worker pool.

Each worker:
  * has its **own single-thread DB executor**, so it uses a dedicated store
    connection (``connection per worker``) - this is what makes the SQLite
    ``BEGIN IMMEDIATE`` claim genuinely contend across workers.
  * claims one job at a time (queued -> running), runs the registered handler,
    then reports the transition (succeeded, or failed -> retry/dead).
  * emits a heartbeat for the in-flight job on a background loop, so the reaper
    can tell a live worker from a crashed one.

Sync handlers run on a shared handler thread-pool, keeping each worker's DB
executor free to heartbeat while a long handler runs. Async handlers run on the
event loop.

At-least-once semantics: a job may run more than once (e.g. a worker crashes
after doing work but before reporting success, and the reaper requeues it).
Handlers must therefore be idempotent - see the README.
"""

from __future__ import annotations

import asyncio
import functools
import os
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from .dag import UPSTREAM_KEY
from .models import JobContext
from .registry import TaskRegistry, get_registry
from .store.base import JobStore


class WorkerPool:
    def __init__(
        self,
        store: JobStore,
        registry: TaskRegistry | None = None,
        *,
        concurrency: int = 4,
        poll_interval: float = 0.05,
        heartbeat_interval: float = 1.0,
        worker_prefix: str | None = None,
    ) -> None:
        self.store = store
        self.registry = registry or get_registry()
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.worker_prefix = worker_prefix or f"worker-{os.getpid()}"

        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._db_executors: list[ThreadPoolExecutor] = []
        self._handler_executor = ThreadPoolExecutor(
            max_workers=max(concurrency, 2), thread_name_prefix="jobq-handler"
        )
        # Lifecycle events for observability / the demo summary.
        self.events: list[dict[str, Any]] = []

    # ---- public API ------------------------------------------------------
    async def start(self) -> None:
        """Launch ``concurrency`` worker coroutines (returns immediately)."""
        loop = asyncio.get_running_loop()
        for i in range(self.concurrency):
            worker_id = f"{self.worker_prefix}-{i}"
            db_exec = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix=f"jobq-db-{i}"
            )
            self._db_executors.append(db_exec)
            self._tasks.append(
                loop.create_task(self._run_worker(worker_id, db_exec), name=worker_id)
            )

    async def stop(self, *, drain: bool = False) -> None:
        """Signal workers to stop and wait for them to finish the current job."""
        if drain:
            await self.drain()
        self._stop.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for ex in self._db_executors:
            ex.shutdown(wait=True)
        self._db_executors.clear()
        self._handler_executor.shutdown(wait=True)

    async def drain(self, *, poll: float = 0.05, timeout: float | None = None) -> bool:
        """Wait until there are no queued or running jobs. Returns True if the
        queue emptied, False on timeout."""
        loop = asyncio.get_running_loop()
        waited = 0.0
        while True:
            counts = await loop.run_in_executor(None, self.store.counts_by_status)
            if counts.get("queued", 0) == 0 and counts.get("running", 0) == 0:
                return True
            if timeout is not None and waited >= timeout:
                return False
            await asyncio.sleep(poll)
            waited += poll

    # ---- worker loop -----------------------------------------------------
    async def _run_worker(self, worker_id: str, db_exec: ThreadPoolExecutor) -> None:
        loop = asyncio.get_running_loop()

        def call(fn: Callable, *a, **k):
            return loop.run_in_executor(db_exec, functools.partial(fn, *a, **k))

        await call(self.store.register_worker, worker_id)
        try:
            while not self._stop.is_set():
                job = await call(self.store.claim, worker_id)
                if job is None:
                    await call(self.store.worker_heartbeat, worker_id)
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=self.poll_interval
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue
                await self._execute(job, worker_id, call, db_exec)
        finally:
            await call(self.store.mark_worker_stopped, worker_id)

    async def _execute(
        self,
        job,
        worker_id: str,
        call: Callable,
        db_exec: ThreadPoolExecutor,
    ) -> None:
        loop = asyncio.get_running_loop()
        await call(self.store.worker_heartbeat, worker_id, current_job_id=job.id)
        self.events.append(
            {"event": "claimed", "job_id": job.id, "task": job.task,
             "attempt": job.attempts, "worker": worker_id}
        )

        kwargs = dict(job.kwargs) if isinstance(job.kwargs, dict) else {}
        upstream = kwargs.pop(UPSTREAM_KEY, {})
        ctx = JobContext(
            job_id=job.id,
            task=job.task,
            args=job.args,
            kwargs=kwargs,
            attempt=job.attempts,
            max_attempts=job.max_attempts,
            workflow_run_id=job.workflow_run_id,
            step_name=job.step_name,
            upstream=upstream,
        )

        # Background heartbeat for this in-flight job.
        hb = loop.create_task(self._heartbeat_loop(job.id, worker_id, call))
        try:
            handler = self.registry.get(job.task)
            if asyncio.iscoroutinefunction(handler):
                result = await handler(ctx)
            else:
                result = await loop.run_in_executor(
                    self._handler_executor, handler, ctx
                )
            applied = await call(self.store.complete, job.id, result, worker_id=worker_id)
            self.events.append(
                {"event": "succeeded" if applied else "lost", "job_id": job.id,
                 "task": job.task, "attempt": job.attempts}
            )
        except Exception as exc:  # noqa: BLE001 - report any handler failure
            err = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}"
            outcome = await call(self.store.fail, job.id, err, worker_id=worker_id)
            # "lost" => this job was reaped and re-claimed elsewhere while we ran;
            # our result is stale, so we drop it rather than corrupt the new owner.
            event = {"retry": "retry", "dead": "dead"}.get(outcome.outcome, "lost")
            self.events.append(
                {
                    "event": event,
                    "job_id": job.id,
                    "task": job.task,
                    "attempt": job.attempts,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        finally:
            hb.cancel()
            try:
                await hb
            except (asyncio.CancelledError, Exception):
                pass

    async def _heartbeat_loop(self, job_id: int, worker_id: str, call: Callable) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                await call(self.store.heartbeat, job_id, worker_id)
                await call(self.store.worker_heartbeat, worker_id, current_job_id=job_id)
        except asyncio.CancelledError:
            pass


def _main() -> None:
    """Entry point for ``python -m jobq.worker``: run a standalone worker pool."""
    import asyncio as _asyncio

    from .config import EngineConfig
    from .engine import run_forever

    try:
        _asyncio.run(run_forever(EngineConfig.from_env()))
    except KeyboardInterrupt:  # pragma: no cover
        pass


if __name__ == "__main__":  # pragma: no cover
    _main()

