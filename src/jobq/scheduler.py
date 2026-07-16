"""Background scheduler: reaper, workflow reconciler, and periodic tasks.

Runs alongside the :class:`jobq.worker.WorkerPool`. The worker pool is the
puller (its ``claim`` already honours ``run_at`` and ``priority``); the
scheduler adds the time- and orchestration-driven concerns:

* **reaper** - periodically requeues jobs whose worker heartbeat/lock is stale
  (crash recovery).
* **workflow reconciler** - advances every running DAG (enqueues newly-eligible
  steps, propagates failures/blocks, finalises workflow status).
* **periodic tasks** - interval- or cron-driven recurring enqueues.
"""

from __future__ import annotations

import asyncio
import functools
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import cron as cronlib
from .dag import advance_workflow
from .models import WorkflowStatus
from .store.base import JobStore


@dataclass
class PeriodicTask:
    task: str
    interval: float | None = None       # seconds between runs
    cron: str | None = None             # OR a 5-field cron expression
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    _next_run: float = 0.0

    def due(self, now: float) -> bool:
        return now >= self._next_run

    def schedule_next(self, now: float) -> None:
        if self.interval is not None:
            self._next_run = now + self.interval
        elif self.cron is not None:
            nxt = cronlib.next_run(self.cron, datetime.fromtimestamp(now))
            self._next_run = nxt.timestamp()
        else:
            self._next_run = float("inf")


class Scheduler:
    def __init__(
        self,
        store: JobStore,
        *,
        reap_interval: float = 1.0,
        heartbeat_timeout: float = 5.0,
        workflow_interval: float = 0.1,
        periodic_interval: float = 0.5,
    ) -> None:
        self.store = store
        self.reap_interval = reap_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.workflow_interval = workflow_interval
        self.periodic_interval = periodic_interval
        self.periodic_tasks: list[PeriodicTask] = []
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        # Observability
        self.reaped_ids: list[int] = []

    def add_periodic(self, pt: PeriodicTask) -> None:
        pt.schedule_next(time.time())
        self.periodic_tasks.append(pt)

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._tasks = [
            loop.create_task(self._reaper_loop(), name="jobq-reaper"),
            loop.create_task(self._workflow_loop(), name="jobq-workflows"),
            loop.create_task(self._periodic_loop(), name="jobq-periodic"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _call(self, fn, *a, **k):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(fn, *a, **k))

    async def _reaper_loop(self) -> None:
        while not self._stop.is_set():
            try:
                ids = await self._call(
                    self.store.reap, heartbeat_timeout=self.heartbeat_timeout
                )
                if ids:
                    self.reaped_ids.extend(ids)
            except Exception:
                pass
            await self._sleep(self.reap_interval)

    async def _workflow_loop(self) -> None:
        while not self._stop.is_set():
            try:
                runs = await self._call(self.store.list_workflow_runs, limit=200)
                for run in runs:
                    if run["status"] == WorkflowStatus.RUNNING.value:
                        await self._call(advance_workflow, self.store, run["id"])
            except Exception:
                pass
            await self._sleep(self.workflow_interval)

    async def _periodic_loop(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            for pt in self.periodic_tasks:
                if pt.due(now):
                    try:
                        await self._call(
                            self.store.enqueue,
                            pt.task,
                            args=pt.args,
                            kwargs=pt.kwargs,
                            priority=pt.priority,
                        )
                    except Exception:
                        pass
                    pt.schedule_next(now)
            await self._sleep(self.periodic_interval)
