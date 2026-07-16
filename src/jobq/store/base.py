"""Abstract JobStore interface.

All storage backends (SQLite default, Postgres adapter) implement this contract.
The engine is written against this interface, so swapping the backend is a matter
of changing ``DATABASE_URL`` (see :mod:`jobq.config`).

The single most important guarantee is :meth:`JobStore.claim`: under concurrent
callers, **no two callers ever receive the same job**. See the concrete backends
for how each achieves it (SQLite ``BEGIN IMMEDIATE`` vs Postgres
``FOR UPDATE SKIP LOCKED``).
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any

from ..models import Job


@dataclass
class ClaimResult:
    """Returned by :meth:`JobStore.claim`; ``job`` is ``None`` when nothing due."""

    job: Job | None


@dataclass
class FailOutcome:
    """Result of :meth:`JobStore.fail`."""

    outcome: str          # "retry", "dead", or "lost"
    next_run_at: float | None
    attempts: int


class JobStore(abc.ABC):
    # ---- lifecycle -------------------------------------------------------
    @abc.abstractmethod
    def init_schema(self) -> None:
        """Create tables/indexes if they do not exist (idempotent)."""

    @abc.abstractmethod
    def close(self) -> None:
        """Release any open connections/handles."""

    # ---- enqueue / claim / complete -------------------------------------
    @abc.abstractmethod
    def enqueue(
        self,
        task: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        priority: int = 0,
        run_at: float | None = None,
        max_attempts: int = 3,
        backoff_base: float = 0.5,
        backoff_factor: float = 2.0,
        backoff_max: float = 30.0,
        jitter: float = 0.1,
        workflow_run_id: int | None = None,
        step_name: str | None = None,
        now: float | None = None,
    ) -> int:
        """Insert a queued job and return its id."""

    @abc.abstractmethod
    def claim(self, worker_id: str, *, now: float | None = None) -> Job | None:
        """Atomically claim (queued -> running) the single highest-priority due
        job, returning it with ``attempts`` already incremented, or ``None``.

        Concurrency-safe: concurrent callers never receive the same job.
        """

    @abc.abstractmethod
    def heartbeat(self, job_id: int, worker_id: str, *, now: float | None = None) -> None:
        """Refresh ``last_heartbeat``/``locked_at`` for an in-flight job."""

    @abc.abstractmethod
    def complete(
        self,
        job_id: int,
        result: Any = None,
        *,
        now: float | None = None,
        worker_id: str | None = None,
    ) -> bool:
        """Mark a running job succeeded and store its result.

        If ``worker_id`` is given the transition is *fenced*: it only applies
        while the job is still ``running`` and owned by that worker. This stops
        a straggling worker whose job was reaped and re-claimed by someone else
        from clobbering the new owner's state. Returns ``True`` if it applied,
        ``False`` if the lock was lost (stale caller, dropped). Callers that
        pass no ``worker_id`` are unfenced (legacy/test convenience)."""

    @abc.abstractmethod
    def fail(
        self,
        job_id: int,
        error: str,
        *,
        now: float | None = None,
        rng=None,
        worker_id: str | None = None,
    ) -> FailOutcome:
        """Record a failed attempt. If attempts remain, requeue with backoff
        (``run_at`` in the future); otherwise move to the dead-letter queue
        (status ``dead``).

        Fenced on ``worker_id`` when given (see :meth:`complete`): if the job is
        no longer ``running`` under this worker the call is a no-op and the
        outcome is ``"lost"`` — the reaper/another worker has taken over."""

    # ---- reaper ----------------------------------------------------------
    @abc.abstractmethod
    def reap(self, *, heartbeat_timeout: float, now: float | None = None) -> list[int]:
        """Requeue jobs stuck in ``running`` whose heartbeat is older than
        ``heartbeat_timeout`` seconds. Returns the list of requeued job ids."""

    # ---- queries ---------------------------------------------------------
    @abc.abstractmethod
    def get_job(self, job_id: int) -> Job | None: ...

    @abc.abstractmethod
    def list_jobs(self, *, status: str | None = None, limit: int = 100) -> list[Job]: ...

    @abc.abstractmethod
    def dead_letter(self, *, limit: int = 100) -> list[Job]:
        """Jobs currently in the dead-letter queue (status ``dead``)."""

    @abc.abstractmethod
    def requeue_dead(self, job_id: int, *, now: float | None = None) -> bool:
        """Manually requeue a dead job (resets attempts). Returns True on success."""

    @abc.abstractmethod
    def counts_by_status(self) -> dict[str, int]: ...

    # ---- workers ---------------------------------------------------------
    @abc.abstractmethod
    def register_worker(self, worker_id: str, *, now: float | None = None) -> None: ...

    @abc.abstractmethod
    def worker_heartbeat(
        self, worker_id: str, *, current_job_id: int | None = None, now: float | None = None
    ) -> None: ...

    @abc.abstractmethod
    def mark_worker_stopped(self, worker_id: str, *, now: float | None = None) -> None: ...

    @abc.abstractmethod
    def list_workers(self) -> list[dict[str, Any]]: ...

    # ---- workflows -------------------------------------------------------
    @abc.abstractmethod
    def create_workflow_run(
        self, name: str, definition: dict[str, Any], *, now: float | None = None
    ) -> int: ...

    @abc.abstractmethod
    def add_workflow_step(
        self,
        workflow_run_id: int,
        name: str,
        task: str,
        depends_on: list[str],
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        max_attempts: int = 3,
        backoff_base: float = 0.2,
        priority: int = 0,
    ) -> int: ...

    @abc.abstractmethod
    def get_workflow_run(self, run_id: int) -> dict[str, Any] | None: ...

    @abc.abstractmethod
    def list_workflow_runs(self, *, limit: int = 100) -> list[dict[str, Any]]: ...

    @abc.abstractmethod
    def list_workflow_steps(self, run_id: int) -> list[dict[str, Any]]: ...

    @abc.abstractmethod
    def update_workflow_run(
        self,
        run_id: int,
        *,
        status: str | None = None,
        context: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> None: ...

    @abc.abstractmethod
    def update_workflow_step(
        self,
        step_id: int,
        *,
        status: str | None = None,
        job_id: int | None = None,
        result: Any = None,
        error: str | None = None,
    ) -> None: ...
