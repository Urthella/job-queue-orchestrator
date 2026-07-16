"""PostgreSQL job store adapter using ``SELECT ... FOR UPDATE SKIP LOCKED``.

This is the production backend. It is **code-complete** but is not exercised in
the default test/demo environment (no Postgres available). ``psycopg`` is
imported lazily inside ``__init__`` so importing this module never breaks the
default SQLite path.

Select it by setting ``DATABASE_URL=postgresql://user:pass@host/db``.

Concurrency design
------------------
Postgres does the heavy lifting natively. The claim is a single statement::

    UPDATE jobs SET status='running', attempts=attempts+1, worker_id=%s, ...
     WHERE id = (
         SELECT id FROM jobs
          WHERE status='queued' AND run_at <= now()
          ORDER BY priority DESC, run_at ASC, id ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
     )
    RETURNING ...;

``FOR UPDATE`` row-locks the candidate; ``SKIP LOCKED`` makes concurrent
claimers ignore rows another transaction already locked, so N workers each grab
a distinct row with no blocking and no double-claim. This is the moral
equivalent of the SQLite ``BEGIN IMMEDIATE`` + atomic ``UPDATE`` approach, but
with row-level (not table-level) locking, so it scales to many workers.

Requires: ``pip install "psycopg[binary]>=3.1"`` and a reachable Postgres.
Run ``PostgresJobStore(dsn).init_schema()`` once to create the tables.
"""

from __future__ import annotations

import json
from typing import Any

from ..models import Job, JobStatus
from ..retry import compute_backoff
from .base import FailOutcome, JobStore

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id             BIGSERIAL PRIMARY KEY,
    task           TEXT    NOT NULL,
    args           JSONB   NOT NULL DEFAULT '[]',
    kwargs         JSONB   NOT NULL DEFAULT '{}',
    status         TEXT    NOT NULL DEFAULT 'queued',
    priority       INTEGER NOT NULL DEFAULT 0,
    run_at         DOUBLE PRECISION NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    worker_id      TEXT,
    locked_at      DOUBLE PRECISION,
    last_heartbeat DOUBLE PRECISION,
    result         JSONB,
    error          TEXT,
    created_at     DOUBLE PRECISION NOT NULL,
    updated_at     DOUBLE PRECISION NOT NULL,
    workflow_run_id BIGINT,
    step_name      TEXT,
    backoff_base   DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    backoff_factor DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    backoff_max    DOUBLE PRECISION NOT NULL DEFAULT 30.0,
    jitter         DOUBLE PRECISION NOT NULL DEFAULT 0.1
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs (status, priority DESC, run_at ASC, id ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_running_hb ON jobs (status, last_heartbeat);

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,
    started_at     DOUBLE PRECISION,
    last_heartbeat DOUBLE PRECISION,
    status         TEXT,
    current_job_id BIGINT
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    definition  JSONB NOT NULL DEFAULT '{}',
    context     JSONB NOT NULL DEFAULT '{}',
    created_at  DOUBLE PRECISION NOT NULL,
    updated_at  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id              BIGSERIAL PRIMARY KEY,
    workflow_run_id BIGINT NOT NULL,
    name            TEXT NOT NULL,
    task            TEXT NOT NULL,
    depends_on      JSONB NOT NULL DEFAULT '[]',
    args            JSONB NOT NULL DEFAULT '[]',
    kwargs          JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'pending',
    job_id          BIGINT,
    result          JSONB,
    error           TEXT,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    backoff_base    DOUBLE PRECISION NOT NULL DEFAULT 0.2,
    priority        INTEGER NOT NULL DEFAULT 0,
    UNIQUE (workflow_run_id, name)
);
"""

_JOB_COLUMNS = (
    "id, task, args, kwargs, status, priority, run_at, attempts, max_attempts, "
    "worker_id, locked_at, last_heartbeat, result, error, created_at, updated_at, "
    "workflow_run_id, step_name, backoff_base, backoff_factor, backoff_max, jitter"
)


def _now() -> float:
    import time

    return time.time()


def _row_to_job(row: dict[str, Any]) -> Job:
    # psycopg returns JSONB as already-decoded Python objects.
    return Job(
        id=row["id"],
        task=row["task"],
        args=row["args"] or [],
        kwargs=row["kwargs"] or {},
        status=row["status"],
        priority=row["priority"],
        run_at=row["run_at"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        worker_id=row["worker_id"],
        locked_at=row["locked_at"],
        last_heartbeat=row["last_heartbeat"],
        result=row["result"],
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        workflow_run_id=row["workflow_run_id"],
        step_name=row["step_name"],
        backoff_base=row["backoff_base"],
        backoff_factor=row["backoff_factor"],
        backoff_max=row["backoff_max"],
        jitter=row["jitter"],
    )


class PostgresJobStore(JobStore):
    """Postgres adapter. Requires psycopg 3 (imported lazily)."""

    def __init__(self, dsn: str, *, minconn: int = 1, maxconn: int = 10) -> None:
        try:
            import psycopg  # noqa: F401
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool
        except ImportError as exc:  # pragma: no cover - depends on optional dep
            raise ImportError(
                "PostgresJobStore requires psycopg 3 and psycopg_pool. Install with:\n"
                '    pip install "psycopg[binary]>=3.1" psycopg_pool'
            ) from exc

        self.dsn = dsn
        self._dict_row = dict_row
        # A small connection pool; each claim uses its own connection, which is
        # what makes SKIP LOCKED contend correctly across workers.
        self._pool = ConnectionPool(
            dsn, min_size=minconn, max_size=maxconn, kwargs={"row_factory": dict_row}
        )

    # ---- helpers ---------------------------------------------------------
    def _exec(self, sql: str, params: tuple = (), *, fetch: str | None = None):
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                return None

    def init_schema(self) -> None:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA)

    def close(self) -> None:
        self._pool.close()

    # ---- enqueue ---------------------------------------------------------
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
        from psycopg.types.json import Json

        now = _now() if now is None else now
        run_at = now if run_at is None else run_at
        row = self._exec(
            """
            INSERT INTO jobs
              (task, args, kwargs, status, priority, run_at, attempts, max_attempts,
               created_at, updated_at, workflow_run_id, step_name,
               backoff_base, backoff_factor, backoff_max, jitter)
            VALUES (%s, %s, %s, 'queued', %s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                task,
                Json(args or []),
                Json(kwargs or {}),
                priority,
                run_at,
                max_attempts,
                now,
                now,
                workflow_run_id,
                step_name,
                backoff_base,
                backoff_factor,
                backoff_max,
                jitter,
            ),
            fetch="one",
        )
        return int(row["id"])

    # ---- claim (FOR UPDATE SKIP LOCKED) ---------------------------------
    def claim(self, worker_id: str, *, now: float | None = None) -> Job | None:
        now = _now() if now is None else now
        # A single autocommitting statement; the sub-select's row lock + SKIP
        # LOCKED guarantees no two workers claim the same row.
        row = self._exec(
            f"""
            UPDATE jobs
               SET status='running', attempts=attempts+1, worker_id=%s,
                   locked_at=%s, last_heartbeat=%s, updated_at=%s
             WHERE id = (
                 SELECT id FROM jobs
                  WHERE status='queued' AND run_at <= %s
                  ORDER BY priority DESC, run_at ASC, id ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
             )
            RETURNING {_JOB_COLUMNS}
            """,
            (worker_id, now, now, now, now),
            fetch="one",
        )
        return _row_to_job(row) if row else None

    def heartbeat(self, job_id: int, worker_id: str, *, now: float | None = None) -> None:
        now = _now() if now is None else now
        self._exec(
            "UPDATE jobs SET last_heartbeat=%s, updated_at=%s WHERE id=%s AND worker_id=%s",
            (now, now, job_id, worker_id),
        )

    def complete(
        self,
        job_id: int,
        result: Any = None,
        *,
        now: float | None = None,
        worker_id: str | None = None,
    ) -> bool:
        from psycopg.types.json import Json

        now = _now() if now is None else now
        sql = (
            "UPDATE jobs SET status='succeeded', result=%s, error=NULL, updated_at=%s, "
            "worker_id=NULL, locked_at=NULL WHERE id=%s"
        )
        params: list[Any] = [Json(result), now, job_id]
        if worker_id is not None:
            sql += " AND status='running' AND worker_id=%s"
            params.append(worker_id)
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount > 0

    def fail(
        self,
        job_id: int,
        error: str,
        *,
        now: float | None = None,
        rng=None,
        worker_id: str | None = None,
    ) -> FailOutcome:
        now = _now() if now is None else now
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT attempts, max_attempts, backoff_base, backoff_factor, "
                    "backoff_max, jitter, status, worker_id FROM jobs WHERE id=%s FOR UPDATE",
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return FailOutcome(outcome="dead", next_run_at=None, attempts=0)
                # Lost the lock: reaped + re-claimed by another worker. Drop silently.
                if worker_id is not None and (
                    row["status"] != "running" or row["worker_id"] != worker_id
                ):
                    return FailOutcome(
                        outcome="lost", next_run_at=None, attempts=row["attempts"]
                    )
                attempts = row["attempts"]
                if attempts >= row["max_attempts"]:
                    cur.execute(
                        "UPDATE jobs SET status='dead', error=%s, updated_at=%s, "
                        "worker_id=NULL, locked_at=NULL WHERE id=%s",
                        (error, now, job_id),
                    )
                    return FailOutcome(outcome="dead", next_run_at=None, attempts=attempts)
                delay = compute_backoff(
                    attempts,
                    base=row["backoff_base"],
                    factor=row["backoff_factor"],
                    maximum=row["backoff_max"],
                    jitter=row["jitter"],
                    rng=rng,
                )
                next_run_at = now + delay
                cur.execute(
                    "UPDATE jobs SET status='queued', error=%s, run_at=%s, updated_at=%s, "
                    "worker_id=NULL, locked_at=NULL WHERE id=%s",
                    (error, next_run_at, now, job_id),
                )
        return FailOutcome(outcome="retry", next_run_at=next_run_at, attempts=attempts)

    def reap(self, *, heartbeat_timeout: float, now: float | None = None) -> list[int]:
        now = _now() if now is None else now
        cutoff = now - heartbeat_timeout
        # Requeue stuck jobs whose attempts remain; dead-letter the rest.
        requeued = self._exec(
            """
            UPDATE jobs SET status='queued', run_at=%s, updated_at=%s, worker_id=NULL,
                   locked_at=NULL, last_heartbeat=NULL,
                   error='reaped: worker heartbeat stale, requeued'
             WHERE status='running'
               AND COALESCE(last_heartbeat, locked_at, 0) < %s
               AND attempts < max_attempts
            RETURNING id
            """,
            (now, now, cutoff),
            fetch="all",
        )
        self._exec(
            """
            UPDATE jobs SET status='dead', updated_at=%s, worker_id=NULL, locked_at=NULL,
                   error='reaped: attempts exhausted after crash'
             WHERE status='running'
               AND COALESCE(last_heartbeat, locked_at, 0) < %s
               AND attempts >= max_attempts
            """,
            (now, cutoff),
        )
        return [r["id"] for r in (requeued or [])]

    # ---- queries ---------------------------------------------------------
    def get_job(self, job_id: int) -> Job | None:
        row = self._exec(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id=%s", (job_id,), fetch="one"
        )
        return _row_to_job(row) if row else None

    def list_jobs(self, *, status: str | None = None, limit: int = 100) -> list[Job]:
        if status:
            rows = self._exec(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE status=%s ORDER BY id DESC LIMIT %s",
                (status, limit),
                fetch="all",
            )
        else:
            rows = self._exec(
                f"SELECT {_JOB_COLUMNS} FROM jobs ORDER BY id DESC LIMIT %s",
                (limit,),
                fetch="all",
            )
        return [_row_to_job(r) for r in (rows or [])]

    def dead_letter(self, *, limit: int = 100) -> list[Job]:
        return self.list_jobs(status=JobStatus.DEAD.value, limit=limit)

    def requeue_dead(self, job_id: int, *, now: float | None = None) -> bool:
        now = _now() if now is None else now
        row = self._exec(
            """
            UPDATE jobs SET status='queued', attempts=0, run_at=%s, error=NULL,
                   updated_at=%s, worker_id=NULL, locked_at=NULL
             WHERE id=%s AND status='dead' RETURNING id
            """,
            (now, now, job_id),
            fetch="one",
        )
        return row is not None

    def counts_by_status(self) -> dict[str, int]:
        rows = self._exec(
            "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status", fetch="all"
        )
        counts = {s.value: 0 for s in JobStatus}
        for r in rows or []:
            counts[r["status"]] = r["n"]
        return counts

    # ---- workers ---------------------------------------------------------
    def register_worker(self, worker_id: str, *, now: float | None = None) -> None:
        now = _now() if now is None else now
        self._exec(
            """
            INSERT INTO workers (id, started_at, last_heartbeat, status, current_job_id)
            VALUES (%s, %s, %s, 'alive', NULL)
            ON CONFLICT (id) DO UPDATE SET started_at=excluded.started_at,
                last_heartbeat=excluded.last_heartbeat, status='alive'
            """,
            (worker_id, now, now),
        )

    def worker_heartbeat(
        self, worker_id: str, *, current_job_id: int | None = None, now: float | None = None
    ) -> None:
        now = _now() if now is None else now
        self._exec(
            "UPDATE workers SET last_heartbeat=%s, current_job_id=%s, status='alive' WHERE id=%s",
            (now, current_job_id, worker_id),
        )

    def mark_worker_stopped(self, worker_id: str, *, now: float | None = None) -> None:
        now = _now() if now is None else now
        self._exec(
            "UPDATE workers SET status='stopped', last_heartbeat=%s, current_job_id=NULL WHERE id=%s",
            (now, worker_id),
        )

    def list_workers(self) -> list[dict[str, Any]]:
        rows = self._exec(
            "SELECT id, started_at, last_heartbeat, status, current_job_id "
            "FROM workers ORDER BY id",
            fetch="all",
        )
        return [dict(r) for r in (rows or [])]

    # ---- workflows -------------------------------------------------------
    def create_workflow_run(
        self, name: str, definition: dict[str, Any], *, now: float | None = None
    ) -> int:
        from psycopg.types.json import Json

        now = _now() if now is None else now
        row = self._exec(
            "INSERT INTO workflow_runs (name, status, definition, context, created_at, updated_at) "
            "VALUES (%s, 'running', %s, '{}', %s, %s) RETURNING id",
            (name, Json(definition), now, now),
            fetch="one",
        )
        return int(row["id"])

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
    ) -> int:
        from psycopg.types.json import Json

        row = self._exec(
            "INSERT INTO workflow_steps (workflow_run_id, name, task, depends_on, args, kwargs, "
            "status, max_attempts, backoff_base, priority) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, %s, %s) RETURNING id",
            (
                workflow_run_id,
                name,
                task,
                Json(depends_on),
                Json(args or []),
                Json(kwargs or {}),
                max_attempts,
                backoff_base,
                priority,
            ),
            fetch="one",
        )
        return int(row["id"])

    def get_workflow_run(self, run_id: int) -> dict[str, Any] | None:
        row = self._exec("SELECT * FROM workflow_runs WHERE id=%s", (run_id,), fetch="one")
        return dict(row) if row else None

    def list_workflow_runs(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._exec(
            "SELECT id, name, status, created_at, updated_at FROM workflow_runs "
            "ORDER BY id DESC LIMIT %s",
            (limit,),
            fetch="all",
        )
        return [dict(r) for r in (rows or [])]

    def list_workflow_steps(self, run_id: int) -> list[dict[str, Any]]:
        rows = self._exec(
            "SELECT * FROM workflow_steps WHERE workflow_run_id=%s ORDER BY id",
            (run_id,),
            fetch="all",
        )
        return [dict(r) for r in (rows or [])]

    def update_workflow_run(
        self,
        run_id: int,
        *,
        status: str | None = None,
        context: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        from psycopg.types.json import Json

        now = _now() if now is None else now
        sets = ["updated_at=%s"]
        params: list[Any] = [now]
        if status is not None:
            sets.append("status=%s")
            params.append(status)
        if context is not None:
            sets.append("context=%s")
            params.append(Json(context))
        params.append(run_id)
        self._exec(f"UPDATE workflow_runs SET {', '.join(sets)} WHERE id=%s", tuple(params))

    def update_workflow_step(
        self,
        step_id: int,
        *,
        status: str | None = None,
        job_id: int | None = None,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        from psycopg.types.json import Json

        sets: list[str] = []
        params: list[Any] = []
        if status is not None:
            sets.append("status=%s")
            params.append(status)
        if job_id is not None:
            sets.append("job_id=%s")
            params.append(job_id)
        if result is not None:
            sets.append("result=%s")
            params.append(Json(result))
        if error is not None:
            sets.append("error=%s")
            params.append(error)
        if not sets:
            return
        params.append(step_id)
        self._exec(f"UPDATE workflow_steps SET {', '.join(sets)} WHERE id=%s", tuple(params))
