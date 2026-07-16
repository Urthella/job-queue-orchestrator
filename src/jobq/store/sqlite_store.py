"""SQLite-backed job store with a race-safe atomic claim.

Concurrency design (the crux of this project)
----------------------------------------------
SQLite has no ``SELECT ... FOR UPDATE SKIP LOCKED``. We get the *same*
guarantee - no two workers ever claim the same job - a different way:

1. **Connection per worker.** Each thread gets its own ``sqlite3.Connection``
   (thread-local). Real, separate connections are what make the claim actually
   contend; a single shared connection would just serialise trivially and would
   not prove anything.

2. **``BEGIN IMMEDIATE``.** The claim runs inside an *immediate* transaction.
   ``BEGIN IMMEDIATE`` acquires a ``RESERVED`` lock at statement start, so at
   most one writer is ever inside the critical section at a time. Contending
   writers block (not error) until ``busy_timeout`` because we set a generous
   ``PRAGMA busy_timeout``.

3. **Atomic ``UPDATE ... RETURNING``.** Inside that transaction we flip exactly
   one eligible row to ``running`` and read it back atomically::

       UPDATE jobs
          SET status='running', attempts=attempts+1, worker_id=?, locked_at=?, ...
        WHERE id = (SELECT id FROM jobs
                     WHERE status='queued' AND run_at <= ?
                     ORDER BY priority DESC, run_at ASC, id ASC
                     LIMIT 1)
       RETURNING ...;

   Because the whole statement executes while holding the write lock, the row
   the sub-select picks cannot be picked by anyone else: another worker either
   ran before us (and we see the new state) or waits for our COMMIT. SQLite
   3.35+ supports ``RETURNING``; a portable select-then-update fallback is
   provided for older builds.

WAL mode lets readers (the dashboard) run concurrently with the writer.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

from ..models import Job, JobStatus, StepStatus, WorkflowStatus
from ..retry import compute_backoff
from .base import FailOutcome, JobStore

_SUPPORTS_RETURNING = sqlite3.sqlite_version_info >= (3, 35, 0)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    task           TEXT    NOT NULL,
    args           TEXT    NOT NULL DEFAULT '[]',
    kwargs         TEXT    NOT NULL DEFAULT '{}',
    status         TEXT    NOT NULL DEFAULT 'queued',
    priority       INTEGER NOT NULL DEFAULT 0,
    run_at         REAL    NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    worker_id      TEXT,
    locked_at      REAL,
    last_heartbeat REAL,
    result         TEXT,
    error          TEXT,
    created_at     REAL    NOT NULL,
    updated_at     REAL    NOT NULL,
    workflow_run_id INTEGER,
    step_name      TEXT,
    backoff_base   REAL    NOT NULL DEFAULT 0.5,
    backoff_factor REAL    NOT NULL DEFAULT 2.0,
    backoff_max    REAL    NOT NULL DEFAULT 30.0,
    jitter         REAL    NOT NULL DEFAULT 0.1
);
-- Claim ordering / eligibility index. Hot path for the sub-select.
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs (status, priority DESC, run_at ASC, id ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_running_hb ON jobs (status, last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_jobs_wf ON jobs (workflow_run_id);

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,
    started_at     REAL,
    last_heartbeat REAL,
    status         TEXT,
    current_job_id INTEGER
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    definition  TEXT NOT NULL DEFAULT '{}',
    context     TEXT NOT NULL DEFAULT '{}',
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_run_id INTEGER NOT NULL,
    name            TEXT NOT NULL,
    task            TEXT NOT NULL,
    depends_on      TEXT NOT NULL DEFAULT '[]',
    args            TEXT NOT NULL DEFAULT '[]',
    kwargs          TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'pending',
    job_id          INTEGER,
    result          TEXT,
    error           TEXT,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    backoff_base    REAL NOT NULL DEFAULT 0.2,
    priority        INTEGER NOT NULL DEFAULT 0,
    UNIQUE (workflow_run_id, name)
);
CREATE INDEX IF NOT EXISTS idx_steps_run ON workflow_steps (workflow_run_id);
"""

_JOB_COLUMNS = (
    "id, task, args, kwargs, status, priority, run_at, attempts, max_attempts, "
    "worker_id, locked_at, last_heartbeat, result, error, created_at, updated_at, "
    "workflow_run_id, step_name, backoff_base, backoff_factor, backoff_max, jitter"
)


def _now() -> float:
    import time

    return time.time()


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"],
        task=row["task"],
        args=json.loads(row["args"]),
        kwargs=json.loads(row["kwargs"]),
        status=row["status"],
        priority=row["priority"],
        run_at=row["run_at"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        worker_id=row["worker_id"],
        locked_at=row["locked_at"],
        last_heartbeat=row["last_heartbeat"],
        result=json.loads(row["result"]) if row["result"] is not None else None,
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


class SQLiteJobStore(JobStore):
    """Thread-safe, race-safe SQLite job store.

    Uses one connection per thread (thread-local). Safe to share a single
    ``SQLiteJobStore`` instance across threads and asyncio ``to_thread`` workers.
    """

    def __init__(self, path: str = "jobq.db", *, busy_timeout_ms: int = 5000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self._local = threading.local()
        # Serialise WAL setup / first-connect races is unnecessary, but keep a
        # lock for close() bookkeeping.
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()

    # ---- connection management ------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn
        # isolation_level=None => autocommit; we drive BEGIN/COMMIT explicitly.
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        self._local.conn = conn
        with self._conns_lock:
            self._all_conns.append(conn)
        return conn

    def init_schema(self) -> None:
        conn = self._conn()
        conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._conns_lock:
            for conn in self._all_conns:
                try:
                    conn.close()
                except Exception:
                    pass
            self._all_conns.clear()
        self._local = threading.local()

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
        now = _now() if now is None else now
        run_at = now if run_at is None else run_at
        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO jobs
              (task, args, kwargs, status, priority, run_at, attempts, max_attempts,
               created_at, updated_at, workflow_run_id, step_name,
               backoff_base, backoff_factor, backoff_max, jitter)
            VALUES (?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task,
                json.dumps(args or []),
                json.dumps(kwargs or {}),
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
        )
        return int(cur.lastrowid)

    # ---- claim (the crux) -----------------------------------------------
    def claim(self, worker_id: str, *, now: float | None = None) -> Job | None:
        now = _now() if now is None else now
        conn = self._conn()
        # BEGIN IMMEDIATE: take the write lock up-front so exactly one claimer is
        # in the critical section. busy_timeout makes contenders wait, not error.
        conn.execute("BEGIN IMMEDIATE")
        try:
            if _SUPPORTS_RETURNING:
                row = conn.execute(
                    f"""
                    UPDATE jobs
                       SET status='running',
                           attempts=attempts+1,
                           worker_id=?,
                           locked_at=?,
                           last_heartbeat=?,
                           updated_at=?
                     WHERE id = (
                         SELECT id FROM jobs
                          WHERE status='queued' AND run_at <= ?
                          ORDER BY priority DESC, run_at ASC, id ASC
                          LIMIT 1
                     )
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (worker_id, now, now, now, now),
                ).fetchone()
            else:
                # Portable fallback: select-then-conditional-update, still inside
                # the immediate transaction so it remains atomic vs other writers.
                pick = conn.execute(
                    """
                    SELECT id FROM jobs
                     WHERE status='queued' AND run_at <= ?
                     ORDER BY priority DESC, run_at ASC, id ASC
                     LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if pick is None:
                    conn.execute("COMMIT")
                    return None
                conn.execute(
                    """
                    UPDATE jobs
                       SET status='running', attempts=attempts+1, worker_id=?,
                           locked_at=?, last_heartbeat=?, updated_at=?
                     WHERE id=? AND status='queued'
                    """,
                    (worker_id, now, now, now, pick["id"]),
                )
                row = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id=?", (pick["id"],)
                ).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if row is None:
            return None
        return _row_to_job(row)

    # ---- heartbeat / complete / fail ------------------------------------
    def heartbeat(self, job_id: int, worker_id: str, *, now: float | None = None) -> None:
        now = _now() if now is None else now
        self._conn().execute(
            "UPDATE jobs SET last_heartbeat=?, updated_at=? WHERE id=? AND worker_id=?",
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
        now = _now() if now is None else now
        # Fence on ownership+state when a worker_id is supplied, so a stale
        # straggler cannot clobber a job the reaper already re-assigned.
        sql = (
            "UPDATE jobs SET status='succeeded', result=?, error=NULL, updated_at=?, "
            "worker_id=NULL, locked_at=NULL WHERE id=?"
        )
        params: list[Any] = [json.dumps(result), now, job_id]
        if worker_id is not None:
            sql += " AND status='running' AND worker_id=?"
            params.append(worker_id)
        cur = self._conn().execute(sql, params)
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
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT attempts, max_attempts, backoff_base, backoff_factor, "
                "backoff_max, jitter, status, worker_id FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return FailOutcome(outcome="dead", next_run_at=None, attempts=0)
            # Lost the lock: reaped + re-claimed by another worker. Drop silently.
            if worker_id is not None and (
                row["status"] != "running" or row["worker_id"] != worker_id
            ):
                conn.execute("COMMIT")
                return FailOutcome(
                    outcome="lost", next_run_at=None, attempts=row["attempts"]
                )
            attempts = row["attempts"]
            if attempts >= row["max_attempts"]:
                conn.execute(
                    """
                    UPDATE jobs SET status='dead', error=?, updated_at=?,
                           worker_id=NULL, locked_at=NULL WHERE id=?
                    """,
                    (error, now, job_id),
                )
                conn.execute("COMMIT")
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
            conn.execute(
                """
                UPDATE jobs SET status='queued', error=?, run_at=?, updated_at=?,
                       worker_id=NULL, locked_at=NULL WHERE id=?
                """,
                (error, next_run_at, now, job_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return FailOutcome(outcome="retry", next_run_at=next_run_at, attempts=attempts)

    # ---- reaper ----------------------------------------------------------
    def reap(self, *, heartbeat_timeout: float, now: float | None = None) -> list[int]:
        now = _now() if now is None else now
        cutoff = now - heartbeat_timeout
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            # A stuck job = running, and its last known liveness signal
            # (heartbeat, else lock time) is older than the cutoff.
            rows = conn.execute(
                """
                SELECT id, attempts, max_attempts FROM jobs
                 WHERE status='running'
                   AND COALESCE(last_heartbeat, locked_at, 0) < ?
                """,
                (cutoff,),
            ).fetchall()
            requeued: list[int] = []
            for r in rows:
                if r["attempts"] >= r["max_attempts"]:
                    conn.execute(
                        "UPDATE jobs SET status='dead', error='reaped: attempts "
                        "exhausted after crash', updated_at=?, worker_id=NULL, "
                        "locked_at=NULL WHERE id=?",
                        (now, r["id"]),
                    )
                else:
                    # Requeue for immediate re-pickup. attempts was already bumped
                    # at claim time (the crashed attempt counts -> at-least-once
                    # with bounded retries).
                    conn.execute(
                        """
                        UPDATE jobs SET status='queued', run_at=?, updated_at=?,
                               worker_id=NULL, locked_at=NULL, last_heartbeat=NULL,
                               error='reaped: worker heartbeat stale, requeued'
                         WHERE id=?
                        """,
                        (now, now, r["id"]),
                    )
                    requeued.append(r["id"])
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return requeued

    # ---- queries ---------------------------------------------------------
    def get_job(self, job_id: int) -> Job | None:
        row = self._conn().execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        return _row_to_job(row) if row else None

    def list_jobs(self, *, status: str | None = None, limit: int = 100) -> list[Job]:
        conn = self._conn()
        if status:
            rows = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE status=? "
                "ORDER BY id DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_row_to_job(r) for r in rows]

    def dead_letter(self, *, limit: int = 100) -> list[Job]:
        return self.list_jobs(status=JobStatus.DEAD.value, limit=limit)

    def requeue_dead(self, job_id: int, *, now: float | None = None) -> bool:
        now = _now() if now is None else now
        cur = self._conn().execute(
            """
            UPDATE jobs SET status='queued', attempts=0, run_at=?, error=NULL,
                   updated_at=?, worker_id=NULL, locked_at=NULL
             WHERE id=? AND status='dead'
            """,
            (now, now, job_id),
        )
        return cur.rowcount > 0

    def counts_by_status(self) -> dict[str, int]:
        rows = self._conn().execute(
            "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
        ).fetchall()
        counts = {s.value: 0 for s in JobStatus}
        for r in rows:
            counts[r["status"]] = r["n"]
        return counts

    # ---- workers ---------------------------------------------------------
    def register_worker(self, worker_id: str, *, now: float | None = None) -> None:
        now = _now() if now is None else now
        self._conn().execute(
            """
            INSERT INTO workers (id, started_at, last_heartbeat, status, current_job_id)
            VALUES (?, ?, ?, 'alive', NULL)
            ON CONFLICT(id) DO UPDATE SET started_at=excluded.started_at,
                last_heartbeat=excluded.last_heartbeat, status='alive'
            """,
            (worker_id, now, now),
        )

    def worker_heartbeat(
        self, worker_id: str, *, current_job_id: int | None = None, now: float | None = None
    ) -> None:
        now = _now() if now is None else now
        self._conn().execute(
            "UPDATE workers SET last_heartbeat=?, current_job_id=?, status='alive' WHERE id=?",
            (now, current_job_id, worker_id),
        )

    def mark_worker_stopped(self, worker_id: str, *, now: float | None = None) -> None:
        now = _now() if now is None else now
        self._conn().execute(
            "UPDATE workers SET status='stopped', last_heartbeat=?, current_job_id=NULL WHERE id=?",
            (now, worker_id),
        )

    def list_workers(self) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT id, started_at, last_heartbeat, status, current_job_id "
            "FROM workers ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- workflows -------------------------------------------------------
    def create_workflow_run(
        self, name: str, definition: dict[str, Any], *, now: float | None = None
    ) -> int:
        now = _now() if now is None else now
        cur = self._conn().execute(
            """
            INSERT INTO workflow_runs (name, status, definition, context, created_at, updated_at)
            VALUES (?, 'running', ?, '{}', ?, ?)
            """,
            (name, json.dumps(definition), now, now),
        )
        return int(cur.lastrowid)

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
        cur = self._conn().execute(
            """
            INSERT INTO workflow_steps
              (workflow_run_id, name, task, depends_on, args, kwargs, status,
               max_attempts, backoff_base, priority)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
            """,
            (
                workflow_run_id,
                name,
                task,
                json.dumps(depends_on),
                json.dumps(args or []),
                json.dumps(kwargs or {}),
                max_attempts,
                backoff_base,
                priority,
            ),
        )
        return int(cur.lastrowid)

    def get_workflow_run(self, run_id: int) -> dict[str, Any] | None:
        row = self._conn().execute(
            "SELECT * FROM workflow_runs WHERE id=?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["definition"] = json.loads(d["definition"])
        d["context"] = json.loads(d["context"])
        return d

    def list_workflow_runs(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT id, name, status, created_at, updated_at FROM workflow_runs "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_workflow_steps(self, run_id: int) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT * FROM workflow_steps WHERE workflow_run_id=? ORDER BY id",
            (run_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["depends_on"] = json.loads(d["depends_on"])
            d["args"] = json.loads(d["args"])
            d["kwargs"] = json.loads(d["kwargs"])
            d["result"] = json.loads(d["result"]) if d["result"] is not None else None
            out.append(d)
        return out

    def update_workflow_run(
        self,
        run_id: int,
        *,
        status: str | None = None,
        context: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        now = _now() if now is None else now
        sets = ["updated_at=?"]
        params: list[Any] = [now]
        if status is not None:
            sets.append("status=?")
            params.append(status)
        if context is not None:
            sets.append("context=?")
            params.append(json.dumps(context))
        params.append(run_id)
        self._conn().execute(
            f"UPDATE workflow_runs SET {', '.join(sets)} WHERE id=?", params
        )

    def update_workflow_step(
        self,
        step_id: int,
        *,
        status: str | None = None,
        job_id: int | None = None,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        sets: list[str] = []
        params: list[Any] = []
        if status is not None:
            sets.append("status=?")
            params.append(status)
        if job_id is not None:
            sets.append("job_id=?")
            params.append(job_id)
        if result is not None:
            sets.append("result=?")
            params.append(json.dumps(result))
        if error is not None:
            sets.append("error=?")
            params.append(error)
        if not sets:
            return
        params.append(step_id)
        self._conn().execute(
            f"UPDATE workflow_steps SET {', '.join(sets)} WHERE id=?", params
        )
