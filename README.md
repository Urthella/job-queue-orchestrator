# jobq: Job Queue & Workflow Orchestration Engine

<!-- badges -->
[![Stars](https://img.shields.io/github/stars/Urthella/job-queue-orchestrator?style=flat-square&logo=github)](https://github.com/Urthella/job-queue-orchestrator)
[![Last commit](https://img.shields.io/github/last-commit/Urthella/job-queue-orchestrator?style=flat-square)](https://github.com/Urthella/job-queue-orchestrator/commits/main)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![asyncio](https://img.shields.io/badge/asyncio-555555?style=flat-square)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat-square&logo=sqlite&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white)

A from-scratch **mini Celery/Sidekiq** built on Python 3.12 + asyncio. It proves
the hard parts of a distributed job system: **race-safe concurrent job claiming**,
retry/backoff with a **dead-letter queue**, **worker heartbeats + a crash reaper**,
and a **DAG workflow engine** — plus a FastAPI dashboard, a REST API, and a CLI.

The default storage backend is **SQLite**, engineered to give the *same*
no-double-claim guarantee as PostgreSQL's `SELECT … FOR UPDATE SKIP LOCKED`.
A **code-complete PostgreSQL adapter** is included and env-selectable
(`DATABASE_URL`) for production; it uses real `FOR UPDATE SKIP LOCKED`.

> Target role: Backend / Distributed Systems. The point of the project is to
> demonstrate an understanding of low-level concurrency, race conditions, and
> workflow execution states (DAGs).

---

## Table of contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Concurrent claiming — why it is race-safe](#concurrent-claiming--why-it-is-race-safe)
- [Job execution state machine](#job-execution-state-machine)
- [Retry / backoff + dead-letter queue](#retry--backoff--dead-letter-queue)
- [Heartbeats + reaper (crash recovery)](#heartbeats--reaper-crash-recovery)
- [DAG / workflow engine](#dag--workflow-engine)
- [At-least-once & idempotency](#at-least-once--idempotency)
- [Dashboard & REST API](#dashboard--rest-api)
- [CLI](#cli)
- [Tests](#tests)
- [Spec milestones → implementation](#spec-milestones--implementation)
- [Production: swap SQLite for Postgres / Redis](#production-swap-sqlite-for-postgres--redis)
- [Project layout](#project-layout)

---

## Quick start

```bash
# 1. Create a venv and install (Windows PowerShell shown; bash is analogous)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .

# 2. Run the end-to-end demo (no external services needed)
python -m jobq.demo

# 3. Run the tests
pytest -q

# 4. In one terminal: run a worker pool + scheduler
python -m jobq.worker

# 5. In another: serve the dashboard, then open http://127.0.0.1:8000
python -m jobq.dashboard
#   enqueue a job from the CLI and watch it flow through:
python -m jobq.cli enqueue flaky --kwargs "{\"fail_times\": 2}" --max-attempts 5
python -m jobq.cli submit-workflow src/jobq/dags/diamond.yaml
```

Everything runs with **zero external services**. The default `DATABASE_URL` is
`sqlite:///./jobq.db`.

---

## Architecture

```mermaid
flowchart TB
    subgraph Clients
        CLI["CLI / REST API<br/>enqueue, submit workflow"]
    end

    subgraph Store["JobStore interface"]
        SQLITE[("SQLite<br/>BEGIN IMMEDIATE +<br/>UPDATE … RETURNING")]
        PG[("PostgreSQL<br/>FOR UPDATE SKIP LOCKED<br/>(env-selectable)")]
    end

    subgraph Engine["Engine (asyncio, one process)"]
        direction TB
        subgraph Pool["WorkerPool"]
            W0["worker-0<br/>own DB connection"]
            W1["worker-1<br/>own DB connection"]
            WN["worker-N<br/>own DB connection"]
        end
        subgraph Sched["Scheduler"]
            REAP["reaper<br/>requeue stale jobs"]
            WF["workflow reconciler<br/>advance DAGs"]
            PER["periodic / cron<br/>recurring enqueues"]
        end
    end

    DASH["FastAPI dashboard<br/>auto-refresh + REST"]

    CLI -->|enqueue| Store
    Store --- SQLITE
    Store -. "DATABASE_URL" .- PG
    W0 & W1 & WN <-->|claim / complete / fail / heartbeat| Store
    REAP --> Store
    WF --> Store
    PER --> Store
    DASH -->|read + enqueue| Store
    Pool -->|run| HANDLERS["registered task handlers<br/>(task registry)"]
```

**Key idea:** every component talks only to the `JobStore` interface
(`src/jobq/store/base.py`). The engine has no knowledge of SQLite vs Postgres.
Each worker owns its **own DB connection**, which is what makes the atomic claim
genuinely contend across workers.

---

## Concurrent claiming — why it is race-safe

The single most important property: **no two workers ever claim the same job.**

### SQLite (default) — `BEGIN IMMEDIATE` + atomic `UPDATE … RETURNING`

`src/jobq/store/sqlite_store.py :: SQLiteJobStore.claim`

```sql
BEGIN IMMEDIATE;                       -- take the write (RESERVED) lock up-front
UPDATE jobs
   SET status='running', attempts=attempts+1,
       worker_id=?, locked_at=?, last_heartbeat=?, updated_at=?
 WHERE id = (
     SELECT id FROM jobs
      WHERE status='queued' AND run_at <= ?
      ORDER BY priority DESC, run_at ASC, id ASC
      LIMIT 1
 )
RETURNING id, task, args, kwargs, attempts, … ;
COMMIT;
```

Why it cannot double-claim:

1. **`BEGIN IMMEDIATE`** acquires SQLite's `RESERVED` write lock immediately, at
   statement start rather than lazily. SQLite allows **at most one writer at a
   time**, so only one claimer is ever inside the critical section. A second
   claimer that arrives concurrently *blocks* (it does not error and it does not
   proceed) until the first `COMMIT`s.
2. **The candidate sub-select and the `UPDATE` execute as one statement** while
   holding that write lock. The row the sub-select picks therefore cannot also
   be picked by anyone else: a competitor either committed *before* us (so we see
   the row already `running` and skip it) or runs *after* our `COMMIT`.
3. **`RETURNING`** hands back exactly the row we flipped, atomically — no
   separate read that could observe a torn state.
4. **`PRAGMA busy_timeout`** makes contending writers *wait* for the lock rather
   than fail with "database is locked".
5. **Connection per worker.** Each worker uses its own `sqlite3.Connection`
   (thread-local). With a single shared connection the claims would serialise
   trivially and prove nothing; separate connections make the lock genuinely
   contend. `PRAGMA journal_mode=WAL` additionally lets the dashboard read while
   a worker writes.

SQLite 3.35+ supports `RETURNING` (bundled with CPython 3.12 → SQLite 3.45 here).
A **portable fallback** (select-then-conditional-`UPDATE … WHERE id=? AND
status='queued'`, still inside the immediate transaction) is implemented for
older builds and selected automatically.

### PostgreSQL (production) — `FOR UPDATE SKIP LOCKED`

`src/jobq/store/postgres_store.py :: PostgresJobStore.claim`

```sql
UPDATE jobs
   SET status='running', attempts=attempts+1, worker_id=%s, …
 WHERE id = (
     SELECT id FROM jobs
      WHERE status='queued' AND run_at <= now
      ORDER BY priority DESC, run_at ASC, id ASC
      FOR UPDATE SKIP LOCKED          -- row-lock the candidate, skip locked ones
      LIMIT 1
 )
RETURNING …;
```

### The comparison

| | SQLite (`BEGIN IMMEDIATE`) | Postgres (`SKIP LOCKED`) |
|---|---|---|
| Lock granularity | **Table-level** write lock (one writer at a time) | **Row-level** lock; other rows stay claimable |
| Contending claimer | **Blocks** until `busy_timeout`, then retries | **Skips** the locked row, grabs the next one — never blocks |
| Correctness mechanism | Serialised critical section | Row lock + skip |
| Scales to many workers | Modest (writers serialise) | High (workers grab distinct rows in parallel) |
| Double-claim possible? | **No** | **No** |

Both achieve the same guarantee. SQLite trades throughput (serialised writers)
for zero-dependency simplicity; Postgres keeps the guarantee while letting N
workers claim N distinct rows with no blocking. The `claim()` contract is
identical, so the engine is unchanged when you switch.

> The headline test (`tests/test_concurrency.py`) spins up 8–16 **threads**, each
> with its own connection, draining a queue of hundreds of jobs, and asserts
> every job was claimed **exactly once** (set equality + no duplicates +
> `attempts == 1`). It is run repeatedly because races are nondeterministic.

---

## Job execution state machine

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue()
    queued --> running: claim()  (BEGIN IMMEDIATE, attempts += 1)
    running --> succeeded: handler returns
    running --> queued: fail() & attempts < max  (requeue after backoff)
    running --> dead: fail() & attempts >= max
    running --> queued: reaper (stale heartbeat, attempts < max)
    running --> dead: reaper (stale heartbeat, attempts >= max)
    dead --> queued: requeue_dead() / dashboard "retry"
    succeeded --> [*]
    dead --> [*]
```

`attempts` is incremented **at claim time**, so a crash *between* claim and
completion still counts as an attempt — giving bounded retries even for crashes.

---

## Retry / backoff + dead-letter queue

`src/jobq/retry.py`, `SQLiteJobStore.fail`

- **Exponential backoff with jitter.** On a failed attempt `a`:
  `delay = min(base · factor^(a−1), max) + U(0, jitter · base_delay)`.
  Configurable per job (`backoff_base`, `backoff_factor`, `backoff_max`,
  `jitter`) and testable via an injectable RNG.
- The failed job is **requeued** with `run_at = now + delay` (so `claim` won't
  pick it up until it is due) while `attempts < max_attempts`.
- When `attempts >= max_attempts`, the job moves to **`status='dead'`** — the
  **dead-letter queue** — retaining its last error. Surfaced via
  `store.dead_letter()`, `GET /api/deadletter`, and the dashboard panel.
- Dead jobs can be **manually requeued** (`requeue_dead`, `POST
  /api/jobs/{id}/retry`, dashboard "retry" button), which resets `attempts`.

Example: `always_fail` with `max_attempts=3` → attempt 1 fails (requeue), attempt
2 fails (requeue), attempt 3 fails (`3 >= 3` → **dead**).

---

## Heartbeats + reaper (crash recovery)

`WorkerPool._heartbeat_loop`, `Scheduler._reaper_loop`, `SQLiteJobStore.reap`

- While a job runs, its worker updates `jobs.last_heartbeat` (and its own
  `workers.last_heartbeat`) on a background loop.
- The **reaper** periodically finds jobs that are still `running` but whose
  `COALESCE(last_heartbeat, locked_at)` is older than `heartbeat_timeout`
  (a crashed or stuck worker). It **requeues** them (`status='queued'`,
  `worker_id=NULL`) so a healthy worker can pick them up — or dead-letters them
  if they have already exhausted `max_attempts`.
- Because `attempts` was bumped at claim time, a reaper requeue naturally carries
  an **attempt bump** → at-least-once with a bounded retry budget.

```mermaid
sequenceDiagram
    participant W1 as Worker A (crashes)
    participant DB as JobStore
    participant R as Reaper
    participant W2 as Worker B
    W1->>DB: claim(job)  status=running, attempts=1, heartbeat=t0
    Note over W1: process dies — no more heartbeats
    R->>DB: reap(now)  heartbeat age > timeout?
    DB-->>R: job stale → status=queued (attempts kept =1)
    W2->>DB: claim(job)  status=running, attempts=2
    W2->>DB: complete(job)  status=succeeded
```

The demo injects exactly this scenario (a job forced into `running` with a stale
heartbeat) and shows the reaper requeueing it and a live worker finishing it.

---

## DAG / workflow engine

`src/jobq/dag.py`

- **Definitions** in a dict, **JSON**, or **YAML** (`src/jobq/dags/diamond.{json,yaml}`).
  Each step has `name`, `task`, `depends_on`, optional `args`/`kwargs`/
  `max_attempts`.
- **Eligibility rule:** a step becomes eligible (its job is enqueued) only when
  **all** upstream dependencies are `succeeded`. Independent steps run
  **concurrently** as ordinary queue jobs.
- **Failure propagation:** a step that fails after its retries marks the workflow
  toward `failed` and **blocks** every downstream dependent (transitively). Steps
  on independent branches still complete.
- **Cycle detection at load time** via Kahn's algorithm — a cyclic definition
  raises `CycleError` before anything is enqueued. Missing/self dependencies are
  rejected too.
- **Passing outputs:** each succeeded step's result is stored in the workflow
  `context`; when a dependent is enqueued, its upstreams' results are passed to
  the handler as `ctx.upstream` (`{dep_name: result}`).
- A **workflow reconciler** loop (in the scheduler) advances every running DAG:
  syncs step states from their jobs, enqueues newly-eligible steps, propagates
  blocking, and finalises the run as `succeeded`/`failed`.

The bundled **diamond** DAG: `A → B`, `A → C`, `(B, C) → D`.

```mermaid
flowchart LR
    A["A: gen_number → 10"] --> B["B: add_const c=5 → 15"]
    A --> C["C: mul_const m=3 → 30"]
    B --> D["D: sum_upstream → 45"]
    C --> D
```

Constraints the tests assert: `B` and `C` start only after `A` ends; `D` starts
only after both `B` and `C` end; `D == 45`. A separate test replaces `B` with an
always-failing task and asserts `C` becomes `blocked`, the workflow `failed`, and
the independent branch still `succeeded`.

---

## At-least-once & idempotency

Delivery is **at-least-once**, not exactly-once. A worker can crash *after* doing
its side effect but *before* recording success; the reaper will then requeue the
job and it runs again. **Handlers must therefore be idempotent.**

Practical guidance (and what the examples model):
- Key side effects by a stable id (`ctx.job_id`, or a business key) and make
  writes upserts / "insert if not exists".
- Treat external calls as retriable; use provider idempotency keys where offered.
- The example `add`, `echo`, and DAG steps are **pure functions of their inputs**,
  so re-execution is harmless. `flaky` keys its behaviour off `ctx.attempt`;
  `flaky_stateful` demonstrates external-state care.

The `claim` guarantee ("exactly one worker at a time") prevents two workers from
claiming the same *queued* row at the same instant. It does **not** span a reaper
requeue: if the reaper false-positives on a *slow-but-alive* worker (its heartbeat
stalled past `heartbeat_timeout` — e.g. an async handler that blocks the event
loop, a GC/swap pause, or a too-low timeout), the job is requeued and re-claimed
while the original worker is still running it — genuinely **concurrent** duplicate
execution. So handlers must tolerate concurrent duplicates, not merely sequential
retries. Two defenses ship here:

- **Fenced terminal writes.** `complete()`/`fail()` are fenced on `worker_id` +
  `status='running'`, so a straggling worker whose job was reaped and re-claimed
  cannot clobber the new owner's state (no `succeeded → queued` re-runs, no
  premature dead-lettering). The stale result is dropped (surfaced as a `lost`
  event). This protects the **state machine**, but cannot un-run a side effect
  that already executed concurrently — hence idempotency is still required.
- **Comfortable margins.** Keep `heartbeat_timeout` well above your worst-case
  handler stall (the default is `5×` the heartbeat interval) to make false-reaps
  rare in the first place.

---

## Dashboard & REST API

Run it and open <http://127.0.0.1:8000>:

```bash
python -m jobq.dashboard          # or: python -m jobq.cli dashboard --port 8000
```

The dashboard (no build step — HTML/CSS/JS inlined, auto-refresh every 2s) shows
job states + retry counts, the dead-letter queue (with a **retry** button),
workers + last-heartbeat age, and workflow runs with a per-step status view.

### Screenshots

_Placeholders — capture from a running instance:_

- `docs/dashboard-overview.png` — job-state tiles, jobs table, workers, DLQ.
- `docs/dashboard-workflow.png` — diamond DAG run with per-step statuses.

### REST API

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/` | HTML dashboard |
| `GET`  | `/api/stats` | counts by status + worker/workflow totals |
| `GET`  | `/api/jobs?status=&limit=` | list jobs |
| `GET`  | `/api/jobs/{id}` | job detail |
| `POST` | `/api/jobs` | enqueue `{task, args, kwargs, priority, max_attempts, delay}` |
| `POST` | `/api/jobs/{id}/retry` | requeue a dead job |
| `GET`  | `/api/deadletter` | dead-letter queue |
| `GET`  | `/api/workers` | workers + last heartbeat |
| `GET`  | `/api/workflows` | workflow runs |
| `GET`  | `/api/workflows/{id}` | run detail + per-step status |
| `POST` | `/api/workflows` | submit a workflow `{name, steps:[…]}` (cycles → 400) |

```bash
curl -s -X POST http://127.0.0.1:8000/api/jobs \
     -H "content-type: application/json" \
     -d '{"task":"add","args":[4,5],"priority":3}'
# {"id":1,"status":"queued","task":"add"}
```

---

## CLI

```bash
python -m jobq.cli worker                                  # worker pool + scheduler
python -m jobq.cli enqueue add --args "[1,2,3]"            # enqueue a job
python -m jobq.cli enqueue echo --kwargs "{\"message\":\"hi\"}" --delay 5   # delayed
python -m jobq.cli submit-workflow src/jobq/dags/diamond.yaml
python -m jobq.cli jobs --status dead                      # list jobs
python -m jobq.cli workers                                 # list workers
python -m jobq.cli dashboard --port 8000                   # serve dashboard
python -m jobq.cli demo                                    # end-to-end demo

# Module shortcuts
python -m jobq.worker      python -m jobq.dashboard      python -m jobq.demo
```

Configuration is via environment variables (see `.env.example`): `DATABASE_URL`,
`JOBQ_CONCURRENCY`, `JOBQ_HEARTBEAT_TIMEOUT`, `JOBQ_REAP_INTERVAL`, etc.

---

## Tests

```bash
pytest -q
# Run the race test many times (races are nondeterministic):
pytest tests/test_concurrency.py -q     # repeat a handful of times
```

Coverage:

| File | What it proves |
|---|---|
| `test_concurrency.py` | **Headline:** N threads × M jobs → each claimed exactly once, no double-claim (repeated runs) |
| `test_retry.py` | backoff math (geometric + jitter bounds), attempt counts, requeue-then-succeed |
| `test_deadletter.py` | always-failing job lands in `dead` after `max_attempts`, appears in DLQ |
| `test_reaper.py` | stale `running` job requeued (fresh one spared), exhausted → dead, engine recovers a crashed job |
| `test_dag.py` | diamond order + result propagation, failing step blocks downstream, cycle/validation rejection, JSON+YAML load |
| `test_store.py` | store round-trip, priority + `run_at` ordering, complete/counts |
| `test_cron.py` | 5-field cron parsing / matching / next-run |
| `test_dashboard.py` | REST API: enqueue, list, detail, stats, submit workflow, cycle → 400 |

---

## Spec milestones → implementation

| # | Milestone | Where |
|---|---|---|
| 1 | **Job storage** with concurrent-safe claiming (Postgres `SKIP LOCKED` / SQLite equivalent) | `store/base.py`, `store/sqlite_store.py` (`BEGIN IMMEDIATE` + `RETURNING`), `store/postgres_store.py` (`FOR UPDATE SKIP LOCKED`) |
| 2 | **Scheduling & workers**: polling claim honouring `run_at`+priority, worker pool, task registry, delayed + periodic/cron jobs | `worker.py`, `scheduler.py`, `registry.py`, `cron.py` |
| 3 | **Reliability**: exponential backoff + jitter, dead-letter queue, heartbeats + reaper crash recovery, at-least-once | `retry.py`, `store/*.fail`/`reap`, `worker.py`, `scheduler.py` |
| 4 | **Workflow / DAG engine**: JSON/YAML DAGs, dependency eligibility, concurrency, failure blocking, cycle detection, result passing | `dag.py`, `dags/diamond.{json,yaml}` |
| 5 | **Dashboard UI + REST API** | `dashboard.py` |
| 6 | **CLI + example handlers + demo** | `cli.py`, `worker.py`/`dashboard.py`/`demo.py` `__main__`, `tasks.py`, `demo.py` |

---

## Production: swap SQLite for Postgres / Redis

The default SQLite backend is perfect for a single-node deployment or a demo and
gives the full no-double-claim guarantee. For multi-node / high-throughput
production, switch the backend with **one environment variable** — no engine code
changes:

```bash
pip install "psycopg[binary]==3.2.3" psycopg-pool==3.2.4
export DATABASE_URL="postgresql://user:pass@host:5432/jobq"
python -c "from jobq.config import get_store; get_store().init_schema()"   # once
python -m jobq.worker
```

**Why Postgres for production**
- `FOR UPDATE SKIP LOCKED` gives **row-level** claiming: many workers across many
  machines grab distinct jobs in parallel with no blocking (SQLite serialises
  writers and is single-host).
- Real client/server concurrency, replication, backups, connection pooling,
  `LISTEN/NOTIFY` for push-style wakeups instead of polling.

The `PostgresJobStore` is code-complete and mirrors the SQLite semantics exactly
(same `JobStore` interface, same state machine). It is **not exercised in this
repo's test/demo runs** because no Postgres server is available in the target
environment; `psycopg` is imported lazily so it never affects the default path.

**Redis / other backends.** The `JobStore` interface is the only contract the
engine depends on. A Redis backend would implement `claim` with a Lua script (or
`BRPOPLPUSH` into a processing list) for atomic pop, a sorted-set keyed by
`run_at`/priority for scheduling, and a periodic sweep of the processing list for
the reaper — the same state machine, a different atomic primitive.

---

## Project layout

```
project-07-job-queue/
├── pyproject.toml          # src layout, pinned deps, pytest config
├── requirements.txt        # pinned runtime + dev deps
├── .env.example            # DATABASE_URL and tunables
├── src/jobq/
│   ├── models.py           # Job, JobStatus, StepStatus, JobContext
│   ├── registry.py         # @task decorator / TaskRegistry
│   ├── retry.py            # exponential backoff + jitter
│   ├── cron.py             # 5-field cron evaluator
│   ├── config.py           # DATABASE_URL → store selection, EngineConfig
│   ├── store/
│   │   ├── base.py         # JobStore interface
│   │   ├── sqlite_store.py # DEFAULT — BEGIN IMMEDIATE + UPDATE … RETURNING
│   │   └── postgres_store.py # FOR UPDATE SKIP LOCKED (lazy import)
│   ├── worker.py           # async WorkerPool (connection per worker)
│   ├── scheduler.py        # reaper + workflow reconciler + periodic/cron
│   ├── dag.py              # DAG load/validate/cycle-detect/advance
│   ├── engine.py           # wires WorkerPool + Scheduler
│   ├── tasks.py            # example handlers
│   ├── dashboard.py        # FastAPI dashboard + REST API
│   ├── cli.py              # CLI
│   ├── demo.py             # end-to-end demo
│   └── dags/diamond.{json,yaml}
├── scripts/demo.py         # `python scripts/demo.py` wrapper
└── tests/                  # pytest + pytest-asyncio
```

MIT-licensed. Built to run fully offline with zero external services.

---

## Related projects

Part of my backend and distributed-systems series:

- [**realtime-collab-editor**](https://github.com/Urthella/realtime-collab-editor): CRDT sync over WebSockets, live presence, debounced persistence
- [**distributed-rate-limiter**](https://github.com/Urthella/distributed-rate-limiter): token bucket, sliding window and fixed window limits with atomic Redis Lua
- [**rag-codebase-qa**](https://github.com/Urthella/rag-codebase-qa): AST-aware chunking, hybrid BM25 + vector retrieval, cited answers
- [**event-driven-order-system**](https://github.com/Urthella/event-driven-order-system): saga orchestration, compensating transactions, idempotent consumers, tracing

More of my work: [github.com/Urthella](https://github.com/Urthella) · [utkudemirtas.tech](https://www.utkudemirtas.tech)
