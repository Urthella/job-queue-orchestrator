"""Command-line interface.

    python -m jobq.cli worker                 # run a worker pool + scheduler
    python -m jobq.cli enqueue add --args '[1,2,3]'
    python -m jobq.cli submit-workflow src/jobq/dags/diamond.yaml
    python -m jobq.cli jobs [--status dead]
    python -m jobq.cli workers
    python -m jobq.cli dashboard              # serve the web dashboard (uvicorn)
    python -m jobq.cli demo                   # run the end-to-end demo

Shortcuts also exist: ``python -m jobq.worker``, ``python -m jobq.dashboard``,
``python -m jobq.demo``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from .config import DEFAULT_DATABASE_URL, EngineConfig, get_store


def _add_db_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--database-url",
        default=None,
        help=f"store URL (default env DATABASE_URL or {DEFAULT_DATABASE_URL})",
    )


def cmd_worker(args: argparse.Namespace) -> int:
    from .engine import run_forever

    cfg = EngineConfig.from_env()
    if args.database_url:
        cfg.database_url = args.database_url
    if args.concurrency:
        cfg.concurrency = args.concurrency
    try:
        asyncio.run(run_forever(cfg))
    except KeyboardInterrupt:
        pass
    return 0


def cmd_enqueue(args: argparse.Namespace) -> int:
    import jobq.tasks  # noqa: F401 - register example tasks

    store = get_store(args.database_url)
    store.init_schema()
    job_id = store.enqueue(
        args.task,
        args=json.loads(args.args) if args.args else [],
        kwargs=json.loads(args.kwargs) if args.kwargs else {},
        priority=args.priority,
        max_attempts=args.max_attempts,
        run_at=None if args.delay == 0 else __import__("time").time() + args.delay,
    )
    print(f"enqueued job {job_id} (task={args.task})")
    store.close()
    return 0


def cmd_submit_workflow(args: argparse.Namespace) -> int:
    import jobq.tasks  # noqa: F401
    from .dag import load_dag_file, submit_workflow

    store = get_store(args.database_url)
    store.init_schema()
    dag = load_dag_file(args.path)
    run_id = submit_workflow(store, dag)
    print(f"submitted workflow '{dag.name}' as run {run_id} ({len(dag.steps)} steps)")
    store.close()
    return 0


def cmd_jobs(args: argparse.Namespace) -> int:
    store = get_store(args.database_url)
    store.init_schema()
    jobs = store.list_jobs(status=args.status, limit=args.limit)
    for j in jobs:
        print(
            f"#{j.id:<5} {j.status:<10} {j.task:<16} "
            f"attempts={j.attempts}/{j.max_attempts} prio={j.priority}"
        )
    counts = store.counts_by_status()
    print("--")
    print("counts:", ", ".join(f"{k}={v}" for k, v in counts.items()))
    store.close()
    return 0


def cmd_workers(args: argparse.Namespace) -> int:
    store = get_store(args.database_url)
    store.init_schema()
    for w in store.list_workers():
        print(
            f"{w['id']:<20} status={w['status']:<8} "
            f"last_heartbeat={w['last_heartbeat']} job={w['current_job_id']}"
        )
    store.close()
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    from .dashboard import serve

    serve(host=args.host, port=args.port, database_url=args.database_url)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import main as demo_main

    return asyncio.run(demo_main(database_url=args.database_url))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobq", description="job queue engine CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("worker", help="run a worker pool + scheduler")
    _add_db_arg(p)
    p.add_argument("--concurrency", type=int, default=0, help="worker pool size")
    p.set_defaults(func=cmd_worker)

    p = sub.add_parser("enqueue", help="enqueue a job")
    _add_db_arg(p)
    p.add_argument("task")
    p.add_argument("--args", default=None, help="JSON list of positional args")
    p.add_argument("--kwargs", default=None, help="JSON object of keyword args")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--delay", type=float, default=0.0, help="delay seconds (run_at)")
    p.set_defaults(func=cmd_enqueue)

    p = sub.add_parser("submit-workflow", help="submit a DAG from JSON/YAML")
    _add_db_arg(p)
    p.add_argument("path")
    p.set_defaults(func=cmd_submit_workflow)

    p = sub.add_parser("jobs", help="list jobs")
    _add_db_arg(p)
    p.add_argument("--status", default=None)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_jobs)

    p = sub.add_parser("workers", help="list workers")
    _add_db_arg(p)
    p.set_defaults(func=cmd_workers)

    p = sub.add_parser("dashboard", help="serve the web dashboard")
    _add_db_arg(p)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=cmd_dashboard)

    p = sub.add_parser("demo", help="run the end-to-end demo")
    _add_db_arg(p)
    p.set_defaults(func=cmd_demo)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
