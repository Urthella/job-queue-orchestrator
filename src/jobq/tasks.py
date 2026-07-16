"""Example task handlers, registered on the default registry.

Import this module to make the handlers available:

    import jobq.tasks  # noqa: F401

Handlers take a single :class:`jobq.models.JobContext` and return a
JSON-serialisable result. They double as the building blocks of the example
diamond DAG (``src/jobq/dags/diamond.{json,yaml}``).

Idempotency note: because delivery is at-least-once, real handlers should be
idempotent (e.g. keyed by ``ctx.job_id`` or a business key). The examples here
are pure functions of their inputs, so they are naturally idempotent.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

from .models import JobContext
from .registry import task

# Thread-safe execution log used by tests/demo to assert ordering.
EXECUTION_LOG: "deque[tuple[str, str, float]]" = deque()
# Per-key attempt counters for the stateful-flaky example.
_ATTEMPT_COUNTS: dict[str, int] = {}
_LOCK = threading.Lock()


def _log(event: str, name: str) -> None:
    EXECUTION_LOG.append((event, name, time.monotonic()))


def _upstream_numbers(ctx: JobContext) -> list[float]:
    nums: list[float] = []
    for v in ctx.upstream.values():
        if isinstance(v, (int, float)):
            nums.append(v)
        elif isinstance(v, dict) and isinstance(v.get("value"), (int, float)):
            nums.append(v["value"])
    return nums


# ---- basic tasks ---------------------------------------------------------
@task("add")
def add(ctx: JobContext) -> int:
    """Return the sum of numeric positional args."""
    return sum(ctx.args)


@task("echo")
def echo(ctx: JobContext) -> Any:
    """Return kwargs['message'] (or args)."""
    return ctx.kwargs.get("message", ctx.args)


@task("sleep_task")
def sleep_task(ctx: JobContext) -> dict[str, Any]:
    """Sleep for kwargs['seconds'] then return. Useful for reaper demos."""
    secs = float(ctx.kwargs.get("seconds", 0.1))
    time.sleep(secs)
    return {"slept": secs, "job_id": ctx.job_id}


@task("recover_me")
def recover_me(ctx: JobContext) -> dict[str, Any]:
    """Trivial task used in the reaper/crash-recovery demo."""
    _log("run", ctx.task)
    return {"recovered": True, "job_id": ctx.job_id, "attempt": ctx.attempt}


# ---- retry / dead-letter examples ---------------------------------------
@task("flaky")
def flaky(ctx: JobContext) -> dict[str, Any]:
    """Fail on the first ``fail_times`` attempts, then succeed.

    Relies on ``ctx.attempt`` (incremented at claim time), so it works across
    real requeues/backoff without external state.
    """
    fail_times = int(ctx.kwargs.get("fail_times", 2))
    if ctx.attempt <= fail_times:
        raise RuntimeError(
            f"flaky failure on attempt {ctx.attempt} (will succeed after {fail_times})"
        )
    return {"ok": True, "succeeded_on_attempt": ctx.attempt}


@task("flaky_stateful")
def flaky_stateful(ctx: JobContext) -> dict[str, Any]:
    """Like ``flaky`` but keyed by an external ``key`` to show idempotency care."""
    key = str(ctx.kwargs.get("key", ctx.job_id))
    fail_times = int(ctx.kwargs.get("fail_times", 2))
    with _LOCK:
        _ATTEMPT_COUNTS[key] = _ATTEMPT_COUNTS.get(key, 0) + 1
        seen = _ATTEMPT_COUNTS[key]
    if seen <= fail_times:
        raise RuntimeError(f"flaky_stateful failure #{seen} for key={key}")
    return {"ok": True, "attempts_seen": seen}


@task("always_fail")
def always_fail(ctx: JobContext) -> None:
    """Always raise - used to demonstrate the dead-letter queue."""
    raise RuntimeError(
        ctx.kwargs.get("reason", "this task always fails (dead-letter demo)")
    )


# ---- diamond DAG tasks ---------------------------------------------------
@task("gen_number")
def gen_number(ctx: JobContext) -> int:
    """Root step: emit a seed number (args[0], default 10)."""
    _log("start", ctx.step_name or ctx.task)
    time.sleep(float(ctx.kwargs.get("delay", 0.02)))
    val = int(ctx.args[0]) if ctx.args else 10
    _log("end", ctx.step_name or ctx.task)
    return val


@task("add_const")
def add_const(ctx: JobContext) -> int:
    """Add kwargs['c'] to the sum of upstream results."""
    _log("start", ctx.step_name or ctx.task)
    time.sleep(float(ctx.kwargs.get("delay", 0.02)))
    base = sum(_upstream_numbers(ctx))
    _log("end", ctx.step_name or ctx.task)
    return int(base + ctx.kwargs.get("c", 0))


@task("mul_const")
def mul_const(ctx: JobContext) -> int:
    """Multiply the sum of upstream results by kwargs['m']."""
    _log("start", ctx.step_name or ctx.task)
    time.sleep(float(ctx.kwargs.get("delay", 0.02)))
    base = sum(_upstream_numbers(ctx))
    _log("end", ctx.step_name or ctx.task)
    return int(base * ctx.kwargs.get("m", 1))


@task("sum_upstream")
def sum_upstream(ctx: JobContext) -> int:
    """Sink step: sum all upstream results."""
    _log("start", ctx.step_name or ctx.task)
    _log("end", ctx.step_name or ctx.task)
    return int(sum(_upstream_numbers(ctx)))


def reset_logs() -> None:
    EXECUTION_LOG.clear()
    with _LOCK:
        _ATTEMPT_COUNTS.clear()
