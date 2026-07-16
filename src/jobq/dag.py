"""Workflow / DAG engine.

A workflow is a set of named steps with dependencies. A step becomes eligible
only when *all* its upstream dependencies have succeeded; independent steps run
concurrently (each is a normal job on the queue). A failed step (after its
retries) blocks its downstream dependents and fails the workflow.

Definitions load from a plain dict, JSON, or YAML. Cycles are rejected at load
time. Step results flow to dependents via the workflow ``context`` dict, exposed
to handlers as ``ctx.upstream`` (a ``{dep_name: result}`` mapping) through the
reserved ``__upstream__`` kwarg.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .models import JobStatus, StepStatus, WorkflowStatus
from .store.base import JobStore

UPSTREAM_KEY = "__upstream__"


class CycleError(ValueError):
    """Raised when a DAG definition contains a cycle."""


class DagValidationError(ValueError):
    """Raised for malformed DAG definitions (missing deps, dup names, ...)."""


@dataclass
class DagStep:
    name: str
    task: str
    depends_on: list[str] = field(default_factory=list)
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    max_attempts: int = 3
    backoff_base: float = 0.2
    priority: int = 0


@dataclass
class DagDefinition:
    name: str
    steps: list[DagStep]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "steps": [
                {
                    "name": s.name,
                    "task": s.task,
                    "depends_on": s.depends_on,
                    "args": s.args,
                    "kwargs": s.kwargs,
                    "max_attempts": s.max_attempts,
                }
                for s in self.steps
            ],
        }


# ---- loading & validation -----------------------------------------------
def load_dag(data: dict[str, Any]) -> DagDefinition:
    """Build and validate a :class:`DagDefinition` from a plain dict."""
    if "steps" not in data or not isinstance(data["steps"], list):
        raise DagValidationError("DAG definition needs a 'steps' list")
    name = data.get("name", "workflow")
    steps: list[DagStep] = []
    seen: set[str] = set()
    for raw in data["steps"]:
        if "name" not in raw or "task" not in raw:
            raise DagValidationError(f"each step needs 'name' and 'task': {raw!r}")
        sname = raw["name"]
        if sname in seen:
            raise DagValidationError(f"duplicate step name {sname!r}")
        seen.add(sname)
        steps.append(
            DagStep(
                name=sname,
                task=raw["task"],
                depends_on=list(raw.get("depends_on", [])),
                args=list(raw.get("args", [])),
                kwargs=dict(raw.get("kwargs", {})),
                max_attempts=int(raw.get("max_attempts", 3)),
                backoff_base=float(raw.get("backoff_base", 0.2)),
                priority=int(raw.get("priority", 0)),
            )
        )
    _validate_deps(steps)
    detect_cycle(steps)  # raises CycleError
    return DagDefinition(name=name, steps=steps)


def load_dag_file(path: str) -> DagDefinition:
    """Load a DAG from a ``.json``, ``.yaml`` or ``.yml`` file."""
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "YAML DAG definitions require PyYAML: pip install pyyaml"
            ) from exc
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    return load_dag(data)


def _validate_deps(steps: list[DagStep]) -> None:
    names = {s.name for s in steps}
    for s in steps:
        for dep in s.depends_on:
            if dep not in names:
                raise DagValidationError(
                    f"step {s.name!r} depends on unknown step {dep!r}"
                )
            if dep == s.name:
                raise CycleError(f"step {s.name!r} depends on itself")


def detect_cycle(steps: list[DagStep]) -> None:
    """Raise :class:`CycleError` if the dependency graph has a cycle.

    Uses Kahn's algorithm: if we cannot topologically order every node, the
    remaining nodes form a cycle.
    """
    indeg: dict[str, int] = {s.name: 0 for s in steps}
    adj: dict[str, list[str]] = {s.name: [] for s in steps}
    for s in steps:
        for dep in s.depends_on:
            adj[dep].append(s.name)
            indeg[s.name] += 1
    queue = [n for n, d in indeg.items() if d == 0]
    visited = 0
    while queue:
        n = queue.pop()
        visited += 1
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    if visited != len(steps):
        remaining = [n for n, d in indeg.items() if d > 0]
        raise CycleError(f"cycle detected among steps: {sorted(remaining)}")


def topological_order(dag: DagDefinition) -> list[str]:
    """Return one valid topological ordering of step names."""
    indeg: dict[str, int] = {s.name: 0 for s in dag.steps}
    adj: dict[str, list[str]] = {s.name: [] for s in dag.steps}
    for s in dag.steps:
        for dep in s.depends_on:
            adj[dep].append(s.name)
            indeg[s.name] += 1
    queue = sorted(n for n, d in indeg.items() if d == 0)
    order: list[str] = []
    while queue:
        n = queue.pop(0)
        order.append(n)
        for m in sorted(adj[n]):
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    return order


# ---- submission & advancement -------------------------------------------
def submit_workflow(store: JobStore, dag: DagDefinition, *, now: float | None = None) -> int:
    """Create a workflow run and its step rows, then enqueue root steps.

    Returns the workflow run id.
    """
    detect_cycle(dag.steps)  # defensive
    run_id = store.create_workflow_run(dag.name, dag.to_dict(), now=now)
    for s in dag.steps:
        store.add_workflow_step(
            run_id,
            s.name,
            s.task,
            s.depends_on,
            args=s.args,
            kwargs=s.kwargs,
            max_attempts=s.max_attempts,
            backoff_base=s.backoff_base,
            priority=s.priority,
        )
    advance_workflow(store, run_id, now=now)
    return run_id


def advance_workflow(store: JobStore, run_id: int, *, now: float | None = None) -> str:
    """Reconcile a workflow run: sync step states from their jobs, enqueue
    newly-eligible steps, propagate blocking, and update the run status.

    Returns the workflow run status after this tick.
    """
    run = store.get_workflow_run(run_id)
    if run is None:
        raise KeyError(f"no workflow run {run_id}")
    if run["status"] != WorkflowStatus.RUNNING.value:
        return run["status"]

    steps = store.list_workflow_steps(run_id)
    by_name = {s["name"]: s for s in steps}
    context = run["context"] if isinstance(run["context"], dict) else {}

    # 1. Sync each step's status from its backing job.
    for s in steps:
        if s["job_id"] is None:
            continue
        if s["status"] in (StepStatus.SUCCEEDED.value, StepStatus.FAILED.value):
            continue
        job = store.get_job(s["job_id"])
        if job is None:
            continue
        if job.status == JobStatus.SUCCEEDED.value:
            store.update_workflow_step(
                s["id"], status=StepStatus.SUCCEEDED.value, result=job.result
            )
            s["status"] = StepStatus.SUCCEEDED.value
            context[s["name"]] = job.result
        elif job.status == JobStatus.DEAD.value:
            store.update_workflow_step(
                s["id"], status=StepStatus.FAILED.value, error=job.error
            )
            s["status"] = StepStatus.FAILED.value
        elif job.status == JobStatus.RUNNING.value:
            if s["status"] != StepStatus.RUNNING.value:
                store.update_workflow_step(s["id"], status=StepStatus.RUNNING.value)
                s["status"] = StepStatus.RUNNING.value

    # 2. Propagate BLOCKED: a step is blocked if any upstream failed/blocked.
    changed = True
    while changed:
        changed = False
        for s in steps:
            if s["status"] in (
                StepStatus.SUCCEEDED.value,
                StepStatus.FAILED.value,
                StepStatus.BLOCKED.value,
            ):
                continue
            for dep in s["depends_on"]:
                dstat = by_name[dep]["status"]
                if dstat in (StepStatus.FAILED.value, StepStatus.BLOCKED.value):
                    store.update_workflow_step(s["id"], status=StepStatus.BLOCKED.value)
                    s["status"] = StepStatus.BLOCKED.value
                    changed = True
                    break

    # 3. Enqueue newly-eligible steps (pending + all deps succeeded).
    for s in steps:
        if s["status"] != StepStatus.PENDING.value:
            continue
        deps = s["depends_on"]
        if all(by_name[d]["status"] == StepStatus.SUCCEEDED.value for d in deps):
            upstream = {d: context.get(d) for d in deps}
            kwargs = dict(s["kwargs"])
            kwargs[UPSTREAM_KEY] = upstream
            job_id = store.enqueue(
                s["task"],
                args=s["args"],
                kwargs=kwargs,
                priority=s.get("priority", 0),
                max_attempts=s.get("max_attempts", 3),
                backoff_base=s.get("backoff_base", 0.2),
                workflow_run_id=run_id,
                step_name=s["name"],
                now=now,
            )
            store.update_workflow_step(
                s["id"], status=StepStatus.QUEUED.value, job_id=job_id
            )
            s["status"] = StepStatus.QUEUED.value

    # 4. Persist context and recompute run status.
    store.update_workflow_run(run_id, context=context, now=now)
    statuses = [s["status"] for s in steps]
    if all(st == StepStatus.SUCCEEDED.value for st in statuses):
        store.update_workflow_run(run_id, status=WorkflowStatus.SUCCEEDED.value, now=now)
        return WorkflowStatus.SUCCEEDED.value
    terminal = {
        StepStatus.SUCCEEDED.value,
        StepStatus.FAILED.value,
        StepStatus.BLOCKED.value,
    }
    if all(st in terminal for st in statuses) and any(
        st in (StepStatus.FAILED.value, StepStatus.BLOCKED.value) for st in statuses
    ):
        store.update_workflow_run(run_id, status=WorkflowStatus.FAILED.value, now=now)
        return WorkflowStatus.FAILED.value
    return WorkflowStatus.RUNNING.value
