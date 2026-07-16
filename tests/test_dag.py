"""Workflow / DAG engine: eligibility, ordering, blocking, cycle detection."""

from __future__ import annotations

import asyncio

import pytest

from jobq.dag import (
    CycleError,
    DagValidationError,
    detect_cycle,
    load_dag,
    load_dag_file,
    submit_workflow,
    topological_order,
)
from jobq.engine import Engine
from jobq.models import StepStatus, WorkflowStatus


# ---- static validation ---------------------------------------------------
def test_cycle_detection_rejects_cyclic_dag():
    cyclic = {
        "name": "bad",
        "steps": [
            {"name": "A", "task": "add", "depends_on": ["C"]},
            {"name": "B", "task": "add", "depends_on": ["A"]},
            {"name": "C", "task": "add", "depends_on": ["B"]},
        ],
    }
    with pytest.raises(CycleError):
        load_dag(cyclic)


def test_self_dependency_rejected():
    with pytest.raises((CycleError, DagValidationError)):
        load_dag({"name": "x", "steps": [{"name": "A", "task": "add", "depends_on": ["A"]}]})


def test_unknown_dependency_rejected():
    with pytest.raises(DagValidationError):
        load_dag({"name": "x", "steps": [{"name": "A", "task": "add", "depends_on": ["ZZ"]}]})


def test_topological_order_of_diamond():
    dag = load_dag(
        {
            "name": "d",
            "steps": [
                {"name": "A", "task": "gen_number", "depends_on": []},
                {"name": "B", "task": "add_const", "depends_on": ["A"]},
                {"name": "C", "task": "mul_const", "depends_on": ["A"]},
                {"name": "D", "task": "sum_upstream", "depends_on": ["B", "C"]},
            ],
        }
    )
    order = topological_order(dag)
    assert order.index("A") < order.index("B")
    assert order.index("A") < order.index("C")
    assert order.index("B") < order.index("D")
    assert order.index("C") < order.index("D")


def test_diamond_files_load():
    import os

    base = os.path.join(os.path.dirname(__file__), "..", "src", "jobq", "dags")
    for fname in ("diamond.json", "diamond.yaml"):
        dag = load_dag_file(os.path.join(base, fname))
        assert dag.name == "diamond"
        assert {s.name for s in dag.steps} == {"A", "B", "C", "D"}


# ---- live execution ------------------------------------------------------
@pytest.mark.asyncio
async def test_diamond_executes_with_correct_order_and_result(store):
    import jobq.tasks as extasks  # noqa: F401

    extasks.reset_logs()
    dag = load_dag(
        {
            "name": "diamond",
            "steps": [
                {"name": "A", "task": "gen_number", "args": [10], "depends_on": []},
                {"name": "B", "task": "add_const", "kwargs": {"c": 5}, "depends_on": ["A"]},
                {"name": "C", "task": "mul_const", "kwargs": {"m": 3}, "depends_on": ["A"]},
                {"name": "D", "task": "sum_upstream", "depends_on": ["B", "C"]},
            ],
        }
    )
    engine = Engine(store, concurrency=4, poll_interval=0.01, heartbeat_interval=0.2)
    await engine.start()
    try:
        run_id = submit_workflow(store, dag)
        for _ in range(600):
            run = store.get_workflow_run(run_id)
            if run["status"] != WorkflowStatus.RUNNING.value:
                break
            await asyncio.sleep(0.02)

        run = store.get_workflow_run(run_id)
        assert run["status"] == WorkflowStatus.SUCCEEDED.value
        steps = {s["name"]: s for s in store.list_workflow_steps(run_id)}
        assert all(s["status"] == StepStatus.SUCCEEDED.value for s in steps.values())

        # Result propagation: A=10, B=A+5=15, C=A*3=30, D=B+C=45.
        assert steps["A"]["result"] == 10
        assert steps["B"]["result"] == 15
        assert steps["C"]["result"] == 30
        assert steps["D"]["result"] == 45

        # Ordering constraints from the execution log (start/end events).
        log = list(extasks.EXECUTION_LOG)
        starts = {name: t for (evt, name, t) in log if evt == "start"}
        ends = {name: t for (evt, name, t) in log if evt == "end"}
        assert ends["A"] <= starts["B"] and ends["A"] <= starts["C"]  # B,C after A
        assert ends["B"] <= starts["D"] and ends["C"] <= starts["D"]  # D after B,C
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_failing_step_blocks_downstream_and_fails_workflow(store):
    import jobq.tasks  # noqa: F401

    # A(ok) -> B(always_fail) -> C ; and A -> D(ok). B fails => C blocked,
    # workflow fails; the independent branch D still succeeds.
    dag = load_dag(
        {
            "name": "with_failure",
            "steps": [
                {"name": "A", "task": "gen_number", "args": [1], "depends_on": []},
                {"name": "B", "task": "always_fail", "depends_on": ["A"],
                 "max_attempts": 1, "backoff_base": 0.02},
                {"name": "C", "task": "sum_upstream", "depends_on": ["B"]},
                {"name": "D", "task": "add_const", "kwargs": {"c": 7}, "depends_on": ["A"]},
            ],
        }
    )
    engine = Engine(store, concurrency=4, poll_interval=0.01, heartbeat_interval=0.2)
    await engine.start()
    try:
        run_id = submit_workflow(store, dag)
        for _ in range(600):
            run = store.get_workflow_run(run_id)
            if run["status"] != WorkflowStatus.RUNNING.value:
                break
            await asyncio.sleep(0.02)

        run = store.get_workflow_run(run_id)
        steps = {s["name"]: s for s in store.list_workflow_steps(run_id)}
        assert run["status"] == WorkflowStatus.FAILED.value
        assert steps["A"]["status"] == StepStatus.SUCCEEDED.value
        assert steps["B"]["status"] == StepStatus.FAILED.value
        assert steps["C"]["status"] == StepStatus.BLOCKED.value      # blocked by B
        assert steps["D"]["status"] == StepStatus.SUCCEEDED.value    # independent branch
    finally:
        await engine.stop()
