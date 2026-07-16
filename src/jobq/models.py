"""Core data models and enums for the job queue engine."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """Execution states of a single job.

    State machine:
        queued -> running -> succeeded
                          -> failed  (transient; requeued as queued if attempts remain)
                          -> dead    (retries exhausted -> dead-letter queue)
        running -> queued  (reaper requeue of a crashed/stuck worker's job)
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"


class StepStatus(str, enum.Enum):
    """State of a single step inside a workflow run."""

    PENDING = "pending"      # not yet eligible (waiting on upstream deps)
    QUEUED = "queued"        # eligible; a job has been enqueued
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"      # an upstream dependency failed


class WorkflowStatus(str, enum.Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class Job:
    """A claimed unit of work handed to a worker."""

    id: int
    task: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: str = JobStatus.QUEUED.value
    priority: int = 0
    run_at: float = 0.0
    attempts: int = 0
    max_attempts: int = 3
    worker_id: str | None = None
    locked_at: float | None = None
    last_heartbeat: float | None = None
    result: Any = None
    error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    workflow_run_id: int | None = None
    step_name: str | None = None
    backoff_base: float = 0.5
    backoff_factor: float = 2.0
    backoff_max: float = 30.0
    jitter: float = 0.1

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "task": self.task,
            "args": self.args,
            "kwargs": self.kwargs,
            "status": self.status,
            "priority": self.priority,
            "run_at": self.run_at,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "worker_id": self.worker_id,
            "locked_at": self.locked_at,
            "last_heartbeat": self.last_heartbeat,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "workflow_run_id": self.workflow_run_id,
            "step_name": self.step_name,
        }


@dataclass
class JobContext:
    """Passed to every task handler. Handlers are called as ``handler(ctx)``.

    ``upstream`` carries the results of upstream workflow steps (keyed by step
    name) when this job belongs to a workflow; it is empty for standalone jobs.
    """

    job_id: int
    task: str
    args: list[Any]
    kwargs: dict[str, Any]
    attempt: int
    max_attempts: int
    workflow_run_id: int | None = None
    step_name: str | None = None
    upstream: dict[str, Any] = field(default_factory=dict)
