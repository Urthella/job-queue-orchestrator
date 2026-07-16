"""jobq - a from-scratch Job Queue & Workflow Orchestration Engine.

A mini Celery/Sidekiq built on Python 3.12 + asyncio with a race-safe
SQLite-backed job store (default) and a code-complete PostgreSQL adapter.

Public API:
    from jobq import task, JobContext, JobStatus
    from jobq.config import get_store
"""

from .models import JobContext, JobStatus, Job, StepStatus, WorkflowStatus
from .registry import task, get_registry, TaskRegistry

__all__ = [
    "task",
    "get_registry",
    "TaskRegistry",
    "JobContext",
    "JobStatus",
    "Job",
    "StepStatus",
    "WorkflowStatus",
    "__version__",
]

__version__ = "0.1.0"
