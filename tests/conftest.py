"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest

from jobq.store.sqlite_store import SQLiteJobStore


@pytest.fixture()
def db_path(tmp_path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture()
def store(db_path):
    """A fresh, initialised SQLite store on a temp file (WAL)."""
    s = SQLiteJobStore(db_path, busy_timeout_ms=5000)
    s.init_schema()
    yield s
    s.close()


@pytest.fixture(autouse=True)
def _reset_example_task_logs():
    # Keep the module-level execution log clean between tests.
    from jobq import tasks

    tasks.reset_logs()
    yield
    tasks.reset_logs()
