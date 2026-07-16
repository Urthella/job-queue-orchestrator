"""Configuration and store selection.

The backend is chosen from ``DATABASE_URL``:

    sqlite:///./jobq.db            -> SQLiteJobStore (default)
    postgresql://user@host/db      -> PostgresJobStore (lazy import)

Other tunables (all optional, with sane defaults):

    JOBQ_CONCURRENCY        worker pool size          (default 4)
    JOBQ_POLL_INTERVAL      empty-queue poll seconds  (default 0.05)
    JOBQ_HEARTBEAT_INTERVAL job heartbeat seconds     (default 1.0)
    JOBQ_REAP_INTERVAL      reaper tick seconds       (default 1.0)
    JOBQ_HEARTBEAT_TIMEOUT  stale-lock threshold secs (default 5.0)
    JOBQ_DASHBOARD_HOST     dashboard bind host       (default 127.0.0.1)
    JOBQ_DASHBOARD_PORT     dashboard port            (default 8000)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

from .store.base import JobStore

DEFAULT_DATABASE_URL = "sqlite:///./jobq.db"


def _sqlite_path_from_url(url: str) -> str:
    # sqlite:///relative/path.db  or  sqlite:////abs/path.db  or sqlite:///:memory:
    rest = url[len("sqlite://") :]
    # rest starts with '/'; strip exactly one leading slash for a relative path.
    if rest.startswith("/"):
        rest = rest[1:]
    return rest or "jobq.db"


def get_store(database_url: str | None = None) -> JobStore:
    """Return a JobStore for ``database_url`` (or the ``DATABASE_URL`` env var).

    Never reads secrets beyond the connection URL you pass or set in the env.
    """
    url = database_url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    scheme = urlparse(url).scheme

    if scheme.startswith("postgres"):  # postgres:// or postgresql://
        # Lazy import so the default path never depends on psycopg.
        from .store.postgres_store import PostgresJobStore

        return PostgresJobStore(url)

    if scheme == "sqlite":
        from .store.sqlite_store import SQLiteJobStore

        return SQLiteJobStore(_sqlite_path_from_url(url))

    # Bare path fallback (treat as sqlite file path).
    from .store.sqlite_store import SQLiteJobStore

    return SQLiteJobStore(url)


@dataclass
class EngineConfig:
    database_url: str = DEFAULT_DATABASE_URL
    concurrency: int = 4
    poll_interval: float = 0.05
    heartbeat_interval: float = 1.0
    reap_interval: float = 1.0
    heartbeat_timeout: float = 5.0
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8000

    @classmethod
    def from_env(cls) -> "EngineConfig":
        env = os.environ.get
        return cls(
            database_url=env("DATABASE_URL", DEFAULT_DATABASE_URL),
            concurrency=int(env("JOBQ_CONCURRENCY", "4")),
            poll_interval=float(env("JOBQ_POLL_INTERVAL", "0.05")),
            heartbeat_interval=float(env("JOBQ_HEARTBEAT_INTERVAL", "1.0")),
            reap_interval=float(env("JOBQ_REAP_INTERVAL", "1.0")),
            heartbeat_timeout=float(env("JOBQ_HEARTBEAT_TIMEOUT", "5.0")),
            dashboard_host=env("JOBQ_DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=int(env("JOBQ_DASHBOARD_PORT", "8000")),
        )
