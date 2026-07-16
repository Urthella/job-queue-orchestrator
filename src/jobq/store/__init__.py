"""Job store implementations.

- :class:`jobq.store.base.JobStore` - the abstract interface.
- :class:`jobq.store.sqlite_store.SQLiteJobStore` - default, race-safe via
  ``BEGIN IMMEDIATE`` + atomic ``UPDATE ... RETURNING``.
- :class:`jobq.store.postgres_store.PostgresJobStore` - code-complete adapter
  using ``FOR UPDATE SKIP LOCKED`` (imported lazily; requires ``psycopg``).
"""

from .base import JobStore, ClaimResult, FailOutcome

__all__ = ["JobStore", "ClaimResult", "FailOutcome"]
