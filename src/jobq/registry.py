"""Task registry: register handlers by name via a decorator.

    from jobq.registry import task

    @task("send_email")
    def send_email(ctx):
        ...
        return {"delivered": True}

Handlers receive a single :class:`jobq.models.JobContext` argument and return a
JSON-serialisable result (or ``None``). Both synchronous and ``async`` handlers
are supported by the worker pool.
"""

from __future__ import annotations

from typing import Callable


class TaskRegistry:
    """A name -> handler mapping."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable] = {}

    def task(self, name: str | None = None) -> Callable:
        """Decorator that registers a handler under ``name`` (defaults to func name)."""

        def decorator(fn: Callable) -> Callable:
            key = name or fn.__name__
            if key in self._handlers:
                raise ValueError(f"task {key!r} is already registered")
            self._handlers[key] = fn
            return fn

        return decorator

    def register(self, name: str, fn: Callable) -> None:
        if name in self._handlers:
            raise ValueError(f"task {name!r} is already registered")
        self._handlers[name] = fn

    def get(self, name: str) -> Callable:
        try:
            return self._handlers[name]
        except KeyError:
            raise KeyError(
                f"no handler registered for task {name!r}; "
                f"registered tasks: {sorted(self._handlers)}"
            ) from None

    def has(self, name: str) -> bool:
        return name in self._handlers

    def names(self) -> list[str]:
        return sorted(self._handlers)


# A process-wide default registry so example tasks and the CLI share handlers.
_DEFAULT = TaskRegistry()


def get_registry() -> TaskRegistry:
    return _DEFAULT


def task(name: str | None = None) -> Callable:
    """Register a handler on the default registry."""
    return _DEFAULT.task(name)
