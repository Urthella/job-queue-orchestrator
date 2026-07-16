"""Engine: wires a WorkerPool + Scheduler over a JobStore.

Convenience layer used by the CLI, the ``python -m jobq.worker`` entrypoint, and
the demo. Not required to use the library - you can drive WorkerPool and
Scheduler directly.
"""

from __future__ import annotations

import asyncio
import signal

from .config import EngineConfig, get_store
from .registry import TaskRegistry, get_registry
from .scheduler import Scheduler
from .store.base import JobStore
from .worker import WorkerPool


class Engine:
    def __init__(
        self,
        store: JobStore,
        registry: TaskRegistry | None = None,
        *,
        concurrency: int = 4,
        poll_interval: float = 0.05,
        heartbeat_interval: float = 1.0,
        reap_interval: float = 1.0,
        heartbeat_timeout: float = 5.0,
    ) -> None:
        self.store = store
        self.pool = WorkerPool(
            store,
            registry or get_registry(),
            concurrency=concurrency,
            poll_interval=poll_interval,
            heartbeat_interval=heartbeat_interval,
        )
        self.scheduler = Scheduler(
            store,
            reap_interval=reap_interval,
            heartbeat_timeout=heartbeat_timeout,
        )

    async def start(self) -> None:
        self.store.init_schema()
        await self.scheduler.start()
        await self.pool.start()

    async def stop(self, *, drain: bool = False) -> None:
        await self.scheduler.stop()
        await self.pool.stop(drain=drain)


async def run_forever(config: EngineConfig | None = None) -> None:
    """Start an engine and run until SIGINT/SIGTERM."""
    config = config or EngineConfig.from_env()
    # Ensure example tasks are importable/registered for the standalone worker.
    import jobq.tasks  # noqa: F401

    store = get_store(config.database_url)
    engine = Engine(
        store,
        concurrency=config.concurrency,
        poll_interval=config.poll_interval,
        heartbeat_interval=config.heartbeat_interval,
        reap_interval=config.reap_interval,
        heartbeat_timeout=config.heartbeat_timeout,
    )
    await engine.start()
    print(
        f"[jobq] worker pool started: concurrency={config.concurrency} "
        f"db={config.database_url}  (Ctrl-C to stop)"
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:  # pragma: no cover - Windows
                pass
    except Exception:  # pragma: no cover
        pass

    try:
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):  # pragma: no cover
        pass
    finally:
        print("\n[jobq] stopping...")
        await engine.stop()
        store.close()
        print("[jobq] stopped.")
