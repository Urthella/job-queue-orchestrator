#!/usr/bin/env python
"""Convenience wrapper so the demo can be run as ``python scripts/demo.py``.

Equivalent to ``python -m jobq.demo``. Requires the package to be importable
(``pip install -e .`` or run from the repo root with ``src`` on the path).
"""

import asyncio
import os
import sys

# Allow running straight from a checkout without installing.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from jobq.demo import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
