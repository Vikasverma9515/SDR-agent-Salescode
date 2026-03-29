"""
Shared role-bucket selection mechanism.

Allows searcher.py (LangGraph node) to pause and wait for SDR to select
which functional role categories to include, without creating a circular
import with api.py.

Message type put into the queue:
  {"action": "select", "bucket_ids": ["c_suite", "marketing_brand", ...]}

Usage:
    # In searcher node — wait for SDR response
    queue = role_selection.register(thread_id)
    ...emit role_selection_required event...
    msg = await asyncio.wait_for(queue.get(), timeout=120)
    selected_bucket_ids = msg["bucket_ids"]
    role_selection.unregister(thread_id)

    # In api.py — receive SDR selection
    await role_selection.submit(thread_id, selected_bucket_ids)
"""
from __future__ import annotations
import asyncio

_queues: dict[str, asyncio.Queue] = {}


def register(thread_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _queues[thread_id] = q
    return q


def unregister(thread_id: str) -> None:
    _queues.pop(thread_id, None)


async def submit(thread_id: str, bucket_ids: list[str]) -> bool:
    """SDR confirmed their role bucket selection."""
    q = _queues.get(thread_id)
    if not q:
        return False
    await q.put({"action": "select", "bucket_ids": bucket_ids})
    return True


def is_waiting(thread_id: str) -> bool:
    return thread_id in _queues
