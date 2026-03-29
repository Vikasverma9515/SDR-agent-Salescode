"""
Shared DM selection mechanism.

Allows searcher.py (LangGraph node) to pause and wait for SDR input
without creating a circular import with api.py.

Message types put into the queue:
  {"action": "select",    "indices": [0, 1, 2]}   — SDR confirmed their pick
  {"action": "find_more", "prompt": "find CEO..."}  — SDR wants more candidates

Usage:
    # In searcher node — wait for SDR response
    queue = dm_selection.register(thread_id)
    ...emit contact_selection_required event...
    msg = await asyncio.wait_for(queue.get(), timeout=300)
    if msg["action"] == "select":
        ...
    elif msg["action"] == "find_more":
        ...
    dm_selection.unregister(thread_id)

    # In api.py — receive SDR selection
    await dm_selection.submit(thread_id, selected_indices)

    # In api.py — SDR wants more candidates
    await dm_selection.request_more(thread_id, prompt)
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


async def submit(thread_id: str, selected_indices: list[int]) -> bool:
    """SDR confirmed their final selection."""
    q = _queues.get(thread_id)
    if not q:
        return False
    await q.put({"action": "select", "indices": selected_indices})
    return True


async def request_more(thread_id: str, prompt: str) -> bool:
    """SDR requested additional search with a natural-language prompt."""
    q = _queues.get(thread_id)
    if not q:
        return False
    await q.put({"action": "find_more", "prompt": prompt})
    return True


def is_waiting(thread_id: str) -> bool:
    return thread_id in _queues
