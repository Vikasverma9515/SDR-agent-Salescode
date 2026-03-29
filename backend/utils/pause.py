"""
Pause gate utility for pipeline runs.

Each run gets an asyncio.Event that starts "set" (not paused).
- pause(thread_id)  → clears the event  → agents block on await_if_paused()
- resume(thread_id) → sets the event    → agents unblock
- await_if_paused(thread_id) → call at natural checkpoint boundaries
"""
from __future__ import annotations

import asyncio
from typing import Callable

_gates: dict[str, asyncio.Event] = {}
_on_paused_callbacks: dict[str, Callable] = {}


def register(thread_id: str) -> None:
    """Create a new gate for a run (starts unpaused)."""
    event = asyncio.Event()
    event.set()  # set = not paused
    _gates[thread_id] = event


def unregister(thread_id: str) -> None:
    """Remove the gate when a run completes or is cancelled."""
    _gates.pop(thread_id, None)
    _on_paused_callbacks.pop(thread_id, None)


def pause(thread_id: str) -> bool:
    """Pause the run. Returns True if gate existed."""
    gate = _gates.get(thread_id)
    if gate is None:
        return False
    gate.clear()
    return True


def resume(thread_id: str) -> bool:
    """Resume the run. Returns True if gate existed."""
    gate = _gates.get(thread_id)
    if gate is None:
        return False
    gate.set()
    return True


def is_paused(thread_id: str) -> bool:
    gate = _gates.get(thread_id)
    return gate is not None and not gate.is_set()


async def await_if_paused(thread_id: str) -> None:
    """
    Call this at natural checkpoints (between companies, between contacts).
    Blocks until the run is resumed. No-op if not paused.
    """
    gate = _gates.get(thread_id)
    if gate is None:
        return
    if not gate.is_set():
        await gate.wait()
