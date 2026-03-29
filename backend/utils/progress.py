"""
Lightweight per-company progress event emitter.
Decouples fini.py from api.py by using a shared registry.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

_queues: dict[str, asyncio.Queue] = {}


def register(thread_id: str, queue: asyncio.Queue) -> None:
    _queues[thread_id] = queue


def unregister(thread_id: str) -> None:
    _queues.pop(thread_id, None)


async def emit(thread_id: str | None, company: str, status: str) -> None:
    """Emit a company_progress event to the WebSocket queue for thread_id."""
    if not thread_id:
        return
    q = _queues.get(thread_id)
    if not q:
        return
    try:
        await q.put({
            "type": "company_progress",
            "data": {"company": company, "status": status},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass


async def emit_log(thread_id: str | None, message: str, level: str = "info") -> None:
    """Emit a log-line event to the WebSocket pipeline log."""
    if not thread_id:
        return
    q = _queues.get(thread_id)
    if not q:
        return
    try:
        await q.put({
            "type": "log",
            "level": level,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass


async def emit_veri_step(
    thread_id: str | None,
    name: str,
    company: str,
    phase: str,    # "web" | "linkedin_zb" | "scoring"
    step: str,     # "ddg" | "theorg" | "perplexity" | "tavily" | "linkedin_discovery"
                   # | "linkedin" | "zerobounce" | "llm_title" | "llm_reason"
                   # | "verdict" | "sheet"
    detail: str,   # human-readable result text
    level: str = "info",  # "info" | "success" | "warning" | "error"
) -> None:
    """Emit a granular per-step event so the frontend activity feed shows every action."""
    if not thread_id:
        return
    q = _queues.get(thread_id)
    if not q:
        return
    try:
        await q.put({
            "type": "veri_step",
            "data": {
                "name": name,
                "company": company,
                "phase": phase,
                "step": step,
                "detail": detail,
                "level": level,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass


async def emit_veri_contact(
    thread_id: str | None,
    name: str,
    company: str,
    phase: str,      # "queued" | "web" | "linkedin_zb" | "scoring" | "done"
    status: str = "",  # "VERIFIED" | "REVIEW" | "REJECT" (for done phase)
) -> None:
    """Emit a veri_contact event so the frontend can track per-contact progress."""
    if not thread_id:
        return
    q = _queues.get(thread_id)
    if not q:
        return
    try:
        await q.put({
            "type": "veri_contact",
            "data": {
                "name": name,
                "company": company,
                "phase": phase,
                "status": status,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass


async def emit_contact(
    thread_id: str | None,
    full_name: str,
    role_title: str,
    role_bucket: str,
    company: str,
    email: str = "",
    linkedin_verified: bool = False,
) -> None:
    """Emit a contact_written event when a contact is written to the sheet."""
    if not thread_id:
        return
    q = _queues.get(thread_id)
    if not q:
        return
    try:
        await q.put({
            "type": "contact_written",
            "data": {
                "full_name": full_name,
                "role_title": role_title,
                "role_bucket": role_bucket,
                "company": company,
                "email": email,
                "linkedin_verified": linkedin_verified,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass
