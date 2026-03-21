"""
Structured logging configuration using structlog.
Every log entry is JSON with agent, step, and timestamp tags.
"""
import logging
import os
import sys
from pathlib import Path

import structlog

_configured = False


def configure_logging(log_dir: str = "logs", level: str = "INFO") -> None:
    global _configured
    if _configured:
        return

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(
            file=open(Path(log_dir) / "pipeline.jsonl", "a", buffering=1)
        ),
        cache_logger_on_first_use=True,
    )

    # Also emit to stderr for console visibility
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    _configured = True


def get_logger(agent: str = "pipeline", **initial_ctx):
    """Return a bound structlog logger with agent tag."""
    return structlog.get_logger(agent=agent, **initial_ctx)
