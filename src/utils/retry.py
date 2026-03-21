"""
Tenacity-based retry decorators with exponential backoff.
Every external call (API, scrape, sheet) must use these.
"""
import functools
from typing import Any, Callable, Type

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)
import logging

logger = logging.getLogger(__name__)


def with_retry(
    *exception_types: Type[Exception],
    attempts: int = 3,
    min_wait: float = 2,
    max_wait: float = 60,
):
    """
    Decorator factory that wraps any async function with tenacity retry logic.

    Usage:
        @with_retry(httpx.HTTPError, gspread.exceptions.APIError, attempts=3)
        async def my_api_call():
            ...
    """
    if not exception_types:
        exception_types = (Exception,)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retrying = retry(
                retry=retry_if_exception_type(exception_types),
                wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
                stop=stop_after_attempt(attempts),
                before_sleep=before_sleep_log(logger, logging.WARNING),
                reraise=True,
            )
            return await retrying(func)(*args, **kwargs)

        return wrapper

    return decorator


# Pre-built decorators for common cases
http_retry = with_retry(httpx.HTTPError, httpx.TimeoutException, attempts=3)
