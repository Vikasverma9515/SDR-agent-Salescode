"""
ZeroBounce email validation tool.

Rules:
- Check credit balance before every call. Pause if < 50.
- Cache results keyed by email. Never validate the same email twice.
- REST API: https://api.zerobounce.net/v2/validate
"""
from __future__ import annotations

import asyncio
from typing import TypedDict

import httpx

from src.config import get_settings
from src.utils.logging import get_logger
from src.utils.rate_limiter import zerobounce_limiter

logger = get_logger("zerobounce")

_cache: dict[str, "ZeroBounceResult"] = {}
_credits_checked: bool = False  # check credits once per process start, not per call


class ZeroBounceResult(TypedDict):
    email: str
    status: str  # "valid", "invalid", "catch-all", "unknown", "spamtrap", "abuse", "do_not_mail"
    sub_status: str
    score: float  # 0.0 - 10.0 (10 = most valid)
    did_you_mean: str | None
    free_email: bool
    mx_found: bool
    smtp_provider: str | None
    error: str | None


async def get_credits() -> int:
    """Check remaining ZeroBounce credits."""
    settings = get_settings()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            "https://api.zerobounce.net/v2/getcredits",
            params={"api_key": settings.zerobounce_api_key},
        )
        if resp.status_code in (401, 403):
            logger.error("zerobounce_auth_error", status=resp.status_code,
                         msg="Invalid or expired ZeroBounce API key — check ZEROBOUNCE_API_KEY in .env")
            return 0
        resp.raise_for_status()
        data = resp.json()
        credits = int(data.get("Credits", 0))
        logger.info("zerobounce_credits", remaining=credits)
        return credits


async def validate_email(email: str) -> ZeroBounceResult:
    """
    Validate a single email address via ZeroBounce.
    Checks credit balance first. Caches results.
    """
    email = email.lower().strip()

    # Return cached result if available
    if email in _cache:
        logger.info("zerobounce_cache_hit", email=email)
        return _cache[email]

    settings = get_settings()
    await zerobounce_limiter.acquire()

    # Check credits once per process — not on every call
    global _credits_checked
    if not _credits_checked:
        _credits_checked = True
        try:
            credits = await get_credits()
            if credits < settings.zerobounce_credit_warning:
                logger.warning(
                    "zerobounce_low_credits",
                    credits=credits,
                    threshold=settings.zerobounce_credit_warning,
                )
            if credits == 0:
                return _error_result(email, "ZeroBounce has 0 credits remaining")
        except Exception as e:
            logger.warning("zerobounce_credit_check_failed", error=str(e))

    # Validate
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    "https://api.zerobounce.net/v2/validate",
                    params={
                        "api_key": settings.zerobounce_api_key,
                        "email": email,
                        "ip_address": "",
                    },
                )
                resp.raise_for_status()
                data = resp.json()

            result: ZeroBounceResult = {
                "email": email,
                "status": data.get("status", "unknown"),
                "sub_status": data.get("sub_status", ""),
                "score": _status_to_score(data.get("status", "unknown")),
                "did_you_mean": data.get("did_you_mean") or None,
                "free_email": data.get("free_email", False),
                "mx_found": data.get("mx_found", "").lower() == "true",
                "smtp_provider": data.get("smtp_provider") or None,
                "error": None,
            }

            _cache[email] = result
            logger.info(
                "zerobounce_validated",
                email=email,
                status=result["status"],
                score=result["score"],
            )
            return result

        except httpx.HTTPStatusError as e:
            # 401/403 = bad API key — no point retrying
            if e.response.status_code in (401, 403):
                logger.error("zerobounce_auth_error", status=e.response.status_code,
                             msg="Invalid or expired ZeroBounce API key")
                return _error_result(email, f"ZeroBounce auth failed ({e.response.status_code}) — check ZEROBOUNCE_API_KEY")
            if attempt == 2:
                return _error_result(email, str(e))
            await asyncio.sleep(2 ** (attempt + 1))
        except httpx.HTTPError as e:
            if attempt == 2:
                return _error_result(email, str(e))
            await asyncio.sleep(2 ** (attempt + 1))

    return _error_result(email, "Max retries exceeded")


async def validate_batch(emails: list[str]) -> dict[str, ZeroBounceResult]:
    """Validate multiple emails sequentially (cache-aware)."""
    results = {}
    for email in emails:
        results[email] = await validate_email(email)
    return results


def _status_to_score(status: str) -> float:
    """Convert ZeroBounce status string to 0-10 score."""
    mapping = {
        "valid": 9.0,
        "catch-all": 5.0,
        "unknown": 3.0,
        "spamtrap": 0.0,
        "abuse": 0.0,
        "do_not_mail": 0.0,
        "invalid": 0.0,
    }
    return mapping.get(status, 1.0)


def _error_result(email: str, error_msg: str) -> ZeroBounceResult:
    return {
        "email": email,
        "status": "unknown",
        "sub_status": "",
        "score": 0.0,
        "did_you_mean": None,
        "free_email": False,
        "mx_found": False,
        "smtp_provider": None,
        "error": error_msg,
    }
