"""
n8n webhook submission tool.

Rules:
- HTTP POST to webhook URL from .env.
- 60-second delay between submissions. Non-negotiable.
- Retry up to 3 times on failure with 30-second backoff.
"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx

from backend.config import get_settings
from backend.utils.logging import get_logger

logger = get_logger("n8n")


async def submit_to_n8n(payload: dict[str, Any]) -> bool:
    """
    POST payload to n8n webhook.
    Returns True on success, False on failure.
    Includes mandatory 60-second delay after each submission.
    """
    settings = get_settings()

    if not settings.n8n_webhook_url:
        logger.warning("n8n_webhook_url_not_set")
        return False

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    settings.n8n_webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()

            logger.info(
                "n8n_submitted",
                status=resp.status_code,
                company=payload.get("company_name", "unknown"),
            )

            # Mandatory delay before next submission
            logger.info("n8n_delay_start", seconds=settings.n8n_submission_delay)
            await asyncio.sleep(settings.n8n_submission_delay)
            return True

        except httpx.HTTPError as e:
            if attempt == 2:
                logger.error("n8n_submission_failed", error=str(e), payload=payload)
                return False

            backoff = 30 * (attempt + 1)
            logger.warning("n8n_retry", attempt=attempt + 1, backoff=backoff, error=str(e))
            await asyncio.sleep(backoff)

    return False


def build_payload(
    company_name: str,
    parent_company_name: str,
    sales_nav_url: str,
    domain: str,
    sdr_assigned: str,
    email_format: str,
    account_type: str,
    account_size: str,
    row: int,
) -> dict[str, Any]:
    """
    Build payload matching the App Script's buildPayload_ exactly.
    Keys are header names with spaces replaced by underscores.
    """
    return {
        "sheetName": "Target Accounts",
        "row": row,
        "Company_Name": company_name,
        "Parent_Company_Name": parent_company_name,
        "Sales_Navigator_Link": sales_nav_url,
        "Company_Domain": domain,
        "SDR_Name": sdr_assigned,
        "Email_Format(_Firstname-amy_,_Lastname-_williams)": email_format,
        "Account_type": account_type,
        "Account_Size": account_size,
    }
