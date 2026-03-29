"""
TheOrg scraper for org chart data.
Scrapes theorg.com for role/reporting structure information.
"""
from __future__ import annotations

import asyncio
import re
from typing import TypedDict

import httpx

from backend.utils.logging import get_logger

logger = get_logger("theorg")

THEORG_BASE = "https://theorg.com"


class OrgChartEntry(TypedDict):
    full_name: str
    role_title: str | None
    department: str | None
    reports_to: str | None
    linkedin_url: str | None
    profile_url: str


async def search_company(company_name: str) -> list[OrgChartEntry]:
    """
    Search TheOrg for a company and return org chart entries.
    Returns empty list if company not found or on error.
    """
    slug = _to_slug(company_name)
    url = f"{THEORG_BASE}/org/{slug}"

    try:
        async with httpx.AsyncClient(
            timeout=20,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
            follow_redirects=True,
        ) as client:
            resp = await client.get(url)

            if resp.status_code == 404:
                logger.info("theorg_company_not_found", company=company_name, slug=slug)
                return []

            resp.raise_for_status()
            html = resp.text

        entries = _parse_org_chart(html, company_name)
        logger.info("theorg_scraped", company=company_name, entries=len(entries))
        return entries

    except httpx.HTTPError as e:
        logger.warning("theorg_http_error", company=company_name, error=str(e))
        return []
    except Exception as e:
        logger.warning("theorg_error", company=company_name, error=str(e))
        return []


async def lookup_person(person_name: str, company_name: str) -> OrgChartEntry | None:
    """Look up a specific person at a company on TheOrg."""
    entries = await search_company(company_name)
    name_lower = person_name.lower()
    for entry in entries:
        if name_lower in entry["full_name"].lower():
            return entry
    return None


def _parse_org_chart(html: str, company_name: str) -> list[OrgChartEntry]:
    """
    Parse TheOrg HTML to extract person entries.
    TheOrg renders server-side HTML with structured data.
    """
    entries = []

    # Look for person cards in the HTML
    # Pattern: name + title in structured divs
    name_title_pattern = re.compile(
        r'<a[^>]*href="(/org/[^/]+/([^/"]+))"[^>]*>.*?'
        r'<span[^>]*>([^<]+)</span>.*?'
        r'<span[^>]*>([^<]+)</span>',
        re.DOTALL,
    )

    for match in name_title_pattern.finditer(html):
        profile_path, _, name, title = match.groups()
        entries.append(
            OrgChartEntry(
                full_name=name.strip(),
                role_title=title.strip(),
                department=None,
                reports_to=None,
                linkedin_url=None,
                profile_url=f"{THEORG_BASE}{profile_path}",
            )
        )

    # Deduplicate by name
    seen = set()
    deduped = []
    for e in entries:
        key = e["full_name"].lower()
        if key not in seen:
            seen.add(key)
            deduped.append(e)

    return deduped


def _to_slug(company_name: str) -> str:
    """Convert company name to URL slug, normalizing unicode characters first."""
    import unicodedata
    normalized = unicodedata.normalize("NFKD", company_name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    slug = ascii_name.lower()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    return slug
