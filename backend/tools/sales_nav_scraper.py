"""
Sales Navigator scraper using Scrapling.

Scrapes the company people page from LinkedIn Sales Navigator to find decision-makers.
Uses Scrapling's DynamicFetcher (Playwright-based) with your LinkedIn session cookie.

Requirements:
    pip install "scrapling[fetchers]"
    scrapling install

Setup (one-time):
    1. Log in to LinkedIn in your browser
    2. Open DevTools → Application → Cookies → .linkedin.com
    3. Copy the value of the 'li_at' cookie
    4. Add to .env:  LINKEDIN_LI_AT_COOKIE=<paste value here>

Why Scrapling:
    LinkedIn blocks standard HTTP requests. DynamicFetcher uses a real browser with
    stealth fingerprinting, making it behave like a real user session.
"""
from __future__ import annotations

import asyncio
import re
from typing import TypedDict

from backend.utils.logging import get_logger

logger = get_logger("sales_nav_scraper")

# Profile URL pattern
_LI_PROFILE_RE = re.compile(r'linkedin\.com/in/([^/?&"\'<>\s]+)')

# DM-relevant seniority keywords — used to pre-filter scraped results
_DM_SENIORITY_KEYWORDS = {
    "chief", "ceo", "cto", "cmo", "cdo", "coo", "cfo", "cio", "cro", "cco",
    "president", "vice president", "vp ",
    "director", "managing director", "general manager", "country manager",
    "head of", "head,",
    # Spanish
    "director general", "gerente general", "director ejecutivo", "consejero delegado",
    "responsable", "jefe de", "jefe ",
    # Portuguese
    "diretor", "gerente geral",
    # French
    "directeur", "responsable",
    # German
    "leiter", "geschäftsführer",
    # Italian
    "direttore",
}


class SalesNavPerson(TypedDict):
    full_name: str
    title: str
    location: str | None
    linkedin_url: str | None
    profile_id: str | None


def _is_dm_title(title: str) -> bool:
    """Quick keyword check — is this a senior decision-maker title?"""
    if not title:
        return False
    t = f" {title.lower()} "
    return any(kw in t for kw in _DM_SENIORITY_KEYWORDS)


def _parse_person_card(card) -> SalesNavPerson | None:
    """
    Parse one person card from a Sales Navigator people list.
    Tries multiple CSS selector strategies since Sales Nav updates its DOM regularly.
    """
    try:
        # ── Name ──────────────────────────────────────────────────────────────
        name = ""
        for sel in [
            "span[data-anonymize='person-name']",
            ".artdeco-entity-lockup__title span",
            ".artdeco-entity-lockup__title",
            "[data-control-name='view_lead_panel_via_people_search'] span",
            "a span.lt-line-clamp",
        ]:
            try:
                els = card.css(sel)
                if els:
                    text = els[0].text.strip()
                    if text and "linkedin member" not in text.lower():
                        name = text
                        break
            except Exception:
                continue

        if not name:
            return None

        # ── Title ─────────────────────────────────────────────────────────────
        title = ""
        for sel in [
            "span[data-anonymize='title']",
            ".artdeco-entity-lockup__subtitle span",
            ".artdeco-entity-lockup__subtitle",
            ".member-list-item__occupation",
        ]:
            try:
                els = card.css(sel)
                if els:
                    title = els[0].text.strip()
                    if title:
                        break
            except Exception:
                continue

        # ── Location ──────────────────────────────────────────────────────────
        location = None
        for sel in [
            "span[data-anonymize='location']",
            ".artdeco-entity-lockup__metadata span",
            ".artdeco-entity-lockup__caption",
        ]:
            try:
                els = card.css(sel)
                if els:
                    loc = els[0].text.strip()
                    if loc:
                        location = loc
                        break
            except Exception:
                continue

        # ── LinkedIn URL ───────────────────────────────────────────────────────
        linkedin_url = None
        profile_id = None
        for sel in [
            "a[href*='/in/']",
            "a[href*='/sales/lead/']",
            "a[data-control-name='view_lead_panel_via_people_search']",
        ]:
            try:
                els = card.css(sel)
                if els:
                    href = els[0].attrib.get("href", "")
                    m = _LI_PROFILE_RE.search(href)
                    if m:
                        profile_id = m.group(1)
                        linkedin_url = f"https://www.linkedin.com/in/{profile_id}"
                        break
            except Exception:
                continue

        return {
            "full_name": name,
            "title": title,
            "location": location,
            "linkedin_url": linkedin_url,
            "profile_id": profile_id or name.lower().replace(" ", "_"),
        }

    except Exception:
        return None


def _extract_people(page) -> list[SalesNavPerson]:
    """
    Extract all person cards from a rendered Sales Navigator page.
    Tries four CSS selector strategies in order of reliability.
    """
    people: list[SalesNavPerson] = []
    seen: set[str] = set()

    card_selectors = [
        # Strategy 1: standard artdeco list items with entity lockup
        "li.artdeco-list__item",
        # Strategy 2: org people module items
        "li.org-people-profiles-module__profile-item",
        # Strategy 3: entity lockup wrappers
        ".artdeco-entity-lockup",
        # Strategy 4: generic list items that contain person-name spans
        "li",
    ]

    for selector in card_selectors:
        try:
            cards = page.css(selector)
            if not cards:
                continue

            candidate_people: list[SalesNavPerson] = []
            for card in cards:
                # Only try parsing if the card contains a person-name element
                try:
                    has_name = bool(
                        card.css("span[data-anonymize='person-name']")
                        or card.css(".artdeco-entity-lockup__title")
                    )
                    if not has_name:
                        continue
                except Exception:
                    continue

                person = _parse_person_card(card)
                if person and person["full_name"]:
                    pid = person["profile_id"] or person["full_name"].lower()
                    if pid not in seen:
                        seen.add(pid)
                        candidate_people.append(person)

            if candidate_people:
                people = candidate_people
                break

        except Exception:
            continue

    return people


async def scrape_company_people(
    org_id: str,
    li_at_cookie: str,
    scroll_rounds: int = 12,
    dm_only: bool = True,
) -> list[SalesNavPerson]:
    """
    Scrape people from a LinkedIn Sales Navigator company people page.

    Strategy:
      1. Navigate to https://www.linkedin.com/sales/company/{org_id}/people/
         with the li_at cookie for authentication.
      2. Use DynamicFetcher (Playwright) with scroll_down to trigger
         Sales Navigator's infinite-scroll pagination.
      3. Parse all visible person cards.
      4. Optionally filter to DM-seniority titles only.

    Args:
        org_id:        LinkedIn numeric org ID
        li_at_cookie:  The li_at session cookie from your browser
        scroll_rounds: How many times to scroll to load more results
                       (each scroll loads ~25 more, 12 scrolls ≈ 300 people)
        dm_only:       If True, return only VP/Director/C-Suite level people

    Returns:
        List of SalesNavPerson dicts.
    """
    try:
        from scrapling.fetchers import DynamicFetcher
    except ImportError:
        logger.warning(
            "sales_nav_scraper_skip",
            reason="scrapling[fetchers] not installed — run: pip install 'scrapling[fetchers]' && scrapling install",
        )
        return []

    url = f"https://www.linkedin.com/sales/company/{org_id}/people/"
    cookies = [
        {"name": "li_at", "value": li_at_cookie, "domain": ".linkedin.com", "path": "/"},
    ]

    logger.info("sales_nav_scraper_start", org_id=org_id, url=url, scroll_rounds=scroll_rounds)

    try:
        # DynamicFetcher.fetch is synchronous (uses Playwright under the hood).
        # We run it in a thread pool to avoid blocking the asyncio event loop.
        page = await asyncio.to_thread(
            DynamicFetcher.fetch,
            url,
            cookies=cookies,
            headless=True,
            network_idle=True,
            scroll_down=scroll_rounds,   # Scrapling scrolls N times to trigger infinite scroll
            timeout=60000,               # 60s timeout — Sales Nav can be slow to load
        )
    except Exception as e:
        logger.warning("sales_nav_scraper_fetch_error", org_id=org_id, error=str(e))
        return []

    # Check if we were redirected to login (cookie expired / invalid)
    try:
        current_url = getattr(page, "url", "") or ""
        if "login" in current_url.lower() or "authwall" in current_url.lower():
            logger.warning(
                "sales_nav_scraper_auth_failed",
                org_id=org_id,
                current_url=current_url,
                hint="LINKEDIN_LI_AT_COOKIE may be expired — refresh it from your browser",
            )
            return []
    except Exception:
        pass

    all_people = _extract_people(page)

    if not all_people:
        logger.warning(
            "sales_nav_scraper_no_results",
            org_id=org_id,
            hint="Page may not have rendered properly or cookie is expired",
        )
        return []

    if dm_only:
        dm_people = [p for p in all_people if _is_dm_title(p["title"])]
        logger.info(
            "sales_nav_scraper_done",
            org_id=org_id,
            total_scraped=len(all_people),
            dm_filtered=len(dm_people),
        )
        return dm_people

    logger.info("sales_nav_scraper_done", org_id=org_id, total=len(all_people))
    return all_people
