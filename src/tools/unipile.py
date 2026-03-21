"""
Unipile LinkedIn API tool.

Replaces Playwright browser relay for:
- Company org ID lookup (for Fini Sales Nav URL)
- Profile verification (for Searcher + Veri)

API base: https://{dsn}/api/v1
Auth: X-API-KEY header

Account pool: fetches all LinkedIn accounts on startup, round-robins across them
to distribute load across all 15 Sales Navigator seats.
"""
from __future__ import annotations

import itertools
import re
import unicodedata
from typing import TypedDict

import httpx

from src.config import get_settings
from src.utils.logging import get_logger

logger = get_logger("unipile")

# ---------------------------------------------------------------------------
# Account pool — loaded once, round-robined on every call
# ---------------------------------------------------------------------------

_account_pool: list[str] = []       # list of account IDs
_pool_cycle: itertools.cycle | None = None


async def _init_pool() -> None:
    """Fetch all LinkedIn accounts from Unipile and build the round-robin pool."""
    global _account_pool, _pool_cycle
    if _account_pool:
        return  # already initialised

    settings = get_settings()
    if not settings.unipile_api_key:
        return

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"https://{settings.unipile_dsn}/api/v1/accounts",
                headers={"X-API-KEY": settings.unipile_api_key, "accept": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

        # Only use LinkedIn accounts with status OK
        ids = [
            item["id"]
            for item in data.get("items", [])
            if item.get("type") == "LINKEDIN"
            and any(s.get("status") == "OK" for s in item.get("sources", []))
        ]

        if ids:
            _account_pool = ids
            _pool_cycle = itertools.cycle(ids)
            logger.info("unipile_pool_ready", accounts=len(ids), ids=ids)
        else:
            logger.warning("unipile_no_accounts", detail="No OK LinkedIn accounts found")

    except Exception as e:
        logger.warning("unipile_pool_init_error", error=str(e))


def _next_account_id() -> str:
    """Return the next account ID from the round-robin pool."""
    if _pool_cycle:
        return next(_pool_cycle)
    # Fallback to configured account
    return get_settings().unipile_account_id


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

class OrgInfo(TypedDict):
    org_id: str | None
    name: str | None
    public_identifier: str | None
    error: str | None


class ProfileVerification(TypedDict):
    linkedin_url: str
    valid: bool
    full_name: str | None
    current_company: str | None
    current_role: str | None
    at_target_company: bool
    still_employed: bool
    error: str | None


def _base_url() -> str:
    settings = get_settings()
    return f"https://{settings.unipile_dsn}/api/v1"


def _headers() -> dict:
    settings = get_settings()
    return {
        "X-API-KEY": settings.unipile_api_key,
        "accept": "application/json",
    }


def _ascii_lower(s: str) -> str:
    """Strip accents and lowercase for fuzzy comparison."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()


_LEGAL_SUFFIXES = re.compile(
    r'\b(ltd|limited|inc|corp|llc|llp|pvt|private|plc|sa|gmbh|ag|group|foods?|agro|'
    r'industries|international|india|global|holdings?|enterprises?|solutions?|services?)\b',
    re.IGNORECASE,
)


def _strip_suffixes(s: str) -> str:
    """Remove legal/generic suffixes for comparison."""
    cleaned = _LEGAL_SUFFIXES.sub('', s)
    return re.sub(r'\s+', ' ', cleaned).strip()


def _extract_alternates(name: str) -> list[str]:
    """
    Extract all name variants from a company name string.
    "Surya Food & Agro Ltd. (Priyagold)" → ["Surya Food & Agro Ltd.", "Priyagold"]
    """
    variants = [re.sub(r'\s*\(.*?\)\s*', '', name).strip()]  # name without parentheticals
    for m in re.finditer(r'\(([^)]+)\)', name):
        variants.append(m.group(1).strip())
    return [v for v in variants if v]


def _company_matches(found: str, target: str) -> bool:
    """
    Fuzzy check: does the found company name match the target?
    Handles parenthetical alternate names, e.g. "Surya Food & Agro Ltd. (Priyagold)".
    Matches if found matches ANY of the extracted name variants.
    """
    def _matches_single(f_raw: str, t_raw: str) -> bool:
        f = _ascii_lower(f_raw)
        t = _ascii_lower(t_raw)
        # Exact match
        if f == t:
            return True
        f_core = _strip_suffixes(f)
        t_core = _strip_suffixes(t)
        if not f_core or not t_core:
            return False
        if f_core == t_core:
            return True
        # Substring containment — only meaningful for multi-word names where one
        # name is a subset of the other (e.g. "Dabur" in "Dabur India Limited").
        # For single-word names (after stripping), require exact core match only —
        # "arico" must NOT match "marico", "mami" must NOT match "emami".
        f_is_single = ' ' not in f_core.strip()
        t_is_single = ' ' not in t_core.strip()
        if not (f_is_single and t_is_single):
            shorter_c, longer_c = (f_core, t_core) if len(f_core) <= len(t_core) else (t_core, f_core)
            if shorter_c in longer_c:
                return True
        f_words = set(re.sub(r'[^a-z0-9]', ' ', f_core).split())
        t_words = set(re.sub(r'[^a-z0-9]', ' ', t_core).split())
        if not t_words:
            return False
        return len(f_words & t_words) / len(t_words) >= 0.5

    return any(_matches_single(found, variant) for variant in _extract_alternates(target))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# GPT slug lookup helper
# ---------------------------------------------------------------------------

_slug_re = re.compile(r'linkedin\.com/company/([^/?&#\s"\'<>()\[\]]+)')


async def _ask_gpt_for_linkedin_slug(company_name: str) -> list[str]:
    """
    Use OpenAI Responses API with web_search to find the LinkedIn company page URL.
    Returns a list of candidate slugs extracted from the response.
    """
    settings = get_settings()
    if not settings.openai_api_key:
        return []

    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-5",
                    "tools": [{"type": "web_search"}],
                    "input": (
                        f"What is the LinkedIn company page URL for {company_name}? "
                        f"Return ONLY the URL in format: https://www.linkedin.com/company/SLUG/ "
                        f"No explanation, no markdown."
                    ),
                },
            )
            resp.raise_for_status()
            data = resp.json()

            content = ""
            for item in data.get("output", []):
                if item.get("type") == "message":
                    for part in item.get("content", []):
                        if part.get("type") == "output_text":
                            content = part.get("text", "").strip()
                            break
                if content:
                    break

            slugs = [s.rstrip('/') for s in _slug_re.findall(content)]
            logger.info("gpt_slug_response", company=company_name, content=content, slugs=slugs)
            return slugs
    except Exception as e:
        logger.warning("gpt_slug_error", company=company_name, error=str(e))
        return []


# ---------------------------------------------------------------------------
# Company lookup — used by Fini to get org ID for Sales Nav URL
# ---------------------------------------------------------------------------

async def get_company_org_id(company_name: str) -> OrgInfo:
    """
    Look up a LinkedIn company and return its numeric org ID.

    Strategy:
    1. Build a slug from the company name (e.g. "Nestle" → "nestle")
    2. GET /api/v1/linkedin/company/{slug}
    3. If not found, try DDG to find the LinkedIn slug first, then retry
    """
    result: OrgInfo = {"org_id": None, "name": None, "public_identifier": None, "error": None}

    settings = get_settings()
    if not settings.unipile_api_key:
        result["error"] = "UNIPILE_API_KEY not set"
        return result

    await _init_pool()
    account_id = _next_account_id()

    async def _fetch_by_slug(client: httpx.AsyncClient, slug: str) -> OrgInfo | None:
        """Fetch company by slug; return OrgInfo if name matches, else None."""
        try:
            resp = await client.get(
                f"{_base_url()}/linkedin/company/{slug}",
                params={"account_id": account_id},
                headers=_headers(),
            )
            if resp.status_code == 200:
                data = resp.json()
                org_id = data.get("id")
                name = data.get("name", "")
                if org_id and _company_matches(name, company_name):
                    logger.info("unipile_company_found", company=company_name, org_id=org_id, name=name, slug=slug)
                    return {
                        "org_id": str(org_id),
                        "name": name,
                        "public_identifier": data.get("public_identifier"),
                        "error": None,
                    }
                else:
                    logger.warning("unipile_company_mismatch", slug=slug, found=name, wanted=company_name)
        except Exception as e:
            logger.warning("unipile_company_error", slug=slug, error=str(e))
        return None

    async with httpx.AsyncClient(timeout=15) as client:
        # --- Phase 1: guessed slugs ---
        clean = re.sub(r'[^a-z0-9\s]', ' ', _ascii_lower(company_name)).strip()
        words = [w for w in clean.split() if w not in ('pvt', 'ltd', 'limited', 'inc', 'corp', 'llc', 'private', 'agro', 'group')]
        primary_slug = '-'.join(words)
        base = ''.join(words)  # no hyphens version e.g. "maricolimited" → but words already stripped "limited" so just "marico"

        # Try multiple slug variations: hyphenated, no-hyphen+limited, hyphen+ltd, etc.
        slug_candidates = [
            primary_slug,                    # marico, dabur-india
            f"{primary_slug}-limited",       # marico-limited
            f"{primary_slug}-ltd",           # marico-ltd
            f"{base}limited",                # maricolimited
            f"{base}ltd",                    # maricoltd
        ]
        seen_phase1: set[str] = set()
        for slug in slug_candidates:
            if slug in seen_phase1:
                continue
            seen_phase1.add(slug)
            found = await _fetch_by_slug(client, slug)
            if found:
                return {**result, **found}

        # --- Phase 2: GPT-4o-mini to get the exact LinkedIn slug ---
        try:
            slugs_from_gpt = await _ask_gpt_for_linkedin_slug(company_name)
            seen_slugs: set[str] = set(slug_candidates)
            for slug in slugs_from_gpt:
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                found = await _fetch_by_slug(client, slug)
                if found:
                    logger.info("unipile_company_found_via_gpt", company=company_name, slug=slug)
                    return {**result, **found}
        except Exception as e:
            logger.warning("unipile_company_slug_gpt_error", company=company_name, error=str(e))

    result["error"] = f"Could not find LinkedIn org ID for: {company_name}"
    return result


# ---------------------------------------------------------------------------
# Profile verification — used by Searcher + Veri
# ---------------------------------------------------------------------------

async def verify_profile(linkedin_url: str, target_company: str) -> ProfileVerification:
    """
    Verify a LinkedIn profile is currently employed at target_company.

    target_company may include a parenthetical alternate name, e.g.
    "Surya Food & Agro Ltd. (Priyagold)" — both the main name and the
    name in parentheses are matched against the profile's current company.
    """
    result: ProfileVerification = {
        "linkedin_url": linkedin_url,
        "valid": False,
        "full_name": None,
        "current_company": None,
        "current_role": None,
        "at_target_company": False,
        "still_employed": False,
        "error": None,
    }

    settings = get_settings()
    if not settings.unipile_api_key:
        result["error"] = "UNIPILE_API_KEY not set"
        return result

    await _init_pool()
    account_id = _next_account_id()

    # Extract public identifier from URL
    m = re.search(r'linkedin\.com/in/([^/?&#]+)', linkedin_url)
    if not m:
        result["error"] = f"Cannot extract LinkedIn identifier from URL: {linkedin_url}"
        return result

    identifier = m.group(1).rstrip('/')

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{_base_url()}/users/{identifier}",
                params=[
                    ("account_id", account_id),
                    ("linkedin_sections", "experience"),
                ],
                headers=_headers(),
            )

            if resp.status_code == 404:
                result["error"] = "Profile not found"
                return result
            if resp.status_code in (401, 403):
                result["error"] = f"Unipile auth error: {resp.status_code}"
                return result

            resp.raise_for_status()
            data = resp.json()

        result["valid"] = True
        first = (data.get("first_name") or "").strip()
        last = (data.get("last_name") or "").strip()
        result["full_name"] = f"{first} {last}".strip() or data.get("name") or None

        # --- Primary: use work_experience entries ---
        # Find current position: current==true OR end is null/empty
        experiences = data.get("work_experience", []) or []
        current_exp = None
        for exp in experiences:
            if not exp.get("end"):  # end is null/missing = current position
                current_exp = exp
                break  # topmost current entry wins

        if current_exp:
            current_company = (current_exp.get("company") or "").strip() or None
            current_role = (current_exp.get("position") or current_exp.get("role") or "").strip() or None
            result["current_company"] = current_company
            result["current_role"] = current_role
            result["still_employed"] = True
            if current_company:
                result["at_target_company"] = _company_matches(current_company, target_company)
        else:
            # --- Fallback: parse headline if no experience data returned ---
            headline = data.get("headline", "") or ""
            if headline:
                current_company, current_role = _parse_headline(headline)
                result["current_company"] = current_company
                result["current_role"] = current_role
                result["still_employed"] = True
                if current_company:
                    result["at_target_company"] = _company_matches(current_company, target_company)

        logger.info(
            "unipile_profile_verified",
            identifier=identifier,
            current_company=result["current_company"],
            current_role=result["current_role"],
            target=target_company,
            match=result["at_target_company"],
            source="experience" if current_exp else "headline",
        )

    except Exception as e:
        result["error"] = str(e)
        logger.warning("unipile_profile_error", identifier=identifier, error=str(e))

    return result


def _looks_like_company(s: str) -> bool:
    """
    Heuristic: does this string look like a company name vs a tagline/description?
    Company names are typically short and mostly alphabetic.
    Taglines tend to be long, contain numbers, currency symbols, or % signs.
    """
    if not s:
        return False
    # Too long to be a company name
    if len(s) > 60:
        return False
    # Tagline signals: numbers with units, currency, percentages, years
    if re.search(r'\d+\s*(?:yrs?|years?|months?|\+|%|M\b|B\b|K\b)|\$|£|€|₹', s):
        return False
    # Mostly non-alpha words = tagline
    words = s.split()
    if not words:
        return False
    alpha_ratio = sum(1 for w in words if re.search(r'[a-zA-Z]', w)) / len(words)
    return alpha_ratio >= 0.6


def _parse_headline(headline: str) -> tuple[str | None, str | None]:
    """
    Extract (current_company, current_role) from a LinkedIn headline.

    Common patterns:
    - "VP Marketing at Nestle"
    - "Head of Digital | Marico"
    - "CDO @ HUL"
    - "Chief Digital Officer, ITC Limited"
    - "Group COO | 25+ yrs global ops"  ← tagline after |, no company → (None, role)
    """
    if not headline:
        return None, None

    for sep in [" at ", " @ ", " | ", ", "]:
        if sep in headline:
            parts = headline.split(sep, 1)
            role = parts[0].strip()
            # Take only the first segment after the separator
            candidate = re.split(r'\s*[\|·•]\s*|\s{2,}', parts[1])[0].strip()
            if candidate and _looks_like_company(candidate):
                return candidate, role
            # Separator found but candidate looks like a tagline — keep role, no company
            if role:
                return None, role

    # No separator — entire headline is likely just a role/title
    return None, headline.strip() if headline.strip() else None


# ---------------------------------------------------------------------------
# People search — used by Searcher as primary discovery source
# ---------------------------------------------------------------------------

class SearchedPerson(TypedDict):
    full_name: str
    linkedin_url: str
    public_identifier: str
    headline: str | None
    location: str | None
    network_distance: str | None


async def search_people(
    org_id: str,
    role_titles: list[str],
    region_id: str = "",
    limit: int = 25,
) -> list[SearchedPerson]:
    """
    Search LinkedIn for people at a company matching given role titles.

    Uses classic LinkedIn search (no Sales Nav subscription needed).
    Searches once per role title and deduplicates by public_identifier.

    Args:
        org_id: LinkedIn numeric org ID (string)
        role_titles: list of job title keywords, e.g. ["VP Marketing", "CMO"]
        region_id: optional LinkedIn geo ID string for location filter
        limit: max results per role title search (capped at 25 by classic API)
    """
    await _init_pool()
    settings = get_settings()
    if not settings.unipile_api_key:
        logger.warning("unipile_search_people_skip", reason="no api key")
        return []

    seen: set[str] = set()
    results: list[SearchedPerson] = []

    async with httpx.AsyncClient(timeout=20) as client:
        for title in role_titles:
            account_id = _next_account_id()
            payload: dict = {
                "api": "classic",
                "category": "people",
                "company": [str(org_id)],
            }
            if title:
                payload["advanced_keywords"] = {"title": title}
            if region_id:
                payload["location"] = [str(region_id)]

            try:
                resp = await client.post(
                    f"{_base_url()}/linkedin/search",
                    params={"account_id": account_id, "limit": min(limit, 25)},
                    json=payload,
                    headers=_headers(),
                )
                if resp.status_code == 403:
                    logger.warning("unipile_search_not_subscribed", title=title)
                    continue
                if resp.status_code != 200:
                    logger.warning("unipile_search_error", title=title, status=resp.status_code)
                    continue

                data = resp.json()
                for item in data.get("items", []):
                    pid = item.get("public_identifier", "")
                    if not pid or pid in seen:
                        continue
                    seen.add(pid)

                    # Clean up profile URL — strip miniProfileUrn query param
                    raw_url = item.get("public_profile_url") or item.get("profile_url") or ""
                    clean_url = f"https://www.linkedin.com/in/{pid}" if pid else raw_url

                    results.append({
                        "full_name": item.get("name", ""),
                        "linkedin_url": clean_url,
                        "public_identifier": pid,
                        "headline": item.get("headline"),
                        "location": item.get("location"),
                        "network_distance": item.get("network_distance"),
                    })

                logger.info(
                    "unipile_search_done",
                    title=title,
                    found=len(data.get("items", [])),
                    total_unique=len(results),
                )

            except Exception as e:
                logger.warning("unipile_search_exception", title=title, error=str(e))

    return results
