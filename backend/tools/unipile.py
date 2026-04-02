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

import asyncio
import itertools
import re
import unicodedata
from typing import TypedDict

import httpx

from backend.config import get_settings
from backend.utils.logging import get_logger

logger = get_logger("unipile")

# ---------------------------------------------------------------------------
# Account pool — loaded once, round-robined on every call
# ---------------------------------------------------------------------------

_account_pool: list[str] = []       # list of account IDs
_pool_cycle: itertools.cycle | None = None


async def _init_pool() -> None:
    """Initialize the round-robin account pool.
    Priority: UNIPILE_ACCOUNT_IDS env var (hardcoded) → Unipile API fetch → single fallback.
    """
    global _account_pool, _pool_cycle
    if _account_pool:
        return  # already initialised

    settings = get_settings()

    # Priority 1: Use hardcoded account IDs from env (most reliable)
    if settings.unipile_account_ids:
        ids = [aid.strip() for aid in settings.unipile_account_ids.split(",") if aid.strip()]
        if ids:
            _account_pool = ids
            _pool_cycle = itertools.cycle(ids)
            logger.info("unipile_pool_from_env", accounts=len(ids))
            return

    # Priority 2: Fetch from Unipile API
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

        ids = [
            item["id"]
            for item in data.get("items", [])
            if item.get("type") == "LINKEDIN"
            and any(s.get("status") == "OK" for s in item.get("sources", []))
        ]

        if ids:
            _account_pool = ids
            _pool_cycle = itertools.cycle(ids)
            logger.info("unipile_pool_from_api", accounts=len(ids), ids=ids)
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
    r'\b(ltd|limited|inc|corp|llc|llp|pvt|private|plc|sa|sl|slu|sau|srl|cb|sca|gmbh|ag|group|foods?|agro|'
    r'industries|international|india|global|holdings?|enterprises?|solutions?|services?)\b'
    # Dotted Spanish forms at end of string: S.L., S.A., S.L.U., S.A.U.
    r'|[,\s]+s\.l\.u?\.?\s*$'
    r'|[,\s]+s\.a\.u?\.?\s*$',
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
    if not settings.openai_api_key and not settings.aws_bearer_token_bedrock:
        return []

    try:
        from backend.tools.llm import llm_web_search
        content = await llm_web_search(
            f"What is the LinkedIn company page URL for {company_name}? "
            f"Return ONLY the URL in format: https://www.linkedin.com/company/SLUG/ "
            f"No explanation, no markdown."
        )

        slugs = [s.rstrip('/') for s in _slug_re.findall(content)]
        logger.info("gpt_slug_response", company=company_name, content=content, slugs=slugs)
        return slugs
    except Exception as e:
        logger.warning("gpt_slug_error", company=company_name, error=str(e))
        return []


# ---------------------------------------------------------------------------
# Company lookup — used by Fini to get org ID for Sales Nav URL
# ---------------------------------------------------------------------------

def _domain_matches(website: str, expected_domain: str) -> bool:
    """Check if a LinkedIn company's website matches the expected domain."""
    if not website or not expected_domain:
        return True  # can't validate — allow it
    # Normalize both to bare domain
    w = re.sub(r'^https?://(?:www\.)?', '', website.lower()).split('/')[0].strip()
    e = expected_domain.lower().strip()
    if w == e:
        return True
    # Allow parent domain match: "dominos.de" matches "dominos.com" (same brand)
    w_base = w.rsplit('.', 1)[0] if '.' in w else w
    e_base = e.rsplit('.', 1)[0] if '.' in e else e
    if w_base == e_base:
        return True
    # Allow substring: "mondelezinternational.com" matches partial "mondelez"
    if w_base in e_base or e_base in w_base:
        return True
    return False


async def get_company_org_id(company_name: str, expected_domain: str = "") -> OrgInfo:
    """
    Look up a LinkedIn company and return its numeric org ID.

    Strategy:
    1. Build a slug from the company name (e.g. "Nestle" → "nestle")
    2. GET /api/v1/linkedin/company/{slug}
    3. If not found, try DDG to find the LinkedIn slug first, then retry

    If expected_domain is provided, candidates whose LinkedIn website doesn't
    match are deprioritized (tried last) rather than accepted immediately.
    """
    result: OrgInfo = {"org_id": None, "name": None, "public_identifier": None, "error": None}

    settings = get_settings()
    if not settings.unipile_api_key:
        result["error"] = "UNIPILE_API_KEY not set"
        return result

    await _init_pool()
    account_id = _next_account_id()

    # Fallback: candidate that name-matched but FAILED domain validation.
    # Only used at the very end if nothing domain-validated was found.
    _domain_fallback: OrgInfo | None = None

    async def _fetch_by_slug(client: httpx.AsyncClient, slug: str) -> OrgInfo | None:
        """Fetch company by slug; return OrgInfo if name matches, else None.
        If expected_domain is set, rejects candidates whose website doesn't match
        (saves them as fallback instead)."""
        nonlocal _domain_fallback
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
                    website = (
                        data.get("website") or data.get("website_url")
                        or data.get("domain") or data.get("company_url") or ""
                    ).strip()

                    info: OrgInfo = {
                        "org_id": str(org_id),
                        "name": name,
                        "public_identifier": data.get("public_identifier"),
                        "website": website,
                        "error": None,
                    }

                    # Domain cross-validation: if we know the expected domain,
                    # reject candidates whose LinkedIn website doesn't match.
                    # This catches "Domino" (Italian tech) vs "Domino's Pizza" (dominos.de).
                    if expected_domain and website and not _domain_matches(website, expected_domain):
                        logger.warning("unipile_company_domain_mismatch",
                                       company=company_name, slug=slug, name=name,
                                       linkedin_website=website, expected=expected_domain)
                        if not _domain_fallback:
                            _domain_fallback = info  # save as last resort
                        return None  # skip — try next candidate

                    logger.info("unipile_company_found", company=company_name, org_id=org_id,
                                name=name, slug=slug, website=website or "(none)",
                                domain_validated=bool(expected_domain and website))
                    return info
                else:
                    logger.warning("unipile_company_mismatch", slug=slug, found=name, wanted=company_name)
        except Exception as e:
            logger.warning("unipile_company_error", slug=slug, error=str(e))
        return None

    async with httpx.AsyncClient(timeout=15) as client:
        # --- Phase 1: guessed slugs ---
        # Strip parenthetical content BEFORE building slug — avoids "atocha-vallecas-mahou"
        # from "Atocha Vallecas (Mahou)" and "heineken-distributor" from "(Heineken distributor)"
        name_for_slug = re.sub(r'\s*\([^)]*\)', '', company_name).strip()
        clean = re.sub(r'[^a-z0-9\s]', ' ', _ascii_lower(name_for_slug)).strip()

        # Strip legal/filler words that are typically absent from LinkedIn slugs
        _SLUG_STRIP_BASE = {
            'pvt', 'ltd', 'limited', 'inc', 'corp', 'llc', 'private', 'agro',
            # Spanish legal forms
            'sl', 'slu', 'sa', 'sau', 'srl', 'cb', 'sca',
        }
        # Regional qualifiers: try WITH region first (e.g. "diageo-espana"),
        # then WITHOUT (e.g. "diageo") since some LinkedIn pages include the region in the slug
        _REGIONAL_IN_SLUG = {
            'espana', 'spain', 'iberia', 'iberica',
            'india', 'france', 'italia', 'italy', 'deutschland', 'germany',
            'brasil', 'brazil', 'mexico', 'china', 'japan', 'latam', 'emea', 'apac',
        }
        words_with_region = [w for w in clean.split() if w not in _SLUG_STRIP_BASE]
        words_no_region = [w for w in clean.split() if w not in (_SLUG_STRIP_BASE | _REGIONAL_IN_SLUG)]
        _has_region_word = words_with_region != words_no_region

        # Primary slug: WITH region if present (so "diageo-espana" is tried before "diageo")
        words = words_with_region if _has_region_word else words_no_region
        primary_slug = '-'.join(words)
        base = ''.join(words)

        # Try multiple slug variations: hyphenated, no-hyphen, with legal suffixes
        slug_candidates = [
            primary_slug,                    # diageo-espana, dabur-india, atocha-vallecas
            f"{primary_slug}-limited",
            f"{primary_slug}-ltd",
            f"{base}limited",
            f"{base}ltd",
        ]

        # If we have a regional word, also try WITHOUT it (e.g. "diageo" after "diageo-espana")
        if _has_region_word:
            no_region_slug = '-'.join(words_no_region)
            no_region_base = ''.join(words_no_region)
            for extra in [no_region_slug, f"{no_region_slug}-limited", f"{no_region_slug}-ltd",
                          f"{no_region_base}limited", f"{no_region_base}ltd"]:
                if extra and extra not in slug_candidates:
                    slug_candidates.append(extra)

        # Also try without a leading Spanish/generic prefix word
        # "Grupo Jalumo" → try "jalumo"; "Bodegas Cortés" → try "cortes"
        _STRIP_PREFIXES = {'grupo', 'group', 'hijos', 'bodegas', 'industrias', 'bedoya',
                           'distribuciones', 'distribuidora'}
        if words and words[0] in _STRIP_PREFIXES and len(words) > 1:
            no_prefix_slug = '-'.join(words[1:])
            if no_prefix_slug not in slug_candidates:
                slug_candidates.insert(1, no_prefix_slug)

        # Also try common Spanish LinkedIn slug variants (many companies add -sl or -sa)
        for extra_suffix in ('-sl', '-sa'):
            candidate = primary_slug + extra_suffix
            if candidate not in slug_candidates:
                slug_candidates.append(candidate)

        seen_phase1: set[str] = set()
        for slug in slug_candidates:
            if not slug or slug in seen_phase1:
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

        # --- Phase 3: LinkedIn company keyword search ---
        # This is how a human would search — type the company name, pick the result.
        # Much more reliable than slug guessing for companies with unusual slugs.
        kw_results = await _search_company_keyword(company_name, account_id)
        for kr in kw_results:
            kr_website = kr.get("website", "")
            # Domain cross-validation for keyword results too
            if expected_domain and kr_website and not _domain_matches(kr_website, expected_domain):
                logger.warning("unipile_kw_domain_mismatch",
                               company=company_name, found=kr["name"],
                               linkedin_website=kr_website, expected=expected_domain)
                if not _domain_fallback:
                    _domain_fallback = {
                        "org_id": kr["org_id"], "name": kr["name"],
                        "public_identifier": kr.get("public_identifier"),
                        "website": kr_website, "error": None,
                    }
                continue  # try next keyword result
            logger.info("unipile_company_found_via_keyword_search",
                        company=company_name, found=kr["name"], org_id=kr["org_id"])
            return {
                **result,
                "org_id": kr["org_id"],
                "name": kr["name"],
                "public_identifier": kr.get("public_identifier"),
                "error": None,
            }

    # If all candidates failed domain validation but we have a name-matched fallback,
    # return it as last resort (better than nothing — Fini's LLM validation will re-check)
    if _domain_fallback:
        logger.warning("unipile_using_domain_fallback",
                       company=company_name, fallback=_domain_fallback.get("name"),
                       website=_domain_fallback.get("website"), expected=expected_domain)
        return {**result, **_domain_fallback}

    result["error"] = f"Could not find LinkedIn org ID for: {company_name}"
    return result


async def search_company_by_keyword(company_name: str) -> list[dict]:
    """
    Public wrapper: LinkedIn company keyword search for use outside get_company_org_id.
    Returns list of {"org_id", "name", "public_identifier"}.
    """
    await _init_pool()
    account_id = _next_account_id()
    return await _search_company_keyword(company_name, account_id)


async def _search_company_keyword(company_name: str, account_id: str) -> list[dict]:
    """
    Search LinkedIn for a company by keyword — like typing into LinkedIn's search bar.
    Returns list of {"org_id", "name", "public_identifier"} for matches.

    This is the most human-like lookup method: it doesn't require knowing the slug,
    and handles companies with unusual slugs, legal-suffix variants, etc.
    """
    # Strip parenthetical context before searching (search for the company, not its parent)
    search_term = re.sub(r'\s*\([^)]*\)', '', company_name).strip()
    # Also try without Spanish legal suffixes for cleaner results
    search_term = re.sub(
        r'\b(?:S\.L\.U?\.?|S\.A\.U?\.?|S\.R\.L\.?|SLU?|SAU?)\s*$', '',
        search_term, flags=re.IGNORECASE,
    ).strip()

    if not search_term or len(search_term) < 3:
        return []

    try:
        payload = {
            "api": "classic",
            "category": "companies",
            "keywords": search_term,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{_base_url()}/linkedin/search",
                params={"account_id": account_id, "limit": 10},
                json=payload,
                headers=_headers(),
            )

        if resp.status_code == 403:
            logger.warning("unipile_company_search_not_subscribed", company=company_name)
            return []
        if resp.status_code != 200:
            logger.warning("unipile_company_search_error",
                           company=company_name, status=resp.status_code)
            return []

        data = resp.json()
        matches = []
        skipped_showcases = []
        for item in data.get("items", []):
            org_id = str(item.get("id") or item.get("org_id") or "")
            name = (item.get("name") or "").strip()
            slug = item.get("public_identifier") or item.get("universal_name") or ""
            if not org_id or not name:
                continue

            # Detect and skip LinkedIn Showcase Pages — they don't exist in Sales Navigator.
            # Showcase pages have type "SHOWCASE" or have a parent_id, or their slug
            # pattern suggests a sub-page (e.g. "rippling-it", "rippling-spend").
            item_type = (item.get("type") or item.get("organization_type") or "").upper()
            has_parent = bool(item.get("parent_id") or item.get("parent"))
            if item_type == "SHOWCASE" or has_parent:
                skipped_showcases.append(name)
                continue

            if _company_matches(name, company_name):
                # Extract website/domain if LinkedIn has it
                website = (
                    item.get("website") or item.get("website_url")
                    or item.get("domain") or item.get("company_url") or ""
                ).strip()
                matches.append({
                    "org_id": org_id,
                    "name": name,
                    "public_identifier": slug,
                    "website": website,
                })

        if skipped_showcases:
            logger.info("unipile_skipped_showcases",
                        query=search_term, skipped=skipped_showcases)
        logger.info("unipile_company_keyword_search_done",
                    query=search_term, results=len(data.get("items", [])), matches=len(matches))
        return matches

    except Exception as e:
        logger.warning("unipile_company_keyword_search_exception",
                       company=company_name, error=str(e))
        return []


async def get_company_domain(slug_or_org_id: str) -> str | None:
    """
    Fetch a company's website domain directly from LinkedIn via Unipile.
    This is the most reliable source — no web search needed.
    Returns bare domain (e.g. 'mahou.es') or None.
    """
    await _init_pool()
    account_id = _next_account_id()

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{_base_url()}/linkedin/company/{slug_or_org_id}",
                params={"account_id": account_id},
                headers=_headers(),
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            website = (
                data.get("website") or data.get("website_url")
                or data.get("domain") or data.get("company_url") or ""
            ).strip()
            if website:
                # Clean URL → bare domain
                website = re.sub(r'^https?://(?:www\.)?', '', website)
                website = re.sub(r'^www\.', '', website)
                website = website.split('/')[0].strip().lower()
                if re.match(r'^[a-z0-9][a-z0-9\-]*\.[a-z]{2,}', website):
                    logger.info("unipile_company_domain_found",
                                slug=slug_or_org_id, domain=website)
                    return website
    except Exception as e:
        logger.warning("unipile_company_domain_error",
                       slug=slug_or_org_id, error=str(e))
    return None


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
        # Collect ALL current positions (end is null/missing = still active).
        # People freelance / hold multiple roles — target company may not be
        # the topmost entry.  We check every current position for a match.
        experiences = data.get("work_experience", []) or []
        current_positions: list[dict] = []
        for exp in experiences:
            if not exp.get("end"):  # end is null/missing = current position
                current_positions.append(exp)

        if current_positions:
            # Check if ANY current position matches the target company
            matched_exp = None
            for exp in current_positions:
                co = (exp.get("company") or "").strip()
                if co and _company_matches(co, target_company):
                    matched_exp = exp
                    break

            if matched_exp:
                # Target company found among current positions
                current_company = (matched_exp.get("company") or "").strip() or None
                current_role = (matched_exp.get("position") or matched_exp.get("role") or "").strip() or None
                result["current_company"] = current_company
                result["current_role"] = current_role
                result["still_employed"] = True
                result["at_target_company"] = True
            else:
                # Target not found — report the topmost current position
                top = current_positions[0]
                current_company = (top.get("company") or "").strip() or None
                current_role = (top.get("position") or top.get("role") or "").strip() or None
                result["current_company"] = current_company
                result["current_role"] = current_role
                result["still_employed"] = True
                result["at_target_company"] = False
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
            current_positions=len(current_positions) if current_positions else 0,
            source="experience" if current_positions else "headline",
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
    All title searches run in PARALLEL (Semaphore(6)) — dramatically faster than sequential
    and, when titles cover different functions, returns many more unique people.

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
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(6)  # max 6 concurrent LinkedIn search calls

    async def _search_one(title: str) -> None:
        async with sem:
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
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.post(
                        f"{_base_url()}/linkedin/search",
                        params={"account_id": account_id, "limit": min(limit, 25)},
                        json=payload,
                        headers=_headers(),
                    )
                if resp.status_code == 403:
                    logger.warning("unipile_search_not_subscribed", title=title)
                    return
                if resp.status_code != 200:
                    logger.warning("unipile_search_error", title=title, status=resp.status_code)
                    return

                data = resp.json()
                items = data.get("items", [])

                async with lock:
                    for item in items:
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
                    found=len(items),
                    total_unique=len(results),
                )

            except Exception as e:
                logger.warning("unipile_search_exception", title=title, error=str(e))

    await asyncio.gather(*[_search_one(title) for title in role_titles])
    return results


async def search_person_by_name(
    full_name: str,
    org_id: str = "",
    limit: int = 5,
) -> list[SearchedPerson]:
    """
    Search LinkedIn for a specific person by full name.
    Optionally filtered by company org_id.
    Used by Scout AI to find LinkedIn URLs for web-only contacts (no URL from initial search).
    """
    await _init_pool()
    settings = get_settings()
    if not settings.unipile_api_key or not full_name:
        return []

    account_id = _next_account_id()
    payload: dict = {
        "api": "classic",
        "category": "people",
        "keywords": full_name,
    }
    if org_id:
        payload["company"] = [str(org_id)]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{_base_url()}/linkedin/search",
                params={"account_id": account_id, "limit": min(limit, 25)},
                json=payload,
                headers=_headers(),
            )
        if resp.status_code != 200:
            logger.warning("unipile_name_search_error", name=full_name, status=resp.status_code)
            return []

        items = resp.json().get("items", [])
        results: list[SearchedPerson] = []
        for item in items:
            pid = item.get("public_identifier", "")
            if not pid:
                continue
            results.append({
                "full_name": item.get("name", ""),
                "linkedin_url": f"https://www.linkedin.com/in/{pid}",
                "public_identifier": pid,
                "headline": item.get("headline"),
                "location": item.get("location"),
                "network_distance": item.get("network_distance"),
            })
        logger.info("unipile_name_search_done", name=full_name, found=len(results))
        return results
    except Exception as e:
        logger.warning("unipile_name_search_exception", name=full_name, error=str(e))
        return []
