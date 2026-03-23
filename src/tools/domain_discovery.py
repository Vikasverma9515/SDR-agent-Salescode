"""
Domain and email format discovery — script only, no LLM.

Strategy:
1. Search for official domain using Perplexity -> Tavily -> DDG
2. Extract domain from result URLs and snippets via heuristics
3. Probe common email patterns against ZeroBounce to find the real format
4. Default to {first}.{last}@domain if probing yields nothing
"""
from __future__ import annotations

import asyncio
import re
from typing import TypedDict

from src.tools.search import search_with_fallback
from src.utils.logging import get_logger

logger = get_logger("domain_discovery")

# All 18 n8n-recognised email format patterns, in order of global prevalence.
# These are the ONLY accepted values for the n8n Email Format field.
# Example: first=amy, last=williams, first_initial=a, last_initial=w
_EMAIL_PATTERNS = [
    "{first}.{last}",               # amy.williams      (most common)
    "{first_initial}.{last}",       # a.williams
    "{first}{last}",                # amywilliams
    "{first_initial}{last}",        # awilliams
    "{first}_{last}",               # amy_williams
    "{first}.{last_initial}",       # amy.w
    "{first}_{last_initial}",       # amy_w
    "{first}{last_initial}",        # amyw
    "{first}",                      # amy
    "{first_initial}_{last}",       # a_williams
    "{last}.{first}",               # williams.amy
    "{last}{first}",                # williamsamy
    "{last}_{first}",               # williams_amy
    "{last}{first_initial}",        # williamsa
    "{last}.{first_initial}",       # williams.a
    "{last}_{first_initial}",       # williams_a
    "{last}",                       # williams
    "{last_initial}-{first_initial}",  # w-a
]

# Domains to skip when extracting from search result URLs
_SKIP_DOMAINS = {
    "google", "linkedin", "twitter", "facebook", "wikipedia",
    "clearbit", "hunter", "rocketreach", "bing", "yahoo",
    "duckduckgo", "perplexity", "tavily", "glassdoor", "crunchbase",
    "zoominfo", "bloomberg", "reuters", "economictimes",
}


class DomainInfo(TypedDict):
    domain: str | None
    email_format: str | None
    confidence: str  # "high", "medium", "low"
    sources: list[str]


async def _ask_gpt_for_domain(company_name: str) -> str | None:
    """
    Use OpenAI Responses API with web_search tool to find the official corporate domain.
    Returns the bare domain (e.g. 'dabur.com') or None on failure.

    Uses live web search — no training-data staleness, no subsidiary URL pollution
    because we instruct it to return HQ domain only.
    """
    from src.config import get_settings
    settings = get_settings()
    if not settings.openai_api_key:
        return None

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
                        f"What is the official corporate website domain for {company_name}? "
                        f"Return ONLY the bare domain (e.g. dabur.com) — the primary HQ domain, "
                        f"not a subsidiary, product, shop, or regional domain. No explanation."
                    ),
                },
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract text from output items
            content = ""
            for item in data.get("output", []):
                if item.get("type") == "message":
                    for part in item.get("content", []):
                        if part.get("type") == "output_text":
                            content = part.get("text", "").strip().lower()
                            break
                if content:
                    break

            logger.info("gpt_domain_response", company=company_name, content=content)

            if not content or content == "unknown":
                return None

            # Strip URL prefix / www if GPT returned a full URL
            content = re.sub(r'^https?://(?:www\.)?', '', content)
            content = re.sub(r'^www\.', '', content)
            content = content.split('/')[0].strip()

            if re.match(r'^[a-z0-9][a-z0-9\-]*\.[a-z]{2,}', content):
                return content

    except Exception as e:
        logger.warning("gpt_domain_error", company=company_name, error=str(e))

    return None


async def discover_domain(company_name: str) -> DomainInfo:
    """
    Discover the official domain and email format for a company.

    Domain lookup strategy (stops at first success):
    1. Ask GPT-4o-mini directly — knows HQ domains for major companies instantly,
       no search result pollution from subsidiary/product domains.
    2. Fallback: search snippets + URLs via Perplexity/Tavily/DDG.
    """
    # --- Step 1: GPT domain lookup (primary) ---
    domain = await _ask_gpt_for_domain(company_name)
    sources_used: list[str] = []

    if not domain:
        # --- Step 2: Search fallback ---
        all_snippets: list[str] = []

        queries = [
            f"{company_name} official website",
            f"{company_name} corporate email contact",
        ]

        for query in queries:
            try:
                results = await asyncio.wait_for(
                    search_with_fallback(query, max_results=6),
                    timeout=15,
                )
            except asyncio.TimeoutError:
                logger.warning("domain_search_timeout", query=query[:60])
                continue

            for r in results:
                all_snippets.append(r.snippet)
                if r.url:
                    sources_used.append(r.url)

            if all_snippets:
                break

        # Try snippets first (Perplexity answer text usually mentions the real domain)
        snippet_domain = _extract_domain_from_snippets(all_snippets, company_name)
        url_domain = _extract_domain_from_urls(sources_used, company_name)

        slug_clean = re.sub(r"[^a-z0-9]", "", company_name.lower())

        def _is_exact_slug(d: str | None) -> bool:
            if not d:
                return False
            root = d.split(".")[0].replace("-", "")
            return root == slug_clean

        if _is_exact_slug(snippet_domain):
            domain = snippet_domain
        elif _is_exact_slug(url_domain):
            domain = url_domain
        else:
            domain = url_domain or snippet_domain

    if not domain:
        logger.warning("domain_not_found", company=company_name)
        return DomainInfo(domain=None, email_format=None, confidence="low", sources=[])

    # --- Step 2: Probe email patterns via ZeroBounce ---
    logger.info("domain_probe_start", company=company_name, domain=domain, sources=sources_used[:3])
    email_format = await _probe_email_format(company_name, domain)

    confidence = "high" if email_format else "medium"
    if not email_format:
        email_format = f"{{first}}.{{last}}@{domain}"

    logger.info("domain_discovered", company=company_name, domain=domain, email_format=email_format, confidence=confidence)
    return DomainInfo(domain=domain, email_format=email_format, confidence=confidence, sources=sources_used[:5])


async def _probe_email_format(company_name: str, domain: str) -> str | None:
    """
    Infer email format using a 4-step funnel — stops as soon as we have enough signal:

    1. Scrape company website pages (contact, about, team) for mailto: links + plain emails
    2. Search snippets for @domain emails in PDFs, press releases, news articles
    3. Search snippets with filetype:pdf for annual reports (always have IR contact emails)
    4. Unipile: fetch a real employee name from LinkedIn, then probe their email
       variants via ZeroBounce. A `valid` hit on a real name is high-confidence.
       Catch-all detected via a single dummy probe upfront (cheap).
    """
    email_re = re.compile(
        r'\b([a-z0-9._%+\-]+)@' + re.escape(re.escape(domain)) + r'\b',
        re.IGNORECASE,
    )
    # simpler version without double-escape for actual use
    email_re = re.compile(
        r'([a-z0-9._%+\-]+)@' + re.escape(domain),
        re.IGNORECASE,
    )

    found_emails: list[str] = []

    # --- Step 1: Scrape company website pages ---
    pages_to_try = [
        f"https://{domain}/contact",
        f"https://{domain}/contact-us",
        f"https://www.{domain}/contact",
        f"https://www.{domain}/contact-us",
        f"https://{domain}/about",
        f"https://www.{domain}/about",
        f"https://{domain}/team",
        f"https://www.{domain}/our-team",
        f"https://{domain}/investor-relations",
        f"https://www.{domain}/investor-relations",
        f"https://{domain}/press",
        f"https://www.{domain}/media",
    ]
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=8, follow_redirects=True,
                                       headers={"User-Agent": "Mozilla/5.0"}) as client:
            for url in pages_to_try:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        text = resp.text
                        # mailto: links first — most reliable
                        mailto_emails = re.findall(r'mailto:([a-z0-9._%+\-]+@' + re.escape(domain) + r')', text, re.IGNORECASE)
                        plain_emails = email_re.findall(text)
                        batch = [e.lower() for e in (mailto_emails + plain_emails)]
                        found_emails.extend(batch)
                        if batch:
                            logger.info("email_from_website", url=url, found=batch[:3])
                        if len(found_emails) >= 3:
                            break
                except Exception:
                    continue
    except Exception as e:
        logger.warning("website_scrape_error", domain=domain, error=str(e))

    if len(found_emails) >= 2:
        fmt = await _infer_pattern_from_emails(found_emails, domain)
        if fmt:
            logger.info("email_format_from_website", domain=domain, fmt=fmt, sample=found_emails[:3])
            return fmt

    # --- Step 2: Search snippets for @domain emails in news/press releases ---
    search_queries = [
        f'"{company_name}" "@{domain}" press release contact',
        f'"@{domain}" email site:prnewswire.com OR site:businesswire.com OR site:globenewswire.com',
        f'"{domain}" contact email',
    ]
    for query in search_queries:
        try:
            results = await asyncio.wait_for(
                search_with_fallback(query, max_results=8),
                timeout=12,
            )
            for r in results:
                matches = email_re.findall(r.snippet)
                found_emails.extend(m.lower() for m in matches)
        except Exception:
            continue
        if len(found_emails) >= 3:
            break

    if found_emails:
        fmt = await _infer_pattern_from_emails(found_emails, domain)
        if fmt:
            logger.info("email_format_from_search", domain=domain, fmt=fmt, sample=found_emails[:3])
            return fmt

    # --- Step 3: PDF search (annual reports, investor docs always have named contacts) ---
    try:
        pdf_results = await asyncio.wait_for(
            search_with_fallback(
                f'"{company_name}" "@{domain}" filetype:pdf annual report investor',
                max_results=5,
            ),
            timeout=12,
        )
        for r in pdf_results:
            matches = email_re.findall(r.snippet)
            found_emails.extend(m.lower() for m in matches)
    except Exception:
        pass

    if found_emails:
        fmt = await _infer_pattern_from_emails(found_emails, domain)
        if fmt:
            logger.info("email_format_from_pdf_search", domain=domain, fmt=fmt, sample=found_emails[:3])
            return fmt

    # --- Step 4: Unipile LinkedIn person lookup + ZeroBounce probe ---
    return await _zerobounce_probe_with_real_name(company_name, domain, found_emails)


async def _infer_pattern_from_emails(
    local_parts: list[str],
    domain: str,
    name_hint: tuple[str, str] | None = None,
) -> str | None:
    """
    Given a list of local parts (before @), determine the email format pattern.

    If name_hint=(first, last) is provided (e.g. from Unipile lookup), we do
    exact matching against all 18 n8n patterns — perfectly accurate.

    Without a name hint, we use structural regex heuristics — handles the
    unambiguous patterns well; ambiguous ones (e.g. 'x.y' could be first.last
    or last.first) default to the more globally prevalent option.

    All 18 n8n patterns supported:
      {first}.{last}            amy.williams
      {first_initial}.{last}    a.williams
      {first}.{last_initial}    amy.w
      {first_initial}{last}     awilliams
      {first}{last}             amywilliams
      {first}_{last}            amy_williams
      {first}                   amy
      {first_initial}_{last}    a_williams
      {first}{last_initial}     amyw
      {first}_{last_initial}    amy_w
      {last}                    williams
      {last}.{first}            williams.amy
      {last}{first_initial}     williamsa
      {last}{first}             williamsamy
      {last}_{first}            williams_amy
      {last}.{first_initial}    williams.a
      {last}_{first_initial}    williams_a
      {last_initial}-{first_initial}  w-a
    """
    from collections import Counter

    # --- Filter: drop generic/functional/placeholder local parts ---
    _SKIP_LOCALS = {
        "info", "contact", "support", "hello", "sales", "hr", "admin",
        "noreply", "no-reply", "press", "media", "careers", "jobs",
        "marketing", "enquiry", "enquiries", "service", "help",
        "legal", "compliance", "finance", "accounts", "billing",
        "privacy", "security", "abuse", "postmaster", "webmaster",
        "newsletter", "notifications", "alerts", "do-not-reply",
        "business", "general", "enquire", "reception", "office",
        "team", "people", "talent", "recruit", "hiring", "work",
        "partnership", "partners", "vendor", "procurement", "ops",
    }
    _SKIP_SUBSTRINGS = (
        "care", "comm", "corp", "invest", "legal", "compli",
        "secure", "notif", "alert", "serv", "dept", "team",
        "office", "contact", "info", "news", "promo", "query",
        "busi", "general", "recept", "partner", "vendor", "procure",
    )
    _PLACEHOLDER_LOCALS = {
        "firstname", "lastname", "fullname", "name", "username",
        "firstnamelastname", "last", "first",
    }

    def _looks_personal(local: str) -> bool:
        if local in _SKIP_LOCALS or local in _PLACEHOLDER_LOCALS:
            return False
        for sub in _SKIP_SUBSTRINGS:
            if sub in local:
                return False
        return True

    name_emails = [e for e in local_parts if _looks_personal(e) and len(e) > 3]
    if not name_emails:
        return None

    # --- Exact matching when we know the real name ---
    if name_hint:
        first, last = name_hint[0].lower(), name_hint[1].lower()
        fi, li = first[0], last[0]

        # Build lookup: exact local → pattern
        exact_map: dict[str, str] = {
            f"{first}.{last}":  "{first}.{last}",
            f"{fi}.{last}":     "{first_initial}.{last}",
            f"{first}.{li}":    "{first}.{last_initial}",
            f"{fi}{last}":      "{first_initial}{last}",
            f"{first}{last}":   "{first}{last}",
            f"{first}_{last}":  "{first}_{last}",
            f"{first}":         "{first}",
            f"{fi}_{last}":     "{first_initial}_{last}",
            f"{first}{li}":     "{first}{last_initial}",
            f"{first}_{li}":    "{first}_{last_initial}",
            f"{last}":          "{last}",
            f"{last}.{first}":  "{last}.{first}",
            f"{last}{fi}":      "{last}{first_initial}",
            f"{last}{first}":   "{last}{first}",
            f"{last}_{first}":  "{last}_{first}",
            f"{last}.{fi}":     "{last}.{first_initial}",
            f"{last}_{fi}":     "{last}_{first_initial}",
            f"{li}-{fi}":       "{last_initial}-{first_initial}",
        }

        scores: Counter = Counter()
        for local in name_emails:
            pattern = exact_map.get(local)
            if pattern:
                scores[pattern] += 1

        if scores:
            best_pattern, count = scores.most_common(1)[0]
            total = len(name_emails)
            if count / total >= 0.4 or count >= 2:
                return f"{best_pattern}@{domain}"

        return None

    # --- gpt-4.1-mini: pick the best pattern from the 18 supported ones ---
    import asyncio as _asyncio
    from src.config import get_settings as _get_settings
    _settings = _get_settings()
    if not _settings.openai_api_key:
        return None

    try:
        from openai import AsyncOpenAI
        _oai = AsyncOpenAI(api_key=_settings.openai_api_key)

        _patterns = [
            "{first}.{last}", "{first_initial}.{last}", "{first}.{last_initial}",
            "{first_initial}{last}", "{first}{last}", "{first}_{last}", "{first}",
            "{first_initial}_{last}", "{first}{last_initial}", "{first}_{last_initial}",
            "{last}", "{last}.{first}", "{last}{first_initial}", "{last}{first}",
            "{last}_{first}", "{last}.{first_initial}", "{last}_{first_initial}",
            "{last_initial}-{first_initial}",
        ]
        _patterns_str = "\n".join(f"- {p}" for p in _patterns)
        _emails_str = ", ".join(name_emails[:20])  # cap at 20

        _prompt = (
            f"These are email local parts (before @) scraped from the domain {domain}:\n"
            f"{_emails_str}\n\n"
            f"Which ONE of the following patterns best describes how this company formats employee emails?\n"
            f"{_patterns_str}\n\n"
            f"IMPORTANT: Only consider local parts that look like personal names (first/last name combinations). "
            f"Ignore brand names, product names, or generic words.\n"
            f"Reply with ONLY the pattern exactly as written above, nothing else. "
            f"If you cannot determine, reply: unknown"
        )

        _resp = await _oai.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": _prompt}],
            temperature=0,
            max_tokens=30,
        )
        _answer = _resp.choices[0].message.content.strip()
        if _answer in _patterns:
            return f"{_answer}@{domain}"
    except Exception:
        pass

    return None


async def _zerobounce_probe_with_real_name(
    company_name: str,
    domain: str,
    found_emails: list[str] | None = None,
) -> str | None:
    """
    Step 4: find a real employee via Unipile, then:
    a) Try exact pattern matching against any emails already scraped in steps 1-3
       using the real name — this is free and instant.
    b) Probe all 18 pattern variants via ZeroBounce with the real name.
       A `valid` hit is high-confidence since we're testing a real person's address.

    Also catches catch-all domains via a single dummy probe upfront.
    Returns None if no real name available — never probes with fake names.
    """
    from src.tools.zerobounce import validate_email

    # --- Check for catch-all first (single cheap probe) ---
    try:
        result = await asyncio.wait_for(
            validate_email(f"zz.noreply.probe@{domain}"), timeout=10
        )
        if result.get("status") == "catch-all":
            logger.info("domain_catchall", domain=domain)
            return f"{{first}}.{{last}}@{domain}"
    except Exception:
        pass

    # --- Get a real employee name from Unipile ---
    first, last = await _get_real_employee_name(company_name)
    have_real_name = bool(first and last)

    if not have_real_name:
        # No real name available — probing with a fake name is pure credit waste.
        # ZeroBounce `valid` on a nonexistent person is essentially impossible.
        logger.info("zerobounce_probe_skip", domain=domain, reason="no real name from unipile")
        return None

    first_i = first[0]
    last_i = last[0]
    logger.info("zerobounce_probe_real_name", domain=domain, first=first, last=last)

    # --- 4a: Exact match against already-scraped emails using the real name ---
    if have_real_name and found_emails:
        fmt = await _infer_pattern_from_emails(found_emails, domain, name_hint=(first, last))
        if fmt:
            logger.info("email_format_from_name_hint", domain=domain, fmt=fmt)
            return fmt

    # --- 4b: ZeroBounce probe all 18 patterns with real name ---
    for pattern in _EMAIL_PATTERNS:
        test_local = (
            pattern
            .replace("{first}", first)
            .replace("{last}", last)
            .replace("{first_initial}", first_i)
            .replace("{last_initial}", last_i)
        )
        test_email = f"{test_local}@{domain}"

        try:
            result = await asyncio.wait_for(validate_email(test_email), timeout=10)
            status = result.get("status", "unknown")

            if status == "catch-all":
                logger.info("domain_catchall", domain=domain)
                return f"{{first}}.{{last}}@{domain}"

            if status == "valid":
                logger.info("zerobounce_probe_hit", domain=domain, pattern=pattern, email=test_email)
                return f"{pattern}@{domain}"

        except Exception:
            continue

    return None


async def _get_real_employee_name(company_name: str) -> tuple[str, str]:
    """
    Fetch one real employee name from Unipile LinkedIn search for the company.
    Returns (first, last) lowercased, or ("", "") if nothing found.

    Tries common senior titles — we want someone whose email almost certainly
    exists (senior people always have corporate emails).
    """
    try:
        from src.tools.unipile import get_company_org_id, search_people

        org_info = await asyncio.wait_for(get_company_org_id(company_name), timeout=15)
        org_id = org_info.get("org_id")
        if not org_id:
            return "", ""

        # Search for any senior person — just need one real name
        titles_to_try = [
            "Director", "VP", "Head", "Manager", "Chief",
        ]
        people = await asyncio.wait_for(
            search_people(org_id, titles_to_try[:2], limit=5),
            timeout=20,
        )

        for person in people:
            full_name = (person.get("full_name") or "").strip()
            parts = full_name.split()
            # Need at least first + last, no weird characters
            if len(parts) >= 2 and all(re.match(r'^[a-zA-Z\-]+$', p) for p in parts[:2]):
                first = parts[0].lower()
                last = parts[-1].lower()
                # Skip if first/last look like initials or are too short
                if len(first) >= 2 and len(last) >= 2:
                    logger.info("zerobounce_real_name_found", company=company_name, first=first, last=last)
                    return first, last

    except Exception as e:
        logger.warning("zerobounce_real_name_error", company=company_name, error=str(e))

    return "", ""


def _extract_domain_from_urls(urls: list[str], company_name: str) -> str | None:
    """
    Extract the most likely corporate domain from search result URLs.

    Scoring priority (higher = better):
      3 — contains company slug AND is a short/clean domain (no geo/regional suffix)
      2 — contains company slug but has a regional/descriptor suffix
      1 — any non-skip domain
    Returns the highest-scoring domain seen.
    """
    slug = re.sub(r"[^a-z0-9]", "", company_name.lower())

    # Words that indicate a subsidiary/product/regional domain rather than HQ corporate
    _SUBSIDIARY_WORDS = (
        "international", "global", "usa", "uk", "india", "uae",
        "africa", "latam", "asia", "europe", "shop", "store",
        "hotels", "infotech", "care", "foundation", "arogya",
        "ayurved", "herbal", "natural", "organic", "health",
        "digital", "ventures", "capital", "realty", "foods",
        "honey", "dairy", "baby", "kids", "pharma", "beauty",
        "hair", "skin", "home", "garden", "pet", "agro",
    )

    def _score(domain: str) -> tuple[int, int]:
        """Return (priority_score, neg_length) — higher score and shorter domain win.

        Priority 4: domain root is EXACTLY the company slug (e.g. dabur.com for Dabur)
        Priority 3: slug is contained in domain root but has extra words (e.g. daburindia.com)
        Priority 2: slug in domain but also has subsidiary/product keywords
        Priority 1: no slug match
        """
        bare = domain.replace(".", "").replace("-", "")
        # domain root = everything before the first dot, hyphens removed
        domain_root = domain.split(".")[0].replace("-", "")
        # Priority 4: exact match — domain root IS the slug (e.g. "dabur" == "dabur")
        is_exact = domain_root == slug
        # Priority 3: slug fully contained but has extra characters appended
        has_slug = slug in bare and len(slug) >= 4  # full slug, min 4 chars to avoid false positives
        has_subsidiary = any(s in bare for s in _SUBSIDIARY_WORDS)
        if is_exact and not has_subsidiary:
            priority = 4
        elif has_slug and not has_subsidiary:
            priority = 3
        elif has_slug:
            priority = 2
        else:
            priority = 1
        return (priority, -len(domain))  # shorter domain wins ties

    candidates: list[tuple[tuple[int, int], str]] = []
    for url in urls:
        m = re.search(r"https?://(?:www\.)?([^/]+)", url)
        if not m:
            continue
        domain = m.group(1).lower()
        if any(skip in domain for skip in _SKIP_DOMAINS):
            continue
        if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", domain):
            continue
        candidates.append((_score(domain), domain))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _extract_domain_from_snippets(snippets: list[str], company_name: str) -> str | None:
    """Extract domain from snippet text (e.g. 'visit us at dabur.com')."""
    slug = re.sub(r"[^a-z0-9]", "", company_name.lower())
    all_text = " ".join(snippets)

    # Look for explicit domain mentions like "www.xxx.com" or "visit xxx.com"
    candidates = re.findall(r'\b(?:www\.)?([a-z0-9-]+\.[a-z]{2,4})\b', all_text.lower())
    exact_hit = None
    partial_hit = None
    for candidate in candidates:
        bare = candidate.replace(".", "").replace("-", "")
        if any(skip in candidate for skip in _SKIP_DOMAINS):
            continue
        root = candidate.split(".")[0].replace("-", "")
        if root == slug:
            # Exact slug match (e.g. dabur.com for "Dabur") — return immediately
            return candidate
        if not exact_hit and slug in bare and len(slug) >= 4:
            partial_hit = candidate

    return partial_hit


def construct_email(full_name: str, email_format: str, domain: str) -> str | None:
    """
    Construct an email address from a name + format pattern + domain.

    Supported placeholders: {first}, {last}, {first_initial}, {last_initial}
    """
    parts = full_name.strip().split()
    if len(parts) < 2:
        return None

    first = parts[0].lower()
    last = parts[-1].lower()
    first_initial = first[0]
    last_initial = last[0]

    try:
        email = email_format.replace("{first}", first)
        email = email.replace("{last}", last)
        email = email.replace("{first_initial}", first_initial)
        email = email.replace("{last_initial}", last_initial)
        email = email.replace("{first_name}", first)
        email = email.replace("{last_name}", last)

        if "@" not in email:
            email = f"{email}@{domain}"
        elif "domain.com" in email:
            email = email.replace("domain.com", domain)

        if re.match(r"^[a-z0-9.+_-]+@[a-z0-9.-]+\.[a-z]{2,}$", email):
            return email

    except Exception:
        pass

    return None
