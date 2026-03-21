"""
Searcher - Contact Gap-Fill Agent (Agent 3)

Runs AFTER Veri. Reads Final Filtered List, identifies companies that are
missing Decision Maker contacts, then discovers and appends new DM contacts
in the same A-U format.

Graph:
START -> load_gap_analysis -> unipile_search -> search_company_website
      -> deduplicate -> validate_linkedin
      -> enrich_contacts -> write_to_sheet -> advance_or_finish -> (loop or END)
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Literal

from langgraph.graph import StateGraph, END

from src.config import get_settings
from src.state import Contact, SearcherState
from src.tools import sheets, zerobounce as zb
from src.tools.domain_discovery import construct_email
from src.tools.search import search, search_with_fallback
from src.tools import theorg, wikidata
from src.utils.logging import get_logger

logger = get_logger("searcher")

# ---------------------------------------------------------------------------
# Role bucket keyword lookup table — deterministic, no LLM
# ---------------------------------------------------------------------------

_DM_KEYWORDS = {
    "chief executive", "ceo", "managing director", "md", "president",
    "chief operating", "coo", "chief commercial", "chief revenue",
    "vp sales", "vice president sales", "head of sales", "sales director",
    "vp marketing", "vice president marketing", "head of marketing", "marketing director",
    "chief marketing", "cmo",
    "chief digital", "cdo", "chief technology", "cto", "chief information", "cio",
    "vp digital", "head of digital", "digital director", "ecommerce director",
    "director of ecommerce", "head of ecommerce",
    "general manager", "gm", "country manager", "regional director",
    "vp operations", "operations director", "supply chain director",
    "head of supply chain", "procurement director", "vp procurement",
}

_INFLUENCER_KEYWORDS = {
    "director", "vp", "vice president", "head of", "senior manager",
    "sr manager", "group manager", "principal", "lead", "manager",
    "senior director", "associate director",
}

_GATEKEEPER_KEYWORDS = {
    "assistant", "coordinator", "executive assistant", "personal assistant",
    "secretary", "administrator", "office manager", "receptionist",
    "executive coordinator",
}


def _classify_role(role_title: str) -> Literal["DM", "Influencer", "GateKeeper", "Unknown"]:
    """
    Classify a role title into a bucket using keyword lookup.
    Priority: DM > GateKeeper > Influencer > Unknown
    """
    if not role_title:
        return "Unknown"
    t = role_title.lower()

    for kw in _DM_KEYWORDS:
        if kw in t:
            return "DM"

    for kw in _GATEKEEPER_KEYWORDS:
        if kw in t:
            return "GateKeeper"

    for kw in _INFLUENCER_KEYWORDS:
        if kw in t:
            return "Influencer"

    return "Unknown"


# ---------------------------------------------------------------------------
# Node: load_gap_analysis
# ---------------------------------------------------------------------------

async def load_gap_analysis(state: SearcherState) -> SearcherState:
    """
    1. Read Target Accounts to get org_id, domain, email_format that Fini already discovered.
    2. Build a set of existing person names from both First Clean List AND Final Filtered List.
    3. Determine which DM roles from input are not yet covered.
    """
    logger.info("searcher_gap_analysis", company=state.target_company)

    # --- Step 1: pull Fini's work from Target Accounts ---
    org_id = ""
    domain = state.target_domain or ""
    email_format = state.target_email_format or ""
    account_type = ""
    normalized_company_name = state.target_company  # fallback to input if not in sheet

    try:
        import re as _re
        from rapidfuzz import fuzz as _fuzz
        ta_records = await sheets.read_all_records(sheets.TARGET_ACCOUNTS)
        input_lower = state.target_company.lower().strip()

        # Pick the best-matching row by company name (token_set_ratio handles
        # "nestle" vs "Nestlé India Limited" correctly).
        # Do NOT filter on Sales Navigator Link — it may be empty for some rows
        # and we still need domain + email_format from them.
        best_row = None
        best_score = 0
        for row in ta_records:
            sheet_name = str(row.get("Company Name", "") or "").strip()
            if not sheet_name:
                continue
            score = _fuzz.token_set_ratio(input_lower, sheet_name.lower())
            if score > best_score:
                best_score = score
                best_row = row

        if best_row is not None and best_score >= 60:
            normalized_company_name = str(best_row.get("Company Name", "") or state.target_company).strip()
            # Extract org_id from Sales Nav URL (col C) if present
            sales_nav_url = str(best_row.get("Sales Navigator Link", "") or "")
            m = _re.search(r"organization%253A(\d+)", sales_nav_url)
            if m:
                org_id = m.group(1)
            domain = domain or str(best_row.get("Company Domain", "") or "").strip()
            # Header may be "Email Format( Firstname-amy , Lastname- williams)" — match by prefix
            _ef_key = next((k for k in best_row if str(k).startswith("Email Format")), "Email Format")
            email_format = email_format or str(best_row.get(_ef_key, "") or "").strip()
            account_type = str(best_row.get("Account type", "") or "").strip()
            logger.info("searcher_target_accounts_match",
                        input=state.target_company,
                        matched=normalized_company_name,
                        score=best_score,
                        org_id=org_id or "(not found)",
                        domain=domain,
                        email_format=email_format or "(not found)")
        else:
            logger.warning("searcher_target_accounts_no_match",
                           input=state.target_company,
                           best_score=best_score,
                           msg="No matching row in Target Accounts — run Fini first")
    except Exception as e:
        logger.warning("searcher_target_accounts_read_error", error=str(e))

    # --- Step 2: read existing contacts from Final Filtered List + Searcher Output ---
    # Find which roles are already covered so we only search for the gaps.
    existing_names: list[str] = []
    existing_role_titles: list[str] = []
    _match_name = normalized_company_name.lower()
    try:
        for tab in (sheets.FINAL_FILTERED_LIST, sheets.FIRST_CLEAN_LIST):
            records = await sheets.read_all_records(tab)
            for row in records:
                company_col = str(row.get("Company Name", "")).lower()
                if _match_name not in company_col and state.target_company.lower() not in company_col:
                    continue
                first = str(row.get("First Name", "")).strip()
                last = str(row.get("Last Name", "")).strip()
                full = f"{first} {last}".strip().lower()
                if full:
                    existing_names.append(full)
                role = str(row.get("Role Title", row.get("Title", row.get("Job Title", "")))).strip().lower()
                if role:
                    existing_role_titles.append(role)
    except Exception as e:
        logger.warning("searcher_existing_names_read_error", error=str(e))

    # --- Step 3: determine which requested roles are not yet covered ---
    requested_roles = list(state.dm_roles) if state.dm_roles else [
        "CEO", "MD", "Managing Director", "CMO", "VP Marketing", "Head of Marketing",
        "CDO", "CTO", "VP Digital", "Head of Digital", "VP Ecommerce", "Head of Ecommerce",
        "VP Sales", "Head of Sales", "Sales Director", "General Manager", "Country Manager",
    ]

    # Check each requested role against existing role titles using fuzzy match
    from rapidfuzz import fuzz as _role_fuzz
    missing_roles = []
    for role in requested_roles:
        already_covered = any(
            _role_fuzz.partial_ratio(role.lower(), existing.lower()) >= 80
            for existing in existing_role_titles
        )
        if not already_covered:
            missing_roles.append(role)

    if not missing_roles:
        logger.info("searcher_gap_analysis_all_covered",
                    company=state.target_company,
                    msg="All requested roles already covered in sheets")
        return state.model_copy(update={
            "target_org_id": org_id,
            "target_domain": domain,
            "target_email_format": email_format,
            "target_region": account_type,
            "target_normalized_name": normalized_company_name,
            "missing_dm_roles": [],
            "phase": "done",
        })

    logger.info("searcher_gap_analysis_roles",
                company=state.target_company,
                requested=requested_roles,
                already_covered=[r for r in requested_roles if r not in missing_roles],
                missing=missing_roles)

    logger.info(
        "searcher_gap_analysis_done",
        company=state.target_company,
        org_id=org_id or "(will fetch)",
        domain=domain,
        email_format=email_format,
        region=account_type,
        existing_names_count=len(existing_names),
        roles=missing_roles,
    )

    return state.model_copy(update={
        "target_org_id": org_id,
        "target_domain": domain,
        "target_email_format": email_format,
        "target_region": account_type,
        "target_normalized_name": normalized_company_name,
        "missing_dm_roles": missing_roles,
        "phase": "unipile_search",
    })


# ---------------------------------------------------------------------------
# Node: unipile_search  (primary LinkedIn discovery)
# ---------------------------------------------------------------------------

async def unipile_search(state: SearcherState) -> SearcherState:
    """
    Primary contact discovery via Unipile LinkedIn search API.

    Uses org_id from Target Accounts (Fini already looked this up) — avoids
    redundant Unipile org lookup. Falls back to fresh lookup only if missing.

    For each result, verifies current employment via Unipile profile fetch.
    Results added to discovered_contacts with provenance="unipile_search".
    """
    from src.tools.unipile import get_company_org_id, search_people, verify_profile
    from src.agents.fini import REGION_IDS

    logger.info("searcher_unipile_search", company=state.target_company)

    role_titles = state.missing_dm_roles or [
        "CEO", "MD", "Managing Director", "CMO", "VP Marketing", "Head of Marketing",
        "CDO", "CTO", "VP Digital", "Head of Digital", "VP Ecommerce", "Head of Ecommerce",
        "VP Sales", "Head of Sales", "Sales Director", "General Manager", "Country Manager",
    ]

    # Use org_id from Target Accounts if Fini already found it;
    # otherwise fall back to a fresh Unipile lookup.
    org_id = state.target_org_id or ""

    if not org_id:
        logger.info("searcher_unipile_org_lookup", company=state.target_company,
                    reason="org_id not in Target Accounts, fetching from Unipile")
        try:
            org_info = await get_company_org_id(state.target_company)
            org_id = org_info.get("org_id", "") or ""
        except Exception as e:
            logger.warning("searcher_unipile_org_error", company=state.target_company, error=str(e))

    if not org_id:
        logger.warning("searcher_unipile_no_org", company=state.target_company,
                       msg="skipping Unipile search, will rely on web/filings search")
        return state

    # Resolve region_id from Target Accounts account_type (e.g. "India" -> "102713980")
    region_id = REGION_IDS.get(state.target_region.lower(), "") if state.target_region else ""
    logger.info("searcher_unipile_org_ready", company=state.target_company, org_id=org_id,
                region=state.target_region or "(none)", region_id=region_id or "(none)")

    # Search LinkedIn for people matching the requested role titles.
    # Try with region filter first; if no results, retry without (small companies often have
    # no region-tagged profiles, so the filter eliminates everyone).
    try:
        people = await search_people(org_id, role_titles, region_id=region_id, limit=25)
        if not people and region_id:
            logger.info("searcher_unipile_retry_no_region", company=state.target_company)
            people = await search_people(org_id, role_titles, region_id="", limit=25)
    except Exception as e:
        logger.warning("searcher_unipile_search_error", error=str(e))
        return state

    # Unipile org_id search can return ex-employees or followers.
    # verify_profile is the hard gate: only keep if still_employed=True at this company.
    new_contacts: list[Contact] = []
    for person in people:
        role_title = person.get("headline") or ""
        still_at_company = False
        try:
            verification = await verify_profile(person["linkedin_url"], state.target_company)
            if verification.get("valid") and verification.get("still_employed") and verification.get("at_target_company"):
                still_at_company = True
                role_title = verification.get("current_role") or role_title
            else:
                logger.info("searcher_unipile_verify_drop",
                            name=person["full_name"],
                            reason="not currently at target company",
                            verification=verification)
                continue  # drop — they're not at this company anymore
        except Exception as e:
            logger.warning("searcher_unipile_verify_error", person=person["full_name"], error=str(e))
            # Can't verify — keep with unverified status rather than dropping entirely
            still_at_company = False

        bucket = _classify_role(role_title)

        company_name = state.target_normalized_name or state.target_company
        contact = Contact(
            full_name=person["full_name"],
            company=company_name,
            domain=state.target_domain,
            role_title=role_title,
            role_bucket=bucket,
            linkedin_url=person["linkedin_url"],
            linkedin_verified=still_at_company,
            provenance=["unipile_search"],
        )
        new_contacts.append(contact)
        logger.info(
            "searcher_unipile_contact",
            name=person["full_name"],
            role=role_title,
            bucket=bucket,
            verified=still_at_company,
        )

    # If Unipile returned results but all were dropped (ex-employees, wrong company),
    # still trigger GPT-5 URL fallback to find verified current employees.
    if not new_contacts:
        logger.info("searcher_unipile_no_results_gpt5_fallback", company=state.target_company,
                    reason="all unipile results failed verify_profile" if people else "unipile returned 0 results")
        gpt_contacts: list[Contact] = []
        try:
            from openai import AsyncOpenAI
            import re as _re2
            _oai = AsyncOpenAI(api_key=get_settings().openai_api_key)
            company_name = state.target_normalized_name or state.target_company
            domain = state.target_domain or ""

            for role in role_titles:
                try:
                    _prompt = (
                        f"Find the LinkedIn profile URL of the current {role} at {company_name} (website: {domain}).\n"
                        f"Search LinkedIn directly and return ONLY the LinkedIn profile URL "
                        f"(linkedin.com/in/...). Nothing else. No explanation."
                    )
                    _resp = await _oai.responses.create(
                        model="gpt-5",
                        tools=[{"type": "web_search"}],
                        input=_prompt,
                    )
                    _text = ""
                    for _item in (_resp.output or []):
                        content = getattr(_item, "content", None) or []
                        for _part in content:
                            t = getattr(_part, "text", None)
                            if t:
                                _text += t
                    # Extract all linkedin.com/in/ URLs from the response
                    urls = _re2.findall(r'https?://(?:www\.|in\.)?linkedin\.com/in/[\w\-]+', _text)
                    urls = [u.rstrip("/") for u in urls]
                    logger.info("searcher_gpt5_urls", role=role, urls=urls)

                    for li_url in urls:
                        try:
                            verification = await verify_profile(li_url, company_name)
                            if not (verification.get("valid") and verification.get("still_employed") and verification.get("at_target_company")):
                                logger.info("searcher_gpt5_verify_fail", url=li_url, reason=verification)
                                continue
                            full_name = verification.get("full_name") or ""
                            current_role = verification.get("current_role") or role
                            if not full_name:
                                continue
                            gpt_contacts.append(Contact(
                                full_name=full_name,
                                company=company_name,
                                domain=state.target_domain,
                                role_title=current_role,
                                role_bucket=_classify_role(current_role),
                                linkedin_url=li_url,
                                linkedin_verified=True,
                                provenance=["gpt5_unipile_verified"],
                            ))
                            logger.info("searcher_gpt5_verified_contact",
                                        name=full_name, role=current_role, url=li_url)
                            break  # found one for this role, move to next
                        except Exception as e:
                            logger.warning("searcher_gpt5_verify_error", url=li_url, error=str(e))
                except Exception as e:
                    logger.warning("searcher_gpt5_role_error", role=role, error=str(e))

        except Exception as e:
            logger.warning("searcher_unipile_gpt5_error", error=str(e))

        if gpt_contacts:
            logger.info("searcher_gpt5_contacts_found", count=len(gpt_contacts))
            existing = list(state.discovered_contacts)
            return state.model_copy(update={
                "discovered_contacts": existing + gpt_contacts,
                "target_org_id": org_id,
            })

    logger.info("searcher_unipile_done", company=state.target_company, found=len(new_contacts))
    existing = list(state.discovered_contacts)
    return state.model_copy(update={
        "discovered_contacts": existing + new_contacts,
        "target_org_id": org_id,  # persist so enrich_contacts probe can use it
    })


# ---------------------------------------------------------------------------
# Node: search_company_website
# ---------------------------------------------------------------------------

_LEADERSHIP_PATHS = [
    "/about", "/about-us", "/about-us/team", "/about/team",
    "/about/leadership", "/about/management", "/about/management-team",
    "/about-us/leadership", "/about-us/management", "/about-us/management-team",
    "/about-us/our-leadership-team", "/aboutus/leadership",
    "/board-of-directors", "/about/board-of-directors",
    "/our-team", "/team", "/leadership-team", "/management-team",
    "/company/leadership", "/company/about", "/who-we-are/leadership",
    "/en/about-us/leadership", "/en/about/management",
    "/people", "/who-we-are", "/meet-the-team",
]

_TITLE_KEYWORDS = [
    "ceo", "cto", "cmo", "cdo", "coo", "cfo", "cio",
    "chief", "president", "vice president",
    "head of", "managing director", "general manager", "country manager",
    "vp of", "vp,", "vp -", "director of", "director,", "director -",
    "co-founder", "co founder", "founder &",
]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xhtml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


_NAME_STOPWORDS = {
    "the", "our", "meet", "team", "about", "us", "company", "welcome", "to",
    "nextgen", "global", "presence", "policy", "privacy", "solutions", "services",
    "platform", "product", "sales", "impact", "digital", "technology", "ai",
    "group", "india", "limited", "pvt", "ltd", "inc", "corp",
    # job title words that look like names
    "lead", "manager", "associate", "senior", "junior", "head", "director",
    "engineer", "analyst", "executive", "officer", "coordinator", "specialist",
    "key", "leadership", "management", "board", "investor", "advisor", "founder",
}

def _looks_like_name(text: str) -> bool:
    text = re.sub(r'^(Mr\.?|Ms\.?|Mrs\.?|Dr\.?)\s+', '', text.strip(), flags=re.I)
    # Must be 2-4 words
    parts = text.split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    # No digits
    if any(c.isdigit() for c in text):
        return False
    # No special characters except hyphens/apostrophes in names
    if re.search(r'[&@#%$|/\\<>]', text):
        return False
    # All parts must start with uppercase letter
    if not all(p[0].isupper() for p in parts if p and p[0].isalpha()):
        return False
    # Each word should be purely alphabetic (allow hyphens/apostrophes)
    if not all(re.match(r"^[A-Za-z][A-Za-z'\-]*$", p) for p in parts):
        return False
    # No stopwords
    if any(p.lower() in _NAME_STOPWORDS for p in parts):
        return False
    # Reasonable length
    if len(text) > 40:
        return False
    return True


def _clean_name(text: str) -> str:
    return re.sub(r'^(Mr\.?|Ms\.?|Mrs\.?|Dr\.?)\s+', '', text.strip(), flags=re.I)


def _role_looks_external(role_text: str) -> bool:
    """
    Return True if a role title looks like an advisor/investor bio rather than
    a current employee title. Clues: 'Former', a company name after the first comma,
    multiple company references separated by commas.
    """
    tl = role_text.lower()
    # Contains "former" → ex-employee or advisor referencing past role
    if "former" in tl or "ex-" in tl or "ex " in tl:
        return True
    # Multiple comma-separated segments where later segments look like org names
    # e.g. "Chairman, Clover Infotech" — the part after the first comma is another company
    parts = [p.strip() for p in role_text.split(",")]
    if len(parts) >= 2:
        # If second segment doesn't look like a sub-role qualifier (e.g. "Head of Sales, India")
        # but instead looks like a company name (capitalised multi-word phrase), it's external
        second = parts[1].strip()
        # Short geographic qualifiers are fine ("India", "APAC", "South Asia")
        if len(second.split()) >= 2 and not any(
            geo in second.lower()
            for geo in ("india", "apac", "asia", "europe", "global", "north", "south", "east", "west", "region")
        ):
            return True
    return False


async def _filter_garbage_names(contacts: list[Contact], company_name: str) -> list[Contact]:
    """
    Use gpt-4.1-mini to filter out garbage names that passed heuristic checks.
    e.g. "Pet Life", "View Larger", "Rx Appoints" — structurally look like FirstName LastName
    but are not real people.
    """
    if not contacts:
        return contacts

    from src.config import get_settings
    settings = get_settings()
    if not settings.openai_api_key:
        return contacts

    # Only filter contacts that don't already have a verified LinkedIn
    # (if Unipile verified them, the name is real)
    needs_check = [c for c in contacts if not c.linkedin_verified]
    already_verified = [c for c in contacts if c.linkedin_verified]

    if not needs_check:
        return contacts

    try:
        from openai import AsyncOpenAI
        _oai = AsyncOpenAI(api_key=settings.openai_api_key)

        names_list = "\n".join(f"{i+1}. {c.full_name}" for i, c in enumerate(needs_check))

        prompt = (
            f"Which of these are REAL HUMAN NAMES (not brand names, product names, "
            f"button text, generic phrases, or company names)?\n\n"
            f"{names_list}\n\n"
            f"Reply with ONLY the numbers of real human names, comma-separated. "
            f"Example: 1,3,5\n"
            f"If none are real names, reply: none"
        )

        response = await _oai.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=100,
        )
        answer = response.choices[0].message.content.strip().lower()
        logger.info("searcher_name_filter_response", company=company_name, answer=answer)

        if answer == "none":
            kept = already_verified
        else:
            keep_indices = set()
            for part in answer.replace(" ", "").split(","):
                try:
                    keep_indices.add(int(part) - 1)
                except ValueError:
                    pass
            kept = already_verified + [c for i, c in enumerate(needs_check) if i in keep_indices]

        dropped = len(contacts) - len(kept)
        if dropped:
            logger.info("searcher_name_filter_dropped",
                        company=company_name, dropped=dropped, kept=len(kept))
        return kept

    except Exception as e:
        logger.warning("searcher_name_filter_error", company=company_name, error=str(e))
        return contacts


def _extract_from_html(html: str, company_name: str, domain: str = "") -> list[Contact]:
    """Sliding-window extractor: finds (title, name) or (name, title) pairs in text nodes."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    if len(soup.get_text(strip=True)) < 200:
        return []  # JS shell — not rendered

    contacts: list[Contact] = []
    seen: set[str] = set()
    all_texts = [t.strip() for t in soup.stripped_strings if t.strip() and len(t.strip()) > 2]

    for i, text in enumerate(all_texts):
        tl = text.lower()
        if any(kw in tl for kw in _TITLE_KEYWORDS) and len(text) < 120:
            if _role_looks_external(text):
                continue  # advisor/investor bio — skip
            for offset in [-2, -1, 1, 2]:
                j = i + offset
                if 0 <= j < len(all_texts):
                    candidate = all_texts[j]
                    if _looks_like_name(candidate):
                        name = _clean_name(candidate)
                        if name not in seen:
                            seen.add(name)
                            contacts.append(Contact(
                                full_name=name,
                                company=company_name,
                                domain=domain,
                                role_title=text,
                                provenance=["company_website"],
                            ))
                        break

    return contacts





async def _scrape_httpx(domain: str, company_name: str) -> tuple[list[Contact], bool]:
    """
    Try common leadership paths via plain HTTP.
    Returns (contacts, hit_403) — hit_403=True means site is blocking us.
    """
    import httpx as _httpx
    _d = domain.strip()
    if _d.startswith("https://"):
        _d = _d[8:]
    elif _d.startswith("http://"):
        _d = _d[7:]
    base = f"https://{_d.rstrip('/')}"
    hit_403 = False

    try:
        async with _httpx.AsyncClient(headers=_HEADERS, follow_redirects=True, timeout=10) as client:
            for path in _LEADERSHIP_PATHS:
                url = base + path
                try:
                    resp = await client.get(url)
                    if resp.status_code == 403:
                        hit_403 = True
                        continue
                    if resp.status_code != 200:
                        continue
                    contacts = _extract_from_html(resp.text, company_name, domain)
                    if contacts:
                        logger.info("searcher_website_httpx_hit",
                                    url=url, contacts=len(contacts))
                        return contacts, False
                    # Page loaded but empty — might be JS-rendered, flag as 403-like
                    hit_403 = True
                except Exception:
                    continue
    except Exception as e:
        logger.warning("searcher_website_httpx_error", domain=domain, error=str(e))

    return [], hit_403


async def _scrape_gpt5(domain: str, company_name: str) -> list[Contact]:
    """
    GPT-5 web search fallback for sites that block httpx (403/JS-rendered).
    Uses gpt-5 with web_search tool to find current leadership from the company's own website.
    """
    from src.config import get_settings
    settings = get_settings()
    if not settings.openai_api_key:
        return []

    import httpx as _httpx
    prompt = (
        f"Find the CURRENT board of directors and leadership team for {company_name} "
        f"from their official website {domain}. "
        f"List only current members — ignore anyone who has resigned or retired. "
        f"Return ONLY a plain list, one per line: Full Name — Job Title. "
        f"No explanation, no markdown, no numbering, no citation brackets."
    )

    try:
        async with _httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-5",
                    "tools": [{"type": "web_search"}],
                    "input": prompt,
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

        if not content:
            return []

        contacts: list[Contact] = []
        seen: set[str] = set()
        for line in content.splitlines():
            line = re.sub(r'\*+', '', line).strip().lstrip('-•*').strip()
            # Remove citation brackets like [1], [2]
            line = re.sub(r'\[\d+\]', '', line).strip()
            if '—' in line or '–' in line or ' - ' in line:
                parts = re.split(r'\s*[—–]\s*|\s+-\s+', line, maxsplit=1)
                if len(parts) == 2:
                    name = _clean_name(parts[0].strip())
                    title = parts[1].strip()
                    if name and title and _looks_like_name(name) and name not in seen:
                        seen.add(name)
                        contacts.append(Contact(
                            full_name=name,
                            company=company_name,
                            role_title=title,
                            provenance=["company_website_gpt5"],
                        ))

        logger.info("searcher_website_gpt5_done",
                    domain=domain, contacts=len(contacts))
        return contacts

    except Exception as e:
        logger.warning("searcher_website_gpt5_error", domain=domain, error=str(e))
        return []


async def search_company_website(state: SearcherState) -> SearcherState:
    """
    Scrape the company's own website for leadership/board pages.
    1. Try httpx on common paths — fast, free, works for most sites.
    2. If 403 or JS-rendered shell → fall back to GPT-5 web search.
    """
    domain = state.target_domain
    if not domain:
        logger.info("searcher_website_skip", reason="no domain")
        return state

    company_name = state.target_normalized_name or state.target_company
    logger.info("searcher_website_search", company=company_name, domain=domain)

    contacts, hit_403 = await _scrape_httpx(domain, company_name)

    if not contacts and hit_403:
        logger.info("searcher_website_fallback_gpt5", domain=domain,
                    reason="httpx blocked or JS-rendered")
        contacts = await _scrape_gpt5(domain, company_name)

    if contacts:
        logger.info("searcher_website_done", company=company_name, found=len(contacts))
        existing = list(state.discovered_contacts)
        return state.model_copy(update={"discovered_contacts": existing + contacts})

    logger.info("searcher_website_no_results", company=company_name)
    return state


# ---------------------------------------------------------------------------
# Node: search_filings
# ---------------------------------------------------------------------------

async def search_filings(state: SearcherState) -> SearcherState:
    """Search exchange filings and annual reports for missing DM contacts."""
    logger.info("searcher_search_filings", company=state.target_company)

    role_str = ", ".join(state.missing_dm_roles) if state.missing_dm_roles else "VP Director CEO"

    queries = [
        f"{state.target_company} annual report key management {role_str} 2023 2024",
        f"{state.target_company} board directors management team filetype:pdf",
        f'"{state.target_company}" {role_str} leadership team',
    ]

    company_name = state.target_normalized_name or state.target_company
    raw_contacts: list[Contact] = []
    for query in queries:
        try:
            results = await search_with_fallback(query, max_results=8)
            for r in results:
                raw_contacts.extend(
                    _extract_names_from_snippet(r["snippet"], company_name, r["url"])
                )
        except Exception as e:
            logger.warning("searcher_filings_query_error", query=query[:60], error=str(e))

    logger.info("searcher_filings_done", company=state.target_company, found=len(raw_contacts))
    new_contacts = list(state.discovered_contacts) + raw_contacts
    return state.model_copy(update={"discovered_contacts": new_contacts, "phase": "web_search"})


# ---------------------------------------------------------------------------
# Node: search_web
# ---------------------------------------------------------------------------

async def search_web(state: SearcherState) -> SearcherState:
    """Multi-source web search targeting missing DM roles."""
    logger.info("searcher_search_web", company=state.target_company)

    role_str = " ".join(state.missing_dm_roles) if state.missing_dm_roles else "VP Director"
    queries = [
        f"{state.target_company} {role_str} ecommerce digital",
        f"{state.target_company} CTO CDO chief digital officer technology head",
        f"{state.target_company} VP marketing data analytics director",
        f"site:linkedin.com/in {state.target_company} {role_str}",
    ]

    company_name = state.target_normalized_name or state.target_company
    raw_contacts: list[Contact] = []

    for query in queries:
        for provider in ["perplexity", "ddg"]:
            try:
                results = await search(query, provider=provider, max_results=6)
                for r in results:
                    raw_contacts.extend(
                        _extract_names_from_snippet(r["snippet"], company_name, r["url"])
                    )
            except Exception as e:
                logger.warning("searcher_web_query_error", provider=provider, error=str(e))

    # TheOrg
    try:
        org_entries = await theorg.search_company(state.target_company)
        for entry in org_entries:
            bucket = _classify_role(entry.get("role_title", ""))
            if bucket in ("DM", "Influencer"):
                raw_contacts.append(Contact(
                    full_name=entry["full_name"],
                    company=company_name,
                    domain=state.target_domain,
                    role_title=entry.get("role_title"),
                    role_bucket=bucket,
                    provenance=["theorg"],
                ))
        logger.info("searcher_theorg", company=state.target_company, found=len(org_entries))
    except Exception as e:
        logger.warning("searcher_theorg_error", company=state.target_company, error=str(e))

    # Wikidata (informational only — logs official website)
    try:
        wiki = await wikidata.lookup_company(state.target_company)
        if wiki.get("official_website"):
            logger.info("searcher_wikidata", company=state.target_company, website=wiki["official_website"])
    except Exception as e:
        logger.warning("searcher_wikidata_error", error=str(e))

    new_contacts = list(state.discovered_contacts) + raw_contacts
    return state.model_copy(update={"discovered_contacts": new_contacts, "phase": "linkedin_validation"})


# ---------------------------------------------------------------------------
# Node: search_pdfs
# ---------------------------------------------------------------------------

async def search_pdfs(state: SearcherState) -> SearcherState:
    """Search for downloadable PDFs — org charts, press releases, leadership pages."""
    logger.info("searcher_search_pdfs", company=state.target_company)

    queries = [
        f"{state.target_company} leadership team PDF organizational chart",
        f"{state.target_company} press release new appointment director VP",
    ]

    raw_contacts: list[Contact] = []
    for query in queries:
        try:
            results = await search_with_fallback(query, max_results=5, providers=["tavily", "ddg"])
            for r in results:
                raw_contacts.extend(
                    _extract_names_from_snippet(r["snippet"], state.target_company, r["url"])
                )
        except Exception as e:
            logger.warning("searcher_pdf_query_error", error=str(e))

    new_contacts = list(state.discovered_contacts) + raw_contacts
    return state.model_copy(update={"discovered_contacts": new_contacts})


# ---------------------------------------------------------------------------
# Node: deduplicate — rapidfuzz, no LLM
# ---------------------------------------------------------------------------

async def deduplicate(state: SearcherState) -> SearcherState:
    """
    Deduplicate discovered contacts using rapidfuzz fuzzy matching.
    Merges records where name similarity >= 90% AND company similarity >= 85%.
    Also removes contacts already present in Final Filtered List for this company.
    """
    logger.info("searcher_deduplicate", company=state.target_company, raw_count=len(state.discovered_contacts))

    if not state.discovered_contacts:
        return state

    try:
        from rapidfuzz import fuzz

        # Collect names already in First Clean List + Final Filtered List for this company.
        # Match on normalized name (from Target Accounts) to avoid wrong substring hits.
        existing_names: list[str] = []
        _norm = (state.target_normalized_name or state.target_company).lower()
        _raw = state.target_company.lower()
        try:
            for tab in (sheets.FIRST_CLEAN_LIST, sheets.FINAL_FILTERED_LIST):
                records = await sheets.read_all_records(tab)
                for row in records:
                    company_col = str(row.get("Company Name", "")).lower()
                    if _norm not in company_col and _raw not in company_col:
                        continue
                    first = str(row.get("First Name", "")).strip()
                    last = str(row.get("Last Name", "")).strip()
                    if first or last:
                        existing_names.append(f"{first} {last}".strip().lower())
        except Exception as e:
            logger.warning("searcher_dedup_existing_read_error", error=str(e))

        # Filter out contacts already in Final Filtered List
        contacts = state.discovered_contacts
        if existing_names:
            def _already_exists(c: Contact) -> bool:
                name = c.full_name.lower()
                for existing in existing_names:
                    if fuzz.token_sort_ratio(name, existing) >= 90:
                        return True
                return False

            contacts = [c for c in contacts if not _already_exists(c)]
            logger.info(
                "searcher_dedup_filtered_existing",
                before=len(state.discovered_contacts),
                after=len(contacts),
            )

        # Deduplicate within the discovered batch — by name AND by LinkedIn URL
        deduped: list[Contact] = []
        seen_urls: set[str] = set()
        for candidate in contacts:
            # LinkedIn URL dedup: if we've already seen this URL, skip
            if candidate.linkedin_url:
                norm_url = candidate.linkedin_url.rstrip("/").lower()
                if norm_url in seen_urls:
                    continue
                seen_urls.add(norm_url)

            merged = False
            for i, existing in enumerate(deduped):
                name_sim = fuzz.token_sort_ratio(
                    candidate.full_name.lower(), existing.full_name.lower()
                )
                company_sim = fuzz.token_sort_ratio(
                    candidate.company.lower(), existing.company.lower()
                )
                if name_sim >= 90 and company_sim >= 85:
                    # Merge: keep the richer record
                    merged_provenance = list(set(existing.provenance + candidate.provenance))
                    merged_contact = existing.model_copy(update={
                        "role_title": existing.role_title or candidate.role_title,
                        "linkedin_url": existing.linkedin_url or candidate.linkedin_url,
                        "email": existing.email or candidate.email,
                        "provenance": merged_provenance,
                    })
                    deduped[i] = merged_contact
                    if merged_contact.linkedin_url:
                        seen_urls.add(merged_contact.linkedin_url.rstrip("/").lower())
                    merged = True
                    break
            if not merged:
                deduped.append(candidate)

        logger.info(
            "searcher_dedup_complete",
            before=len(contacts),
            after=len(deduped),
        )

        # gpt-4.1-mini name quality gate: filter out garbage names from web scraping
        # (e.g. "Pet Life", "View Larger", "Rx Appoints" — structurally 2 words but not people)
        if deduped:
            deduped = await _filter_garbage_names(deduped, state.target_company)

        return state.model_copy(update={"discovered_contacts": deduped, "phase": "linkedin_validation"})

    except ImportError:
        logger.warning("searcher_dedup_rapidfuzz_missing", msg="rapidfuzz not installed, skipping dedup")
        return state
    except Exception as e:
        logger.warning("searcher_dedup_error", error=str(e))
        return state


# ---------------------------------------------------------------------------
# Node: validate_linkedin
# ---------------------------------------------------------------------------

async def validate_linkedin(state: SearcherState) -> SearcherState:
    """Validate LinkedIn profiles via Chrome relay. Sequential only."""
    logger.info("searcher_validate_linkedin", company=state.target_company, contacts=len(state.discovered_contacts))

    updated_contacts: list[Contact] = []
    for contact in state.discovered_contacts:
        if not contact.linkedin_url:
            # Try to find LinkedIn URL — use normalized company name for better results
            company_for_search = state.target_normalized_name or state.target_company
            for _provider in ("ddg", "perplexity"):
                try:
                    results = await search(
                        f"site:linkedin.com/in {contact.full_name} {company_for_search}",
                        provider=_provider,
                        max_results=3,
                    )
                    for r in results:
                        if "linkedin.com/in/" in r["url"]:
                            contact = contact.model_copy(update={"linkedin_url": r["url"].split("?")[0]})
                            break
                    if contact.linkedin_url:
                        break
                except Exception:
                    pass

        if contact.linkedin_url and not contact.linkedin_verified:
            # Skip re-verification for contacts already verified by unipile_search
            try:
                from src.tools.unipile import verify_profile
                verification = await verify_profile(contact.linkedin_url, state.target_company)
                if verification["valid"]:
                    contact = contact.model_copy(update={
                        "linkedin_verified": verification["at_target_company"] and verification["still_employed"],
                        "role_title": verification["current_role"] or contact.role_title,
                    })
                else:
                    contact = contact.model_copy(update={"linkedin_verified": False})
            except Exception as e:
                logger.warning("searcher_linkedin_validate_error", contact=contact.full_name, error=str(e))

        # Drop contacts that Unipile confirmed are NOT at this company.
        # - verified=True  → keep (currently employed here)
        # - verified=False + from Unipile search → drop (Unipile org_id hit but left)
        # - verified=False + from website/web → drop if LinkedIn found but mismatch,
        #                                        keep if no LinkedIn found yet (Veri can try)
        from_unipile = "unipile_search" in (contact.provenance or []) or \
                       "gpt5_unipile_verified" in (contact.provenance or [])
        from_website = "company_website" in (contact.provenance or []) or \
                       "company_website_gpt5" in (contact.provenance or [])

        if not contact.linkedin_verified:
            if from_unipile:
                # Unipile confirmed they're not here — drop
                logger.info("searcher_validate_drop_unverified",
                            name=contact.full_name, provenance=contact.provenance)
                continue
            if from_website and contact.linkedin_url:
                # LinkedIn was found and Unipile checked — they're not at this company
                logger.info("searcher_validate_drop_unverified",
                            name=contact.full_name, provenance=contact.provenance,
                            reason="linkedin found but not at target company")
                continue
            if not contact.linkedin_url:
                # No LinkedIn found at all — drop, can't verify
                logger.info("searcher_validate_drop_unverified",
                            name=contact.full_name, provenance=contact.provenance,
                            reason="no linkedin url found")
                continue

        updated_contacts.append(contact)

    return state.model_copy(update={"discovered_contacts": updated_contacts, "phase": "enrichment"})


# ---------------------------------------------------------------------------
# Node: enrich_contacts
# ---------------------------------------------------------------------------

async def enrich_contacts(state: SearcherState) -> SearcherState:
    """
    Role classification (keyword table), email construction, ZeroBounce.
    Only process contacts that pass dedup and belong to missing DM roles.
    """
    logger.info("searcher_enrich",
                company=state.target_company,
                contacts=len(state.discovered_contacts),
                email_format=state.target_email_format or "(empty)",
                domain=state.target_domain or "(empty)")

    domain = state.target_domain
    email_format = ""
    # Always ask GPT-5 web search for the email format — more reliable than any cached value.
    # Sheet value may be stale; GPT-5 finds real evidence (PDFs, event pages, etc.).
    if domain:
        try:
            from openai import AsyncOpenAI
            from src.tools.domain_discovery import _EMAIL_PATTERNS
            _oai = AsyncOpenAI(api_key=get_settings().openai_api_key)
            patterns_str = "\n".join(f"- {p}" for p in _EMAIL_PATTERNS)
            _prompt = (
                f"You are an email format detective. Given a company domain, find the email format they use for employees.\n\n"
                f"Domain: {domain}\nCompany: {state.target_company}\n\n"
                f"Search for real employee emails at this domain — look at LinkedIn, email finders, event pages, press releases, PDFs, anything public.\n\n"
                f"Once you find evidence, pick EXACTLY ONE format from this list:\n{patterns_str}\n\n"
                f"Where: first=firstname, last=lastname, first_initial=first letter of firstname, last_initial=first letter of lastname\n\n"
                f"Reply with ONLY the format string from the list above, nothing else. Example reply: {{first}}.{{last}}\n"
                f"If you truly cannot determine it, reply: unknown"
            )
            _resp = await _oai.responses.create(
                model="gpt-5",
                tools=[{"type": "web_search"}],
                input=_prompt,
            )
            _gpt_fmt = ""
            for _item in _resp.output:
                if _item.type == "message":
                    for _part in _item.content:
                        if _part.type == "output_text":
                            _gpt_fmt = _part.text.strip()
                            break
            if _gpt_fmt and _gpt_fmt != "unknown" and _gpt_fmt in _EMAIL_PATTERNS:
                email_format = f"{_gpt_fmt}@{domain}"
                logger.info("searcher_enrich_format_gpt5", domain=domain, pattern=_gpt_fmt)
                # Save back to Target Accounts col F so Fini has it for future runs
                try:
                    from rapidfuzz import fuzz as _fuzz2
                    ta_records = await sheets.read_all_records(sheets.TARGET_ACCOUNTS)
                    match_name = (state.target_normalized_name or state.target_company).lower()
                    for i, row in enumerate(ta_records):
                        sn = str(row.get("Company Name", "") or "").lower()
                        if _fuzz2.token_set_ratio(match_name, sn) >= 60:
                            await sheets.update_row_cells(
                                sheets.TARGET_ACCOUNTS, i + 2, 6, [email_format]
                            )
                            logger.info("searcher_enrich_format_saved", row=i + 2, fmt=email_format)
                            break
                except Exception as _se:
                    logger.warning("searcher_enrich_format_save_error", error=str(_se))
            else:
                logger.warning("searcher_enrich_format_gpt5_miss", domain=domain, response=_gpt_fmt)
        except Exception as e:
            logger.warning("searcher_enrich_format_gpt5_error", domain=domain, error=str(e))

    if not email_format and domain:
        # Absolute fallback
        email_format = f"{{first}}.{{last}}@{domain}"
        logger.warning("searcher_enrich_fallback_format",
                       company=state.target_company,
                       domain=domain,
                       msg="GPT-5 found no pattern — using {first}.{last} fallback")

    enriched: list[Contact] = []
    for contact in state.discovered_contacts:
        # Role classification — keyword table, no LLM
        bucket = _classify_role(contact.role_title or "")
        contact = contact.model_copy(update={"role_bucket": bucket})

        # Only keep DM and Influencer contacts — skip GateKeeper and Unknown
        if bucket not in ("DM", "Influencer"):
            logger.info(
                "searcher_enrich_skip",
                contact=contact.full_name,
                bucket=bucket,
                reason="not DM or Influencer",
            )
            continue

        # Email construction
        if not contact.email and email_format:
            email = construct_email(contact.full_name, email_format, state.target_domain)
            if email:
                contact = contact.model_copy(update={"email": email})

        # ZeroBounce validation
        if contact.email and contact.email_status == "pending":
            try:
                result = await zb.validate_email(contact.email)
                status = result.get("status", "unknown")
                if status not in ("valid", "invalid", "catch-all", "unknown"):
                    status = "unknown"
                contact = contact.model_copy(update={
                    "email_status": status,
                    "zerobounce_score": result.get("score"),
                })
            except Exception as e:
                logger.warning("searcher_zerobounce_error", email=contact.email, error=str(e))
                contact = contact.model_copy(update={"email_status": "unknown"})

        # Set domain
        if not contact.domain:
            contact = contact.model_copy(update={"domain": state.target_domain})

        enriched.append(contact)

    logger.info("searcher_enrich_done", total=len(state.discovered_contacts), kept=len(enriched))
    return state.model_copy(update={"discovered_contacts": enriched, "phase": "write_output"})


# ---------------------------------------------------------------------------
# Node: write_to_sheet
# ---------------------------------------------------------------------------

async def write_contacts_to_sheet(state: SearcherState) -> SearcherState:
    """
    Write each enriched contact to Searcher Output (columns A-H).
    Veri reads from here and promotes verified contacts to Final Filtered List.
    """
    logger.info("searcher_write_sheet", company=state.target_company, contacts=len(state.discovered_contacts))

    if not state.discovered_contacts:
        logger.info("searcher_write_sheet_skip", reason="no contacts to write")
        return state.model_copy(update={"phase": "done"})

    try:
        await sheets.ensure_headers(sheets.SEARCHER_OUTPUT, sheets.SEARCHER_OUTPUT_HEADERS)

        # Build a name→row_number index of existing rows for this company so we can
        # update instead of append when the contact is already in the sheet.
        from rapidfuzz import fuzz as _rfuzz
        existing_rows: dict[str, int] = {}  # full_name.lower() -> 1-based row number
        try:
            all_vals = await sheets.read_all_rows(sheets.SEARCHER_OUTPUT)  # excludes header, 0-based
            company_match = (state.target_normalized_name or state.target_company).lower()
            for i, row_vals in enumerate(all_vals, start=2):  # +2: 1 for header, 1 for 1-based
                company_cell = row_vals[0].lower() if row_vals else ""
                name_cell = row_vals[1].lower() if len(row_vals) > 1 else ""
                if not name_cell:
                    continue
                if company_match in company_cell or state.target_company.lower() in company_cell:
                    existing_rows[name_cell] = i
        except Exception as e:
            logger.warning("searcher_write_existing_lookup_error", error=str(e))

        for contact in state.discovered_contacts:
            row = [
                contact.company,                                            # A: Company
                contact.full_name,                                          # B: Full Name
                contact.role_title or "",                                   # C: Job Title
                contact.role_bucket,                                        # D: Role Bucket
                contact.linkedin_url or "",                                 # E: LinkedIn URL
                "Verified" if contact.linkedin_verified else "Unverified",  # F: LinkedIn Status
                contact.email or "",                                        # G: Email Address
                contact.email_status if contact.email else "",              # H: Email Status
            ]

            # Check if this contact already exists in the sheet (fuzzy name match)
            existing_row_num = None
            for existing_name, row_num in existing_rows.items():
                if _rfuzz.token_sort_ratio(contact.full_name.lower(), existing_name) >= 90:
                    existing_row_num = row_num
                    break

            if existing_row_num:
                # Update existing row in-place
                await sheets.update_row_cells(sheets.SEARCHER_OUTPUT, existing_row_num, 1, row)
                logger.info("searcher_contact_updated", contact=contact.full_name, row=existing_row_num)
            else:
                await sheets.append_row(sheets.SEARCHER_OUTPUT, row)
                logger.info(
                    "searcher_contact_written",
                    contact=contact.full_name,
                    company=contact.company,
                    role=contact.role_title,
                    tab=sheets.SEARCHER_OUTPUT,
                )

        logger.info("searcher_sheet_written", count=len(state.discovered_contacts), tab=sheets.SEARCHER_OUTPUT)

    except Exception as e:
        logger.error("searcher_sheet_write_error", error=str(e))
        errors = list(state.errors) + [f"Sheet write failed: {e}"]
        return state.model_copy(update={"errors": errors, "phase": "done"})

    return state.model_copy(update={"phase": "done"})


# ---------------------------------------------------------------------------
# Node: advance_or_finish
# ---------------------------------------------------------------------------

def advance_or_finish(state: SearcherState) -> SearcherState:
    """Advance to next company or mark as completed."""
    if not state.target_companies:
        return state.model_copy(update={"phase": "done"})
    next_index = state.current_index + 1
    if next_index >= len(state.target_companies):
        logger.info(
            "searcher_completed",
            total=len(state.target_companies),
            errors=len(state.errors),
        )
        return state.model_copy(update={"phase": "done"})
    else:
        next_company = state.target_companies[next_index]
        logger.info("searcher_advance", next_index=next_index, company=next_company.get("name", ""))
        return state.model_copy(update={
            "current_index": next_index,
            "target_company": next_company.get("name", ""),
            # Reset per-company fields — load_gap_analysis will populate from Target Accounts
            "target_domain": "",
            "target_org_id": "",
            "target_email_format": "",
            "target_region": "",
            "target_normalized_name": "",
            "discovered_contacts": [],
            "phase": "unipile_search",
        })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_names_from_snippet(snippet: str, company: str, source_url: str) -> list[Contact]:
    """
    Heuristic extraction of person names from search snippets.
    Returns partial Contact objects for later enrichment.

    Strict rules to avoid garbage:
    - Name must be exactly 2 tokens (First Last) — no multi-word or extra tokens
    - Neither token may be a role/title keyword
    - Name tokens must be purely alphabetic (no digits, no punctuation)
    """
    _TITLE_WORDS = {
        "chief", "officer", "executive", "director", "vice", "president", "manager",
        "head", "vp", "senior", "global", "lead", "principal", "general", "regional",
        "independent", "chairman", "ceo", "cto", "cmo", "cdo", "coo", "cfo",
    }

    contacts: list[Contact] = []
    patterns = [
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+([A-Z][^,.]+(?:VP|Director|Manager|Chief|Head|President|Officer)[^,.]*)",
        r"([A-Z][a-z]+ [A-Z][a-z]+) is (?:the |a )?([A-Z][^.]+(?:VP|Director|Manager|Chief|Head|President|Officer)[^.]*)",
    ]
    seen_names: set[str] = set()
    for pat in patterns:
        for match in re.finditer(pat, snippet):
            name = match.group(1).strip()
            title = match.group(2).strip()[:100]
            parts = name.split()
            # Exactly 2 tokens, both purely alpha, neither is a title keyword
            if len(parts) != 2:
                continue
            if not all(p.isalpha() for p in parts):
                continue
            if any(p.lower() in _TITLE_WORDS for p in parts):
                continue
            if name in seen_names:
                continue
            seen_names.add(name)
            contacts.append(Contact(
                full_name=name,
                company=company,
                domain="",
                role_title=title,
                provenance=[source_url],
            ))
    return contacts


async def _get_email_format(company_name: str, domain: str) -> str | None:
    """Get email format from Target Accounts sheet. Uses best fuzzy match on company name."""
    try:
        from rapidfuzz import fuzz as _fuzz
        records = await sheets.read_all_records(sheets.TARGET_ACCOUNTS)
        input_lower = company_name.lower().strip()
        best_fmt = None
        best_score = 0
        for row in records:
            sheet_name = str(row.get("Company Name", "") or "").lower()
            if not sheet_name:
                continue
            score = _fuzz.token_set_ratio(input_lower, sheet_name)
            if score > best_score:
                best_score = score
                _ef_key = next((k for k in row if str(k).startswith("Email Format")), "Email Format")
                best_fmt = row.get(_ef_key) or None
        return best_fmt if best_score >= 60 else None
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def route_after_gap_analysis(state: SearcherState) -> str:
    if state.phase == "done":
        return "advance_or_finish"
    return "unipile_search"


def route_after_write(state: SearcherState) -> str:
    return "advance_or_finish"


def route_advance(state: SearcherState) -> str:
    if state.phase == "done":
        return END
    return "load_gap_analysis"


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

class AioSqliteConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn

    def is_alive(self):
        return True

    def __getattr__(self, name):
        return getattr(self._conn, name)


async def build_searcher_graph():
    # No checkpointer — Searcher is a straight pipeline with no human-in-the-loop.
    # Checkpointing was silently killing execution between nodes due to serialization issues.
    graph = StateGraph(SearcherState)

    graph.add_node("load_gap_analysis", load_gap_analysis)
    graph.add_node("unipile_search", unipile_search)
    graph.add_node("search_company_website", search_company_website)
    graph.add_node("deduplicate", deduplicate)
    graph.add_node("validate_linkedin", validate_linkedin)
    graph.add_node("enrich_contacts", enrich_contacts)
    graph.add_node("write_to_sheet", write_contacts_to_sheet)
    graph.add_node("advance_or_finish", advance_or_finish)

    graph.set_entry_point("load_gap_analysis")

    graph.add_conditional_edges("load_gap_analysis", route_after_gap_analysis)
    graph.add_edge("unipile_search", "search_company_website")
    graph.add_edge("search_company_website", "deduplicate")
    graph.add_edge("deduplicate", "validate_linkedin")
    graph.add_edge("validate_linkedin", "enrich_contacts")
    graph.add_edge("enrich_contacts", "write_to_sheet")
    graph.add_conditional_edges("write_to_sheet", route_after_write)
    graph.add_conditional_edges("advance_or_finish", route_advance)

    return graph.compile()
