"""
Searcher - Contact Gap-Fill Agent (Agent 3)

Runs AFTER Veri. Reads First Clean List, identifies companies that are
missing Decision Maker contacts, then discovers and appends new DM contacts
in the same A-U format.

Graph:
START -> load_gap_analysis -> unipile_search -> search_company_website
      -> deduplicate -> validate_linkedin
      -> enrich_contacts -> write_to_sheet -> advance_or_finish -> (loop or END)
"""
from __future__ import annotations

import asyncio
import json
import re

from typing import Literal

from langgraph.graph import StateGraph, END

from backend.config import get_settings
from backend.state import Contact, SearcherState
from backend.tools import sheets
from backend.tools.domain_discovery import construct_email
from backend.tools.search import search, search_with_fallback
from backend.tools import theorg, wikidata
from backend.utils.logging import get_logger

logger = get_logger("searcher")

# ---------------------------------------------------------------------------
# Role bucket keyword lookup table — deterministic, no LLM
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 5-tier role classification matching Gopal's mapping requirements:
#   Tier 1: CEO/MD         — must have per company
#   Tier 2: CTO/CIO        — must have per company
#   Tier 3: CSO/Head of Sales — must have per company
#   Tier 4: P1 Influencer  — directors, VPs, senior managers
#   Tier 5: Gatekeeper     — assistants, coordinators
# ---------------------------------------------------------------------------

_CEO_MD_KEYWORDS = {
    "chief executive", "ceo", "managing director", "md", "president",
    "founder", "co-founder", "owner", "general manager", "gm",
    "country manager", "country head", "country director",
    "chief operating", "coo", "chief commercial", "cco",
    # Spanish
    "director general", "director ejecutivo", "gerente general",
    "consejero delegado",
    # Portuguese
    "diretor geral", "diretor executivo",
    # French
    "directeur général", "président directeur",
    # German
    "geschäftsführer",
    # Italian
    "direttore generale", "amministratore delegato",
}

_CTO_CIO_KEYWORDS = {
    "chief technology", "cto", "chief information", "cio",
    "chief digital", "cdo", "chief data", "chief product", "cpo",
    "vp technology", "vp engineering", "vp it", "vp digital",
    "head of technology", "head of engineering", "head of it", "head of digital",
    "director of technology", "director of engineering", "it director",
    "digital director", "technology director",
    # Spanish
    "director de tecnologia", "director de sistemas", "director digital",
    "responsable digital", "responsable de tecnologia",
    # Portuguese
    "diretor de tecnologia", "diretor digital",
    # French
    "directeur digital", "directeur des systèmes", "directeur technique",
    # German
    "leiter digital", "leiter it",
}

_CSO_SALES_KEYWORDS = {
    "chief sales", "cso", "chief revenue", "cro",
    "vp sales", "vice president sales", "head of sales", "sales director",
    "director of sales", "commercial director", "chief commercial",
    "vp marketing", "vice president marketing", "head of marketing",
    "marketing director", "chief marketing", "cmo",
    "vp ecommerce", "head of ecommerce", "ecommerce director",
    "director of ecommerce", "director of marketing",
    "national sales manager", "national head", "business director",
    "business head", "business development director",
    "key account director", "trade marketing director",
    "category director", "commercial head",
    # Spanish
    "director de ventas", "director comercial", "director de marketing",
    "director de ecommerce", "gerente comercial", "gerente de ventas",
    "gerente de marketing", "jefe comercial", "jefe de ventas",
    "jefe de marketing", "responsable de ventas", "responsable de marketing",
    "responsable de ecommerce", "responsable comercial",
    # Portuguese
    "diretor comercial", "diretor de vendas", "diretor de marketing",
    # French
    "directeur commercial", "directeur marketing", "directeur des ventes",
    "responsable marketing", "responsable commercial", "responsable ecommerce",
    # German
    "leiter marketing", "leiter vertrieb",
    # Italian
    "direttore commerciale", "direttore marketing", "responsabile commerciale",
    "responsabile marketing",
}

_P1_INFLUENCER_KEYWORDS = {
    "director", "vp", "vice president", "head of", "senior manager",
    "sr manager", "group manager", "principal", "lead", "manager",
    "senior director", "associate director",
    "regional director", "area director", "regional head",
    "zonal sales manager", "zonal manager", "cluster manager",
    "trade marketing manager", "category head",
    "vp operations", "operations director", "supply chain director",
    "head of supply chain", "procurement director", "vp procurement",
    "chief financial", "cfo", "finance director",
    # Spanish
    "director de operaciones", "jefe de ecommerce",
}

_GATEKEEPER_KEYWORDS = {
    "assistant", "coordinator", "executive assistant", "personal assistant",
    "secretary", "administrator", "office manager", "receptionist",
    "executive coordinator",
}

# Must-have roles — Searcher will specifically web-search for these if missing
MUST_HAVE_TIERS = [
    {"tier": "CEO/MD", "keywords": _CEO_MD_KEYWORDS, "search_queries": [
        "CEO", "Co-Founder", "Founder", "Managing Director", "General Manager", "Country Manager",
    ]},
    {"tier": "CTO/CIO", "keywords": _CTO_CIO_KEYWORDS, "search_queries": [
        "CTO", "CIO", "Head of Technology", "VP Engineering",
    ]},
    # NOTE: CMO, Head of Marketing, Commercial Director are EXCLUDED from search —
    # they are classified as "Irrelevant" per the buying-role prompt.
    # Only Sales leadership roles are searched here.
    {"tier": "CSO/Head of Sales", "keywords": _CSO_SALES_KEYWORDS, "search_queries": [
        "Head of Sales", "Sales Director", "CRO", "VP Sales",
    ]},
]


def _classify_role(role_title: str) -> str:
    """
    Classify a role title into Gopal's 5-tier system.
    Returns: "CEO/MD" | "CTO/CIO" | "CSO/Head of Sales" | "P1 Influencer" | "Gatekeeper" | "Unknown"
    """
    if not role_title:
        return "Unknown"
    t = role_title.lower()

    for kw in _CEO_MD_KEYWORDS:
        if kw in t:
            return "CEO/MD"

    for kw in _CTO_CIO_KEYWORDS:
        if kw in t:
            return "CTO/CIO"

    for kw in _CSO_SALES_KEYWORDS:
        if kw in t:
            return "CSO/Head of Sales"

    for kw in _GATEKEEPER_KEYWORDS:
        if kw in t:
            return "Gatekeeper"

    for kw in _P1_INFLUENCER_KEYWORDS:
        if kw in t:
            return "P1 Influencer"

    return "Unknown"

    return "Unknown"


# ---------------------------------------------------------------------------
# Node: load_gap_analysis
# ---------------------------------------------------------------------------

async def load_gap_analysis(state: SearcherState) -> SearcherState:
    """
    1. Read Target Accounts to get org_id, domain, email_format that Fini already discovered.
    2. Build a set of existing person names from both First Clean List AND First Clean List.
    3. Determine which DM roles from input are not yet covered.
    """
    from backend.utils.progress import emit as _emit_progress, emit_log as _emit_log
    await _emit_progress(state.thread_id, state.target_company, "processing")
    logger.info("searcher_gap_analysis", company=state.target_company)
    await _emit_log(state.thread_id, f"[{state.target_company}] Starting gap analysis — reading Target Accounts & existing sheets…", level="info")

    # --- Step 1: pull Fini's work from Target Accounts ---
    org_id = ""
    domain = state.target_domain or ""
    email_format = state.target_email_format or ""
    account_type = ""
    account_size = ""
    normalized_company_name = state.target_company  # fallback to input if not in sheet
    sales_nav_url_full = ""

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

        # Also try matching against Parent Company Name (col B)
        if best_score < 60:
            for row in ta_records:
                parent_name = str(row.get("Parent Company Name", "") or "").strip()
                if not parent_name:
                    continue
                score = _fuzz.token_set_ratio(input_lower, parent_name.lower())
                if score > best_score:
                    best_score = score
                    best_row = row

        if best_row is not None and best_score >= 60:
            normalized_company_name = str(best_row.get("Company Name", "") or state.target_company).strip()
            # Extract org_id from Sales Nav URL (col C) if present
            sales_nav_url = str(best_row.get("Sales Navigator Link", "") or "")
            # Match both single-encoded (%3A) and double-encoded (%253A) formats
            m = _re.search(r"organization%(?:25)?3A(\d+)", sales_nav_url)
            if m:
                org_id = m.group(1)
            domain = domain or str(best_row.get("Company Domain", "") or "").strip()
            # Header may be "Email Format( Firstname-amy , Lastname- williams)" — match by prefix
            _ef_key = next((k for k in best_row if str(k).startswith("Email Format")), "Email Format")
            email_format = email_format or str(best_row.get(_ef_key, "") or "").strip()
            account_type = str(best_row.get("Account type", "") or "").strip()
            account_size = str(best_row.get("Account Size", "") or "").strip()
            # Preserve the full Sales Nav URL so scrape_sales_nav node can use it
            sales_nav_url_full = str(best_row.get("Sales Navigator Link", "") or "").strip()
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

    # --- Step 2: read existing contacts from First Clean List + Searcher Output ---
    # Find which roles are already covered so we only search for the gaps.
    existing_names: list[str] = []
    existing_role_titles: list[str] = []
    _match_name = normalized_company_name.lower()
    try:
        for tab in (sheets.FIRST_CLEAN_LIST,):
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
                role = str(row.get("Role Title", row.get("Title", row.get("Job Title", row.get("Job titles (English)", row.get("Job Title (English)", "")))))).strip().lower()
                if role:
                    existing_role_titles.append(role)
    except Exception as e:
        logger.warning("searcher_existing_names_read_error", error=str(e))

    # --- Step 3: LLM-powered gap analysis ---
    # Use LLM to intelligently reason about which must-have roles are covered
    # vs missing. Much smarter than keyword matching — understands that
    # "General Manager Eurasia" = CEO/MD tier, "Head of Global Sales Enablement" ≠ CSO, etc.
    from backend.tools.llm import llm_complete
    import json as _json

    contacts_summary = "\n".join(f"- {t}" for t in existing_role_titles) if existing_role_titles else "(no contacts found)"

    gap_prompt = (
        f"You are a B2B sales intelligence analyst. Analyze the existing contacts for {state.target_company} "
        f"and determine which of the 3 MUST-HAVE leadership tiers are covered vs missing.\n\n"
        f"EXISTING CONTACTS (job titles):\n{contacts_summary}\n\n"
        f"MUST-HAVE TIERS:\n"
        f"1. CEO/MD — CEO, Managing Director, President, Founder, General Manager, Country Manager, COO\n"
        f"2. CTO/CIO — CTO, CIO, Chief Digital Officer, VP Technology, VP Engineering, Head of IT/Digital/Technology\n"
        f"3. CSO/Head of Sales — CSO, CRO, VP Sales, Head of Sales, Sales Director, CMO, Head of Marketing, Commercial Director\n\n"
        f"RULES:\n"
        f"- A title covers a tier ONLY if the person genuinely holds that level of responsibility\n"
        f"- 'Director of Business Development' is NOT a CSO — it's mid-level\n"
        f"- 'Associate Director of IT' is NOT a CTO — it's mid-level\n"
        f"- 'VP, Managing Director' IS CEO/MD tier\n"
        f"- 'CFO' is NOT any of these 3 tiers — it's finance\n"
        f"- Be strict: only C-suite, VP-level, or Head-of-department count for the tier\n\n"
        f"Return ONLY valid JSON (no markdown):\n"
        f'{{"covered": [{{"tier": "CEO/MD", "covered_by": "title that covers it"}}], '
        f'"missing": ["CTO/CIO", "CSO/Head of Sales"], '
        f'"reasoning": "one sentence explaining your analysis"}}'
    )

    missing_tiers: list[dict] = []
    existing_tiers: dict[str, list[str]] = {"CEO/MD": [], "CTO/CIO": [], "CSO/Head of Sales": []}

    try:
        raw = await llm_complete(gap_prompt, model="gpt-4.1-mini", max_tokens=300, temperature=0)
        import re as _re
        cleaned = _re.sub(r'^```(?:json)?\s*|\s*```$', '', (raw or "").strip())
        gap_result = _json.loads(cleaned)

        covered_names = [c["tier"] for c in gap_result.get("covered", [])]
        missing_names = gap_result.get("missing", [])
        reasoning = gap_result.get("reasoning", "")

        for c in gap_result.get("covered", []):
            tier_name = c.get("tier", "")
            covered_by = c.get("covered_by", "")
            if tier_name in existing_tiers:
                existing_tiers[tier_name].append(covered_by)

        for tier_def in MUST_HAVE_TIERS:
            if tier_def["tier"] in missing_names:
                missing_tiers.append(tier_def)

        logger.info("searcher_llm_gap_analysis",
                    company=state.target_company,
                    covered=covered_names, missing=missing_names,
                    reasoning=reasoning)
        await _emit_log(state.thread_id,
            f"[{state.target_company}] LLM gap analysis: covered={covered_names}, "
            f"missing={missing_names} — {reasoning}",
            level="info" if missing_names else "success")

    except Exception as e:
        logger.warning("searcher_llm_gap_error", error=str(e),
                       msg="Falling back to keyword matching")
        await _emit_log(state.thread_id,
            f"[{state.target_company}] LLM gap analysis failed ({e}), using keyword fallback",
            level="warning")
        # Fallback to keyword matching
        for title in existing_role_titles:
            tier = _classify_role(title)
            if tier in existing_tiers:
                existing_tiers[tier].append(title)
        for tier_def in MUST_HAVE_TIERS:
            if not existing_tiers.get(tier_def["tier"]):
                missing_tiers.append(tier_def)

    # Build the missing_roles list from must-have tiers
    missing_roles = []
    for tier_def in missing_tiers:
        missing_roles.extend(tier_def["search_queries"])

    # Also add any explicitly requested roles from input
    if state.dm_roles:
        for role in state.dm_roles:
            if role not in missing_roles:
                missing_roles.append(role)

    if not missing_roles:
        logger.info("searcher_gap_analysis_all_covered",
                    company=state.target_company,
                    msg="All must-have roles covered — no gaps to fill")
        await _emit_log(state.thread_id,
            f"[{state.target_company}] All must-have tiers covered — no gaps to fill",
            level="success")
        return state.model_copy(update={
            "target_org_id": org_id,
            "target_domain": domain,
            "target_email_format": email_format,
            "target_region": account_type,
            "target_account_size": account_size,
            "target_normalized_name": normalized_company_name,
            "target_sales_nav_url": sales_nav_url_full,
            "missing_dm_roles": [],
            "existing_names": existing_names,
            "phase": "done",
        })

    covered_tiers = [t["tier"] for t in MUST_HAVE_TIERS if t not in missing_tiers]
    logger.info("searcher_gap_analysis_roles",
                company=state.target_company,
                covered_tiers=covered_tiers,
                missing_tiers=[t["tier"] for t in missing_tiers],
                missing_roles=missing_roles)
    await _emit_log(state.thread_id,
        f"[{state.target_company}] Gap analysis done — missing: {', '.join(t['tier'] for t in missing_tiers)} "
        f"→ searching for {len(missing_roles)} role(s)",
        level="info")

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
        "target_account_size": account_size,
        "target_normalized_name": normalized_company_name,
        "target_sales_nav_url": sales_nav_url_full,
        "missing_dm_roles": missing_roles,
        "existing_names": existing_names,
        "phase": "unipile_search",
    })


# ---------------------------------------------------------------------------
# Node: expand_search_terms  — multilingual role expansion via LLM
# ---------------------------------------------------------------------------

async def expand_search_terms(state: SearcherState) -> SearcherState:
    """
    Use LLM to expand English DM roles into multilingual variants for the company's region.

    For Spain: "VP Ecommerce" → ["Director de Ecommerce", "Responsable Digital",
    "Director de Marketing Digital", "Director Comercial", ...]

    These expanded titles are stored in state.expanded_dm_roles and used by:
    - unipile_search (instead of broad generic terms like "Director", "VP")
    - score_and_rank (as scoring targets)
    """
    roles = state.missing_dm_roles
    if not roles:
        return state

    region = state.target_region or "Global"

    language_map = {
        "spain": "Spanish", "españa": "Spanish",
        "latam": "Spanish", "latin america": "Spanish", "mexico": "Spanish",
        "argentina": "Spanish", "colombia": "Spanish", "chile": "Spanish",
        "brazil": "Portuguese", "brasil": "Portuguese",
        "france": "French", "germany": "German",
        "italy": "Italian", "netherlands": "Dutch",
        "india": "English", "us": "English", "uk": "English",
        "global": "English",
    }
    region_lower = region.lower()
    language = next((v for k, v in language_map.items() if k in region_lower), "English")

    logger.info("searcher_expand_roles_start", company=state.target_company,
                region=region, language=language, roles=roles)

    from backend.utils.progress import emit_log as _expand_log
    await _expand_log(state.thread_id,
        f"[{state.target_company}] Expanding {len(roles)} role(s) to multilingual variants for {region} ({language})…",
        level="info")

    try:
        from backend.tools.llm import llm_complete
        import json as _json
        import re as _re

        prompt = (
            f"You are helping find decision-makers at companies in {region} (language: {language}).\n\n"
            f"Target roles: {roles}\n\n"
            f"Generate a comprehensive list of equivalent LinkedIn job title strings to search for.\n"
            f"Include:\n"
            f"- English titles as-is\n"
            f"- {language} equivalents (exact local-language titles used on LinkedIn profiles in {region})\n"
            f"- Seniority synonyms (VP ↔ Director ↔ Head of ↔ Managing Director)\n"
            f"- C-suite equivalents (CMO, CDO, CTO, CRO, CCO)\n\n"
            f"Rules:\n"
            f"- SENIOR roles only: C-suite, VP, Director, Head of, Country/Regional Manager, General Manager\n"
            f"- NO junior titles: coordinator, analyst, specialist, associate, junior manager\n"
            f"- Max 35 titles total\n"
            f"- Return ONLY a JSON array: [\"title1\", \"title2\", ...]\n"
            f"- No explanation, no markdown fences."
        )

        response = await llm_complete(prompt, max_tokens=800, temperature=0)
        match = _re.search(r'\[.*?\]', response, _re.DOTALL)
        if match:
            titles = _json.loads(match.group(0))
            expanded = [str(t).strip() for t in titles if isinstance(t, str) and 2 < len(t.strip()) < 80]
            logger.info("searcher_expand_roles_done",
                        company=state.target_company, original=len(roles), expanded=len(expanded),
                        sample=expanded[:5])
            await _expand_log(state.thread_id,
                f"[{state.target_company}] Role expansion complete — {len(expanded)} search terms ready",
                level="info")
            return state.model_copy(update={"expanded_dm_roles": expanded})

    except Exception as e:
        logger.warning("searcher_expand_roles_error", company=state.target_company, error=str(e))

    # Fallback: use original roles unchanged
    return state.model_copy(update={"expanded_dm_roles": list(roles)})


# ---------------------------------------------------------------------------
# Function-level search query builder — drives parallel Unipile searches
# ---------------------------------------------------------------------------

# Each bucket: (bucket_name, list_of_keywords_to_detect_in_role)
# We pick ONE representative query per bucket from expanded_dm_roles.
# Each bucket targets a DIFFERENT business function → searches return different people.
#
# ALIGNED WITH BUYING-ROLE PROMPT: Only search for roles that can be FDM/KDM/P1/Influencer.
# REMOVED: marketing (CMO/marketing-only → Irrelevant), operations (→ Irrelevant),
#          finance (except CFO which is in c_suite).
_FUNCTION_BUCKETS: list[tuple[str, list[str]]] = [
    ("c_suite",    ["ceo", "cfo", "coo", "director general", "gerente general", "managing director",
                    "consejero delegado", "président", "président directeur",
                    "geschäftsführer", "direttore generale", "directeur général", "president"]),
    ("digital",    ["cdo", "chief digital", "digital director", "director digital",
                    "head of digital", "vp digital", "directeur digital",
                    "responsable digital", "jefe digital",
                    "digital transformation", "chief data"]),
    ("ecommerce",  ["ecommerce", "e-commerce", "commerce director", "head of ecommerce",
                    "director ecommerce", "jefe ecommerce", "responsable ecommerce"]),
    ("sales",      ["chief revenue", "cro", "sales director", "director ventas",
                    "vp sales", "head of sales", "national sales manager",
                    "directeur des ventes", "direttore commerciale",
                    "leiter vertrieb", "sales excellence", "commercial excellence"]),
    ("technology", ["cto", "cio", "chief technology", "chief information",
                    "technology director", "director tecnologia", "director de sistemas",
                    "it director", "head of it", "directeur technique"]),
    ("strategy",   ["chief strategy", "vp strategy", "strategy director",
                    "head of strategy", "director de estrategia"]),
    ("general_mgmt", ["general manager", "country manager", "regional director",
                      "gerente general", "director regional", "country director",
                      "business director"]),
    ("rtm_gtm",   ["route to market", "go to market", "rtm", "gtm",
                    "channel development", "distribution director",
                    "head of distribution", "head of retail"]),
]

# Fallback English queries per bucket (used when no match found in expanded_roles)
_BUCKET_FALLBACKS = {
    "c_suite":     "CEO Managing Director CFO COO",
    "digital":     "CDO Digital Director Digital Transformation",
    "ecommerce":   "Ecommerce Director Head of Ecommerce",
    "sales":       "Sales Director VP Sales Head of Sales CRO",
    "technology":  "CTO CIO Technology Director IT Director",
    "strategy":    "VP Strategy Chief Strategy Officer",
    "general_mgmt": "General Manager Country Manager",
    "rtm_gtm":    "Head of Distribution RTM GTM Director",
}


def _build_function_queries(expanded_roles: list[str], base_roles: list[str]) -> list[str]:
    """
    Build function-level search queries from the expanded+base role lists.

    Strategy: for each business function bucket, find the best matching title from our
    expanded roles. Run 8-9 parallel searches — one per function — each covering a
    DIFFERENT area of the org chart and returning different employees.

    This yields ~9 × 25 = 225 raw candidates (vs. 30 sequential searches returning the
    same top 25 people over and over). After dedup: typically 60-120 unique contacts.
    """
    all_roles = list(dict.fromkeys((expanded_roles or []) + (base_roles or [])))

    selected: list[str] = []
    covered_buckets: set[str] = set()

    for bucket_name, bucket_keywords in _FUNCTION_BUCKETS:
        best_match: str | None = None

        # Prefer localized versions from expanded_roles (e.g. "Director de Marketing" for Spain)
        for role in all_roles:
            role_lower = role.lower()
            for kw in bucket_keywords:
                if kw in role_lower:
                    best_match = role
                    break
            if best_match:
                break

        query = best_match or _BUCKET_FALLBACKS.get(bucket_name, bucket_name.replace("_", " ").title())
        if query not in selected:
            selected.append(query)
            covered_buckets.add(bucket_name)

    return selected


# ---------------------------------------------------------------------------
# AI importance note generator — batch LLM call for all scored contacts
# ---------------------------------------------------------------------------

async def _generate_importance_notes_batch(
    contacts: list,  # list[Contact]
    company: str,
    dm_roles: list[str],
) -> list:
    """
    Generate a one-liner "why this person matters" note for each contact via LLM.
    Single batch call — fast (2-4s) regardless of contact count.
    Falls back gracefully: returns contacts unchanged on any error.
    """
    if not contacts:
        return contacts

    try:
        from backend.tools.llm import llm_complete
        import json as _json
        import re as _re

        lines = "\n".join(
            f"{i + 1}. {c.full_name} — {c.role_title or 'Unknown Role'}"
            for i, c in enumerate(contacts)
        )
        roles_str = ", ".join(dm_roles[:6]) if dm_roles else "digital/ecommerce decision makers"

        prompt = (
            f"Company: {company}\n"
            f"We sell digital commerce / ecommerce technology. Looking for: {roles_str}\n\n"
            f"People found at {company}:\n{lines}\n\n"
            f"For each person, write ONE sentence (max 15 words) explaining why they matter "
            f"for our sales — focus on budget authority or decision-making power.\n"
            f"ALSO assign a priority_score (1-100) based on their seniority and decision-making power (e.g. C-level=90-100, VP=70-90, Manager=40-70).\n\n"
            f"Return ONLY a JSON array:\n"
            f'[{{"index": 0, "note": "...", "priority_score": 85}}, ...]\n'
            f"No markdown, no explanation."
        )

        response = await llm_complete(prompt, max_tokens=1500, temperature=0)
        match = _re.search(r'\[.*?\]', response, _re.DOTALL)
        if match:
            notes_list = _json.loads(match.group(0))
            notes_map: dict[int, dict] = {}
            for item in notes_list:
                if isinstance(item, dict) and "index" in item:
                    notes_map[item["index"]] = {
                        "note": item.get("note", ""),
                        "score": item.get("priority_score", 0)
                    }
            return [
                c.model_copy(update={
                    "importance_note": notes_map.get(i, {}).get("note", ""),
                    "priority_score": notes_map.get(i, {}).get("score", 0)
                })
                for i, c in enumerate(contacts)
            ]

    except Exception as e:
        logger.warning("searcher_importance_notes_error", company=company, error=str(e))

    return contacts


# ---------------------------------------------------------------------------
# LLM-based buying-role classifier (replaces fuzzy scoring)
# ---------------------------------------------------------------------------

_BUYING_ROLE_PROMPT = """\
You are a buying-role classifier for B2B FMCG/CPG sales systems. Be conservative and minimize false positives.

Your job: for each contact, return the correct tag using ONLY the defined allow-lists and hierarchy rules.
If uncertain, always default to "Irrelevant".

TAG ALLOW-LISTS
──────────────
• FDM: Founder, Co-Founder, CEO, CBO, Managing Director, Vice President,
  President, COO, CFO, Executive Director (if Sales/IT/Digital/ComEx/RTM-GTM/Analytics or generic),
  Country Manager/Head/Director, Regional Manager (national/multi-country),
  VP/EVP/SVP (enterprise-wide), GM with explicit P&L/signatory, CIO, CTO.
  Also: Director General — equivalent to MD/CEO when used as apex executive title.

• KDM: VP/Head/Director of Sales; Sales Director (any qualifier); CIO; IT Director/Head with budget;
  CRO; CCO; Senior Sales Manager; National Sales Manager; Sales Manager LATAM/EMEA/APAC;
  Regional Sales Manager (multi-country); Customer Development Director (national);
  Head of Sales; Head of Retail/Distribution/GTM (national/multi-country); Director (generic);
  General Manager.

• P1 Influencer: Commercial Excellence Director/Head; Sales Excellence Director/Head; Field Sales
  Director; Chief Digital Officer; Digital Transformation Director/Head; RTM/GTM Director/Head;
  Channel Development Director/Head; Sales Operations Director/Head (national);
  IT Head/Director (non-CIO without explicit budget); BI/Analytics Director/Head;
  Head of Retail/GTM/Distribution (sub-national); CSO; VP/EVP/SVP Strategy;
  Strategy Director/Head (enterprise/national).

• Influencer: Trade Marketing Manager/Lead; Sales Effectiveness/Capability Manager;
  Sales Automation Lead; SFA Manager; RTM/GTM Manager; Customer Development Manager;
  BI/Analytics Manager.

• Irrelevant: Marketing-only (Brand/Performance/CMO), Commercial Director,
  Directors of Marketing/Brand/Procurement/Finance/HR/Legal/Plant/Manufacturing/
  Operations/SCM/Logistics, generic Owner/Founder/Partner (no enterprise mandate),
  territory/area sales managers (ASM/RSM/ZSM/city/state/zone) without
  national/multi-country scope, IT Support/Engineer/Admin.

STRICTNESS & PRECEDENCE
───────────────────────
• Check thoroughly, think, then default to Irrelevant if not matching the allow-list.
• Apply exclusions BEFORE promotions.
• Country Managers are always FDM. CFO is FDM.
• Strategy titles (VP Strategy, Strategy Director/Head) are P1 Influencer unless also owning Sales/IT/Digital.
• Exclude Commercial Directors and Directors of Marketing/Brand/Procurement/Finance/HR/Legal/Plant/Ops/SCM/Logistics.
• Precedence when multiple tags fire: FDM > KDM > P1 Influencer > Influencer.
• Director General → FDM (apex executive in LATAM, Africa, Middle East, Europe).
  Exception: scoped to sub-unit → KDM or lower.

DECISION POLICY (STRICT ORDER)
──────────────────────────────
A) EXCLUDE first → Irrelevant if title contains:
   marketing|brand|procurement|legal|hr|human resources|plant|manufacturing|
   operations|scm|logistics|commercial director|finance
   *Exception: CFO / Chief Financial Officer are NOT excluded.*

B) DOWNSCOPE GUARD: If title contains manager|lead|supervisor|analyst|specialist|
   engineer|coordinator and lacks any national/multi-country token →
   Influencer if role touches sales/rtm/gtm/distribution/retail/commercial excellence/
   digital/analytics/bi/sfa/crm/it/strategy; otherwise Irrelevant.

C) POSITIVE MATCHES (only if not excluded/downsized):
   FDM: c-suite/md/president/coo; CIO; Head of Technology; country (manager|head|director);
        regional with multi-country; enterprise-wide vp/evp/svp; gm with p&l; CFO; director general.
   KDM: sales director (any qualifier); vp sales; head of sales; director (generic);
        cio; it director/head with budget; national/multi-country senior sales manager;
        head of retail/distribution/gtm (national/multi-country).
   P1:  commercial/sales excellence (director/head); digital/cdo; rtm/gtm/channel development;
        sales ops (director/head); it head/director (non-CIO); bi/analytics (director/head);
        head of retail/gtm/distribution (sub-national); CSO/VP Strategy.
   Influencer: trade marketing manager, sales capability/effectiveness manager,
        sfa/rtm/gtm manager, analytics manager.

D) Precedence: FDM > KDM > P1 Influencer > Influencer.

NORMALIZATION
─────────────
• Lowercase, trim, remove punctuation.
• Expand: md→managing director; vp/evp/svp→vice president; gm→general manager;
  cdo→chief digital officer; cio→chief information officer; cro→chief revenue officer;
  cco→chief commercial officer; dg→director general.
• Region tokens for multi-country/national scope: national, latam, emea, apac, mena, sea, gcc,
  europe, americas, global, international, country, multi-country.

CONFIDENCE GUIDANCE
───────────────────
≥0.80: Exact allow-list or clear senior role.
0.65-0.79: Strong inference (Head of Retail/GTM with national scope, Senior Sales Manager).
0.50-0.64: Ambiguous with evidence.
<0.50: Ambiguous → Irrelevant.

FEW-SHOT EXAMPLES
─────────────────
"Commercial Excellence Director" → P1 Influencer (0.85) — shapes sales process, not final budget.
"Area Sales Manager – Delhi NCR" → Irrelevant (0.92) — territory-only, no national mandate.
"Country Manager – Indonesia" → FDM (0.97) — national P&L/signatory authority.
"Director" (generic) → KDM (0.90) — senior decision authority unless excluded scope evident.
"Director of Procurement" → Irrelevant (0.96) — procurement directors excluded.
"Head of Sales – APAC" → KDM (0.93) — multi-country sales head with budget authority.
"CFO" → FDM (0.94) — C-suite with enterprise-wide financial authority.
"VP Strategy" → P1 Influencer (0.82) — shapes direction but not direct sales/IT owner.
"Commercial Director" → Irrelevant (0.95) — excluded by policy.
"Director General" → FDM (0.90) — apex executive equivalent to MD/CEO.
"Director General – Northern Region" → KDM (0.78) — scoped to sub-national region.
"Regional HR Manager" → Irrelevant (0.95) — HR roles are excluded.
"Brand Marketing Director" → Irrelevant (0.93) — marketing-only directors excluded.
"Trade Marketing Manager" → Influencer (0.75) — manager-level in trade marketing.
"IT Support Engineer" → Irrelevant (0.94) — IT Support/Engineer/Admin excluded.
"""


async def _classify_buying_roles_batch(
    contacts: list,  # list[Contact]
    company: str,
    target_roles: list[str],
) -> list:
    """
    Use GPT-4.1 to classify contacts into buying-role tags (FDM/KDM/P1/Influencer/Irrelevant).
    Also generates importance notes and priority scores in the same call.

    Returns contacts with updated role_bucket, importance_note, priority_score.
    Falls back gracefully — returns contacts unchanged on any error.
    """
    if not contacts:
        return contacts

    from backend.tools.llm import llm_complete
    import json as _json
    import re as _re

    BATCH_SIZE = 15
    all_results = list(contacts)

    roles_str = ", ".join(target_roles[:8]) if target_roles else "senior decision makers"

    for batch_start in range(0, len(contacts), BATCH_SIZE):
        batch = contacts[batch_start:batch_start + BATCH_SIZE]

        lines = "\n".join(
            f'{i}. designation="{c.role_title or "Unknown"}" | '
            f'linkedin_verified={c.linkedin_verified} | '
            f'company="{company}"'
            for i, c in enumerate(batch)
        )

        prompt = (
            f"{_BUYING_ROLE_PROMPT}\n"
            f"COMPANY: {company}\n"
            f"WE ARE LOOKING FOR: {roles_str}\n\n"
            f"CONTACTS TO CLASSIFY:\n{lines}\n\n"
            f"For EACH contact return a JSON object with:\n"
            f'- "index": (integer, 0-based)\n'
            f'- "tag": "FDM" | "KDM" | "P1 Influencer" | "Influencer" | "Irrelevant"\n'
            f'- "confidence": 0.0 to 1.0\n'
            f'- "reason": 1-2 sentences citing which rule applies\n'
            f'- "importance_note": max 15 words on why this person matters for B2B sales (empty if Irrelevant)\n'
            f'- "priority_score": 0-100 (FDM=85-100, KDM=70-89, P1=50-69, Influencer=30-49, Irrelevant=0)\n\n'
            f"Return ONLY a JSON array. No markdown, no explanation."
        )

        try:
            raw = await llm_complete(prompt, model="gpt-4.1", max_tokens=3000, temperature=0)
            cleaned = _re.sub(r'^```(?:json)?\s*|\s*```$', '', raw.strip())
            match = _re.search(r'\[.*\]', cleaned, _re.DOTALL)
            if not match:
                logger.warning("classify_buying_roles_no_json", batch=batch_start,
                               raw_snippet=raw[:200])
                continue

            results = _json.loads(match.group(0))
            _VALID_TAGS = {"FDM", "KDM", "P1 Influencer", "Influencer", "Irrelevant"}

            for item in results:
                if not isinstance(item, dict) or "index" not in item:
                    continue
                idx = item["index"]
                if not (0 <= idx < len(batch)):
                    continue
                global_idx = batch_start + idx
                tag = item.get("tag", "Irrelevant")
                if tag not in _VALID_TAGS:
                    tag = "Irrelevant"

                all_results[global_idx] = all_results[global_idx].model_copy(update={
                    "role_bucket": tag,
                    "importance_note": item.get("importance_note", item.get("reason", "")),
                    "priority_score": item.get("priority_score", 0),
                })

            logger.info("classify_buying_roles_batch_done",
                        batch_start=batch_start, batch_size=len(batch),
                        tags=[all_results[batch_start + i].role_bucket for i in range(len(batch))])

        except Exception as e:
            logger.warning("classify_buying_roles_error", batch=batch_start, error=str(e))
            # On error, contacts keep their existing role_bucket from keyword classifier

    return all_results


# ---------------------------------------------------------------------------
# Valid role detection helper (for bucket grouping)
# ---------------------------------------------------------------------------

_ENTRY_LEVEL_PATTERNS = [
    " intern", "student ", "trainee ", "graduate", "assistant", "coordinator",
    "entry", "jr ", "junior ", "apprenti", "becario", "estagiario", "praktikant"
]


def _is_valid_candidate(role_title: str) -> bool:
    """Return True if the role is NOT entry-level or junior, allowing almost everyone else."""
    if not role_title:
        return False
    t = f" {role_title.lower()} "
    return not any(p in t for p in _ENTRY_LEVEL_PATTERNS)


# ---------------------------------------------------------------------------
# Node: score_and_rank  — LLM buying-role classification, no manual thresholds
# ---------------------------------------------------------------------------

async def score_and_rank(state: SearcherState) -> SearcherState:
    """
    Use GPT-4.1 to classify every discovered contact into buying-role tags.
    The LLM reasons about each title using the B2B FMCG/CPG allow-lists — no
    fuzzy matching or numeric thresholds.

    discovered_contacts:   FDM + KDM  → shown pre-checked to SDR
    pending_dm_candidates: P1 Influencer + Influencer → shown unchecked
    Irrelevant:            dropped with log
    """
    from backend.utils.progress import emit_log as _score_log

    raw_contacts = state.discovered_contacts
    if not raw_contacts:
        return state

    # Filter out people who already exist in the sheets
    existing_set = {n.lower().strip() for n in state.existing_names if n.strip()}
    contacts = []
    for c in raw_contacts:
        if c.full_name.strip().lower() in existing_set:
            logger.info("searcher_dedupe_sheet", name=c.full_name)
            continue
        contacts.append(c)

    if not contacts:
        return state.model_copy(update={"discovered_contacts": [], "pending_dm_candidates": []})

    target_roles = state.missing_dm_roles or state.dm_roles or []

    await _score_log(state.thread_id,
        f"[{state.target_company}] Classifying {len(contacts)} contacts with LLM buying-role analysis (GPT-4.1)…",
        level="info")

    # LLM classifies each contact: FDM / KDM / P1 Influencer / Influencer / Irrelevant
    classified = await _classify_buying_roles_batch(contacts, state.target_company, target_roles)

    # Split by classification — the LLM has already reasoned about each title
    matched: list[Contact] = []
    bonus_candidates: list[Contact] = []
    dropped = 0

    for c in classified:
        tag = c.role_bucket
        if tag in ("FDM", "KDM"):
            matched.append(c)
            logger.info("searcher_llm_match", name=c.full_name, role=c.role_title,
                        tag=tag, score=c.priority_score, note=c.importance_note)
        elif tag in ("P1 Influencer", "Influencer"):
            bonus_candidates.append(c)
            logger.info("searcher_llm_bonus", name=c.full_name, role=c.role_title,
                        tag=tag, score=c.priority_score)
        else:
            dropped += 1
            logger.info("searcher_llm_drop", name=c.full_name, role=c.role_title,
                        tag=tag, reason=c.importance_note or "LLM classified as Irrelevant")

    # Sort by priority score (LLM-assigned)
    matched.sort(key=lambda x: x.priority_score or 0, reverse=True)
    bonus_candidates.sort(key=lambda x: x.priority_score or 0, reverse=True)

    logger.info("searcher_score_rank_done",
                company=state.target_company,
                total_input=len(contacts),
                matched=len(matched),
                bonus=len(bonus_candidates),
                dropped=dropped)
    await _score_log(state.thread_id,
        f"[{state.target_company}] LLM classification: {len(matched)} decision-makers (FDM/KDM), "
        f"{len(bonus_candidates)} influencers, {dropped} irrelevant dropped",
        level="success" if matched else "info")

    return state.model_copy(update={
        "discovered_contacts": matched,
        "pending_dm_candidates": bonus_candidates,
    })


# ---------------------------------------------------------------------------
# Node: await_full_selection  — SDR reviews ALL found contacts, picks who to process
# ---------------------------------------------------------------------------

async def _find_more_agents(
    state: SearcherState,
    prompt: str,
    existing_contacts: list[Contact],
) -> list[Contact]:
    """
    Run three parallel agents to find additional contacts based on SDR's prompt.

    Agent 1 — LLM-guided Unipile: parse the prompt → extract role titles → parallel
               Unipile searches with those titles.
    Agent 2 — Sales Navigator Scrapling: re-scrape with prompt-derived seniority hint.
    Agent 3 — Web search: direct search for "<company> <prompt>" for any named people.

    Returns only contacts NOT already discovered (deduped by LinkedIn URL + name).
    """
    from backend.tools.unipile import search_people, verify_profile
    from backend.tools.llm import llm_complete
    import json as _json
    import re as _re

    existing_keys: set[str] = set()
    for c in existing_contacts:
        if c.linkedin_url:
            existing_keys.add(c.linkedin_url.rstrip("/").lower())
        existing_keys.add(c.full_name.lower().strip())

    company_name = state.target_normalized_name or state.target_company
    org_id = state.target_org_id or ""

    # ── Agent 1: LLM → role titles → Unipile parallel search ─────────────────
    async def _unipile_agent() -> list[Contact]:
        if not org_id:
            return []
        try:
            role_prompt = (
                f"Company: {company_name}\n"
                f"SDR request: \"{prompt}\"\n\n"
                f"Act as an expert SDR Brain. We need to map all prominent decision-makers related to this request.\n"
                f"1. Generate 4-6 specific LinkedIn job title search strings based strictly on the request.\n"
                f"2. Suggest 4-6 ADJACENT, highly valuable roles (e.g., if they asked for 'Sales', include 'Marketing' or 'Ops' or 'Growth').\n"
                f"Target ONLY non-entry roles (exclude interns/students).\n"
                f"Include local-language variants if the company is in a non-English country.\n"
                f"Return ONLY a JSON array of 8-12 combined string titles: [\"title1\", \"title2\", ...]\nNo explanation."
            )
            response = await llm_complete(role_prompt, max_tokens=300, temperature=0)
            match = _re.search(r'\[.*?\]', response, _re.DOTALL)
            if not match:
                return []
            titles: list[str] = _json.loads(match.group(0))
            titles = [str(t).strip() for t in titles if isinstance(t, str) and t.strip()]
            if not titles:
                return []

            logger.info("find_more_agent1_titles", company=company_name, titles=titles)
            people = await search_people(org_id, titles, limit=50)

            sem = asyncio.Semaphore(5)
            results: list[Contact] = []

            async def _verify(person: dict) -> Contact | None:
                async with sem:
                    key_url = person["linkedin_url"].rstrip("/").lower()
                    key_name = person["full_name"].lower().strip()
                    if key_url in existing_keys or key_name in existing_keys:
                        return None
                    try:
                        v = await verify_profile(person["linkedin_url"], state.target_company)
                        # We searched by org_id, so we trust they are at the target company if they are still employed.
                        if v.get("valid") and v.get("still_employed"):
                            role = v.get("current_role") or person.get("headline") or ""
                            return Contact(
                                full_name=person["full_name"],
                                company=company_name,
                                domain=state.target_domain or "",
                                role_title=role or None,
                                role_bucket=_classify_role(role),
                                linkedin_url=person["linkedin_url"],
                                linkedin_verified=True,
                                provenance=["find_more_unipile"],
                            )
                    except Exception:
                        pass
                    return None

            verified = await asyncio.gather(*[_verify(p) for p in people])
            results = [c for c in verified if c is not None]
            logger.info("find_more_agent1_done", company=company_name, found=len(results))
            return results
        except Exception as e:
            logger.warning("find_more_agent1_error", error=str(e))
            return []

    # ── Agent 2: Scrapling Sales Navigator targeted scrape ────────────────────
    async def _scrapling_agent() -> list[Contact]:
        settings = get_settings()
        li_at = settings.linkedin_li_at_cookie or ""
        if not li_at or not org_id:
            return []
        try:
            from backend.tools.sales_nav_scraper import scrape_company_people
            scraped = await scrape_company_people(
                org_id=org_id,
                li_at_cookie=li_at,
                scroll_rounds=12,
                dm_only=False,  # Get all roles, filter by prompt context below
            )
            contacts: list[Contact] = []
            for person in scraped:
                key_url = (person["linkedin_url"] or "").rstrip("/").lower()
                key_name = person["full_name"].lower().strip()
                if key_url in existing_keys or key_name in existing_keys:
                    continue
                # Filter: if the person's title or name is relevant to the prompt
                prompt_lower = prompt.lower()
                title_lower = (person["title"] or "").lower()
                name_lower = person["full_name"].lower()
                if not (
                    any(w in title_lower for w in prompt_lower.split() if len(w) > 3)
                    or any(w in name_lower for w in prompt_lower.split() if len(w) > 3)
                    or _is_valid_candidate(person["title"])
                ):
                    continue
                contacts.append(Contact(
                    full_name=person["full_name"],
                    company=company_name,
                    domain=state.target_domain or "",
                    role_title=person["title"] or None,
                    role_bucket=_classify_role(person["title"]),
                    linkedin_url=person["linkedin_url"] or None,
                    linkedin_verified=False,
                    provenance=["find_more_scrapling"],
                ))
            logger.info("find_more_agent2_done", company=company_name, found=len(contacts))
            return contacts
        except Exception as e:
            logger.warning("find_more_agent2_error", error=str(e))
            return []

    # ── Agent 3: Web search for named people or role ──────────────────────────
    async def _web_agent() -> list[Contact]:
        try:
            from backend.tools.search import search_with_fallback
            query = f"{company_name} {prompt} LinkedIn site:linkedin.com/in"
            results = await search_with_fallback(query, max_results=8)
            contacts: list[Contact] = []
            li_re = _re.compile(r'linkedin\.com/in/([^/?&\s"\'<>]+)')
            seen_slugs: set[str] = set()
            for r in results:
                for m in li_re.finditer(r.url + " " + r.snippet):
                    slug = m.group(1).rstrip("/")
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)
                    url = f"https://www.linkedin.com/in/{slug}"
                    if url.lower() in existing_keys:
                        continue
                    # Try to extract name from snippet or title
                    name_m = _re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', r.snippet)
                    if not name_m:
                        continue
                    name = name_m.group(1)
                    if name.lower() in existing_keys:
                        continue
                    contacts.append(Contact(
                        full_name=name,
                        company=company_name,
                        domain=state.target_domain or "",
                        role_title=None,
                        linkedin_url=url,
                        linkedin_verified=False,
                        provenance=["find_more_web"],
                    ))
            logger.info("find_more_agent3_done", company=company_name, found=len(contacts))
            return contacts
        except Exception as e:
            logger.warning("find_more_agent3_error", error=str(e))
            return []

    # Run all three agents in parallel
    agent_results = await asyncio.gather(
        _unipile_agent(),
        _scrapling_agent(),
        _web_agent(),
        return_exceptions=True,
    )

    new_contacts: list[Contact] = []
    seen_new: set[str] = set(existing_keys)
    for result in agent_results:
        if not isinstance(result, list):
            continue
        for c in result:
            key = (c.linkedin_url or "").rstrip("/").lower() or c.full_name.lower().strip()
            if key and key not in seen_new:
                seen_new.add(key)
                new_contacts.append(c)

    # Classify new contacts with LLM buying-role classifier + importance notes
    if new_contacts:
        try:
            dm_roles = state.missing_dm_roles or state.dm_roles or []
            new_contacts = await _classify_buying_roles_batch(
                new_contacts, company_name, dm_roles
            )
            # Filter out Irrelevant contacts from Find More results
            new_contacts = [c for c in new_contacts if c.role_bucket != "Irrelevant"]
        except Exception:
            # Fallback to simpler importance notes if classifier fails
            try:
                dm_roles = state.missing_dm_roles or state.dm_roles or []
                new_contacts = await _generate_importance_notes_batch(
                    new_contacts, company_name, dm_roles
                )
            except Exception:
                pass

    logger.info("find_more_agents_total", company=company_name, new=len(new_contacts), prompt=prompt)
    return new_contacts


async def await_full_selection(state: SearcherState) -> SearcherState:
    """
    Pause for SDR review. Emits 'contact_selection_required' with all found contacts.

    SDR can:
      A) Select contacts and click "Process" → proceed with those contacts
      B) Type a prompt and click "Find More" → 3 parallel agents search again →
         updated list shown with NEW badges → SDR confirms again

    Loop continues until SDR confirms (or 5-min timeout auto-proceeds with matched contacts).
    """
    matched = state.discovered_contacts
    bonus = state.pending_dm_candidates

    if not matched and not bonus:
        return state.model_copy(update={"pending_dm_candidates": []})
    if not state.thread_id:
        return state.model_copy(update={"pending_dm_candidates": []})

    # Auto mode: skip SDR pause, take all matched contacts automatically
    if state.auto_approve:
        from backend.utils.progress import emit_log as _emit_log
        await _emit_log(state.thread_id,
                        f"Auto mode — processing all {len(matched)} matched contacts for {state.target_company}",
                        level="info")
        return state.model_copy(update={
            "discovered_contacts": list(matched),
            "pending_dm_candidates": [],
        })

    from backend.utils import dm_selection as _dm_sel
    from backend.utils.progress import _queues as _pq, emit_log as _emit_log, emit as _emit_progress
    from datetime import datetime as _dt, timezone as _tz

    logger.info("searcher_await_full_selection",
                company=state.target_company, matched=len(matched), bonus=len(bonus))

    all_candidates: list[Contact] = list(matched) + list(bonus)
    # Track which indices are "new" (added by find_more agents) for UI highlighting
    new_since_index: int = len(all_candidates)  # initially no "new" ones

    def _to_dict(c: Contact, idx: int, pre_selected: bool, group: str, is_new: bool = False) -> dict:
        return {
            "index": idx,
            "full_name": c.full_name,
            "role_title": c.role_title or "",
            "company": c.company,
            "linkedin_url": c.linkedin_url or "",
            "linkedin_verified": c.linkedin_verified,
            "source": (c.provenance or ["unknown"])[0],
            "pre_selected": pre_selected,
            "group": group,
            "importance_note": c.importance_note or "",
            "is_new": is_new,
        }

    def _build_event(candidates: list[Contact], new_from_idx: int) -> dict:
        dicts = []
        n_matched = len(matched)
        for i, c in enumerate(candidates):
            is_new = i >= new_from_idx
            if i < n_matched:
                dicts.append(_to_dict(c, i, True, "matched", is_new))
            else:
                dicts.append(_to_dict(c, i, False, "bonus", is_new))
        return {
            "company": state.target_company,
            "candidates": dicts,
            "matched_count": n_matched,
            "bonus_count": len(candidates) - n_matched,
            "total": len(candidates),
            "timeout_secs": 300,
        }

    async def _emit_selection(candidates: list[Contact], new_from_idx: int) -> None:
        q = _pq.get(state.thread_id)
        if not q:
            return
        try:
            await q.put({
                "type": "contact_selection_required",
                "data": _build_event(candidates, new_from_idx),
                "timestamp": _dt.now(_tz.utc).isoformat(),
            })
        except Exception:
            pass

    selection_queue = _dm_sel.register(state.thread_id)
    await _emit_selection(all_candidates, new_since_index)

    final_contacts: list[Contact] = []
    find_more_round = 0

    try:
        while True:
            try:
                msg = await asyncio.wait_for(selection_queue.get(), timeout=300)
            except asyncio.TimeoutError:
                final_contacts = [c for c in all_candidates[:len(matched)]]
                logger.info("searcher_full_selection_timeout",
                            company=state.target_company, count=len(final_contacts))
                await _emit_log(state.thread_id,
                    f"Timeout — auto-processing {len(final_contacts)} matched contact(s) for {state.target_company}",
                    level="info")
                break

            action = msg.get("action") if isinstance(msg, dict) else "select"

            if action == "select":
                indices: list[int] = msg.get("indices", msg) if isinstance(msg, dict) else msg
                final_contacts = [all_candidates[i] for i in indices if 0 <= i < len(all_candidates)]
                logger.info("searcher_full_selection_received",
                            company=state.target_company, selected=len(final_contacts))
                await _emit_log(state.thread_id,
                    f"Selection confirmed — validating & enriching {len(final_contacts)} contact(s) for {state.target_company}",
                    level="info")
                await _emit_progress(state.thread_id, state.target_company, "validating")
                break

            elif action == "find_more":
                find_more_round += 1
                prompt_text = msg.get("prompt", "find more senior people")
                logger.info("searcher_find_more_requested",
                            company=state.target_company, prompt=prompt_text, round=find_more_round)
                await _emit_log(state.thread_id,
                    f"Searching more for '{prompt_text}' at {state.target_company} (round {find_more_round})…",
                    level="info")

                new_contacts = await _find_more_agents(state, prompt_text, all_candidates)

                if new_contacts:
                    new_since_index = len(all_candidates)
                    all_candidates = all_candidates + new_contacts
                    await _emit_log(state.thread_id,
                        f"Found {len(new_contacts)} additional people — updated list sent",
                        level="success")
                else:
                    await _emit_log(state.thread_id,
                        f"No additional people found for '{prompt_text}' — try a different prompt",
                        level="warning")
                    new_since_index = len(all_candidates)  # no new, no badges

                # Re-emit updated list (SDR stays in the loop)
                await _emit_selection(all_candidates, new_since_index)
                # Continue while loop to wait for next SDR action

    finally:
        _dm_sel.unregister(state.thread_id)

    return state.model_copy(update={
        "discovered_contacts": final_contacts,
        "pending_dm_candidates": [],
    })


# ---------------------------------------------------------------------------
# Helper: web search for must-have roles (CEO, CTO, CSO)
# ---------------------------------------------------------------------------

async def _web_search_must_have_people(
    company_name: str,
    missing_tiers: list[dict],
    thread_id: str | None = None,
) -> list[dict]:
    """
    Use web search to find specific must-have people.
    e.g. "Who is the CEO of Zomato?" → "Deepinder Goyal"
    Returns list of {"name": str, "title": str, "tier": str}
    """
    from backend.tools.llm import llm_web_search
    from backend.utils.progress import emit_log as _ws_log

    found_people: list[dict] = []

    for tier_def in missing_tiers:
        tier_name = tier_def["tier"]
        # Search for the top role in this tier
        search_title = tier_def["search_queries"][0]  # e.g. "CEO", "CTO", "Head of Sales"

        try:
            await _ws_log(thread_id,
                          f"[{company_name}] searching web: who is the {search_title}?")

            prompt = (
                f"Who is the {search_title} of {company_name}? "
                f"If the exact title doesn't exist, find the closest equivalent "
                f"(e.g. Managing Director instead of CEO, VP Engineering instead of CTO). "
                f"Return ONLY: Full Name — Exact Title. Nothing else. "
                f"If unknown, return: unknown"
            )
            raw = await llm_web_search(prompt)
            content = (raw or "").strip()

            if not content or content.lower() == "unknown":
                await _ws_log(thread_id,
                              f"[{company_name}] {tier_name}: not found via web search")
                continue

            # Parse "Name — Title" or "Name - Title"
            import re as _re
            parts = _re.split(r'\s*[—–\-]\s*', content, maxsplit=1)
            if len(parts) >= 2:
                name = parts[0].strip()
                title = parts[1].strip()
            else:
                name = content.strip()
                title = search_title

            # Basic validation
            if len(name) < 3 or len(name) > 60:
                continue
            # Skip if it looks like a sentence, not a name
            if any(w in name.lower() for w in ["the", "is", "was", "company", "unknown"]):
                continue

            found_people.append({
                "name": name,
                "title": title,
                "tier": tier_name,
            })
            await _ws_log(thread_id,
                          f"[{company_name}] {tier_name}: found {name} — {title}",
                          "success")

        except Exception as e:
            logger.warning("web_search_must_have_error",
                           company=company_name, tier=tier_name, error=str(e))

    return found_people


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
    from backend.tools.unipile import get_company_org_id, search_people, verify_profile
    from backend.agents.fini import REGION_IDS

    logger.info("searcher_unipile_search", company=state.target_company)
    from backend.utils.progress import emit_log as _uni_log
    await _uni_log(state.thread_id,
        f"[{state.target_company}] Starting LinkedIn search — building role queries…",
        level="info")

    # --- Web search for must-have roles (CEO, CTO, CSO) ---
    # Identify which tiers are missing from existing contacts and search the web.
    _existing_tiers_covered: set[str] = set()
    for _et in (state.missing_dm_roles or []):
        _et_tier = _classify_role(_et)
        if _et_tier != "Unknown":
            _existing_tiers_covered.add(_et_tier)
    # Actually check what tiers are MISSING (not covered in gap analysis)
    _missing_tier_defs = [
        t for t in MUST_HAVE_TIERS
        if any(q in (state.missing_dm_roles or []) for q in t["search_queries"])
    ]
    _web_found: list[dict] = []
    if _missing_tier_defs:
        company_name_for_search = state.target_normalized_name or state.target_company
        _web_found = await _web_search_must_have_people(
            company_name_for_search, _missing_tier_defs, state.thread_id
        )
        if _web_found:
            await _uni_log(state.thread_id,
                f"[{state.target_company}] Web search found {len(_web_found)} must-have people — will verify on LinkedIn",
                level="success")

    # Build function-level search queries from expanded roles.
    # Strategy: one query PER BUSINESS FUNCTION (marketing, digital, sales, ops, …)
    # Each query targets a DIFFERENT area of the org chart → returns different people.
    # With 8-9 parallel searches × 25 results each we get up to 200 raw candidates
    # before dedup — vs the old sequential approach that returned ~8 overlapping results.
    function_queries = _build_function_queries(
        state.expanded_dm_roles or [],
        state.missing_dm_roles or [],
    )
    if not function_queries:
        function_queries = [
            "CEO Managing Director", "CMO Marketing Director",
            "CDO Digital Director", "Sales Director",
            "COO Operations Director", "CTO Technology Director",
            "General Manager Country Manager",
        ]

    # Broad seniority-level queries catch people whose titles don't match any
    # function bucket keyword — e.g. "VP Pricing", "Chief Growth Officer".
    # REMOVED empty string "" — it returned everyone (HR, Admin, etc.) causing noise.
    # These queries are targeted at seniority levels that map to FDM/KDM/P1 roles.
    _BROAD_SENIORITY_QUERIES = [
        "Vice President",
        "Chief",
        "Managing Director",
        "Country Manager",
        "Gerente",       # Spanish/Portuguese
        "Directeur",     # French
        "Geschäftsführer",  # German
    ]
    # Merge: function queries first (they're more targeted), then broad seniority
    seen_queries: set[str] = set(function_queries)
    for q in _BROAD_SENIORITY_QUERIES:
        if q not in seen_queries:
            function_queries.append(q)
            seen_queries.add(q)

    role_titles = function_queries
    logger.info("searcher_unipile_function_queries",
                company=state.target_company,
                queries=role_titles,
                count=len(role_titles))

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
    await _uni_log(state.thread_id,
        f"[{state.target_company}] Searching LinkedIn with {len(role_titles)} role queries (org: {org_id})…",
        level="info")

    # Search LinkedIn for people matching the requested role titles.
    # Try with region filter first; if no results, retry without (small companies often have
    # no region-tagged profiles, so the filter eliminates everyone).
    try:
        people = await search_people(org_id, role_titles, region_id=region_id, limit=50)
        if not people and region_id:
            logger.info("searcher_unipile_retry_no_region", company=state.target_company)
            people = await search_people(org_id, role_titles, region_id="", limit=25)
    except Exception as e:
        logger.warning("searcher_unipile_search_error", error=str(e))
        return state

    # Verify all profiles in parallel (max 5 concurrent to avoid rate-limiting).
    sem = asyncio.Semaphore(5)
    company_name = state.target_normalized_name or state.target_company

    async def _verify_one(person: dict) -> Contact | None:
        role_title = person.get("headline") or ""
        still_at_company = False
        async with sem:
            try:
                verification = await verify_profile(person["linkedin_url"], state.target_company)
                # We searched by org_id, so we trust they are at the target company if they are still employed.
                # Strict string matching on 'at_target_company' drops too many valid people at subsidiaries.
                if verification.get("valid") and verification.get("still_employed"):
                    still_at_company = True
                    role_title = verification.get("current_role") or role_title
                else:
                    logger.info("searcher_unipile_verify_drop",
                                name=person["full_name"],
                                reason="not currently at target company",
                                verification=verification)
                    return None
            except Exception as e:
                logger.warning("searcher_unipile_verify_error", person=person["full_name"], error=str(e))
                still_at_company = False

        bucket = _classify_role(role_title)
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
        logger.info("searcher_unipile_contact",
                    name=person["full_name"], role=role_title,
                    bucket=bucket, verified=still_at_company)
        return contact

    await _uni_log(state.thread_id,
        f"[{state.target_company}] LinkedIn returned {len(people)} people — verifying current employment…",
        level="info")
    results = await asyncio.gather(*[_verify_one(p) for p in people])
    new_contacts: list[Contact] = [c for c in results if c is not None]

    if not new_contacts:
        logger.info("searcher_unipile_no_results", company=state.target_company,
                    reason="all unipile results failed verify_profile" if people else "unipile returned 0 results",
                    msg="will rely on web/filings search")

    logger.info("searcher_unipile_done", company=state.target_company, found=len(new_contacts))

    # --- Add web-found must-have people (verify on LinkedIn first) ---
    if _web_found:
        from backend.tools.unipile import search_person_by_name
        _existing_names_lower = {c.full_name.lower() for c in new_contacts}
        for wf in _web_found:
            wf_name = wf["name"]
            if wf_name.lower() in _existing_names_lower:
                await _uni_log(state.thread_id,
                    f"[{state.target_company}] {wf_name} ({wf['tier']}) already found via LinkedIn — skipping")
                continue
            # Try to find this person on LinkedIn
            try:
                matches = await search_person_by_name(wf_name, org_id=org_id, limit=3)
                if matches:
                    best = matches[0]
                    contact = Contact(
                        full_name=best.get("full_name") or wf_name,
                        company=company_name,
                        domain=state.target_domain or "",
                        role_title=wf["title"],
                        role_bucket=_classify_role(wf["title"]),
                        linkedin_url=best.get("linkedin_url", ""),
                        linkedin_verified=True,
                        provenance=["web_search_must_have"],
                    )
                    new_contacts.append(contact)
                    _existing_names_lower.add(contact.full_name.lower())
                    await _uni_log(state.thread_id,
                        f"[{state.target_company}] {wf['tier']}: {contact.full_name} — {wf['title']} (verified on LinkedIn)",
                        level="success")
                else:
                    # Add without LinkedIn URL — Veri will try to find them
                    contact = Contact(
                        full_name=wf_name,
                        company=company_name,
                        domain=state.target_domain or "",
                        role_title=wf["title"],
                        role_bucket=_classify_role(wf["title"]),
                        provenance=["web_search_must_have"],
                    )
                    new_contacts.append(contact)
                    await _uni_log(state.thread_id,
                        f"[{state.target_company}] {wf['tier']}: {wf_name} — {wf['title']} (from web, needs LinkedIn verification)",
                        level="info")
            except Exception as e:
                logger.warning("web_found_verify_error", name=wf_name, error=str(e))

    await _uni_log(state.thread_id,
        f"[{state.target_company}] Search done — {len(new_contacts)} contact(s) found",
        level="success" if new_contacts else "info")
    existing = list(state.discovered_contacts)
    return state.model_copy(update={
        "discovered_contacts": existing + new_contacts,
        "target_org_id": org_id,  # persist so enrich_contacts probe can use it
    })


# ---------------------------------------------------------------------------
# Node: scrape_sales_nav  — Scrapling-powered Sales Navigator scraper
# ---------------------------------------------------------------------------

async def scrape_sales_nav(state: SearcherState) -> SearcherState:
    """
    Scrape the company's Sales Navigator people page using Scrapling + LinkedIn cookie.

    This node runs AFTER unipile_search and complements it by scraping Sales Navigator
    directly — giving access to ALL employees LinkedIn shows for the company (up to 2,500),
    filtered to VP/Director/C-Suite seniority.

    Requires:
        LINKEDIN_LI_AT_COOKIE in .env  (li_at session cookie from your browser)
        pip install "scrapling[fetchers]" && scrapling install

    Skipped gracefully if:
        - LINKEDIN_LI_AT_COOKIE is not set
        - scrapling[fetchers] is not installed
        - org_id is not available
    """
    settings = get_settings()
    li_at = settings.linkedin_li_at_cookie or ""

    if not li_at:
        logger.info("scrape_sales_nav_skip",
                    company=state.target_company,
                    reason="LINKEDIN_LI_AT_COOKIE not set in .env")
        return state

    org_id = state.target_org_id or ""
    if not org_id:
        logger.info("scrape_sales_nav_skip",
                    company=state.target_company,
                    reason="no org_id available")
        return state

    from backend.utils.progress import emit_log as _emit_log
    await _emit_log(
        state.thread_id,
        f"Scraping Sales Navigator for {state.target_company} ({org_id}) — finding all senior people…",
        level="info",
    )

    try:
        from backend.tools.sales_nav_scraper import scrape_company_people
        scraped = await scrape_company_people(
            org_id=org_id,
            li_at_cookie=li_at,
            scroll_rounds=16,   # ~400 people (25 per scroll)
            dm_only=False,      # get ALL people — SDR will filter by role bucket
        )
    except Exception as e:
        logger.warning("scrape_sales_nav_error", company=state.target_company, error=str(e))
        return state

    if not scraped:
        logger.info("scrape_sales_nav_empty", company=state.target_company)
        return state

    # Convert SalesNavPerson → Contact; mark as unverified (verify_linkedin runs later)
    company_name = state.target_normalized_name or state.target_company
    new_contacts: list[Contact] = []
    for person in scraped:
        bucket = _classify_role(person["title"])
        new_contacts.append(Contact(
            full_name=person["full_name"],
            company=company_name,
            domain=state.target_domain or "",
            role_title=person["title"] or None,
            role_bucket=bucket,
            linkedin_url=person["linkedin_url"] or None,
            linkedin_verified=False,
            provenance=["sales_nav_scraper"],
        ))

    logger.info("scrape_sales_nav_done",
                company=state.target_company,
                scraped=len(scraped),
                converted=len(new_contacts))

    await _emit_log(
        state.thread_id,
        f"Sales Navigator: found {len(new_contacts)} senior people at {state.target_company}",
        level="info",
    )

    existing = list(state.discovered_contacts)
    return state.model_copy(update={"discovered_contacts": existing + new_contacts})


# ---------------------------------------------------------------------------
# Node: search_company_website
# ---------------------------------------------------------------------------

_LEADERSHIP_PATHS = [
    "/about/leadership", "/about-us/leadership", "/about/management",
    "/about-us/management", "/leadership-team", "/management-team",
    "/about/team", "/our-team", "/board-of-directors", "/about",
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


async def _filter_garbage_names(contacts: list[Contact], _company_name: str) -> list[Contact]:
    """
    Filter out garbage names using heuristics only (no LLM).
    _looks_like_name already does the heavy lifting during extraction.
    This is a lightweight second pass.
    """
    if not contacts:
        return contacts
    # All contacts already passed _looks_like_name during extraction — just return them
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
        async with _httpx.AsyncClient(headers=_HEADERS, follow_redirects=True, timeout=8) as client:
            sem = asyncio.Semaphore(6)

            async def _try_path(path: str) -> tuple[str, list[Contact], bool]:
                async with sem:
                    url = base + path
                    try:
                        resp = await client.get(url)
                        if resp.status_code == 403:
                            return url, [], True
                        if resp.status_code != 200:
                            return url, [], False
                        contacts = _extract_from_html(resp.text, company_name, domain)
                        if not contacts:
                            return url, [], True  # loaded but JS-rendered/empty
                        return url, contacts, False
                    except Exception:
                        return url, [], False

            path_results = await asyncio.gather(*[_try_path(p) for p in _LEADERSHIP_PATHS])

        for url, contacts, was_blocked in path_results:
            if contacts:
                logger.info("searcher_website_httpx_hit", url=url, contacts=len(contacts))
                return contacts, False
            if was_blocked:
                hit_403 = True

    except Exception as e:
        logger.warning("searcher_website_httpx_error", domain=domain, error=str(e))

    return [], hit_403


async def _scrape_llm(domain: str, company_name: str) -> list[Contact]:
    """
    LLM web search fallback for sites that block httpx (403/JS-rendered).
    Uses LLM with web_search to find current leadership from the company's own website.
    """
    from backend.config import get_settings
    settings = get_settings()
    if not settings.openai_api_key and not settings.aws_bearer_token_bedrock:
        return []

    prompt = (
        f"Find the CURRENT board of directors and leadership team for {company_name} "
        f"from their official website {domain}. "
        f"List only current members — ignore anyone who has resigned or retired. "
        f"Return ONLY a plain list, one per line: Full Name — Job Title. "
        f"No explanation, no markdown, no numbering, no citation brackets."
    )

    try:
        from backend.tools.llm import llm_web_search
        content = await asyncio.wait_for(llm_web_search(prompt), timeout=20)
        content = content.strip() if content else ""

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
                            provenance=["company_website_llm"],
                        ))

        logger.info("searcher_website_llm_done",
                    domain=domain, contacts=len(contacts))
        return contacts

    except asyncio.TimeoutError:
        logger.warning("searcher_website_llm_timeout", domain=domain)
        return []
    except Exception as e:
        logger.warning("searcher_website_llm_error", domain=domain, error=str(e))
        return []


async def search_company_website(state: SearcherState) -> SearcherState:
    """
    Scrape the company's own website for leadership/board pages.
    1. Try httpx on common paths — fast, free, works for most sites.
    2. If 403 or JS-rendered shell → fall back to LLM web search (20s timeout).
    Skipped if Unipile already found ≥ 3 verified DM contacts.
    """
    # Skip if Unipile already found enough verified DMs — website scraping adds little value
    verified_dms = [
        c for c in state.discovered_contacts
        if c.linkedin_verified and c.role_bucket == "DM"
    ]
    if len(verified_dms) >= 3:
        logger.info("searcher_website_skip", reason="unipile found sufficient DMs",
                    verified_dms=len(verified_dms))
        return state

    domain = state.target_domain
    if not domain:
        logger.info("searcher_website_skip", reason="no domain")
        return state

    company_name = state.target_normalized_name or state.target_company
    logger.info("searcher_website_search", company=company_name, domain=domain)

    from backend.utils.progress import emit_log as _web_log
    await _web_log(state.thread_id,
        f"[{company_name}] Checking company website ({domain}) for leadership team…",
        level="info")

    contacts, hit_403 = await _scrape_httpx(domain, company_name)

    if not contacts and hit_403:
        logger.info("searcher_website_fallback_llm", domain=domain,
                    reason="httpx blocked or JS-rendered")
        contacts = await _scrape_llm(domain, company_name)

    if contacts:
        logger.info("searcher_website_done", company=company_name, found=len(contacts))
        await _web_log(state.thread_id,
            f"[{company_name}] Website found {len(contacts)} leadership contact(s)",
            level="info")
        existing = list(state.discovered_contacts)
        return state.model_copy(update={"discovered_contacts": existing + contacts})

    logger.info("searcher_website_no_results", company=company_name)
    await _web_log(state.thread_id,
        f"[{company_name}] Website: no structured leadership data found",
        level="info")
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

    async def _run_filings_query(query: str) -> list[Contact]:
        try:
            results = await search_with_fallback(query, max_results=8)
            contacts: list[Contact] = []
            for r in results:
                contacts.extend(_extract_names_from_snippet(r.snippet, company_name, r.url))
            return contacts
        except Exception as e:
            logger.warning("searcher_filings_query_error", query=query[:60], error=str(e))
            return []

    batches = await asyncio.gather(*[_run_filings_query(q) for q in queries])
    raw_contacts: list[Contact] = [c for batch in batches for c in batch]

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

    async def _run_web_query(query: str, provider: str) -> list[Contact]:
        try:
            results = await search(query, provider=provider, max_results=6)
            contacts: list[Contact] = []
            for r in results:
                contacts.extend(_extract_names_from_snippet(r.snippet, company_name, r.url))
            return contacts
        except Exception as e:
            logger.warning("searcher_web_query_error", provider=provider, error=str(e))
            return []

    tasks = [_run_web_query(q, p) for q in queries for p in ["perplexity", "ddg"]]
    batches = await asyncio.gather(*tasks)
    raw_contacts: list[Contact] = [c for batch in batches for c in batch]

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
                    _extract_names_from_snippet(r.snippet, state.target_company, r.url)
                )
        except Exception as e:
            logger.warning("searcher_pdf_query_error", error=str(e))

    new_contacts = list(state.discovered_contacts) + raw_contacts
    return state.model_copy(update={"discovered_contacts": new_contacts})


# ---------------------------------------------------------------------------
# Node: perplexity_executive_search — direct AI-powered executive discovery
# ---------------------------------------------------------------------------

async def perplexity_executive_search(state: SearcherState) -> SearcherState:
    """
    Use Perplexity AI to directly find the current executive/leadership team.

    Sends two natural-language queries:
      1. C-suite and director-level people
      2. VP and head-of-level people

    Parses "Full Name — Job Title" patterns from the chat response text.
    Falls back gracefully if Perplexity key not set or API fails.
    """
    settings = get_settings()
    if not settings.perplexity_api_key:
        logger.info("perplexity_executive_skip",
                    company=state.target_company, reason="no PERPLEXITY_API_KEY")
        return state

    import httpx as _httpx
    company_name = state.target_normalized_name or state.target_company

    queries = [
        (
            f"Who are the current C-suite executives, directors, and vice presidents at {company_name}? "
            f"Include their exact full names and current job titles. "
            f"Format each person strictly as: Full Name — Job Title"
        ),
        (
            f"List the current heads of department and senior managers at {company_name}. "
            f"Include marketing, digital, ecommerce, sales, operations, finance, and technology leads. "
            f"Format each person strictly as: Full Name — Job Title"
        ),
    ]

    new_contacts: list[Contact] = []
    seen_names: set[str] = set()

    for query in queries:
        try:
            async with _httpx.AsyncClient(timeout=35) as client:
                resp = await client.post(
                    "https://api.perplexity.ai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {settings.perplexity_api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "sonar",
                        "messages": [
                            {
                                "role": "system",
                                "content": (
                                    "You are a business research assistant. "
                                    "List ONLY current employees with their exact names and current titles. "
                                    "Format each on its own line: Full Name — Job Title. "
                                    "No introductions, no explanations, no markdown."
                                ),
                            },
                            {"role": "user", "content": query},
                        ],
                        "max_tokens": 1500,
                        "temperature": 0,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

            content = data["choices"][0]["message"]["content"]

            for line in content.splitlines():
                # Strip citation brackets [1], bullet points, numbering
                line = re.sub(r'\[\d+\]', '', line).strip()
                line = re.sub(r'^[\d]+[.)]\s*', '', line).strip()
                line = line.lstrip('-•*').strip()

                if not line or len(line) < 5:
                    continue

                # Parse "Name — Title" or "Name – Title" or "Name - Title"
                for sep in ('—', '–', ' - '):
                    if sep in line:
                        parts = line.split(sep, 1)
                        if len(parts) == 2:
                            name = _clean_name(parts[0].strip())
                            title = parts[1].strip()
                            if (name and title and _looks_like_name(name)
                                    and name.lower() not in seen_names
                                    and len(title) > 3):
                                seen_names.add(name.lower())
                                bucket = _classify_role(title)
                                new_contacts.append(Contact(
                                    full_name=name,
                                    company=company_name,
                                    domain=state.target_domain or "",
                                    role_title=title,
                                    role_bucket=bucket,
                                    provenance=["perplexity_executive"],
                                ))
                        break

        except Exception as e:
            logger.warning("perplexity_executive_error",
                           company=company_name, error=str(e))

    if new_contacts:
        logger.info("perplexity_executive_done",
                    company=company_name, found=len(new_contacts))
        from backend.utils.progress import emit_log as _elog
        await _elog(state.thread_id,
                    f"Perplexity: found {len(new_contacts)} executives at {company_name}",
                    level="info")
        existing = list(state.discovered_contacts)
        return state.model_copy(update={"discovered_contacts": existing + new_contacts})

    return state


# ---------------------------------------------------------------------------
# Role bucket classification — deterministic, no LLM
# ---------------------------------------------------------------------------

# Ordered list: (bucket_id, display_label, keyword_substrings_to_match_in_title)
# First matching bucket wins — order matters (C-suite before general management).
# ALIGNED WITH BUYING-ROLE PROMPT: Only buckets that can produce FDM/KDM/P1/Influencer.
# REMOVED: marketing_brand, operations, finance, hr_people, product_category
# (all map to "Irrelevant" per buying-role classification rules).
_ROLE_BUCKETS_DEF: list[tuple[str, str, list[str]]] = [
    ("c_suite", "C-Suite & Executive", [
        "ceo", " coo", " cfo", " cdo", " cto", " cio", " cro",
        "chief executive", "chief operating", "chief financial",
        "chief digital", "chief technology", "chief information", "chief revenue",
        "consejero delegado", "directeur général",
        "geschäftsführer", "direttore generale", "director general",
        "président directeur", "vice chairman", "chairman",
    ]),
    ("digital_ecommerce", "Digital & Ecommerce", [
        "digital", "ecommerce", "e-commerce", "omnichannel", "marketplace",
        "direct to consumer", " d2c", "commercio digitale", "commerce digitale",
        "comercio electronico", "online retail", "digital transformation",
    ]),
    ("sales_commercial", "Sales & Commercial", [
        "sales", "revenue", "business development", "key account",
        " trade ", "channel sales", "retail sales", "ventas", "vendas",
        "distribution", "national accounts", "account director",
        "commercial excellence", "sales excellence", "sales operations",
        "customer development",
    ]),
    ("technology_data", "Technology & Data", [
        "technology", " data ", "analytics", "information systems",
        " it director", " it manager", "systems director",
        "engineering director", "digital transformation", "tech director",
        "bi director", "business intelligence",
    ]),
    ("strategy_innovation", "Strategy & Innovation", [
        "strategy", "innovation", "transformation", "strategic planning",
        "growth director", "corporate development", "estrategia",
    ]),
    ("general_management", "General Management", [
        "general manager", "country manager", "regional director",
        "country director", "managing director",
        "president", "vice president",
    ]),
    ("rtm_gtm", "RTM / GTM / Distribution", [
        "route to market", "go to market", "rtm", "gtm",
        "channel development", "field sales",
        "head of distribution", "head of retail",
    ]),
    ("other_senior", "Other Senior", []),  # catch-all for senior people
]

# All remaining buckets are DM-relevant — pre-select all by default
_DEFAULT_SELECTED_BUCKETS = {
    "c_suite", "digital_ecommerce", "sales_commercial",
    "general_management", "technology_data", "strategy_innovation",
    "rtm_gtm",
}


def _classify_into_bucket(role_title: str) -> str:
    """Classify a role title into the first matching bucket."""
    if not role_title:
        return "other_senior"
    t = f" {role_title.lower()} "
    for bucket_id, _label, keywords in _ROLE_BUCKETS_DEF:
        if bucket_id == "other_senior":
            continue
        if any(kw in t for kw in keywords):
            return bucket_id
    if _is_valid_candidate(role_title):
        return "other_senior"
    return "other_senior"


# ---------------------------------------------------------------------------
# Node: group_into_role_buckets — deterministic keyword grouping, no LLM
# ---------------------------------------------------------------------------

async def _rank_buckets_with_llm(company_name: str, role_buckets: list[dict]) -> list[dict]:
    """
    Use LLM to rank role buckets by SDR priority for the given company.
    Adds priority_rank (1=highest) and priority_reason (short phrase) to each bucket.
    Falls back gracefully — returns buckets unchanged if LLM fails.
    """
    try:
        from backend.tools.llm import llm_complete
        bucket_lines = "\n".join(
            f'- id="{b["id"]}" label="{b["label"]}" ({b["count"]} people)'
            f'{(" e.g. " + ", ".join(b["sample_roles"][:2])) if b["sample_roles"] else ""}'
            for b in role_buckets
        )
        prompt = (
            f'You are a B2B sales expert. For the company "{company_name}", rank these departments '
            f"by how important they are as SDR outreach targets (budget control, buying authority, deal relevance).\n\n"
            f"Departments:\n{bucket_lines}\n\n"
            f"Return ONLY a JSON array sorted by priority (most important first), no markdown:\n"
            f'[{{"id": "bucket_id", "priority_rank": 1, "priority_reason": "6-8 word reason"}}, ...]'
        )
        raw = await llm_complete(prompt, temperature=0.0)
        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        rankings: list[dict] = json.loads(raw)
        rank_map = {r["id"]: r for r in rankings}
        for b in role_buckets:
            r = rank_map.get(b["id"], {})
            b["priority_rank"] = r.get("priority_rank", 99)
            b["priority_reason"] = r.get("priority_reason", "")
        role_buckets.sort(key=lambda x: x.get("priority_rank", 99))
    except Exception as exc:
        logger.warning("rank_buckets_llm_failed", error=str(exc))
        # Assign sequential ranks so frontend always has the field
        for i, b in enumerate(role_buckets):
            b.setdefault("priority_rank", i + 1)
            b.setdefault("priority_reason", "")
    return role_buckets


async def group_into_role_buckets(state: SearcherState) -> SearcherState:
    """
    Group all discovered contacts by functional role bucket.
    Emits role_buckets to state — used by await_role_selection for SDR display.

    Each bucket:
      {id, label, count, sample_roles (up to 5 unique titles), pre_selected,
       priority_rank, priority_reason}

    Keyword grouping is deterministic; priority ranking uses LLM.
    """
    contacts = state.discovered_contacts
    if not contacts:
        return state

    # Classify each contact
    bucket_indices: dict[str, list[int]] = {b[0]: [] for b in _ROLE_BUCKETS_DEF}
    for i, c in enumerate(contacts):
        bid = _classify_into_bucket(c.role_title or "")
        bucket_indices[bid].append(i)

    # Build role_buckets list (skip empty buckets)
    role_buckets = []
    for bucket_id, label, _keywords in _ROLE_BUCKETS_DEF:
        indices = bucket_indices[bucket_id]
        if not indices:
            continue
        sample_roles = list(dict.fromkeys(
            contacts[i].role_title
            for i in indices
            if contacts[i].role_title
        ))[:5]
        role_buckets.append({
            "id": bucket_id,
            "label": label,
            "count": len(indices),
            "sample_roles": sample_roles,
            "people_indices": indices,
            "pre_selected": bucket_id in _DEFAULT_SELECTED_BUCKETS,
        })

    # LLM-rank buckets by SDR priority for this specific company
    role_buckets = await _rank_buckets_with_llm(state.target_company or "", role_buckets)

    logger.info("group_into_role_buckets_done",
                company=state.target_company,
                total_contacts=len(contacts),
                buckets={b["id"]: b["count"] for b in role_buckets})

    from backend.utils.progress import emit_log as _elog
    await _elog(
        state.thread_id,
        f"Grouped {len(contacts)} people into {len(role_buckets)} role categories — awaiting SDR role selection",
        level="info",
    )

    return state.model_copy(update={"role_buckets": role_buckets})


# ---------------------------------------------------------------------------
# Node: await_role_selection — SDR picks which functional departments matter
# ---------------------------------------------------------------------------

async def await_role_selection(state: SearcherState) -> SearcherState:
    """
    Pause for SDR to choose which role categories to include.
    Emits 'role_selection_required' event with all non-empty role buckets.

    After SDR picks buckets (or 120s timeout → auto-select defaults),
    filters discovered_contacts to only those in selected buckets.
    """
    if not state.role_buckets:
        return state
    if not state.thread_id:
        return state

    # Auto mode: skip SDR pause, select all role buckets automatically
    if state.auto_approve:
        from backend.utils.progress import emit_log as _emit_log
        all_indices: set[int] = set()
        for b in state.role_buckets:
            all_indices.update(b.get("people_indices", []))
        filtered = [state.discovered_contacts[i] for i in sorted(all_indices)
                    if i < len(state.discovered_contacts)]
        await _emit_log(state.thread_id,
                        f"Auto mode — selecting all {len(filtered)} contacts across all role buckets for {state.target_company}",
                        level="info")
        return state.model_copy(update={"discovered_contacts": filtered})


    from backend.utils import role_selection as _rs
    from backend.utils.progress import _queues as _pq, emit_log as _emit_log, emit as _emit_progress
    from datetime import datetime as _dt, timezone as _tz

    async def _emit_event() -> None:
        q = _pq.get(state.thread_id)
        if not q:
            return
        try:
            await q.put({
                "type": "role_selection_required",
                "data": {
                    "company": state.target_company,
                    "buckets": state.role_buckets,
                    "total_found": len(state.discovered_contacts),
                },
                "timestamp": _dt.now(_tz.utc).isoformat(),
            })
        except Exception:
            pass

    await _emit_progress(state.thread_id, state.target_company, "processing")

    selection_queue = _rs.register(state.thread_id)
    await _emit_event()

    try:
        try:
            msg = await asyncio.wait_for(selection_queue.get(), timeout=120)
            selected_ids: list[str] = msg.get("bucket_ids", [])
            logger.info("await_role_selection_received",
                        company=state.target_company, selected=selected_ids)
        except asyncio.TimeoutError:
            # Auto-select DM-relevant buckets on timeout
            selected_ids = [
                b["id"] for b in state.role_buckets
                if b["id"] in _DEFAULT_SELECTED_BUCKETS
            ]
            await _emit_log(
                state.thread_id,
                f"Timeout — auto-selecting {len(selected_ids)} role bucket(s) for {state.target_company}",
                level="info",
            )
    finally:
        _rs.unregister(state.thread_id)

    # If SDR selected nothing (edge case), fall back to defaults
    if not selected_ids:
        selected_ids = [b["id"] for b in state.role_buckets if b["id"] in _DEFAULT_SELECTED_BUCKETS]

    # Filter contacts to selected buckets
    selected_indices: set[int] = set()
    for b in state.role_buckets:
        if b["id"] in selected_ids:
            selected_indices.update(b["people_indices"])

    filtered = [state.discovered_contacts[i] for i in sorted(selected_indices)
                if i < len(state.discovered_contacts)]

    await _emit_log(
        state.thread_id,
        (
            f"Role selection confirmed — {len(filtered)} people from "
            f"{len(selected_ids)} department(s) for {state.target_company}"
        ),
        level="info",
    )

    logger.info("await_role_selection_done",
                company=state.target_company,
                selected_buckets=selected_ids,
                before=len(state.discovered_contacts),
                after=len(filtered))

    return state.model_copy(update={
        "discovered_contacts": filtered,
        "role_buckets": [],
    })


# ---------------------------------------------------------------------------
# Node: deduplicate — rapidfuzz, no LLM
# ---------------------------------------------------------------------------

async def deduplicate(state: SearcherState) -> SearcherState:
    """
    Deduplicate discovered contacts using rapidfuzz fuzzy matching.
    Merges records where name similarity >= 90% AND company similarity >= 85%.
    Also removes contacts already present in First Clean List for this company.
    """
    logger.info("searcher_deduplicate", company=state.target_company, raw_count=len(state.discovered_contacts))
    from backend.utils.progress import emit_log as _dedup_log
    await _dedup_log(state.thread_id,
        f"[{state.target_company}] Deduplicating {len(state.discovered_contacts)} raw contacts…",
        level="info")

    if not state.discovered_contacts:
        return state

    try:
        from rapidfuzz import fuzz

        # Collect names already in First Clean List + First Clean List for this company.
        # Match on normalized name (from Target Accounts) to avoid wrong substring hits.
        existing_names: list[str] = []
        _norm = (state.target_normalized_name or state.target_company).lower()
        _raw = state.target_company.lower()
        try:
            for tab in (sheets.FIRST_CLEAN_LIST,):
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

        # Filter out contacts already in First Clean List
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
        await _dedup_log(state.thread_id,
            f"[{state.target_company}] Dedup complete — {len(deduped)} unique contact(s) (removed {len(contacts) - len(deduped)} duplicate(s))",
            level="info")

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
    from backend.utils.progress import emit_log as _val_log
    await _val_log(state.thread_id,
        f"[{state.target_company}] Verifying {len(state.discovered_contacts)} contact(s) on LinkedIn…",
        level="info")

    from backend.tools.unipile import verify_profile as _verify_profile
    sem = asyncio.Semaphore(5)

    async def _validate_one(contact: Contact) -> Contact | None:
        async with sem:
            # Only Unipile-sourced contacts get a URL search — scrape-sourced contacts
            # without a URL are dropped immediately (searching is too slow and rarely works).
            from_unipile = "unipile_search" in (contact.provenance or [])
            if not contact.linkedin_url and not from_unipile:
                return None

            # Step 2: verify via Unipile
            if contact.linkedin_url and not contact.linkedin_verified:
                try:
                    verification = await _verify_profile(contact.linkedin_url, state.target_company)
                    if verification["valid"]:
                        contact = contact.model_copy(update={
                            "linkedin_verified": verification["at_target_company"] and verification["still_employed"],
                            "role_title": verification["current_role"] or contact.role_title,
                        })
                    else:
                        contact = contact.model_copy(update={"linkedin_verified": False})
                except Exception as e:
                    logger.warning("searcher_linkedin_validate_error", contact=contact.full_name, error=str(e))

        from_unipile = "unipile_search" in (contact.provenance or []) or \
                       "llm_unipile_verified" in (contact.provenance or [])
        from_website = "company_website" in (contact.provenance or []) or \
                       "company_website_llm" in (contact.provenance or [])

        if not contact.linkedin_verified:
            if from_unipile:
                logger.info("searcher_validate_drop_unverified",
                            name=contact.full_name, provenance=contact.provenance)
                return None
            if from_website and contact.linkedin_url:
                logger.info("searcher_validate_drop_unverified",
                            name=contact.full_name, provenance=contact.provenance,
                            reason="linkedin found but not at target company")
                return None
            if not contact.linkedin_url:
                logger.info("searcher_validate_drop_unverified",
                            name=contact.full_name, provenance=contact.provenance,
                            reason="no linkedin url found")
                return None

        return contact

    results = await asyncio.gather(*[_validate_one(c) for c in state.discovered_contacts])
    updated_contacts = [c for c in results if c is not None]

    await _val_log(state.thread_id,
        f"[{state.target_company}] LinkedIn verification done — {len(updated_contacts)} contact(s) confirmed",
        level="success" if updated_contacts else "info")
    return state.model_copy(update={"discovered_contacts": updated_contacts, "phase": "enrichment"})


# ---------------------------------------------------------------------------
# Email format detection via LLM Structured Outputs
# ---------------------------------------------------------------------------

async def _detect_email_format_llm(domain: str, company: str) -> str | None:
    """
    Use LLM with web_search to detect the email format for a domain.
    Tries OpenAI first, falls back to Claude Bedrock.
    Returns a full format string like '{first}.{last}@domain.com', or None if unknown.
    """
    from backend.tools.domain_discovery import _EMAIL_PATTERNS
    from backend.tools.llm import llm_web_search

    settings = get_settings()
    if not settings.openai_api_key and not settings.aws_bearer_token_bedrock:
        return None

    patterns_str = "\n".join(f"- {p}" for p in _EMAIL_PATTERNS)
    prompt = (
        f"Find the email format used by employees at {company} (domain: {domain}).\n"
        f"Search for real employee emails — LinkedIn, email finders, press releases, PDFs.\n"
        f"Pick EXACTLY ONE pattern from this list:\n{patterns_str}\n"
        f"Or return 'unknown' if you cannot determine it with confidence.\n"
        f"Reply with ONLY the pattern, nothing else."
    )

    try:
        answer = await llm_web_search(prompt)
        answer = answer.strip()
        # Try to extract a pattern from the response
        for pattern in _EMAIL_PATTERNS:
            if pattern in answer:
                logger.info("searcher_enrich_format_llm", domain=domain, pattern=pattern)
                return f"{pattern}@{domain}"
        if answer and answer != "unknown":
            logger.warning("searcher_enrich_format_llm_unknown", domain=domain, answer=answer)
    except Exception as e:
        logger.warning("searcher_enrich_format_llm_error", domain=domain, error=str(e))

    return None


def _learn_email_format_from_existing(emails: list[str], names: list[str], domain: str) -> str | None:
    """
    Reverse-engineer email format from existing n8n-provided emails for the same company.

    Given real emails like 'john.smith@acme.com' and names like 'John Smith',
    deduces the pattern (e.g. '{first}.{last}@acme.com') so we can construct
    emails for gap-fill contacts without calling ZeroBounce.
    """
    from backend.tools.domain_discovery import _EMAIL_PATTERNS

    if not emails or not names or not domain:
        return None

    # Build pairs of (email_local_part, first_name, last_name)
    pairs: list[tuple[str, str, str]] = []
    for email, name in zip(emails, names):
        if not email or "@" not in email:
            continue
        local = email.split("@")[0].lower()
        parts = name.strip().split()
        if len(parts) < 2:
            continue
        first = parts[0].lower()
        last = parts[-1].lower()
        pairs.append((local, first, last))

    if not pairs:
        return None

    # Try each pattern against known email/name pairs, score by match count
    best_pattern: str | None = None
    best_count = 0

    for pattern in _EMAIL_PATTERNS:
        match_count = 0
        for local, first, last in pairs:
            expected = (
                pattern
                .replace("{first}", first)
                .replace("{last}", last)
                .replace("{first_initial}", first[0])
                .replace("{last_initial}", last[0])
                .replace("{first_name}", first)
                .replace("{last_name}", last)
            )
            if expected == local:
                match_count += 1
        if match_count > best_count:
            best_count = match_count
            best_pattern = pattern

    # Need at least 1 match to be confident
    if best_pattern and best_count >= 1:
        fmt = f"{best_pattern}@{domain}"
        logger.info("searcher_learned_email_format",
                    pattern=best_pattern, matches=best_count, total_pairs=len(pairs), domain=domain)
        return fmt

    return None


# ---------------------------------------------------------------------------
# Node: enrich_contacts
# ---------------------------------------------------------------------------

async def enrich_contacts(state: SearcherState) -> SearcherState:
    """
    Role classification (keyword table), email construction.

    Email is constructed from known format — no ZeroBounce validation.
    The email format is already known from:
    1. Fini's email_format (stored in Target Accounts)
    2. Existing n8n-provided emails in First Clean List (reverse-engineered)
    3. Fallback: {first}.{last}@domain (most common B2B pattern)
    """
    logger.info("searcher_enrich",
                company=state.target_company,
                contacts=len(state.discovered_contacts),
                email_format=state.target_email_format or "(empty)",
                domain=state.target_domain or "(empty)")
    from backend.utils.progress import emit_log as _enrich_log

    domain = state.target_domain
    fini_format = state.target_email_format or ""  # e.g. "{first}.{last}@domain.com"

    # If Fini didn't provide a format, learn it from existing n8n emails in FFL
    learned_format: str | None = None
    if domain and not fini_format:
        try:
            ffl_records = await sheets.read_all_records(sheets.FIRST_CLEAN_LIST)
            _match_lower = state.target_company.lower()
            _norm_lower = (state.target_normalized_name or "").lower()
            existing_emails: list[str] = []
            existing_names: list[str] = []
            for row in ffl_records:
                co = str(row.get("Company Name", "") or "").strip().lower()
                if _match_lower not in co and _norm_lower not in co:
                    continue
                email = str(row.get("Email", "") or "").strip()
                first = str(row.get("First Name", "") or "").strip()
                last = str(row.get("Last Name", "") or "").strip()
                if email and "@" in email and first and last:
                    # Only use emails from the same domain
                    email_domain = email.split("@")[1].lower()
                    if email_domain == domain.lower():
                        existing_emails.append(email)
                        existing_names.append(f"{first} {last}")
            if existing_emails:
                learned_format = _learn_email_format_from_existing(existing_emails, existing_names, domain)
                if learned_format:
                    await _enrich_log(state.thread_id,
                        f"[{state.target_company}] Learned email format from {len(existing_emails)} existing contact(s): {learned_format}",
                        level="info")
                else:
                    logger.info("searcher_enrich_no_learned_format", domain=domain,
                                emails=len(existing_emails),
                                msg="Could not deduce format from existing emails")
        except Exception as e:
            logger.warning("searcher_learn_format_error", error=str(e))

    # Priority: fini_format > learned_format > {first}.{last}@domain (most common B2B default)
    email_format = fini_format or learned_format or (f"{{first}}.{{last}}@{domain}" if domain else "")
    format_source = "fini" if fini_format else ("learned_from_ffl" if learned_format else "default_first.last")
    logger.info("searcher_email_format_resolved",
                format=email_format or "(none)", source=format_source, domain=domain or "(none)")
    await _enrich_log(state.thread_id,
        f"[{state.target_company}] Enriching {len(state.discovered_contacts)} contact(s) — constructing emails from {format_source}",
        level="info")

    # ── Sheet-level dedup: read existing emails + LinkedIn URLs to avoid duplicates ──
    _existing_emails: set[str] = set()
    _existing_linkedin: set[str] = set()
    try:
        _fcl_records = await sheets.read_all_records(sheets.FIRST_CLEAN_LIST)
        for _row in _fcl_records:
            _e = str(_row.get("Email", "") or "").strip().lower()
            if _e and "@" in _e:
                _existing_emails.add(_e)
            _li = str(_row.get("LinkedIn URL", _row.get("LinkedIn Url", _row.get("Linekdin Url", ""))) or "").strip().lower().rstrip("/")
            if _li and "linkedin.com" in _li:
                _existing_linkedin.add(_li)
        logger.info("searcher_sheet_dedup_loaded",
                    emails=len(_existing_emails), linkedin=len(_existing_linkedin))
    except Exception as e:
        logger.warning("searcher_sheet_dedup_load_error", error=str(e))

    # Shared mutable state — protected by write_lock
    write_lock = asyncio.Lock()
    enriched: list[Contact] = []
    fcl_start_box: list[int | None] = [None]
    fcl_end_box:   list[int | None] = [None]

    bucket_label_map = {
        # New LLM buying-role tags
        "FDM": "FDM", "KDM": "KDM",
        "P1 Influencer": "P1 Influencer", "Influencer": "Influencer",
        "Irrelevant": "Irrelevant",
        # Legacy keyword-classifier tags (backwards compat)
        "CEO/MD": "FDM", "CTO/CIO": "FDM",
        "CSO/Head of Sales": "KDM",
        "Gatekeeper": "Irrelevant",
        "DM": "FDM", "Champion": "KDM",
        "GateKeeper": "Irrelevant", "Unknown": "",
    }

    enrich_sem = asyncio.Semaphore(8)

    async def _enrich_and_write(contact: Contact) -> None:
        """Enrich one contact — construct email from known format, no ZeroBounce."""
        async with enrich_sem:
            # Preserve LLM buying-role classification if already set by score_and_rank.
            # Only fall back to keyword classifier for contacts that bypassed LLM.
            _LLM_TAGS = {"FDM", "KDM", "P1 Influencer", "Influencer", "Irrelevant"}
            bucket = contact.role_bucket
            if bucket not in _LLM_TAGS:
                bucket = _classify_role(contact.role_title or "")
                contact = contact.model_copy(update={"role_bucket": bucket})

            # Drop contacts classified as irrelevant / gatekeeper / unknown
            if bucket in ("Gatekeeper", "Unknown", "Irrelevant"):
                logger.info("searcher_enrich_skip", contact=contact.full_name,
                            bucket=bucket, reason="not a decision-maker or influencer role")
                return

            # ── Sheet-level dedup: skip if this person already exists in the sheet ──
            _li_check = (contact.linkedin_url or "").strip().lower().rstrip("/")
            if _li_check and "linkedin.com" in _li_check and _li_check in _existing_linkedin:
                logger.info("searcher_sheet_dedup_skip", contact=contact.full_name,
                            reason="LinkedIn URL already in sheet", linkedin=_li_check)
                return
            # Also check by email (constructed later, but check existing email if set)
            if contact.email and contact.email.lower() in _existing_emails:
                logger.info("searcher_sheet_dedup_skip", contact=contact.full_name,
                            reason="email already in sheet", email=contact.email)
                return

            await _enrich_log(state.thread_id,
                f"[{contact.company}] Enriching {contact.full_name} ({contact.role_title or 'unknown role'}) — constructing email…",
                level="info")

            # Construct email from known format (no ZeroBounce validation)
            if not contact.email and domain and email_format:
                constructed = construct_email(contact.full_name, email_format, domain)
                if constructed:
                    # Dedup check on constructed email too
                    if constructed.lower() in _existing_emails:
                        logger.info("searcher_sheet_dedup_skip", contact=contact.full_name,
                                    reason="constructed email already in sheet", email=constructed)
                        return
                    contact = contact.model_copy(update={
                        "email": constructed,
                        "email_status": "constructed",
                    })
                    logger.info("searcher_email_constructed",
                                contact=contact.full_name, email=constructed,
                                format_source=format_source)

            if not contact.domain:
                contact = contact.model_copy(update={"domain": domain})

        # ── Write to sheet immediately, one at a time ──────────────────────
        async with write_lock:
            try:
                await sheets.ensure_headers(sheets.SEARCHER_OUTPUT, sheets.SEARCHER_OUTPUT_HEADERS)
                so_row = [
                    contact.company,
                    contact.full_name,
                    contact.role_title or "",
                    contact.role_bucket,
                    contact.linkedin_url or "",
                    "Verified" if contact.linkedin_verified else "Unverified",
                    contact.email or "",
                    contact.email_status if contact.email else "",
                ]
                await sheets.append_row(sheets.SEARCHER_OUTPUT, so_row)

                await sheets.ensure_headers(sheets.FIRST_CLEAN_LIST, sheets.FIRST_CLEAN_LIST_HEADERS)
                name_parts = contact.full_name.strip().split(None, 1)
                first_name = name_parts[0] if name_parts else ""
                last_name  = name_parts[1] if len(name_parts) > 1 else ""
                fcl_row = [
                    contact.company,                                    # A
                    state.target_normalized_name or contact.company,    # B
                    contact.domain or state.target_domain or "",        # C
                    state.target_region or "",                          # D
                    state.target_account_size or "",                    # E
                    state.target_region or "",                          # F
                    first_name, last_name,                              # G, H
                    contact.role_title or "",                           # I
                    bucket_label_map.get(contact.role_bucket, ""),      # J
                    contact.linkedin_url or "",                         # K
                    contact.email or "",                                # L
                    "", "",                                             # M, N (Phone-1, Phone-2)
                    "searcher",                                         # O (Source)
                    "",                                                 # P (Pipeline Status — empty, Veri will fill)
                ]
                written_fcl_row = await sheets.append_row(sheets.FIRST_CLEAN_LIST, fcl_row)
                data_row = written_fcl_row - 1
                if fcl_start_box[0] is None:
                    fcl_start_box[0] = data_row
                fcl_end_box[0] = data_row

                enriched.append(contact)
                # Add to dedup sets so subsequent contacts in this batch are also caught
                if contact.email:
                    _existing_emails.add(contact.email.lower())
                _li = (contact.linkedin_url or "").strip().lower().rstrip("/")
                if _li and "linkedin.com" in _li:
                    _existing_linkedin.add(_li)
                logger.info("searcher_contact_written",
                            contact=contact.full_name, company=contact.company,
                            email=contact.email or "(none)")
                email_info = f" · {contact.email} ({contact.email_status})" if contact.email else " · no email"
                await _enrich_log(state.thread_id,
                    f"[{contact.company}] ✓ {contact.full_name} — {contact.role_title or contact.role_bucket}{email_info} → written to sheet",
                    level="success")
                from backend.utils.progress import emit_contact as _emit_contact
                await _emit_contact(
                    state.thread_id,
                    full_name=contact.full_name,
                    role_title=contact.role_title or "",
                    role_bucket=contact.role_bucket or "",
                    company=contact.company,
                    email=contact.email or "",
                    linkedin_verified=contact.linkedin_verified,
                )
            except Exception as e:
                logger.warning("searcher_write_error",
                               contact=contact.full_name, error=str(e))

    await asyncio.gather(*[_enrich_and_write(c) for c in state.discovered_contacts])

    new_total = state.total_contacts_written + len(enriched)
    new_fcl_start = state.fcl_row_start if state.fcl_row_start is not None else fcl_start_box[0]
    new_fcl_end = fcl_end_box[0] if fcl_end_box[0] is not None else state.fcl_row_end
    logger.info("searcher_enrich_done", total=len(state.discovered_contacts), kept=len(enriched),
                fcl_start=new_fcl_start, fcl_end=new_fcl_end)
    return state.model_copy(update={
        "discovered_contacts": enriched,
        "phase": "write_output",
        "total_contacts_written": new_total,
        "fcl_row_start": new_fcl_start,
        "fcl_row_end": new_fcl_end,
    })


# ---------------------------------------------------------------------------
# Node: write_to_sheet
# ---------------------------------------------------------------------------

async def write_contacts_to_sheet(state: SearcherState) -> SearcherState:
    """
    Contacts are now written immediately during enrich_contacts.
    This node just finalizes the phase.
    """
    from backend.utils.progress import emit as _emit_progress
    await _emit_progress(state.thread_id, state.target_company, "done")
    logger.info("searcher_write_sheet_pass", company=state.target_company,
                contacts_written=state.total_contacts_written)
    # All writing already happened in enrich_contacts — just finalize
    return state.model_copy(update={"phase": "done"})


# ---------------------------------------------------------------------------
# Node: advance_or_finish
# ---------------------------------------------------------------------------

async def advance_or_finish(state: SearcherState) -> SearcherState:
    """Advance to next company or mark as completed."""
    # Pause gate — blocks here between companies if user pressed pause
    if state.thread_id:
        from backend.utils.pause import await_if_paused
        from backend.utils.progress import emit_log as _emit_log
        from backend.utils.pause import is_paused
        if is_paused(state.thread_id):
            await _emit_log(state.thread_id, "Paused between companies — press Resume to continue", level="info")
        await await_if_paused(state.thread_id)

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
            "target_domain": next_company.get("domain", ""),
            "target_org_id": "",
            "target_email_format": "",
            "target_region": "",
            "target_normalized_name": "",
            "target_sales_nav_url": "",
            "missing_dm_roles": [],
            "expanded_dm_roles": [],
            "role_buckets": [],
            "discovered_contacts": [],
            "pending_dm_candidates": [],
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


async def _get_email_format(company_name: str, _domain: str) -> str | None:
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
    return "expand_search_terms"


def route_after_write(_state: SearcherState) -> str:
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
    graph.add_node("expand_search_terms", expand_search_terms)
    graph.add_node("unipile_search", unipile_search)
    graph.add_node("scrape_sales_nav", scrape_sales_nav)
    graph.add_node("search_company_website", search_company_website)
    graph.add_node("perplexity_executive_search", perplexity_executive_search)
    graph.add_node("deduplicate", deduplicate)
    graph.add_node("group_into_role_buckets", group_into_role_buckets)
    graph.add_node("await_role_selection", await_role_selection)
    graph.add_node("score_and_rank", score_and_rank)
    graph.add_node("await_full_selection", await_full_selection)
    graph.add_node("validate_linkedin", validate_linkedin)
    graph.add_node("enrich_contacts", enrich_contacts)
    graph.add_node("write_to_sheet", write_contacts_to_sheet)
    graph.add_node("advance_or_finish", advance_or_finish)

    graph.set_entry_point("load_gap_analysis")

    graph.add_conditional_edges("load_gap_analysis", route_after_gap_analysis)
    graph.add_edge("expand_search_terms", "unipile_search")
    graph.add_edge("unipile_search", "scrape_sales_nav")
    graph.add_edge("scrape_sales_nav", "search_company_website")
    graph.add_edge("search_company_website", "perplexity_executive_search")
    graph.add_edge("perplexity_executive_search", "deduplicate")
    graph.add_edge("deduplicate", "group_into_role_buckets")
    graph.add_edge("group_into_role_buckets", "await_role_selection")
    graph.add_edge("await_role_selection", "score_and_rank")
    graph.add_edge("score_and_rank", "await_full_selection")
    graph.add_edge("await_full_selection", "validate_linkedin")
    graph.add_edge("validate_linkedin", "enrich_contacts")
    graph.add_edge("enrich_contacts", "write_to_sheet")
    graph.add_conditional_edges("write_to_sheet", route_after_write)
    graph.add_conditional_edges("advance_or_finish", route_advance)

    return graph.compile()
