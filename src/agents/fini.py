"""
Fini - Target Builder Agent

Graph:
START -> scrape_linkedin_org -> normalize_company -> discover_domain
      -> confirm_with_operator [INTERRUPT] -> write_to_sheet
      -> submit_n8n (conditional) -> advance_or_finish -> (loop or END)
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from urllib.parse import quote

from langgraph.graph import StateGraph, END

from src.config import get_settings
from src.state import FiniState, TargetCompany
from src.tools import n8n as n8n_tool, sheets
from src.tools import unipile
from src.tools.domain_discovery import discover_domain
from src.tools.search import search_with_fallback
from src.utils.logging import get_logger

logger = get_logger("fini")


# Region name → LinkedIn geo ID
REGION_IDS: dict[str, str] = {
    # Continents
    "africa": "103537801",
    "asia": "102393603",
    "europe": "100506914",
    "north america": "102221843",
    "south america": "104514572",
    # Countries
    "nigeria": "105365761",
    "france": "105015875",
    "belgium": "100565514",
    "spain": "105646813",
    "england": "102299470",
    "germany": "101282230",
    "italy": "103350119",
    "united states": "103644278",
    "usa": "103644278",
    "canada": "101174742",
    "australia": "101452733",
    "india": "102713980",
    "china": "102890883",
    "japan": "101355337",
    "brazil": "106057199",
    "mexico": "103323778",
    "netherlands": "102890719",
    "singapore": "102454443",
    "switzerland": "106693272",
    "sweden": "105117694",
    "south korea": "105149562",
    "russia": "101728296",
    "united arab emirates": "104305776",
    "uae": "104305776",
    "indonesia": "102478259",
    "thailand": "105146118",
    "argentina": "100446943",
    "chile": "104621616",
    "colombia": "100876405",
    "israel": "101620260",
    "saudi arabia": "100459316",
    "oman": "103619019",
    "jordan": "103710677",
    "egypt": "106155005",
    "kuwait": "103239229",
    "qatar": "104170880",
    "malaysia": "106808692",
    "philippines": "103121230",
    "vietnam": "104195383",
    "myanmar": "104136533",
    "nepal": "104630404",
    "cambodia": "102500897",
    "greece": "104677530",
    "turkey": "102105699",
    "latvia": "104341318",
    "estonia": "102974008",
    "mena": "103537801",  # fallback to Africa region for MENA — add proper MENA if available
    "latam": "104514572",  # South America
    "southeast asia": "102393603",  # Asia
}


def _build_sales_nav_url(org_id: str, company_name: str, region: str = "") -> str:
    """
    Build a Sales Navigator people-search URL filtered to the given company.
    Always excludes Entry Level seniority.
    Optionally adds a REGION filter if region is provided and recognised.
    """
    from urllib.parse import quote as _quote

    # Double-encode the org URN (LinkedIn requires urn%253Ali%253Aorganization%253A)
    company_text = _quote(company_name, safe="")

    # Build filter list
    company_filter = (
        f"(type%3ACURRENT_COMPANY%2C"
        f"values%3AList((id%3Aurn%253Ali%253Aorganization%253A{org_id}%2C"
        f"text%3A{company_text}%2CselectionType%3AINCLUDED%2Cparent%3A(id%3A0))))"
    )

    region_filter = ""
    if region:
        region_key = region.strip().lower()
        region_id = REGION_IDS.get(region_key)
        if region_id:
            region_text = _quote(region.strip(), safe="")
            region_filter = (
                f"%2C(type%3AREGION%2C"
                f"values%3AList((id%3A{region_id}%2C"
                f"text%3A{region_text}%2CselectionType%3AINCLUDED)))"
            )

    seniority_filter = (
        "%2C(type%3ASENIORITY_LEVEL%2C"
        "values%3AList((id%3A110%2Ctext%3AEntry%2520Level%2CselectionType%3AEXCLUDED)))"
    )

    return (
        f"https://www.linkedin.com/sales/search/people?"
        f"query=(recentSearchParam%3A(doLogHistory%3Atrue)%2C"
        f"filters%3AList({company_filter}{region_filter}{seniority_filter}))"
    )


# ---------------------------------------------------------------------------
# Node: scrape_linkedin_org
# ---------------------------------------------------------------------------

async def scrape_linkedin_org(state: FiniState) -> FiniState:
    """Scrape LinkedIn for company org ID and Sales Nav URL."""
    company = state.companies[state.current_index]
    if company.operator_confirmed:
        return state  # already done before confirmation, skip
    logger.info("fini_scrape_linkedin", step="scrape_linkedin_org", company=company.raw_name)

    try:
        lookup_name = company.normalized_name or company.raw_name
        org_info = await unipile.get_company_org_id(lookup_name)
        if org_info["org_id"]:
            sales_nav_url = _build_sales_nav_url(org_info["org_id"], org_info["name"] or company.raw_name, state.region)
            updated = company.model_copy(update={
                "linkedin_org_id": org_info["org_id"],
                "sales_nav_url": sales_nav_url,
            })
            companies = list(state.companies)
            companies[state.current_index] = updated
            return state.model_copy(update={"companies": companies})
        elif org_info["error"]:
            logger.warning("fini_org_id_failed", company=company.raw_name, error=org_info["error"])

    except Exception as e:
        logger.warning("fini_scrape_linkedin_error", company=company.raw_name, error=str(e))
        errors = list(state.errors) + [f"LinkedIn org lookup failed for {company.raw_name}: {e}"]
        return state.model_copy(update={"errors": errors})

    return state


# ---------------------------------------------------------------------------
# Node: normalize_company  (script-based — no LLM)
# ---------------------------------------------------------------------------

async def normalize_company(state: FiniState) -> FiniState:
    """
    Normalize company name using search snippets only — no LLM.

    Strategy:
    1. Search for "{raw_name} company official name"
    2. Look for the name appearing consistently across multiple snippets
    3. Use the most frequently occurring clean form
    4. Fall back to raw_name if nothing better found
    """
    company = state.companies[state.current_index]
    if company.operator_confirmed:
        return state  # already done before confirmation, skip
    logger.info("fini_normalize", step="normalize_company", company=company.raw_name)

    try:
        results = await search_with_fallback(
            f'"{company.raw_name}" company official name',
            max_results=5,
        )

        all_text = " ".join(r["snippet"] for r in results)

        # Try to find the most commonly referenced clean name in snippets
        normalized = _extract_normalized_name(company.raw_name, all_text, results)

        account_size = await _fetch_account_size(company.raw_name)

        updated = company.model_copy(update={
            "normalized_name": normalized,
            "account_type": state.region or company.account_type or "",
            "account_size": account_size,
        })
        companies = list(state.companies)
        companies[state.current_index] = updated
        logger.info(
            "fini_normalized",
            raw=company.raw_name,
            normalized=normalized,
            account_type=state.region,
            account_size=account_size,
        )
        return state.model_copy(update={"companies": companies})

    except Exception as e:
        logger.warning("fini_normalize_error", company=company.raw_name, error=str(e))
        updated = company.model_copy(update={
            "normalized_name": company.raw_name,
            "account_type": state.region or company.account_type or "",
        })
        companies = list(state.companies)
        companies[state.current_index] = updated
        return state.model_copy(update={"companies": companies})


async def _fetch_account_size(company_name: str) -> str:
    """
    Ask Perplexity/DDG how large the company is and map to Small/Medium/Large.
    Looks for revenue or employee count signals in the snippet.
    Falls back to 'Medium' if nothing conclusive.
    """
    try:
        results = await asyncio.wait_for(
            search_with_fallback(
                f"{company_name} annual revenue employees company size",
                max_results=3,
            ),
            timeout=12,
        )
        text = " ".join(r["snippet"] for r in results).lower()

        # Revenue signals (USD/INR billions → Large)
        if re.search(r'\$\s*\d+\s*b(?:illion)?|\b\d+[,\d]*\s*crore|\bfortune\s*\d+|\bfootball\b|\blisted\b', text):
            # crude billion-revenue check
            billions = re.findall(r'\$\s*(\d+(?:\.\d+)?)\s*b', text)
            if billions and float(billions[0]) >= 1:
                return "Large"
            crores = re.findall(r'(\d+[,\d]*)\s*crore', text)
            if crores:
                val = float(crores[0].replace(",", ""))
                if val >= 5000:
                    return "Large"
                if val >= 500:
                    return "Medium"

        # Employee count signals
        emp = re.findall(r'(\d[\d,]*)\s*(?:employees|staff|people|workforce)', text)
        if emp:
            count = int(emp[0].replace(",", ""))
            if count >= 10000:
                return "Large"
            if count >= 500:
                return "Medium"
            return "Small"

        # Keyword signals
        if any(kw in text for kw in ["multinational", "global", "listed", "nse", "bse", "nyse", "nasdaq", "fortune"]):
            return "Large"
        if any(kw in text for kw in ["startup", "seed", "series a", "series b", "small business", "sme"]):
            return "Small"

    except Exception as e:
        logger.warning("account_size_fetch_error", company=company_name, error=str(e))

    return "Medium"


def _extract_normalized_name(raw_name: str, all_text: str, results: list) -> str:
    """
    Extract the cleanest/most-official company name from search results.

    Heuristics:
    - Strip common suffixes: Ltd, Limited, Inc, Corp, LLC, Pvt, Private
    - Find the form of the name that appears most frequently in snippets
    - Use page titles from search results as they tend to be official
    - Fall back to raw_name
    """
    # Collect candidate names from page titles (more official than snippets)
    title_candidates = []
    for r in results:
        title = r.get("title", "")
        # Look for patterns like "CompanyName - About" or "CompanyName | Official"
        # Extract the part before " - ", " | ", " : "
        for sep in [" - ", " | ", " : ", " – "]:
            if sep in title:
                candidate = title.split(sep)[0].strip()
                if _name_overlap(raw_name, candidate) > 0.5:
                    title_candidates.append(candidate)
                break

    if title_candidates:
        # Pick shortest candidate (least junk appended)
        best = min(title_candidates, key=len)
        # Strip legal suffixes
        best = _strip_legal_suffix(best)
        if best and len(best) >= 3:
            return best

    # Fall back to stripping legal suffix from raw name
    stripped = _strip_legal_suffix(raw_name)
    return stripped if stripped else raw_name


def _strip_legal_suffix(name: str) -> str:
    """Remove common legal entity suffixes."""
    suffixes = [
        r"\s+Ltd\.?$", r"\s+Limited$", r"\s+Inc\.?$", r"\s+Corp\.?$",
        r"\s+LLC$", r"\s+LLP$", r"\s+Pvt\.?$", r"\s+Private$",
        r"\s+PLC$", r"\s+S\.A\.?$", r"\s+GmbH$", r"\s+AG$",
        r"\s+\(India\)$", r"\s+India$",
    ]
    result = name.strip()
    for suffix in suffixes:
        result = re.sub(suffix, "", result, flags=re.IGNORECASE).strip()
    return result


def _name_overlap(a: str, b: str) -> float:
    """Simple word-overlap ratio between two strings."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / max(len(words_a), len(words_b))


# ---------------------------------------------------------------------------
# Node: discover_domain
# ---------------------------------------------------------------------------

async def fini_discover_domain(state: FiniState) -> FiniState:
    """Discover domain and email format."""
    company = state.companies[state.current_index]
    if company.operator_confirmed:
        return state  # already done before confirmation, skip
    name = company.normalized_name or company.raw_name
    logger.info("fini_discover_domain", step="discover_domain", company=name)

    try:
        domain_info = await discover_domain(name)
        updated = company.model_copy(update={
            "domain": domain_info["domain"],
            "email_format": domain_info["email_format"],
        })
        companies = list(state.companies)
        companies[state.current_index] = updated
        return state.model_copy(update={"companies": companies})

    except Exception as e:
        logger.warning("fini_domain_error", company=name, error=str(e))
        errors = list(state.errors) + [f"Domain discovery failed for {name}: {e}"]
        return state.model_copy(update={"errors": errors})


# ---------------------------------------------------------------------------
# Node: confirm_with_operator
# ---------------------------------------------------------------------------

async def confirm_with_operator(state: FiniState) -> FiniState:
    """
    Pause point for operator review. Sets status to awaiting_confirmation
    so the API layer can surface the confirmation UI and wait.
    When operator_confirmed is already True (after resume), pass through.
    """
    company = state.companies[state.current_index]
    if company.operator_confirmed:
        return state.model_copy(update={"status": "running"})
    logger.info(
        "fini_awaiting_confirmation",
        company=company.normalized_name,
        domain=company.domain,
    )
    return state.model_copy(update={"status": "awaiting_confirmation"})


# ---------------------------------------------------------------------------
# Node: write_to_sheet
# ---------------------------------------------------------------------------

async def write_to_sheet(state: FiniState) -> FiniState:
    """Write confirmed company to Target Accounts sheet."""
    company = state.companies[state.current_index]
    name = company.normalized_name or company.raw_name
    logger.info("fini_write_sheet", step="write_to_sheet", company=name)

    try:
        await sheets.ensure_headers(sheets.TARGET_ACCOUNTS, sheets.TARGET_ACCOUNTS_HEADERS)

        row = [
            company.normalized_name or company.raw_name,  # A: Company Name
            company.raw_name,                              # B: Parent Company Name
            company.sales_nav_url or "",                   # C: Sales Navigator Link
            company.domain or "",                          # D: Company Domain
            company.sdr_assigned or "",                    # E: SDR Name
            company.email_format or "",                    # F: Email Format
            company.account_type or "",                    # G: Account type (region)
            company.account_size or "",                    # H: Account Size (S/M/L)
        ]

        written_row = await sheets.append_row(sheets.TARGET_ACCOUNTS, row)

        # Submit directly to n8n webhook — App Script onEdit doesn't fire on API writes.
        if state.submit_to_n8n and all(v != "" for v in row):
            from src.tools.n8n import submit_to_n8n, build_payload
            payload = build_payload(
                company_name=company.normalized_name or company.raw_name,
                parent_company_name=company.raw_name,
                sales_nav_url=company.sales_nav_url or "",
                domain=company.domain or "",
                sdr_assigned=company.sdr_assigned or "",
                email_format=company.email_format or "",
                account_type=company.account_type or "",
                account_size=company.account_size or "",
                row=written_row,
            )
            success = await submit_to_n8n(payload)
            logger.info("fini_n8n_submitted", company=name, success=success, row=written_row)

        updated = company.model_copy(update={"sheet_row_written": True})
        companies = list(state.companies)
        companies[state.current_index] = updated
        return state.model_copy(update={"companies": companies, "status": "running"})

    except Exception as e:
        logger.error("fini_sheet_write_error", company=name, error=str(e))
        errors = list(state.errors) + [f"Sheet write failed for {name}: {e}"]
        return state.model_copy(update={"errors": errors})


# ---------------------------------------------------------------------------
# Node: submit_n8n
# ---------------------------------------------------------------------------

async def submit_n8n(state: FiniState) -> FiniState:
    """Submit to n8n webhook if enabled."""
    if not state.submit_to_n8n:
        return state

    company = state.companies[state.current_index]
    name = company.normalized_name or company.raw_name
    logger.info("fini_submit_n8n", step="submit_n8n", company=name)

    try:
        payload = n8n_tool.build_payload(
            company_name=company.raw_name,
            normalized_name=company.normalized_name or company.raw_name,
            domain=company.domain or "",
            email_format=company.email_format or "",
            linkedin_org_id=company.linkedin_org_id or "",
            sales_nav_url=company.sales_nav_url or "",
            sdr_assigned=company.sdr_assigned or "",
        )
        # Log account_type and account_size even though build_payload doesn't include them
        logger.info(
            "fini_n8n_payload_extra",
            company=name,
            account_type=company.account_type or "",
            account_size=company.account_size or "",
        )
        success = await n8n_tool.submit_to_n8n(payload)

        updated = company.model_copy(update={"n8n_submitted": success})
        companies = list(state.companies)
        companies[state.current_index] = updated
        return state.model_copy(update={"companies": companies})

    except Exception as e:
        logger.error("fini_n8n_error", company=name, error=str(e))
        errors = list(state.errors) + [f"n8n submission failed for {name}: {e}"]
        return state.model_copy(update={"errors": errors})


# ---------------------------------------------------------------------------
# Node: advance_or_finish
# ---------------------------------------------------------------------------

def advance_or_finish(state: FiniState) -> FiniState:
    """Advance to next company or mark as completed."""
    next_index = state.current_index + 1
    if next_index >= len(state.companies):
        logger.info("fini_completed", total=len(state.companies), errors=len(state.errors))
        return state.model_copy(update={"status": "completed"})
    else:
        logger.info("fini_advance", next_index=next_index)
        return state.model_copy(update={"current_index": next_index, "status": "running"})


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def after_confirm(state: FiniState) -> str:
    if state.status == "awaiting_confirmation":
        return END
    return "write_to_sheet"


def should_submit_n8n(state: FiniState) -> str:
    return "submit_n8n" if state.submit_to_n8n else "advance_or_finish"


def should_continue(state: FiniState) -> str:
    if state.status == "completed":
        return END
    if state.current_index < len(state.companies):
        company = state.companies[state.current_index]
        if company.operator_confirmed and not company.sheet_row_written:
            return "write_to_sheet"
    return "scrape_linkedin_org"


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


async def build_fini_graph():
    import aiosqlite
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    import os

    settings = get_settings()
    os.makedirs(os.path.dirname(settings.checkpoint_db), exist_ok=True)

    graph = StateGraph(FiniState)

    graph.add_node("scrape_linkedin_org", scrape_linkedin_org)
    graph.add_node("normalize_company", normalize_company)
    graph.add_node("discover_domain", fini_discover_domain)
    graph.add_node("confirm_with_operator", confirm_with_operator)
    graph.add_node("write_to_sheet", write_to_sheet)
    graph.add_node("submit_n8n", submit_n8n)
    graph.add_node("advance_or_finish", advance_or_finish)

    graph.set_entry_point("normalize_company")

    graph.add_edge("normalize_company", "scrape_linkedin_org")
    graph.add_edge("scrape_linkedin_org", "discover_domain")
    graph.add_edge("discover_domain", "confirm_with_operator")
    graph.add_conditional_edges("confirm_with_operator", after_confirm)
    graph.add_conditional_edges("write_to_sheet", should_submit_n8n)
    graph.add_edge("submit_n8n", "advance_or_finish")
    graph.add_conditional_edges("advance_or_finish", should_continue)

    raw_conn = await aiosqlite.connect(settings.checkpoint_db)
    conn = AioSqliteConnectionWrapper(raw_conn)
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    serde = JsonPlusSerializer(allowed_msgpack_modules=True)
    checkpointer = AsyncSqliteSaver(conn, serde=serde)

    return graph.compile(checkpointer=checkpointer)
