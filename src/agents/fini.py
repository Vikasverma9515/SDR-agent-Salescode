"""
Fini - Target Builder Agent

Graph:
START -> parallel_enrich_all (normalize + org-lookup + domain for ALL companies concurrently)
      -> confirm_with_operator [INTERRUPT] -> write_to_sheet
      -> submit_n8n (conditional) -> advance_or_finish -> (loop back to confirm or END)
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
    "mena": "103537801",
    "latam": "104514572",
    "southeast asia": "102393603",
    # Europe
    "portugal": "100364860",
    "poland": "105072130",
    "austria": "103883259",
    "denmark": "104514075",
    "norway": "103819153",
    "finland": "100456013",
    "ireland": "104738515",
    "czech republic": "104508036",
    "czechia": "104508036",
    "romania": "106670623",
    "hungary": "100288700",
    "uk": "101165590",
    "united kingdom": "101165590",
    "scotland": "101165590",
    "wales": "101165590",
    "croatia": "104688944",
    "serbia": "101855366",
    "bulgaria": "105333783",
    "slovakia": "103061721",
    "slovenia": "106137034",
    "lithuania": "101464403",
    "iceland": "105238872",
    "luxembourg": "104042105",
    "malta": "100994331",
    "cyprus": "104084526",
    "bosnia": "100501484",
    "north macedonia": "106442863",
    "albania": "102845717",
    "montenegro": "100733275",
    "moldova": "106178099",
    "ukraine": "102264497",
    "belarus": "101705918",
    "georgia": "100490383",
    "armenia": "100355325",
    "azerbaijan": "100188092",
    # Americas
    "peru": "102927786",
    "venezuela": "101490751",
    "ecuador": "106373484",
    "uruguay": "100867946",
    "paraguay": "104065273",
    "bolivia": "104379274",
    "costa rica": "100422673",
    "panama": "100808673",
    "dominican republic": "105765876",
    "guatemala": "100877388",
    "honduras": "101937718",
    "el salvador": "103117009",
    "nicaragua": "100947750",
    "cuba": "106482999",
    "puerto rico": "105245958",
    "jamaica": "100655965",
    "trinidad": "104766925",
    # Asia Pacific
    "pakistan": "101022442",
    "bangladesh": "106471528",
    "sri lanka": "100446943",
    "new zealand": "105490917",
    "taiwan": "104187078",
    "hong kong": "103291313",
    "mongolia": "105723438",
    "laos": "104498776",
    "brunei": "100606914",
    "maldives": "100336308",
    "fiji": "102924498",
    "papua new guinea": "105571498",
    "uzbekistan": "106366767",
    "kazakhstan": "101735227",
    "afghanistan": "101620260",
    # Africa
    "south africa": "104035573",
    "kenya": "100505942",
    "ghana": "105765362",
    "tanzania": "103571671",
    "ethiopia": "100267870",
    "uganda": "106436890",
    "rwanda": "106906318",
    "senegal": "106057037",
    "ivory coast": "100648897",
    "cameroon": "103481974",
    "algeria": "104024592",
    "tunisia": "102134353",
    "morocco": "102787409",
    "libya": "104221888",
    "angola": "105266956",
    "mozambique": "106022431",
    "zambia": "107563082",
    "zimbabwe": "101543816",
    "botswana": "106883637",
    "namibia": "105765365",
    "mauritius": "103617495",
    # Middle East
    "bahrain": "101883610",
    "iraq": "103883898",
    "iran": "103356411",
    "lebanon": "101834488",
    "yemen": "103540826",
    "palestine": "106512065",
    "syria": "103759581",
}

# Maps regional hints found in company names → region key for REGION_IDS
_REGION_HINTS: dict[str, str] = {
    # Spanish / Iberian
    "españa": "spain", "espana": "spain", "iberia": "spain", "ibérica": "spain",
    "iberica": "spain", "peninsular": "spain",
    # Portuguese
    "portugal": "portugal", "brasil": "brazil", "brazil": "brazil",
    "brasileira": "brazil",
    # French
    "france": "france", "française": "france", "francaise": "france",
    "francophone": "france",
    # German / DACH
    "deutschland": "germany", "dach": "germany", "österreich": "austria",
    "osterreich": "austria", "austria": "austria", "schweiz": "switzerland",
    "suisse": "switzerland", "switzerland": "switzerland",
    # Italian
    "italia": "italy", "italy": "italy", "italiana": "italy",
    # UK / Ireland
    "uk": "united kingdom", "britain": "united kingdom",
    "england": "united kingdom", "scotland": "united kingdom",
    "ireland": "ireland", "éire": "ireland",
    # Benelux / Nordics
    "benelux": "netherlands", "nederland": "netherlands", "netherlands": "netherlands",
    "holland": "netherlands", "belgique": "belgium", "belgium": "belgium",
    "belgien": "belgium", "luxembourg": "luxembourg",
    "nordics": "europe", "scandinavia": "europe", "nordic": "europe",
    "denmark": "denmark", "danmark": "denmark",
    "norway": "norway", "norge": "norway",
    "sweden": "sweden", "sverige": "sweden",
    "finland": "finland", "suomi": "finland",
    "iceland": "iceland",
    # Eastern Europe
    "poland": "poland", "polska": "poland",
    "czech": "czech republic", "czechia": "czech republic",
    "romania": "romania", "hungary": "hungary", "magyarország": "hungary",
    "croatia": "croatia", "hrvatska": "croatia",
    "serbia": "serbia", "srbija": "serbia",
    "bulgaria": "bulgaria", "slovakia": "slovakia",
    "slovenia": "slovenia", "ukraine": "ukraine",
    "baltic": "europe", "baltics": "europe",
    "latvia": "latvia", "lithuania": "lithuania", "estonia": "estonia",
    "greece": "greece", "hellas": "greece",
    "turkey": "turkey", "türkiye": "turkey", "turkiye": "turkey",
    "cyprus": "cyprus",
    # Americas
    "usa": "usa", "united states": "usa", "americas": "north america",
    "latam": "latam", "latin america": "latam",
    "méxico": "mexico", "mexico": "mexico",
    "canada": "canada", "canadá": "canada",
    "argentina": "argentina", "chile": "chile", "colombia": "colombia",
    "peru": "peru", "perú": "peru",
    "venezuela": "venezuela", "ecuador": "ecuador",
    "uruguay": "uruguay", "paraguay": "paraguay", "bolivia": "bolivia",
    "costa rica": "costa rica", "panama": "panama", "panamá": "panama",
    "dominicana": "dominican republic", "guatemala": "guatemala",
    "caribbean": "north america", "caribe": "north america",
    "centroamérica": "north america", "centroamerica": "north america",
    "andina": "south america", "andean": "south america",
    "mesoamerica": "north america", "cono sur": "south america",
    # Asia Pacific
    "india": "india", "bharat": "india",
    "china": "china", "中国": "china", "zhongguo": "china",
    "japan": "japan", "nippon": "japan", "日本": "japan",
    "apac": "asia", "asia": "asia", "asia pacific": "asia",
    "asia-pacific": "asia",
    "singapore": "singapore", "malaysia": "malaysia",
    "indonesia": "indonesia", "thailand": "thailand",
    "philippines": "philippines", "pilipinas": "philippines",
    "vietnam": "vietnam", "viet nam": "vietnam",
    "korea": "south korea", "한국": "south korea",
    "taiwan": "taiwan", "hong kong": "hong kong",
    "pakistan": "pakistan", "bangladesh": "bangladesh",
    "sri lanka": "sri lanka", "nepal": "nepal",
    "myanmar": "myanmar", "cambodia": "cambodia",
    "australia": "australia", "new zealand": "new zealand",
    "oceania": "australia", "australasia": "australia",
    "asean": "asia",
    # Middle East
    "uae": "uae", "emirates": "uae", "dubai": "uae", "abu dhabi": "uae",
    "saudi": "saudi arabia", "saudi arabia": "saudi arabia",
    "qatar": "qatar", "kuwait": "kuwait", "bahrain": "bahrain",
    "oman": "oman", "jordan": "jordan", "lebanon": "lebanon",
    "iraq": "iraq", "iran": "iran",
    "middle east": "mena", "mea": "mena", "gcc": "uae",
    "gulf": "uae", "levant": "mena", "mashreq": "mena",
    "maghreb": "mena",
    # Africa
    "africa": "africa", "emea": "europe",
    "south africa": "south africa", "nigeria": "nigeria",
    "kenya": "kenya", "ghana": "ghana", "egypt": "egypt",
    "morocco": "morocco", "maroc": "morocco",
    "algeria": "algeria", "tunisia": "tunisia",
    "east africa": "africa", "west africa": "africa",
    "sub-saharan": "africa", "subsaharan": "africa",
    "francophone africa": "africa", "anglophone africa": "africa",
    # Generic (no filter)
    "international": "", "global": "", "worldwide": "",
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

        all_text = " ".join(r.snippet for r in results)

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
        text = " ".join(r.snippet for r in results).lower()

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
        title = r.title or ""
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
# Helper: GPT fallback for Sales Nav URL / org ID
# ---------------------------------------------------------------------------

async def _gpt_find_sales_nav(company_name: str, region: str = "") -> dict:
    """
    Ask GPT with web_search to find the LinkedIn company page or Sales Nav URL.
    Tries multiple strategies to ensure a URL is always returned.
    Returns {"org_id": str, "sales_nav_url": str} — either or both may be set.
    """
    from src.tools.llm import llm_web_search

    result = {}
    region_hint = f" (based in {region})" if region else ""

    def _extract_org_id(text: str) -> str | None:
        """Try all patterns to extract a numeric org ID from text."""
        m = re.search(r'ORG_ID:\s*(\d+)', text)
        if m:
            return m.group(1)
        m = re.search(r'organization%(?:25)?3A(\d+)', text)
        if m:
            return m.group(1)
        m = re.search(r'linkedin\.com/company/(\d+)', text)
        if m:
            return m.group(1)
        return None

    def _extract_slug(text: str) -> str | None:
        """Extract LinkedIn company slug from text."""
        m = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]+)', text, re.IGNORECASE)
        if m:
            slug = m.group(1).rstrip('/')
            if not slug.isdigit():
                return slug
        return None

    try:
        # --- Attempt 1: Ask for org ID directly ---
        prompt1 = (
            f"Find the LinkedIn company page for \"{company_name}\"{region_hint}. "
            f"I need the numeric LinkedIn organization ID. "
            f"Search LinkedIn and return ONLY in this format:\n"
            f"ORG_ID: <number>\n"
            f"URL: <linkedin company page url>\n"
            f"The org ID is the number in URLs like linkedin.com/company/12345 or "
            f"in Sales Navigator URLs after 'organization%3A'. "
            f"If you can only find the company page URL, return that. No explanation."
        )
        content1 = await llm_web_search(prompt1)
        logger.info("gpt_sales_nav_attempt1", company=company_name, content=content1[:300])

        org_id = _extract_org_id(content1)
        if org_id:
            result["org_id"] = org_id
            return result

        slug = _extract_slug(content1)
        if slug:
            logger.info("gpt_sales_nav_slug_found", company=company_name, slug=slug)
            try:
                org_info = await unipile.get_company_org_id(slug)
                if org_info["org_id"]:
                    result["org_id"] = org_info["org_id"]
                    return result
            except Exception:
                pass

        nav_match = re.search(
            r'(https://www\.linkedin\.com/sales/search/people\?[^\s"<>]+)', content1
        )
        if nav_match:
            result["sales_nav_url"] = nav_match.group(1)
            return result

        # --- Attempt 2: Ask for Sales Nav URL directly ---
        prompt2 = (
            f"Go to LinkedIn Sales Navigator and search for people at \"{company_name}\"{region_hint}. "
            f"Return the full Sales Navigator search URL. "
            f"The URL should look like: https://www.linkedin.com/sales/search/people?query=... "
            f"Return ONLY the URL. No explanation."
        )
        content2 = await llm_web_search(prompt2)
        logger.info("gpt_sales_nav_attempt2", company=company_name, content=content2[:300])

        org_id = _extract_org_id(content2)
        if org_id:
            result["org_id"] = org_id
            return result

        slug = _extract_slug(content2)
        if slug:
            try:
                org_info = await unipile.get_company_org_id(slug)
                if org_info["org_id"]:
                    result["org_id"] = org_info["org_id"]
                    return result
            except Exception:
                pass

        nav_match = re.search(
            r'(https://www\.linkedin\.com/sales/[^\s"<>]+)', content2
        )
        if nav_match:
            result["sales_nav_url"] = nav_match.group(1)
            return result

        # --- Last resort: keyword-based Sales Nav URL ---
        encoded_name = quote(company_name, safe="")
        result["sales_nav_url"] = (
            f"https://www.linkedin.com/sales/search/people?"
            f"query=(keywords%3A{encoded_name})"
        )
        logger.info("gpt_sales_nav_keyword_fallback", company=company_name)
        return result

    except Exception as e:
        logger.warning("gpt_sales_nav_error", company=company_name, error=str(e))

    # Even on total failure, return a keyword search URL
    try:
        encoded_name = quote(company_name, safe="")
        return {"sales_nav_url": (
            f"https://www.linkedin.com/sales/search/people?"
            f"query=(keywords%3A{encoded_name})"
        )}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Helper: detect region from company name
# ---------------------------------------------------------------------------

def _detect_region_from_name(raw_name: str) -> str:
    """
    Detect a geography/region from the company name.
    E.g. "Red Bull España" → "spain", "Heineken Iberia" → "spain"
    Returns the region key (for REGION_IDS) or "" if none detected.
    """
    # Check each word/phrase in the name against region hints
    name_lower = raw_name.lower()
    # Try multi-word hints first (e.g. "middle east", "south korea")
    for hint, region_key in sorted(_REGION_HINTS.items(), key=lambda x: -len(x[0])):
        if not region_key:
            continue  # skip "international", "global"
        if hint in name_lower:
            return region_key
    return ""


# ---------------------------------------------------------------------------
# Helper: extract parent company and clean name from raw input
# ---------------------------------------------------------------------------

# Regional suffixes to strip when looking for the global parent
_REGIONAL_SUFFIXES = re.compile(
    r'\s*[-/]\s*|\b(?:España|Espana|Iberia|Spain|France|Deutschland|Germany|Italy|Italia|'
    r'UK|Brasil|Brazil|Mexico|México|India|China|Japan|APAC|EMEA|LATAM|'
    r'Europe|Americas|Asia|Middle East|Africa|International|Global|'
    r'Professional|Foodservice)\b',
    re.IGNORECASE,
)

def _parse_company_variants(raw_name: str) -> dict:
    """
    Parse a company name to extract:
    - parent_company: from parenthetical hints like "(Mahou owned)", "(Heineken distributor)"
    - global_name: strip regional suffixes like "España", "Iberia"
    - short_names: progressively shorter forms

    Examples:
        "Voldis (Mahou owned distribution)" → parent="Mahou", global="Voldis"
        "Heineken España" → parent=None, global="Heineken"
        "Antonio Sainero (Mahou distributor)" → parent="Mahou", global="Antonio Sainero"
        "Nestlé España / Nestlé Professional" → parent=None, global="Nestlé"
        "Coca-Cola Europacific Partners (CCEP) Iberia" → parent="Coca-Cola Europacific Partners", global="CCEP"
    """
    result = {"parent_company": None, "global_name": None, "short_names": []}

    # --- Extract parent from parenthetical hints ---
    # Patterns: "(Mahou owned)", "(Heineken distributor)", "(Damm owned)", "(Mahou)"
    parent_patterns = [
        r'\((\w[\w\s]*?)\s+(?:owned|distributor|distribution)\s+\w+\)',  # (Mahou owned distribution)
        r'\((\w[\w\s]*?)\s+(?:owned|distributor|distribution|group)\)',  # (Mahou owned)
    ]
    for pat in parent_patterns:
        m = re.search(pat, raw_name, re.IGNORECASE)
        if m:
            result["parent_company"] = m.group(1).strip()
            break

    # Also check for simple parenthetical that looks like a parent: "(Mahou)"
    if not result["parent_company"]:
        m = re.search(r'\(([A-Z][\w\s]{2,20})\)', raw_name)
        if m:
            candidate = m.group(1).strip()
            # Only treat as parent if it's a known pattern (short, capitalized, no common words)
            skip_words = {"JV", "formerly", "now", "aka", "including", "and", "or"}
            if not any(w in candidate for w in skip_words) and len(candidate.split()) <= 3:
                result["parent_company"] = candidate

    # --- Strip parenthetical text for clean name ---
    clean = re.sub(r'\s*\([^)]*\)', '', raw_name).strip()

    # --- Handle "X / Y" format (take both parts) ---
    slash_parts = [p.strip() for p in clean.split('/') if p.strip()]

    # --- Strip regional suffixes to get global name ---
    for part in slash_parts:
        global_candidate = _REGIONAL_SUFFIXES.sub(' ', part).strip()
        global_candidate = re.sub(r'\s+', ' ', global_candidate).strip()
        # Strip trailing legal suffixes
        global_candidate = _strip_legal_suffix(global_candidate)
        if global_candidate and len(global_candidate) >= 3:
            result["global_name"] = global_candidate
            break

    # --- Build short name variants ---
    base = result["global_name"] or clean
    words = base.split()
    for length in range(len(words) - 1, 0, -1):
        variant = " ".join(words[:length])
        if len(variant) >= 3:
            result["short_names"].append(variant)

    return result


# ---------------------------------------------------------------------------
# Helper: enrich a single company (normalize + org lookup + domain)
# ---------------------------------------------------------------------------

async def _enrich_single_company(company: TargetCompany, region: str) -> TargetCompany:
    """Run all enrichment steps for one company. Called concurrently for all companies."""

    # --- Step 1: Normalize name ---
    try:
        results = await search_with_fallback(
            f'"{company.raw_name}" company official name',
            max_results=5,
        )
        all_text = " ".join(r.snippet for r in results)
        normalized = _extract_normalized_name(company.raw_name, all_text, results)
        account_size = await _fetch_account_size(company.raw_name)
        company = company.model_copy(update={
            "normalized_name": normalized,
            "account_type": region or company.account_type or "",
            "account_size": account_size,
        })
    except Exception as e:
        logger.warning("enrich_normalize_error", company=company.raw_name, error=str(e))
        company = company.model_copy(update={
            "normalized_name": company.raw_name,
            "account_type": region or company.account_type or "",
        })

    # --- Step 2: LinkedIn org lookup + Sales Nav URL ---
    # Phase 1: Build a list of ALL possible names this company could be under on LinkedIn
    # Phase 2: Try each name with Unipile until one hits
    lookup_name = company.normalized_name or company.raw_name
    org_id = None
    sales_nav_url = None

    # Auto-detect region from company name if not explicitly set
    effective_region = region
    if not effective_region:
        detected = _detect_region_from_name(company.raw_name)
        if detected:
            effective_region = detected
            logger.info("enrich_auto_region", company=company.raw_name, detected_region=detected)

    # ---- PHASE 1: Gather all candidate names ----
    candidate_names = []
    seen_lower = set()

    def _add_candidate(name: str):
        """Add a name to candidates if not already seen."""
        n = name.strip()
        if n and len(n) >= 2 and n.lower() not in seen_lower:
            seen_lower.add(n.lower())
            candidate_names.append(n)

    # 1a: The normalized/lookup name itself
    _add_candidate(lookup_name)

    # 1b: Regex-parsed variants (fast, no API calls)
    parsed = _parse_company_variants(company.raw_name)
    if parsed["global_name"]:
        _add_candidate(parsed["global_name"])       # "Heineken" from "Heineken España"
    if parsed["parent_company"]:
        _add_candidate(parsed["parent_company"])     # "Mahou" from "(Mahou distributor)"
    for short in parsed["short_names"]:
        _add_candidate(short)                        # "Heineken", "Coca-Cola" etc.
    if company.raw_name.strip() != lookup_name:
        _add_candidate(company.raw_name.strip())     # original raw input

    # 1c: Ask LLM for the definitive list of names to try
    try:
        from src.tools.llm import llm_complete
        llm_names = await llm_complete(
            f"Company: \"{company.raw_name}\"\n\n"
            f"I need to find this company on LinkedIn. List ALL possible names it could be "
            f"registered under on LinkedIn, one per line. Consider:\n"
            f"- The company's own name\n"
            f"- The parent/holding company (if subsidiary or distributor)\n"
            f"- The global brand name (if this is a regional branch like 'X España')\n"
            f"- Common abbreviations or trading names\n"
            f"- The LinkedIn slug (e.g. 'heineken' for Heineken)\n\n"
            f"Reply with ONLY the names, one per line. Most likely LinkedIn name first. "
            f"No numbering, no explanation.",
            max_tokens=150,
        )
        if llm_names:
            for line in llm_names.strip().splitlines():
                name = line.strip().lstrip('- •*0123456789.)')
                if name:
                    _add_candidate(name)
        logger.info("enrich_candidate_names", company=company.raw_name,
                    count=len(candidate_names), names=candidate_names[:8])
    except Exception as e:
        logger.warning("enrich_llm_names_error", company=company.raw_name, error=str(e))

    # 1d: Also ask LLM with web search to find the actual LinkedIn URL
    llm_slugs = []
    try:
        from src.tools.llm import llm_web_search
        llm_url_response = await llm_web_search(
            f"Find the LinkedIn company page for \"{company.raw_name}\".\n"
            f"If this is a subsidiary, regional branch, or distributor of a bigger company, "
            f"find the parent company's LinkedIn page instead.\n"
            f"Return ONLY the LinkedIn URL like: https://www.linkedin.com/company/SLUG\n"
            f"Nothing else."
        )
        if llm_url_response:
            for m in re.finditer(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]+)', llm_url_response, re.IGNORECASE):
                slug = m.group(1).rstrip('/')
                if slug not in llm_slugs:
                    llm_slugs.append(slug)
            logger.info("enrich_llm_slugs", company=company.raw_name, slugs=llm_slugs)
    except Exception as e:
        logger.warning("enrich_llm_url_error", company=company.raw_name, error=str(e))

    # ---- PHASE 2: Try each candidate with Unipile ----
    # Try LLM-found slugs first (most likely to be correct)
    for slug in llm_slugs:
        if slug.isdigit():
            org_id = slug
            logger.info("enrich_org_from_slug_numeric", company=company.raw_name, org_id=org_id)
            break
        try:
            org_info = await unipile.get_company_org_id(slug)
            if org_info["org_id"]:
                org_id = org_info["org_id"]
                logger.info("enrich_org_found", company=company.raw_name, slug=slug, org_id=org_id)
                break
        except Exception:
            pass

    # Then try each candidate name
    if not org_id:
        for name in candidate_names:
            try:
                org_info = await unipile.get_company_org_id(name)
                if org_info["org_id"]:
                    org_id = org_info["org_id"]
                    logger.info("enrich_org_found", company=company.raw_name, name=name, org_id=org_id)
                    break
            except Exception:
                pass

    # ---- PHASE 3: Search engines as last resort before keyword fallback ----
    if not org_id:
        for search_name in candidate_names[:3]:
            try:
                search_results = await search_with_fallback(
                    f'{search_name} LinkedIn company page',
                    max_results=5,
                )
                for r in search_results:
                    slug_match = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]+)', r.url, re.IGNORECASE)
                    if slug_match:
                        slug = slug_match.group(1).rstrip('/')
                        try:
                            org_info = await unipile.get_company_org_id(slug)
                            if org_info["org_id"]:
                                org_id = org_info["org_id"]
                                logger.info("enrich_org_from_search", company=company.raw_name,
                                            slug=slug, org_id=org_id)
                                break
                        except Exception:
                            pass
                if org_id:
                    break
            except Exception:
                pass

    # ---- Build Sales Nav URL ----
    if org_id:
        sales_nav_url = _build_sales_nav_url(
            org_id,
            company.normalized_name or company.raw_name,
            effective_region,
        )

    # ---- Fallback: keyword-based Sales Nav URL ----
    link_not_found = False
    if not sales_nav_url:
        link_not_found = True
        logger.warning("enrich_no_org_id_keyword_fallback", company=company.raw_name,
                       candidates_tried=len(candidate_names))
        encoded_name = quote(lookup_name, safe="")
        sales_nav_url = (
            f"https://www.linkedin.com/sales/search/people?"
            f"query=(keywords%3A{encoded_name})"
        )

    # Apply results
    update = {"sales_nav_url": sales_nav_url}
    if org_id:
        update["linkedin_org_id"] = org_id
    # Store detected region as account_type if not already set
    if effective_region and not company.account_type:
        update["account_type"] = effective_region.title()
    company = company.model_copy(update=update)
    logger.info("enrich_step2_done", company=company.raw_name,
                org_id=org_id or "(none)", region=effective_region or "(none)",
                link_not_found=link_not_found)

    # --- Step 3: Domain + email format ---
    try:
        name = company.normalized_name or company.raw_name
        domain_info = await discover_domain(name)
        company = company.model_copy(update={
            "domain": domain_info["domain"],
            "email_format": domain_info["email_format"],
        })
    except Exception as e:
        logger.warning("enrich_domain_error", company=company.raw_name, error=str(e))

    logger.info(
        "enrich_single_done",
        company=company.raw_name,
        normalized=company.normalized_name,
        org_id=company.linkedin_org_id or "(none)",
        domain=company.domain or "(none)",
    )
    return company


# ---------------------------------------------------------------------------
# Node: parallel_enrich_all
# ---------------------------------------------------------------------------

async def _enrich_and_write_if_ready(
    company: TargetCompany, region: str, submit_to_n8n: bool
) -> TargetCompany:
    """
    Enrich a single company. If a real Sales Nav link is found (org_id),
    write directly to the sheet — no waiting for other companies.
    Returns the updated company with sheet_row_written=True if auto-written.
    """
    company = await _enrich_single_company(company, region)

    # Auto-write to sheet if we found a real link
    if company.linkedin_org_id:
        try:
            await sheets.ensure_headers(sheets.TARGET_ACCOUNTS, sheets.TARGET_ACCOUNTS_HEADERS)
            row = [
                company.normalized_name or company.raw_name,
                company.raw_name,
                company.sales_nav_url or "",
                company.domain or "",
                company.sdr_assigned or "",
                company.email_format or "",
                company.account_type or "",
                company.account_size or "",
            ]
            written_row = await sheets.append_row(sheets.TARGET_ACCOUNTS, row)
            logger.info("fini_auto_written", company=company.raw_name, row=written_row)

            if submit_to_n8n and all(v != "" for v in row):
                try:
                    from src.tools.n8n import submit_to_n8n as _submit, build_payload
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
                    await _submit(payload)
                except Exception as e:
                    logger.warning("fini_auto_n8n_error", company=company.raw_name, error=str(e))

            company = company.model_copy(update={
                "operator_confirmed": True,
                "sheet_row_written": True,
            })
        except Exception as e:
            logger.warning("fini_auto_write_error", company=company.raw_name, error=str(e))

    return company


async def parallel_enrich_all(state: FiniState) -> FiniState:
    """
    Enrich ALL companies concurrently. Companies with real links are written
    to the sheet immediately as they finish. Companies without links are
    queued for operator review.
    Skips on re-entry after confirmation (enrichment_done flag).
    """
    if state.enrichment_done:
        return state  # already enriched on a previous invocation

    logger.info("fini_parallel_enrich_start", count=len(state.companies))

    tasks = [
        _enrich_and_write_if_ready(company, state.region, state.submit_to_n8n)
        for company in state.companies
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    companies = list(state.companies)
    errors = []
    auto_written = 0
    needs_review = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            errors.append(f"Enrichment failed for {companies[i].raw_name}: {result}")
            logger.error("enrich_company_failed", company=companies[i].raw_name, error=str(result))
        else:
            companies[i] = result
            if result.sheet_row_written:
                auto_written += 1
            else:
                needs_review += 1

    logger.info("fini_parallel_enrich_done", count=len(companies),
                auto_written=auto_written, needs_review=needs_review, errors=len(errors))
    return state.model_copy(update={
        "companies": companies,
        "enrichment_done": True,
        "current_index": 0,
        "errors": errors,
    })


# ---------------------------------------------------------------------------
# Node: discover_domain (kept for backwards compat, but unused in new graph)
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
    Pause point for operator review.
    - If company has a real Sales Nav link (org_id found) → auto-confirm, write directly
    - If company has a keyword fallback URL (no org_id) → pause for operator to paste link
    When operator_confirmed is already True (after resume), pass through.
    """
    company = state.companies[state.current_index]
    if company.operator_confirmed:
        return state.model_copy(update={"status": "running"})

    # Auto-confirm if we have a real link (org_id was found)
    has_real_link = bool(company.linkedin_org_id)
    if has_real_link:
        logger.info("fini_auto_confirmed", company=company.normalized_name,
                    org_id=company.linkedin_org_id)
        companies = list(state.companies)
        companies[state.current_index] = company.model_copy(update={"operator_confirmed": True})
        return state.model_copy(update={"companies": companies, "status": "running"})

    # No real link — need operator to review / paste correct URL
    logger.info(
        "fini_awaiting_confirmation",
        company=company.normalized_name,
        domain=company.domain,
        reason="no org_id found, needs manual Sales Nav URL",
    )
    return state.model_copy(update={"status": "awaiting_confirmation"})


# ---------------------------------------------------------------------------
# Node: write_to_sheet
# ---------------------------------------------------------------------------

async def write_to_sheet(state: FiniState) -> FiniState:
    """Write confirmed company to Target Accounts sheet."""
    company = state.companies[state.current_index]
    if company.sheet_row_written:
        logger.info("fini_write_sheet_skip", company=company.raw_name, reason="already written")
        return state.model_copy(update={"status": "running"})
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
    """Advance to next company that needs attention, or mark as completed."""
    next_index = state.current_index + 1

    # Skip past companies that are already fully written
    while next_index < len(state.companies):
        c = state.companies[next_index]
        if c.operator_confirmed and c.sheet_row_written:
            next_index += 1  # already done, skip
        else:
            break

    if next_index >= len(state.companies):
        logger.info("fini_completed", total=len(state.companies), errors=len(state.errors))
        return state.model_copy(update={"status": "completed"})
    else:
        logger.info("fini_advance", next_index=next_index,
                    company=state.companies[next_index].raw_name)
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
    return "confirm_with_operator"


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

    graph.add_node("parallel_enrich_all", parallel_enrich_all)
    graph.add_node("confirm_with_operator", confirm_with_operator)
    graph.add_node("write_to_sheet", write_to_sheet)
    graph.add_node("submit_n8n", submit_n8n)
    graph.add_node("advance_or_finish", advance_or_finish)

    graph.set_entry_point("parallel_enrich_all")

    graph.add_edge("parallel_enrich_all", "confirm_with_operator")
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
