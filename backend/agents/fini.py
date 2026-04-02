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
from urllib.parse import quote

from langgraph.graph import StateGraph, END

from backend.config import get_settings
from backend.state import FiniState, TargetCompany
from backend.tools import unipile
from backend.tools.domain_discovery import discover_domain
from backend.tools.search import search_with_fallback
from backend.utils.logging import get_logger

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
    Determine company size (Small / Medium / Large) from search signals.

    Priority (stops at first conclusive signal):
    1. Revenue (billions → Large, millions → Medium/Small)
    2. Employee count — handles plain numbers, K-notation, LinkedIn ranges
    3. Exchange / Fortune / funding stage keywords
    Falls back to 'Medium' if nothing conclusive.
    """
    try:
        results = await asyncio.wait_for(
            search_with_fallback(
                f'"{company_name}" employees OR revenue OR headcount company size',
                max_results=5,
            ),
            timeout=12,
        )
        text = " ".join(r.snippet for r in results).lower()

        # --- Revenue signals ---
        # USD billions (e.g. "$2.5 billion", "$3b revenue")
        usd_billions = re.findall(r'\$\s*(\d+(?:\.\d+)?)\s*b(?:illion)?', text)
        if usd_billions and float(usd_billions[0]) >= 1:
            return "Large"
        # USD millions (e.g. "$450 million", "$800m")
        usd_millions = re.findall(r'\$\s*(\d+(?:\.\d+)?)\s*m(?:illion)?', text)
        if usd_millions:
            val = float(usd_millions[0])
            if val >= 500:
                return "Large"
            if val >= 50:
                return "Medium"
            return "Small"
        # INR crore (India)
        crores = re.findall(r'(\d[\d,]*)\s*crore', text)
        if crores:
            val = float(crores[0].replace(",", ""))
            if val >= 5000:
                return "Large"
            if val >= 500:
                return "Medium"
            return "Small"

        # --- Employee count signals ---
        # K-notation: "10k employees", "2.5k staff"
        emp_k = re.findall(r'(\d+(?:\.\d+)?)\s*k\+?\s*(?:employees|staff|people|workforce)', text)
        if emp_k:
            count = float(emp_k[0]) * 1000
            if count >= 10000:
                return "Large"
            if count >= 500:
                return "Medium"
            return "Small"
        # LinkedIn-style ranges: "10,001+ employees", "1,001-5,000 employees"
        linkedin_large = re.search(
            r'(?:10[,.]?001|5[,.]?001|1[,.]?001)\s*[-–]\s*[\d,]+\s*employees'
            r'|10[,.]?001\+\s*employees', text)
        if linkedin_large:
            return "Large"
        linkedin_medium = re.search(
            r'(?:501|201)\s*[-–]\s*[\d,]+\s*employees', text)
        if linkedin_medium:
            return "Medium"
        linkedin_small = re.search(
            r'(?:51|11|2|1)\s*[-–]\s*[\d,]+\s*employees'
            r'|self.?employed', text)
        if linkedin_small:
            return "Small"
        # Plain number: "12,000 employees", "850 staff"
        emp = re.findall(r'(\d[\d,]*)\s*(?:employees|staff|people|workforce)', text)
        if emp:
            count = int(emp[0].replace(",", ""))
            if count >= 10000:
                return "Large"
            if count >= 500:
                return "Medium"
            return "Small"

        # --- Exchange / Fortune / funding signals ---
        if any(kw in text for kw in [
            "multinational", "listed on", "nse:", "bse:", "nyse:", "nasdaq:",
            "fortune 500", "fortune 1000", "ftse", "pre-ipo", "unicorn",
            "series d", "series e", "series f", "late-stage", "growth stage",
        ]):
            return "Large"
        if any(kw in text for kw in ["series c", "series b"]):
            return "Medium"
        if any(kw in text for kw in [
            "startup", "seed", "series a", "early stage", "small business", "sme",
        ]):
            return "Small"

    except Exception as e:
        logger.warning("account_size_fetch_error", company=company_name, error=str(e))

    return "Medium"


def _extract_normalized_name(raw_name: str, _all_text: str, results: list) -> str:
    """
    Extract the cleanest/most-official company name from search results.

    Heuristics (priority order):
    1. LinkedIn company page title (most authoritative source)
    2. Official website home-page title (e.g. "Heineken - Official Site")
    3. Any page title where the company name is the FIRST segment before a separator
    4. Strip legal suffixes from raw_name as last resort
    """
    # Titles we should skip entirely — not company name pages
    _SKIP_TITLE_PREFIXES = (
        "jobs at", "careers at", "work at", "apply at",
        "about ", "contact ", "home -", "home |",
        "wikipedia", "linkedin", "glassdoor",
    )

    linkedin_candidates = []
    website_candidates = []
    title_candidates = []

    for r in results:
        title = r.title or ""
        url = (r.url or "").lower()

        # Skip noisy page types
        title_lower = title.lower()
        if any(title_lower.startswith(p) for p in _SKIP_TITLE_PREFIXES):
            continue

        for sep in [" - ", " | ", " : ", " – ", " · "]:
            if sep in title:
                candidate = title.split(sep)[0].strip()
                if len(candidate) < 3 or len(candidate) > 80:
                    break
                if _name_overlap(raw_name, candidate) > 0.4:
                    # LinkedIn pages are most authoritative
                    if "linkedin.com" in url:
                        linkedin_candidates.append(candidate)
                    elif any(
                        slug in url
                        for slug in [raw_name.split()[0].lower()[:6], "official"]
                        if len(slug) >= 3
                    ):
                        website_candidates.append(candidate)
                    else:
                        title_candidates.append(candidate)
                break

    # Use LinkedIn name if available (best source)
    for pool in [linkedin_candidates, website_candidates, title_candidates]:
        if pool:
            # Prefer names with highest word overlap, break ties by shortest
            best = max(pool, key=lambda c: (_name_overlap(raw_name, c), -len(c)))
            best = _strip_legal_suffix(best)
            if best and len(best) >= 3:
                return best

    # Fall back to stripping legal suffix from raw name
    stripped = _strip_legal_suffix(raw_name)
    return stripped if stripped else raw_name


def _strip_legal_suffix(name: str) -> str:
    """Remove common legal entity suffixes (English, Spanish, German, French)."""
    suffixes = [
        r"\s+Ltd\.?$", r"\s+Limited$", r"\s+Inc\.?$", r"\s+Corp\.?$",
        r"\s+LLC$", r"\s+LLP$", r"\s+Pvt\.?$", r"\s+Private$",
        r"\s+PLC$", r"\s+S\.A\.?$", r"\s+GmbH$", r"\s+AG$",
        r"\s+\(India\)$", r"\s+India$",
        # Spanish legal forms (dotted and plain)
        r",?\s+S\.L\.U?\.?$", r",?\s+S\.A\.U?\.?$", r",?\s+S\.R\.L\.?$",
        r"\s+SLU?\.?$", r"\s+SAU?\.?$", r"\s+SRL?\.?$",
        r"\s+S\.Coop\.?$", r"\s+C\.B\.?$",
        # Generic Spanish business type words (often stripped in LinkedIn slugs)
        r"\s+España$", r"\s+Espana$", r"\s+Iberia$", r"\s+Ibérica$",
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
    from backend.tools.llm import llm_web_search

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
    - abbreviation: uppercase short form like "DDI" from "DDI - Distribución..." or "CCEP" from "(CCEP)"
    - parent_company: from ownership hints like "(Mahou owned)", "(Heineken distributor)"
    - global_name: strip regional suffixes like "España", "Iberia"
    - short_names: progressively shorter forms

    Examples:
        "DDI - Distribución Directa Integral (Damm owned)"
            → abbreviation="DDI", parent="Damm", global="Distribución Directa Integral"
        "Coca-Cola Europacific Partners (CCEP) Iberia"
            → abbreviation="CCEP", parent=None, global="Coca-Cola Europacific Partners"
        "Voldis (Mahou owned distribution)"
            → abbreviation=None, parent="Mahou", global="Voldis"
        "Heineken España" → abbreviation=None, parent=None, global="Heineken"
        "Antonio Sainero (Mahou distributor)"
            → abbreviation=None, parent="Mahou", global="Antonio Sainero"
        "Nestlé España / Nestlé Professional" → abbreviation=None, parent=None, global="Nestlé"
        "Hijos de Rivera (Estrella Galicia)"
            → abbreviation=None, parent="Estrella Galicia", global="Hijos de Rivera"
    """
    result: dict = {"parent_company": None, "global_name": None, "short_names": [], "abbreviation": None}

    # --- Detect "ABBR - Full Name" or "ABBR – Full Name" pattern ---
    # MUST have spaces around the dash so "Coca-Cola" is NOT treated as ABBR.
    # Valid:   "DDI - Distribución Directa Integral"  → abbreviation="DDI"
    # Valid:   "Diresa - Distribuciones Región Este"  → abbreviation="Diresa"
    # Invalid: "Coca-Cola Europacific Partners ..."   → no spaces → no match
    abbr_dash_m = re.match(r'^([A-Za-z]{2,8})\s+[-–]\s+(.+)', raw_name)
    abbr_from_dash: str | None = None
    raw_for_parse = raw_name
    if abbr_dash_m:
        abbr_candidate = abbr_dash_m.group(1).strip()
        rest = abbr_dash_m.group(2).strip()
        # Only treat as abbreviation if all-caps OR clearly short vs the long-form name
        if abbr_candidate.isupper() or (len(abbr_candidate) <= 6 and len(rest) > len(abbr_candidate) * 3):
            abbr_from_dash = abbr_candidate
            # Use the long-form part as the base for slug building
            raw_for_parse = rest

    # --- Extract abbreviation from all-uppercase parenthetical: "(CCEP)" ---
    abbr_from_paren: str | None = None
    m_upper_paren = re.search(r'\(([A-Z]{2,6})\)', raw_for_parse)
    if m_upper_paren:
        abbr_from_paren = m_upper_paren.group(1)

    result["abbreviation"] = abbr_from_dash or abbr_from_paren

    # --- Extract parent from ownership hints in parentheticals ---
    # "(Mahou owned distribution)", "(Damm owned)", "(Heineken distributor)"
    parent_patterns = [
        r'\((\w[\w\s\-]*?)\s+(?:owned|distributor|distribution)\s+\w+\)',  # (Mahou owned distribution)
        r'\((\w[\w\s\-]*?)\s+(?:owned|distributor|distribution|group)\)',  # (Mahou owned)
    ]
    for pat in parent_patterns:
        m = re.search(pat, raw_for_parse, re.IGNORECASE)
        if m:
            result["parent_company"] = m.group(1).strip()
            break

    # --- For bare parentheticals like "(Mahou)" or "(Estrella Galicia)" → parent company ---
    # But NOT if it was already identified as an uppercase abbreviation
    if not result["parent_company"] and not abbr_from_paren:
        m = re.search(r'\(([A-Z][\w\s\-]{2,30})\)', raw_for_parse)
        if m:
            candidate = m.group(1).strip()
            # Skip relationship/JV descriptions
            skip_words = {"JV", "formerly", "now", "aka", "including", "and", "or", "with", "de", "del"}
            paren_words = set(candidate.split())
            if not (paren_words & skip_words) and len(candidate.split()) <= 4 and not candidate.startswith('+'):
                result["parent_company"] = candidate

    # --- Strip ALL parenthetical text for the clean base name ---
    clean = re.sub(r'\s*\([^)]*\)', '', raw_for_parse).strip()
    # Also strip the "ABBR - " prefix if we extracted it, leaving just the long form
    if abbr_from_dash:
        clean = raw_for_parse
        clean = re.sub(r'\s*\([^)]*\)', '', clean).strip()

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
# Helper: smart company lookup — reasoning-first LinkedIn discovery
# ---------------------------------------------------------------------------

async def _smart_company_lookup(raw_name: str, region: str = "") -> dict:
    """
    GPT-first company intelligence — ONE powerful web search call that returns
    everything we need: domain, parent company, LinkedIn slug, company type.

    Strategy:
    1. GPT web search (latest model) → gets domain + parent + slug + type in one call
    2. Unipile validates the slug (not the other way around)

    Returns:
        {
          "org_id": str | None,
          "slugs": list[str],
          "parent_slugs": list[str],
          "company_type": str,
          "clean_name": str,
          "parent_brand": str | None,
          "domain": str | None,
          "has_own_page": bool | None,
          "notes": str,
        }
    """
    from backend.tools.llm import llm_web_search
    import json as _json

    result: dict = {
        "org_id": None,
        "slugs": [],
        "parent_slugs": [],
        "company_type": "unknown",
        "clean_name": raw_name,
        "parent_brand": None,
        "domain": None,
        "has_own_page": None,
        "notes": "",
    }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 1: Deep web search — find domain + parent (search first, reason later)
    # This is what ChatGPT does: search deeply for one thing, then reason.
    # Asking for 9 fields in one shot makes GPT do shallow search + formatting.
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    try:
        region_hint = f" (operating region: {region})" if region else ""

        # Step 1: SEARCH — focused on finding the correct domain and parent
        search_prompt = (
            f"Search the web for this company: \"{raw_name}\"{region_hint}\n\n"
            f"Find and verify:\n"
            f"1. What is the official website domain for this company? "
            f"(e.g. dominos.de, nestle.com — the ACTUAL primary corporate domain, not a social media page)\n"
            f"2. Is this company a subsidiary, brand, distributor, or regional branch of a larger company? "
            f"If yes, what is the parent company and its domain?\n"
            f"3. What is this company's LinkedIn company page URL?\n\n"
            f"IMPORTANT:\n"
            f"- Search the web to VERIFY the domain. Don't guess from the company name.\n"
            f"- Many tech/SaaS companies use .ai, .io, .co domains — check ALL variants.\n"
            f"  e.g. salescode.ai (NOT salescode.io), perplexity.ai, jasper.ai\n"
            f"- Visit the actual website to confirm it's the right company.\n"
            f"- For obscure/small companies, search harder — try '{raw_name} website', "
            f"'{raw_name} official site', '{raw_name} contact'.\n\n"
            f"Reply in this exact format (no JSON, no markdown):\n"
            f"DOMAIN: example.com\n"
            f"PARENT: Parent Company Name (or NONE if independent)\n"
            f"PARENT_DOMAIN: parent.com (or NONE)\n"
            f"LINKEDIN: https://www.linkedin.com/company/slug\n"
            f"PARENT_LINKEDIN: https://www.linkedin.com/company/parent-slug (or NONE)\n"
            f"TYPE: one of brand/subsidiary/distributor/regional_branch/holding/independent\n"
            f"COUNTRY: XX (ISO 2-letter code)"
        )

        search_response = await llm_web_search(search_prompt, model="gpt-5")
        logger.info("smart_lookup_step1", company=raw_name,
                    content=(search_response or "")[:400])

        if search_response:
            # Parse line-by-line response (much more reliable than JSON parsing)
            for line in search_response.strip().splitlines():
                line = line.strip()
                if line.upper().startswith("DOMAIN:"):
                    val = line.split(":", 1)[1].strip().lower()
                    val = re.sub(r'^https?://(?:www\.)?', '', val).split('/')[0].strip()
                    if val and val != "none" and '.' in val:
                        result["domain"] = val
                elif line.upper().startswith("PARENT:") and "PARENT_" not in line.upper()[:12]:
                    val = line.split(":", 1)[1].strip()
                    if val.upper() != "NONE" and val:
                        result["parent_brand"] = val
                elif line.upper().startswith("PARENT_DOMAIN:"):
                    pass  # stored for reference but not used in result
                elif line.upper().startswith("LINKEDIN:") and "PARENT" not in line.upper()[:12]:
                    val = line.split(":", 1)[1].strip()
                    if "NONE" not in val.upper():
                        # Reconstruct URL if it was split by ":"
                        if "linkedin.com" not in val and len(line.split(":")) > 2:
                            val = ":".join(line.split(":")[1:]).strip()
                        m = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]{1,60})', val, re.IGNORECASE)
                        if m:
                            slug = m.group(1).rstrip('/')
                            if slug not in result["slugs"]:
                                result["slugs"].insert(0, slug)
                        m = re.search(r'linkedin\.com/company/(\d{6,})', val, re.IGNORECASE)
                        if m:
                            result["org_id"] = m.group(1)
                elif line.upper().startswith("PARENT_LINKEDIN:"):
                    val = line.split(":", 1)[1].strip()
                    if "NONE" not in val.upper():
                        if "linkedin.com" not in val and len(line.split(":")) > 2:
                            val = ":".join(line.split(":")[1:]).strip()
                        m = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]{1,60})', val, re.IGNORECASE)
                        if m:
                            slug = m.group(1).rstrip('/')
                            if slug not in result["parent_slugs"] and slug not in result["slugs"]:
                                result["parent_slugs"].append(slug)
                elif line.upper().startswith("TYPE:"):
                    val = line.split(":", 1)[1].strip().lower()
                    if val in ("brand", "subsidiary", "distributor", "regional_branch",
                               "holding", "joint_venture", "independent"):
                        result["company_type"] = val
                elif line.upper().startswith("COUNTRY:"):
                    pass  # used in notes only

        logger.info("smart_lookup_step1_parsed", company=raw_name,
                    domain=result["domain"], parent=result["parent_brand"],
                    slugs=result["slugs"][:3], type=result["company_type"])

    except Exception as e:
        logger.warning("smart_lookup_step1_error", company=raw_name, error=str(e))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 2: Fast LLM reasoning (NO web search) — classify and clean up
    # Uses the facts from Step 1 + model knowledge to fill in gaps
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    try:
        from backend.tools.llm import llm_complete

        context_parts = []
        if result["domain"]:
            context_parts.append(f"domain: {result['domain']}")
        if result["parent_brand"]:
            context_parts.append(f"parent: {result['parent_brand']}")
        if result["company_type"] != "unknown":
            context_parts.append(f"type: {result['company_type']}")
        context_str = ", ".join(context_parts) if context_parts else "no web data found"

        reason_prompt = (
            f"Analyze this company name and return a JSON object.\n\n"
            f"COMPANY: \"{raw_name}\"{f' (region: {region})' if region else ''}\n"
            f"WEB RESEARCH FOUND: {context_str}\n\n"
            f"Return JSON with:\n"
            f"- clean_name: the company's actual trading name (strip S.L., GmbH, parentheticals)\n"
            f"- company_type: brand/subsidiary/distributor/regional_branch/holding/independent\n"
            f"- has_own_page: true/false/maybe (does it have its own LinkedIn company page?)\n"
            f"- primary_slug: most likely LinkedIn URL slug (lowercase, hyphens)\n"
            f"- alt_slugs: list of 2-3 alternative slugs\n\n"
            f"Return ONLY valid JSON, no markdown."
        )

        reason_raw = await llm_complete(reason_prompt, model="gpt-4.1", max_tokens=250)
        cleaned = re.sub(r'^```(?:json)?\s*|\s*```$', '', (reason_raw or "").strip())
        analysis = _json.loads(cleaned)

        result["clean_name"] = analysis.get("clean_name") or raw_name
        if result["company_type"] == "unknown":
            result["company_type"] = analysis.get("company_type", "unknown")
        result["has_own_page"] = analysis.get("has_own_page")

        # Add slugs from reasoning (only if Step 1 didn't find any)
        if not result["slugs"]:
            primary = analysis.get("primary_slug", "")
            if primary:
                result["slugs"].append(primary)
            for s in (analysis.get("alt_slugs") or []):
                if s and s not in result["slugs"]:
                    result["slugs"].append(s)

        logger.info("smart_lookup_step2", company=raw_name,
                    clean_name=result["clean_name"],
                    company_type=result["company_type"],
                    has_own_page=result["has_own_page"])

    except Exception as e:
        logger.warning("smart_lookup_step2_error", company=raw_name, error=str(e))

    # Build notes
    result["notes"] = (
        f"type={result['company_type']}, "
        f"domain={result['domain'] or '?'}, "
        f"parent={result['parent_brand'] or 'none'}, "
        f"has_own_page={result['has_own_page']}"
    )

    logger.info("smart_lookup_done", company=raw_name,
                company_type=result["company_type"],
                clean_name=result["clean_name"],
                domain=result["domain"],
                parent=result["parent_brand"],
                slugs=result["slugs"][:3],
                parent_slugs=result["parent_slugs"][:2])

    return result


async def _llm_validate_linkedin_matches(target_company: str, candidates: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Use LLM to validate LinkedIn candidates against acronym collisions and bad parent mappings.
    Returns: (exact_or_subsidiary_matches, invalid_matches, valid_parent_fallbacks)
    """
    from backend.tools.llm import llm_complete
    import json as _json
    import re as _re

    if not candidates:
        return [], [], []

    lines = "\n".join(f'{i}. Name: "{c.get("name")}" (Slug: {c.get("slug")})' for i, c in enumerate(candidates))

    prompt = (
        f"You are a strict B2B company-matching AI.\n"
        f"Target Company requested: \"{target_company}\"\n\n"
        f"Candidates found on LinkedIn:\n{lines}\n\n"
        f"Task: Evaluate if each candidate is truly the requested company.\n"
        f"- ACRONYM WARNING: If target is 'DDI - Distribución' (beverages) and candidate is 'Development Dimensions International' (HR), that is completely INVALID.\n"
        f"- Match type 'EXACT': A direct match or the precise regional subsidiary (e.g. 'Heineken España').\n"
        f"- Match type 'PARENT': The candidate is the explicit global parent/owner, but NOT the specific subsidiary requested.\n"
        f"- Match type 'INVALID': Completely different company, wrong industry (IT/Consulting when target is beverages), or random acronym match.\n\n"
        f"Return ONLY a JSON array, no markdown/explanation:\n"
        f'[{{"index": 0, "match_type": "EXACT", "reason": "..."}}]\n'
    )
    
    response = await llm_complete(prompt, model="gpt-4.1", max_tokens=1000, temperature=0)
    exact, parent, invalid = [], [], []
    try:
        match = _re.search(r'\[.*?\]', response, _re.DOTALL)
        if match:
            data = _json.loads(match.group(0))
            type_map = {item.get("index"): item.get("match_type", "INVALID") for item in data if isinstance(item, dict)}
            for i, c in enumerate(candidates):
                t = type_map.get(i, "INVALID")
                if t == "EXACT":
                    exact.append(c)
                elif t == "PARENT":
                    parent.append(c)
                else:
                    invalid.append(c)
        else:
            return candidates, [], []  # fallback
    except Exception as e:
        logger.warning("llm_match_validation_failed", error=str(e))
        return candidates, [], []
        
    return exact, invalid, parent


# ---------------------------------------------------------------------------
# Helper: enrich a single company (normalize + org lookup + domain)
# ---------------------------------------------------------------------------

async def _enrich_single_company(company: TargetCompany, region: str, thread_id: str | None = None) -> TargetCompany:
    """Run all enrichment steps for one company. Called concurrently for all companies."""
    from backend.utils.progress import emit_log as _emit_log

    async def _log(msg: str, level: str = "info") -> None:
        await _emit_log(thread_id, msg, level)

    await _log(f"[{company.raw_name}] starting enrichment")

    # --- Step 1: Normalize name ---
    try:
        # Use LinkedIn search as the primary source (most authoritative company names)
        results = await search_with_fallback(
            f'"{company.raw_name}" LinkedIn company',
            max_results=5,
        )
        # Fallback: broader web search if LinkedIn results are sparse
        if len(results) < 2:
            results = await search_with_fallback(
                f'{company.raw_name} company official website',
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

    # ---- PHASE 0: Smart reasoning lookup + mechanical slug candidates (CONCURRENT) ----
    # Run the smart LLM lookup and build regex-based candidates at the same time.
    # Smart lookup = human-like reasoning about the company name + targeted web search.
    # Regex candidates = fast deterministic fallback that runs instantly.

    candidate_names: list[str] = []
    seen_lower: set[str] = set()

    def _add_candidate(name: str):
        n = name.strip()
        if n and len(n) >= 2 and n.lower() not in seen_lower:
            seen_lower.add(n.lower())
            candidate_names.append(n)

    parsed = _parse_company_variants(company.raw_name)
    _parent_company_fallback: str | None = parsed["parent_company"] if parsed["parent_company"] else None

    # Build mechanical candidate list (instant — no API calls)
    _add_candidate(lookup_name)
    if parsed.get("abbreviation"):
        _add_candidate(parsed["abbreviation"])   # "DDI", "CCEP" — likely the exact slug
    if parsed["global_name"]:
        _add_candidate(parsed["global_name"])    # "Heineken" from "Heineken España"
    for short in parsed["short_names"]:
        _add_candidate(short)
    if company.raw_name.strip() != lookup_name:
        _add_candidate(company.raw_name.strip())

    # Determine the best keyword search term:
    # - Abbreviation first ("DDI", "CCEP")
    # - If the name has a DIRECT regional suffix (España, India, France... outside parentheses),
    #   keep the full name WITH region — LinkedIn keyword search "Diageo España" returns the
    #   Spanish entity, not the global page. Don't use global_name here.
    # - Otherwise fall back to global_name (strips region) then lookup_name.
    _name_no_parens = re.sub(r'\s*\([^)]*\)', '', company.raw_name).strip()
    # Detect GEOGRAPHIC words directly in the name (not separators, not business-type words)
    _GEO_WORDS = re.compile(
        r'\b(?:España|Espana|Iberia|Ibérica|Spain|France|Deutschland|Germany|Italy|Italia|'
        r'UK|Brasil|Brazil|Mexico|México|India|China|Japan|Korea|Australia|'
        r'APAC|EMEA|LATAM|Europe|Americas|Africa|'
        r'Middle\s+East|Southeast\s+Asia|South\s+Korea|North\s+America|South\s+America)\b',
        re.IGNORECASE,
    )
    _has_direct_region = bool(_GEO_WORDS.search(_name_no_parens))
    _kw_term = (
        parsed.get("abbreviation")
        or (_name_no_parens if _has_direct_region else None)
        or parsed.get("global_name")
        or _name_no_parens
    )

    # Launch CONCURRENTLY:
    # 1. Smart lookup  — LLM analysis + targeted web search
    # 2. Keyword search — direct LinkedIn company search (like a human typing in the search bar)
    await _log(f"[{company.raw_name}] searching LinkedIn (keyword: {_kw_term}) + web analysis")
    smart_task = asyncio.create_task(
        _smart_company_lookup(company.raw_name, effective_region)
    )
    kw_task = asyncio.create_task(
        unipile.search_company_by_keyword(_kw_term)
    )

    # Await both
    smart: dict = {}
    kw_results: list[dict] = []
    try:
        smart, kw_results = await asyncio.gather(smart_task, kw_task, return_exceptions=False)
    except Exception:
        # Gather with return_exceptions=False raises on first exception;
        # fall back to sequential to preserve partial results
        try:
            smart = await smart_task
        except Exception as e:
            logger.warning("smart_lookup_failed", company=company.raw_name, error=str(e))
        try:
            kw_results = await kw_task
        except Exception as e:
            logger.warning("kw_search_failed", company=company.raw_name, error=str(e))

    # If kw_results are from the wrong company, discard (validation happens later)
    logger.info(
        "enrich_parallel_done",
        company=company.raw_name,
        smart_type=smart.get("company_type", "?"),
        kw_matches=[r["name"] for r in kw_results[:3]],
    )
    if kw_results:
        await _log(f"[{company.raw_name}] LinkedIn keyword results: {', '.join(r['name'] for r in kw_results[:3])}")
    if smart.get("company_type") and smart["company_type"] != "unknown":
        await _log(f"[{company.raw_name}] identified as: {smart['company_type']}" + (f" (parent: {smart['parent_brand']})" if smart.get("parent_brand") else ""))

    # Enrich parent fallback from smart lookup if not already set
    if not _parent_company_fallback and smart.get("parent_brand"):
        _parent_company_fallback = smart["parent_brand"]
    # Smart lookup may have identified parent-brand slugs — add them to fallback pool
    # so they're tried ONLY if all company-specific lookups fail
    _parent_slugs_from_smart: list[str] = smart.get("parent_slugs", [])

    # Add smart lookup's clean_name and suggested slugs as priority candidates
    if smart.get("clean_name") and smart["clean_name"] != company.raw_name:
        _add_candidate(smart["clean_name"])
    for slug in smart.get("slugs", []):
        _add_candidate(slug)

    logger.info(
        "enrich_candidates_ready",
        company=company.raw_name,
        smart_type=smart.get("company_type", "?"),
        smart_slugs=smart.get("slugs", [])[:4],
        total_candidates=len(candidate_names),
        candidates=candidate_names[:8],
    )

    # ---- PHASE 1: Collect all high-quality signals ----
    llm_slugs: list[str] = []
    all_linkedin_matches: list[dict] = []
    org_id = None
    org_match_how = ""

    # 1a: Smart lookup found a numeric org ID directly
    if smart.get("org_id"):
        all_linkedin_matches.append({
            "org_id": smart["org_id"],
            "name": smart.get("clean_name") or company.raw_name,
            "slug": smart.get("slugs", [""])[0] if smart.get("slugs") else "",
            "how": "smart_lookup",
        })
        org_id = smart["org_id"]
        org_match_how = "smart_lookup"
        logger.info("enrich_org_from_smart_lookup", company=company.raw_name, org_id=org_id)
        await _log(f"[{company.raw_name}] matched LinkedIn page via web search (org {org_id})", "success")
    else:
        # Smart slugs → tried first in Phase 2
        for slug in smart.get("slugs", []):
            if slug and slug not in llm_slugs:
                llm_slugs.append(slug)

    # 1b: Keyword search results (LinkedIn search-bar style — most reliable signal)
    for kr in kw_results:
        if not any(m["org_id"] == kr["org_id"] for m in all_linkedin_matches):
            all_linkedin_matches.append({
                "org_id": kr["org_id"],
                "name": kr["name"],
                "slug": kr.get("public_identifier", ""),
                "how": "keyword_search",
                "website": kr.get("website", ""),
            })
            if not org_id:
                org_id = kr["org_id"]
                org_match_how = "keyword_search"
                logger.info("enrich_org_from_keyword_search",
                            company=company.raw_name, found=kr["name"], org_id=org_id)
                await _log(f"[{company.raw_name}] matched LinkedIn page: {kr['name']}", "success")

    # ---- PHASE 2: Mechanical Unipile validation (skipped if smart lookup already found org_id) ----
    # Try smart-suggested slugs first, then candidate names.
    if not org_id:
        await _log(f"[{company.raw_name}] trying {len(candidate_names)} name variants on LinkedIn")
        # Try smart/LLM-found slugs first (most likely to be correct)
        for slug in llm_slugs:
            if slug.isdigit():
                org_id = slug
                org_match_how = "smart_lookup_numeric"
                logger.info("enrich_org_from_slug_numeric", company=company.raw_name, org_id=org_id)
                break
            try:
                org_info = await unipile.get_company_org_id(slug)
                if org_info["org_id"]:
                    if not any(m["org_id"] == org_info["org_id"] for m in all_linkedin_matches):
                        match = {"org_id": org_info["org_id"], "name": org_info.get("name", slug), "slug": slug, "how": "smart_slug"}
                        all_linkedin_matches.append(match)
                    if not org_id:
                        org_id = org_info["org_id"]
                        org_match_how = "smart_slug"
                        logger.info("enrich_org_found_smart_slug", company=company.raw_name, slug=slug, org_id=org_id)
            except Exception:
                pass

    if not org_id:
        # Try each mechanical candidate name
        for name in candidate_names:
            if len(all_linkedin_matches) >= 3:
                break
            try:
                org_info = await unipile.get_company_org_id(name)
                if org_info["org_id"]:
                    if not any(m["org_id"] == org_info["org_id"] for m in all_linkedin_matches):
                        match = {"org_id": org_info["org_id"], "name": org_info.get("name", name), "slug": name, "how": "candidate_name"}
                        all_linkedin_matches.append(match)
                    if not org_id:
                        org_id = org_info["org_id"]
                        org_match_how = "candidate_name"
                        logger.info("enrich_org_found", company=company.raw_name, name=name, org_id=org_id)
                        await _log(f"[{company.raw_name}] matched LinkedIn page: {org_info.get('name', name)}", "success")
            except Exception:
                pass

    # Fallback: only try parent_company / parent_slugs if ALL primary lookups found nothing.
    # This avoids "Atocha Vallecas (Mahou)" → searching "Mahou" → returning "Mahou San Miguel".
    if not all_linkedin_matches:
        # Try smart-identified parent slugs first (already resolved to slug form)
        for _ps in _parent_slugs_from_smart:
            try:
                org_info = await unipile.get_company_org_id(_ps)
                if org_info["org_id"]:
                    all_linkedin_matches.append({
                        "org_id": org_info["org_id"],
                        "name": org_info.get("name", _ps),
                        "slug": _ps,
                        "how": "parent_company_fallback",
                    })
                    org_id = org_info["org_id"]
                    org_match_how = "parent_company_fallback"
                    logger.info("enrich_org_found_parent_slug_fallback",
                                company=company.raw_name, slug=_ps, org_id=org_id)
                    break
            except Exception:
                pass

        # Then try the parent company name if still nothing found
        if not all_linkedin_matches and _parent_company_fallback:
            try:
                org_info = await unipile.get_company_org_id(_parent_company_fallback)
                if org_info["org_id"]:
                    all_linkedin_matches.append({
                        "org_id": org_info["org_id"],
                        "name": org_info.get("name", _parent_company_fallback),
                        "slug": _parent_company_fallback,
                        "how": "parent_company_fallback",
                    })
                    org_id = org_info["org_id"]
                    org_match_how = "parent_company_fallback"
                    logger.info("enrich_org_found_parent_fallback", company=company.raw_name,
                                parent=_parent_company_fallback, org_id=org_id)
            except Exception:
                pass

    # ---- PHASE 3: Search engines as last resort before keyword fallback ----
    if not org_id:
        for search_name in candidate_names[:3]:
            try:
                # Try site-specific search first (more targeted), then broader search
                for query in [
                    f'site:linkedin.com/company "{search_name}"',
                    f'{search_name} LinkedIn company page',
                ]:
                    search_results = await search_with_fallback(query, max_results=5)
                    for r in search_results:
                        # Check URL first
                        slug_match = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]+)', r.url, re.IGNORECASE)
                        if not slug_match:
                            # Also check snippet for LinkedIn URLs
                            slug_match = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]+)', r.snippet, re.IGNORECASE)
                        if slug_match:
                            slug = slug_match.group(1).rstrip('/')
                            if slug.isdigit():
                                org_id = slug
                                org_match_how = "search_numeric"
                                logger.info("enrich_org_from_search_numeric", company=company.raw_name, org_id=org_id)
                                break
                            try:
                                org_info = await unipile.get_company_org_id(slug)
                                if org_info["org_id"]:
                                    org_id = org_info["org_id"]
                                    org_match_how = "search_engine"
                                    logger.info("enrich_org_from_search", company=company.raw_name,
                                                slug=slug, org_id=org_id)
                                    break
                            except Exception:
                                pass
                    if org_id:
                        break
                if org_id:
                    break
            except Exception:
                pass

    # Deduplicate matches by org_id (multiple lookup paths can find the same company)
    _seen_org_ids: set[str] = set()
    _deduped: list[dict] = []
    for _m in all_linkedin_matches:
        if _m["org_id"] not in _seen_org_ids:
            _seen_org_ids.add(_m["org_id"])
            _deduped.append(_m)
    all_linkedin_matches = _deduped

    # ---- LLM Validation of Matches ----
    if all_linkedin_matches:
        try:
            _exact, _invalid, _parent = await _llm_validate_linkedin_matches(company.raw_name, all_linkedin_matches)
            
            if _exact:
                # We have a perfect match or direct subsidiary. Discard the rest.
                all_linkedin_matches = _exact
                if org_id and not any(m["org_id"] == org_id for m in _exact):
                    org_id = _exact[0]["org_id"]  # shift primary ID to the best exact match
                    org_match_how = _exact[0].get("how", "candidate_name")
            elif _parent:
                # No exact match, but we safely found the global parent. Use it as fallback.
                all_linkedin_matches = _parent[:1]
                org_id = all_linkedin_matches[0]["org_id"]
                org_match_how = "parent_company_fallback"
                logger.info("enrich_using_parent_fallback_only", company=company.raw_name,
                            found=all_linkedin_matches[0].get("name"))
            else:
                # Everything was dropped by the AI (e.g. acronym collision)
                logger.warning("enrich_all_matches_invalid", company=company.raw_name)
                all_linkedin_matches = _invalid  # keep them in the array for UI debugging visibility
                org_id = None
        except Exception as e:
            logger.warning("enrich_validation_error", error=str(e))

    # ---- Build Sales Nav URL ----
    link_not_found = not bool(org_id)
    sales_nav_url = ""
    if org_id:
        # Detect whether the region is embedded in the company name itself
        # (e.g. "Diageo España", "Heineken Iberia").
        name_implied_region = bool(_detect_region_from_name(company.raw_name))

        if company.account_size == "Small" and name_implied_region:
            # The org_id we found belongs to the regional subsidiary (e.g. Diageo Spain,
            # ~130 employees). Instead, use the global parent so Sales Nav shows the full
            # headcount (e.g. Diageo global, ~13k employees) with no region filter.
            global_name = parsed.get("global_name") or ""
            global_org_id = None

            if global_name:
                # 1) Check candidates already fetched during the Unipile lookup
                for match in all_linkedin_matches:
                    m_name = (match.get("name") or "").lower()
                    if m_name == global_name.lower() or m_name.startswith(global_name.lower() + " "):
                        if match["org_id"] != org_id:
                            global_org_id = match["org_id"]
                            break

                # 2) Fresh Unipile lookup for the global name if still not found
                if not global_org_id:
                    try:
                        g_info = await unipile.get_company_org_id(global_name)
                        if g_info.get("org_id") and g_info["org_id"] != org_id:
                            global_org_id = g_info["org_id"]
                    except Exception:
                        pass

            use_org_id = global_org_id or org_id
            use_name = global_name or company.normalized_name or company.raw_name
            sales_nav_url = _build_sales_nav_url(use_org_id, use_name, "")
        else:
            # Non-name-implied Small → no region filter (avoids missing employees)
            # Medium / Large → apply effective_region as normal
            url_region = "" if company.account_size == "Small" else effective_region
            sales_nav_url = _build_sales_nav_url(
                org_id,
                company.normalized_name or company.raw_name,
                url_region,
            )

    # ---- Fallback: keyword-based Sales Nav URL (with region if available) ----
    if not sales_nav_url:
        logger.warning("enrich_no_org_id_keyword_fallback", company=company.raw_name,
                       candidates_tried=len(candidate_names))
        await _log(f"[{company.raw_name}] no LinkedIn page found — using keyword search fallback", "warning")
        encoded_name = quote(lookup_name, safe="")
        # Apply region filter in the keyword fallback URL the same way _build_sales_nav_url does
        fallback_region_filter = ""
        if effective_region:
            _fb_region_key = effective_region.strip().lower()
            _fb_region_id = REGION_IDS.get(_fb_region_key)
            if _fb_region_id:
                _fb_region_text = quote(effective_region.strip(), safe="")
                fallback_region_filter = (
                    f"%2Cfilters%3AList((type%3AREGION%2C"
                    f"values%3AList((id%3A{_fb_region_id}%2C"
                    f"text%3A{_fb_region_text}%2CselectionType%3AINCLUDED))))"
                )
        sales_nav_url = (
            f"https://www.linkedin.com/sales/search/people?"
            f"query=(recentSearchParam%3A(doLogHistory%3Atrue)%2C"
            f"keywords%3A{encoded_name}{fallback_region_filter})"
        )

    # ---- Compute LinkedIn confidence ----
    if org_match_how in ("smart_lookup", "smart_lookup_numeric", "keyword_search"):
        linkedin_confidence = "high"
    elif org_match_how in ("smart_slug",):
        linkedin_confidence = "high"
    elif org_match_how == "candidate_name":
        linkedin_confidence = "high" if candidate_names and candidate_names[0] == lookup_name else "medium"
    elif org_match_how in ("search_engine", "search_numeric"):
        linkedin_confidence = "medium"
    elif org_match_how == "parent_company_fallback":
        linkedin_confidence = "medium"
    else:
        linkedin_confidence = "low"  # keyword fallback

    # ---- Build agent notes for LinkedIn ----
    notes_parts = []
    # Include smart lookup's company type analysis
    if smart.get("company_type") and smart["company_type"] != "unknown":
        notes_parts.append(f"Identified as: {smart['company_type']}.")
    if smart.get("parent_brand"):
        notes_parts.append(f"Parent/brand: {smart['parent_brand']}.")
    if link_not_found:
        notes_parts.append(f"Could not find LinkedIn company page — using keyword search fallback. Tried {len(candidate_names)} name variants.")
    elif org_match_how in ("smart_lookup", "smart_lookup_numeric"):
        notes_parts.append("LinkedIn match found via smart reasoning + web search.")
    elif org_match_how == "keyword_search":
        notes_parts.append("LinkedIn match found via LinkedIn company keyword search.")
    elif org_match_how == "smart_slug":
        notes_parts.append("LinkedIn match found via smart slug suggestion.")
    elif org_match_how == "candidate_name":
        matched_name = next((m["name"] for m in all_linkedin_matches if m["org_id"] == org_id), lookup_name)
        notes_parts.append(f"LinkedIn match found via Unipile lookup for '{matched_name}'.")
    elif org_match_how in ("search_engine", "search_numeric"):
        notes_parts.append("LinkedIn match found via search engine (org ID verified).")
    elif org_match_how == "parent_company_fallback":
        matched_name = next((m["name"] for m in all_linkedin_matches if m["org_id"] == org_id), lookup_name)
        notes_parts.append(f"Using parent company fallback '{matched_name}'.")
        
    if len(all_linkedin_matches) > 1:
        other_names = [m["name"] for m in all_linkedin_matches if m["org_id"] != org_id][:2]
        notes_parts.append(f"Other possible matches: {', '.join(other_names)}.")

    # Apply results
    update = {
        "sales_nav_url": sales_nav_url,
        "linkedin_confidence": linkedin_confidence,
        "linkedin_candidates": all_linkedin_matches[:3],
    }
    if org_id:
        update["linkedin_org_id"] = org_id
    # Store detected region as account_type if not already set
    if effective_region and not company.account_type:
        update["account_type"] = effective_region.title()
    company = company.model_copy(update=update)
    logger.info("enrich_step2_done", company=company.raw_name,
                org_id=org_id or "(none)", region=effective_region or "(none)",
                confidence=linkedin_confidence, link_not_found=link_not_found)

    # --- Step 3: Per-candidate independent enrichment ---
    # Each LinkedIn candidate is a DIFFERENT company — enrich domain/email/size independently.

    # Filter out likely LinkedIn Showcase Pages before enriching.
    # Showcase pages (e.g. "Rippling IT", "Rippling Spend") are product sub-pages that
    # don't exist in Sales Navigator's company filter — enriching them wastes API calls.
    _raw_lower = re.sub(r'[^a-z0-9]', '', company.raw_name.lower())
    _filtered_matches: list[dict] = []
    _showcase_names: list[str] = []
    for _m in all_linkedin_matches:
        _m_name = (_m.get("name") or "").strip()
        _m_lower = re.sub(r'[^a-z0-9]', '', _m_name.lower())
        # A showcase page typically has the parent name as a prefix + extra word(s)
        # e.g. "Rippling IT" starts with "Rippling" but is longer.
        # Only flag as showcase if there's ALSO an exact match in the list.
        is_potential_showcase = (
            _m_lower != _raw_lower  # not exact match
            and _m_lower.startswith(_raw_lower)  # starts with search term
            and len(_m_lower) > len(_raw_lower) + 2  # has meaningful suffix
        )
        if is_potential_showcase and any(
            re.sub(r'[^a-z0-9]', '', (x.get("name") or "").lower()) == _raw_lower
            for x in all_linkedin_matches
        ):
            _showcase_names.append(_m_name)
        else:
            _filtered_matches.append(_m)

    if _showcase_names:
        await _log(f"[{company.raw_name}] filtered {len(_showcase_names)} showcase pages: {', '.join(_showcase_names)}")
        logger.info("enrich_filtered_showcases", company=company.raw_name, showcases=_showcase_names)

    # Optimization: if one candidate is a clear exact name match, only fully enrich that one
    # and do lightweight enrichment (just Sales Nav URL) for the rest.
    enriched_candidates: list[dict] = []
    _exact_name_matches = [
        c for c in _filtered_matches
        if re.sub(r'[^a-z0-9]', '', (c.get("name") or "").lower()) == _raw_lower
    ]
    if len(_exact_name_matches) == 1 and len(_filtered_matches) > 1:
        # One clear exact match — only fully enrich that one, others get Sales Nav URL only
        primary = _exact_name_matches[0]
        candidates_to_enrich = [primary]
        lightweight_candidates = [c for c in _filtered_matches if c is not primary][:4]
    else:
        candidates_to_enrich = _filtered_matches[:3]  # cap at 3 to avoid API waste
        lightweight_candidates = _filtered_matches[3:5]

    if candidates_to_enrich:
        await _log(f"[{company.raw_name}] enriching {len(candidates_to_enrich)} candidate(s)"
                   + (f" + {len(lightweight_candidates)} lightweight" if lightweight_candidates else ""))

        async def _enrich_candidate(cand: dict) -> dict:
            """Enrich a single LinkedIn candidate with domain, email, size.

            ORDER MATTERS — domain-first strategy:
            1. Discover domain (reliable — dominos.de)
            2. Look up Sales Nav org_id WITH domain validation (filters out wrong companies)
            3. Build Sales Nav URL (correct org_id guaranteed)
            """
            cand_name = cand.get("name") or company.normalized_name or company.raw_name
            cand_org_id = cand.get("org_id")
            cand_enriched = {**cand}  # copy original fields
            _ctype = smart.get("company_type", "")
            _parent = smart.get("parent_brand", "")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STEP 1: Discover domain FIRST — this is our most reliable signal
            # GPT web search already found domain in _smart_company_lookup — use it
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            found_domain = smart.get("domain") or None  # GPT already found this
            if found_domain:
                await _log(f"[{company.raw_name}] {cand_name}: domain from GPT web search = {found_domain}")
            try:
                _domain_ctx = {
                    "company_type": smart.get("company_type"),
                    "parent_brand": smart.get("parent_brand"),
                    "region": effective_region,
                    "linkedin_slug": cand.get("public_identifier") or cand.get("slug") or "",
                    "org_id": cand.get("org_id") or "",
                }

                # Only run domain discovery if GPT web search didn't already find it
                if not found_domain:
                    # Fast path: LinkedIn keyword search already returned the website
                    _linkedin_website = (cand.get("website") or "").strip()
                    if _linkedin_website:
                        _linkedin_website = re.sub(r'^https?://(?:www\.)?', '', _linkedin_website)
                        _linkedin_website = re.sub(r'^www\.', '', _linkedin_website)
                        _linkedin_website = _linkedin_website.split('/')[0].strip().lower()

                    if _linkedin_website and re.match(r'^[a-z0-9][a-z0-9\-]*\.[a-z]{2,}$', _linkedin_website):
                        found_domain = _linkedin_website
                        await _log(f"[{company.raw_name}] {cand_name}: domain from LinkedIn = {found_domain}")
                    else:
                        d_info = await discover_domain(cand_name, context=_domain_ctx)
                        found_domain = d_info.get("domain")
                        if d_info.get("email_format") and "@" in d_info["email_format"]:
                            cand_enriched["_email_from_discovery"] = d_info["email_format"]

                # Parent fallback if no domain found
                if not found_domain and _parent:
                    await _log(f"[{company.raw_name}] no domain for {cand_name}, trying parent: {_parent}")
                    d_info = await discover_domain(_parent, context=_domain_ctx)
                    found_domain = d_info.get("domain")
                    if found_domain:
                        await _log(f"[{company.raw_name}] using parent domain: {found_domain}")
                        if d_info.get("email_format") and "@" in d_info["email_format"]:
                            cand_enriched["_email_from_discovery"] = d_info["email_format"]

                # Probe email format
                email_fmt = cand_enriched.pop("_email_from_discovery", None)
                if found_domain and not email_fmt:
                    try:
                        from backend.tools.domain_discovery import _probe_email_format
                        email_fmt = await _probe_email_format(cand_name, found_domain)
                    except Exception as e:
                        logger.warning("email_probe_error", candidate=cand_name, error=str(e))

                if not email_fmt and found_domain:
                    email_fmt = f"{{first}}.{{last}}@{found_domain}"

                d_confidence = "high" if (email_fmt and found_domain) else ("medium" if found_domain else "low")
                cand_enriched["domain"] = found_domain
                cand_enriched["email_format"] = email_fmt
                cand_enriched["domain_confidence"] = d_confidence
                ec = d_confidence if email_fmt and "@" in email_fmt else "low"
                if ec == "low" and email_fmt:
                    ec = "medium"
                cand_enriched["email_confidence"] = ec
                if found_domain:
                    await _log(f"[{company.raw_name}] {cand_name}: domain={found_domain} · email={email_fmt or '?'}")
                else:
                    await _log(f"[{company.raw_name}] {cand_name}: no domain found")
            except Exception as e:
                logger.warning("enrich_candidate_domain_error", candidate=cand_name, error=str(e))
                cand_enriched["domain"] = None
                cand_enriched["email_format"] = None
                cand_enriched["domain_confidence"] = "low"
                cand_enriched["email_confidence"] = "low"

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STEP 2: Find Sales Nav org_id WITH domain validation
            # Domain is now known — use it to filter out wrong LinkedIn companies
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            _use_org_id = cand_org_id
            _use_org_name = cand_name
            _use_region = effective_region or ""

            # For regional branches / distributors / brands, search parent WITH domain validation
            if _ctype in ("regional_branch", "distributor", "subsidiary", "brand") and _parent:
                try:
                    parent_org = await unipile.get_company_org_id(
                        _parent, expected_domain=found_domain or ""
                    )
                    if parent_org.get("org_id"):
                        _use_org_id = parent_org["org_id"]
                        _use_org_name = parent_org.get("name") or _parent
                        await _log(f"[{company.raw_name}] using parent org ({_use_org_name}) for Sales Nav"
                                   + (f" [domain-validated: {found_domain}]" if found_domain else ""))
                except Exception:
                    pass
            elif found_domain and _use_org_id:
                # For non-parent cases: validate the candidate's own org_id against domain
                try:
                    _org_website = await unipile.get_company_domain(str(_use_org_id))
                    if _org_website and not unipile._domain_matches(_org_website, found_domain):
                        await _log(f"[{company.raw_name}] org website '{_org_website}' ≠ '{found_domain}' — re-searching with domain validation")
                        _revalidated = await unipile.get_company_org_id(
                            cand_name, expected_domain=found_domain
                        )
                        if _revalidated.get("org_id"):
                            _use_org_id = _revalidated["org_id"]
                            _use_org_name = _revalidated.get("name") or cand_name
                            await _log(f"[{company.raw_name}] domain-validated org: {_use_org_name}")
                except Exception:
                    pass

            # Check employee count in region — if < 400, drop region filter
            if _use_org_id and _use_region:
                _region_key = _use_region.strip().lower()
                _region_id = REGION_IDS.get(_region_key, "")
                if _region_id:
                    try:
                        regional_count = await unipile.count_employees_in_region(
                            _use_org_id, _region_id
                        )
                        if regional_count < 400:
                            await _log(
                                f"[{company.raw_name}] {regional_count} people in {_use_region} "
                                f"(< 400) — removing region filter"
                            )
                            _use_region = ""
                        else:
                            await _log(f"[{company.raw_name}] {regional_count} people in {_use_region} — keeping filter")
                    except Exception:
                        pass

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STEP 3: Build Sales Nav URL with the validated org_id
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if _use_org_id:
                cand_enriched["sales_nav_url"] = _build_sales_nav_url(
                    _use_org_id, _use_org_name, _use_region
                )

            # Account size
            try:
                cand_enriched["account_size"] = await _fetch_account_size(cand_name)
                sc = "medium" if cand_enriched["account_size"] and cand_enriched["account_size"] != "Medium" else "low"
                cand_enriched["size_confidence"] = sc
            except Exception:
                cand_enriched["account_size"] = "Medium"
                cand_enriched["size_confidence"] = "low"

            # Account type (region)
            cand_enriched["account_type"] = effective_region.title() if effective_region else "Global"

            return cand_enriched

        # Enrich all candidates in parallel
        enrich_tasks = [_enrich_candidate(c) for c in candidates_to_enrich]
        enrich_results = await asyncio.gather(*enrich_tasks, return_exceptions=True)
        for i, result in enumerate(enrich_results):
            if isinstance(result, Exception):
                logger.warning("enrich_candidate_failed", candidate=candidates_to_enrich[i].get("name"), error=str(result))
                enriched_candidates.append({**candidates_to_enrich[i], "domain": None, "email_format": None})
            else:
                enriched_candidates.append(result)

        # Add lightweight candidates (Sales Nav URL only — no domain/email probing)
        for lc in lightweight_candidates:
            lc_name = lc.get("name") or company.raw_name
            lc_enriched = {**lc}
            if lc.get("org_id"):
                lc_enriched["sales_nav_url"] = _build_sales_nav_url(
                    lc["org_id"], lc_name, effective_region or ""
                )
            lc_enriched["domain"] = None
            lc_enriched["email_format"] = None
            lc_enriched["account_size"] = None
            lc_enriched["account_type"] = effective_region.title() if effective_region else "Global"
            enriched_candidates.append(lc_enriched)
    else:
        # No LinkedIn matches — do single domain discovery with normalized name
        await _log(f"[{company.raw_name}] no LinkedIn matches — discovering domain for raw name")
        try:
            name = company.normalized_name or company.raw_name
            domain_info = await discover_domain(name)
            company = company.model_copy(update={
                "domain": domain_info["domain"],
                "email_format": domain_info["email_format"],
                "domain_confidence": domain_info.get("confidence", "low"),
                "email_confidence": "medium" if domain_info["email_format"] else "low",
            })
            if domain_info["domain"]:
                notes_parts.append(f"Domain '{domain_info['domain']}' found.")
                await _log(f"[{company.raw_name}] domain: {domain_info['domain']}")
        except Exception as e:
            logger.warning("enrich_domain_error", company=company.raw_name, error=str(e))
            notes_parts.append("Domain lookup failed.")

    # --- Step 4: LLM reasoning — pick the best candidate ---
    selection_reasoning = None
    if len(enriched_candidates) >= 1:
        from backend.tools.llm import llm_complete as _llm_pick

        # Build context for the LLM
        cand_lines = []
        for i, ec in enumerate(enriched_candidates):
            cand_lines.append(
                f"{i}. \"{ec.get('name')}\" — org_id={ec.get('org_id')}, "
                f"domain={ec.get('domain') or '?'}, size={ec.get('account_size') or '?'}, "
                f"match_method={ec.get('how', '?')}"
            )
        cand_text = "\n".join(cand_lines)

        pick_prompt = (
            f"You are a B2B sales research AI. A user searched for the company: \"{company.raw_name}\"\n\n"
            f"LinkedIn returned these candidates, each independently enriched:\n{cand_text}\n\n"
            f"TASK: Decide which candidate is the BEST match for \"{company.raw_name}\".\n\n"
            f"RULES (in order of priority):\n"
            f"1. NAME MATCH IS KING — the candidate whose name is closest to what the user typed wins.\n"
            f"   - If the user typed 'salescode', prefer 'SalesCode.ai' over 'Salescode' (salescode.io) because\n"
            f"     the user likely means the specific company they know, not a similarly-named one.\n"
            f"   - If the user included a suffix like '.ai', '.io', '.com', that's a strong signal — match it exactly.\n"
            f"2. REAL COMPANY KNOWLEDGE — use your world knowledge. If you know 'salescode' is actually\n"
            f"   'SalesCode.ai' (the sales tech company), pick that even if another candidate has more data.\n"
            f"3. Data completeness is a TIE-BREAKER ONLY — only prefer a candidate with more data if\n"
            f"   the names are equally good matches. Never pick a worse name match just because it has a domain.\n"
            f"4. If the search term is generic (e.g. 'yellow'), pick the most prominent/well-known company.\n"
            f"5. If there's a clear match, mark confidence as HIGH. If ambiguous, mark LOW.\n\n"
            f"Return ONLY a JSON object (no markdown):\n"
            f'{{"best_index": 0, "confidence": "HIGH"|"LOW", '
            f'"reasoning": "2-3 sentence explanation of your choice and why others were rejected"}}'
        )

        try:
            await _log(f"[{company.raw_name}] reasoning about {len(enriched_candidates)} candidates...")
            pick_raw = await _llm_pick(pick_prompt, model="gpt-4.1", max_tokens=300, temperature=0)
            pick_cleaned = re.sub(r'^```(?:json)?\s*|\s*```$', '', (pick_raw or "").strip())
            import json as _json
            pick_data = _json.loads(pick_cleaned)
            best_idx = int(pick_data.get("best_index", 0))
            pick_confidence = pick_data.get("confidence", "LOW").upper()
            selection_reasoning = pick_data.get("reasoning", "")

            if 0 <= best_idx < len(enriched_candidates):
                best = enriched_candidates[best_idx]
            else:
                best = enriched_candidates[0]
                pick_confidence = "LOW"

            # If the selected candidate was lightweight (no domain/email), enrich it now
            if best.get("domain") is None and best.get("org_id"):
                await _log(f"[{company.raw_name}] selected candidate was lightweight — enriching now")
                try:
                    enriched_best = await _enrich_candidate(best)
                    enriched_candidates[best_idx if 0 <= best_idx < len(enriched_candidates) else 0] = enriched_best
                    best = enriched_best
                except Exception as enrich_err:
                    logger.warning("enrich_selected_lightweight_failed", company=company.raw_name, error=str(enrich_err))

            await _log(
                f"[{company.raw_name}] best match: \"{best.get('name')}\" "
                f"(confidence: {pick_confidence}) — {selection_reasoning}",
                "success" if pick_confidence == "HIGH" else "info",
            )

            # Apply the best candidate's data to the company
            org_id = best.get("org_id")
            company = company.model_copy(update={
                "linkedin_org_id": org_id,
                "sales_nav_url": best.get("sales_nav_url") or company.sales_nav_url,
                "domain": best.get("domain"),
                "email_format": best.get("email_format"),
                "domain_confidence": best.get("domain_confidence", "low"),
                "email_confidence": best.get("email_confidence", "low"),
                "account_size": best.get("account_size") or company.account_size,
                "account_type": best.get("account_type") or company.account_type,
                "linkedin_confidence": "high" if pick_confidence == "HIGH" else "medium",
                "linkedin_candidates": enriched_candidates,
                "selection_reasoning": selection_reasoning,
            })

            # Determine auto-commit eligibility
            is_auto_eligible = (
                pick_confidence == "HIGH"
                and best.get("domain")
                and company.linkedin_org_id
            )
            if is_auto_eligible:
                notes_parts.append(f"Auto-eligible: high-confidence match for \"{best.get('name')}\".")
            else:
                notes_parts.append(f"Needs review: {selection_reasoning}")

        except Exception as e:
            logger.warning("llm_pick_failed", company=company.raw_name, error=str(e))
            # Fallback: use first candidate
            if enriched_candidates:
                best = enriched_candidates[0]
                company = company.model_copy(update={
                    "domain": best.get("domain"),
                    "email_format": best.get("email_format"),
                    "domain_confidence": best.get("domain_confidence", "low"),
                    "email_confidence": best.get("email_confidence", "low"),
                    "account_size": best.get("account_size") or company.account_size,
                    "linkedin_candidates": enriched_candidates,
                })
            notes_parts.append("LLM candidate selection failed — using first match.")

    # ---- Final assembly ----
    account_size = company.account_size
    size_confidence = "medium" if account_size and account_size != "Medium" else "low"
    agent_notes = " ".join(notes_parts) if notes_parts else None

    company = company.model_copy(update={
        "size_confidence": size_confidence,
        "agent_notes": agent_notes,
    })

    logger.info(
        "enrich_single_done",
        company=company.raw_name,
        normalized=company.normalized_name,
        org_id=company.linkedin_org_id or "(none)",
        domain=company.domain or "(none)",
        linkedin_confidence=company.linkedin_confidence,
        candidates=len(enriched_candidates),
        agent_notes=agent_notes,
    )
    return company


# ---------------------------------------------------------------------------
# Node: parallel_enrich_all
# ---------------------------------------------------------------------------

async def _enrich_with_progress(company: TargetCompany, region: str, thread_id: str | None) -> TargetCompany:
    """Wraps _enrich_single_company with per-company progress events."""
    from backend.utils.progress import emit as _emit_progress
    await _emit_progress(thread_id, company.raw_name, "processing")
    try:
        result = await _enrich_single_company(company, region, thread_id=thread_id)
        await _emit_progress(thread_id, company.raw_name, "done")
        return result
    except Exception:
        await _emit_progress(thread_id, company.raw_name, "error")
        raise


async def parallel_enrich_all(state: FiniState) -> FiniState:
    """
    Enrich companies with controlled parallelism (3 at a time).
    Streams each card to the frontend as soon as it completes.
    Auto-commits high-confidence matches to sheet + n8n immediately.
    """
    if state.enrichment_done:
        return state

    total = len(state.companies)
    logger.info("fini_enrich_start", count=total, auto_mode=state.auto_mode)
    from backend.utils.progress import emit_log as _emit_log

    _CONCURRENCY = 3
    sem = asyncio.Semaphore(_CONCURRENCY)
    await _emit_log(state.thread_id,
                    f"Enriching {total} companies ({_CONCURRENCY} at a time)...")

    companies = list(state.companies)
    errors: list[str] = []
    _completed = {"count": 0, "auto": 0, "review": 0}
    _lock = asyncio.Lock()
    # Background n8n tasks — fire and forget, don't block enrichment
    _n8n_tasks: list[asyncio.Task] = []

    async def _process_one(i: int, company: TargetCompany) -> None:
        async with sem:
            # --- Enrich ---
            try:
                result = await _enrich_with_progress(company, state.region, state.thread_id)
                async with _lock:
                    companies[i] = result
            except Exception as e:
                async with _lock:
                    errors.append(f"{company.raw_name}: {e}")
                logger.error("enrich_company_failed", company=company.raw_name, error=str(e))
                await _emit_log(state.thread_id,
                                f"[{company.raw_name}] failed: {e}", "error")
                # Stream error card
                await _emit_enriched_card(state.thread_id, companies[i], "error")
                return

            comp = companies[i]
            async with _lock:
                _completed["count"] += 1
                done = _completed["count"]

            # --- Stream card to frontend immediately ---
            is_auto_eligible = (
                state.auto_mode
                and comp.linkedin_confidence == "high"
                and comp.domain
                and comp.linkedin_org_id
                and comp.sales_nav_url
            )

            if is_auto_eligible:
                # Auto-commit to sheet
                try:
                    row_num = await _auto_commit_to_sheet(comp, state.sdr_name, state.region)
                    async with _lock:
                        companies[i] = comp.model_copy(update={
                            "auto_committed": True,
                            "sheet_row_written": True,
                        })
                        comp = companies[i]
                        _completed["auto"] += 1
                    await _emit_log(state.thread_id,
                                    f"[{comp.raw_name}] auto-committed to sheet row {row_num} ({done}/{total})",
                                    "success")

                    # n8n trigger in background (non-blocking)
                    if state.submit_to_n8n:
                        task = asyncio.create_task(
                            _background_n8n_submit(comp, row_num, state.sdr_name, state.thread_id)
                        )
                        _n8n_tasks.append(task)

                except Exception as e:
                    logger.warning("auto_commit_failed", company=comp.raw_name, error=str(e))
                    await _emit_log(state.thread_id,
                                    f"[{comp.raw_name}] auto-commit failed: {e}", "warning")
            else:
                async with _lock:
                    _completed["review"] += 1
                reason = comp.selection_reasoning or "needs review"
                await _emit_log(state.thread_id,
                                f"[{comp.raw_name}] needs review — {reason} ({done}/{total})",
                                "info")

            # Stream the card to frontend
            await _emit_enriched_card(state.thread_id, comp, "sent" if comp.auto_committed else "pending")

    # Launch all tasks with semaphore controlling concurrency
    tasks = [_process_one(i, c) for i, c in enumerate(companies)]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Wait for any background n8n tasks to finish
    if _n8n_tasks:
        await _emit_log(state.thread_id,
                        f"Waiting for {len(_n8n_tasks)} n8n submissions to complete...")
        await asyncio.gather(*_n8n_tasks, return_exceptions=True)

    auto_n = _completed["auto"]
    review_n = _completed["review"]
    await _emit_log(state.thread_id,
                    f"Done. {auto_n} auto-committed, {review_n} need review, {len(errors)} errors.",
                    "success")

    logger.info("fini_enrich_done", count=total, auto=auto_n, review=review_n, errors=len(errors))

    return state.model_copy(update={
        "companies": companies,
        "enrichment_done": True,
        "status": "completed",
        "errors": errors,
    })


async def _emit_enriched_card(thread_id: str | None, comp: TargetCompany, card_status: str):
    """Emit a company_enriched event so the frontend can show the card immediately."""
    if not thread_id:
        return
    from backend.utils.progress import _queues
    q = _queues.get(thread_id)
    if not q:
        return
    from datetime import datetime, timezone
    try:
        await q.put({
            "type": "company_enriched",
            "data": {
                "raw_name": comp.raw_name,
                "company_name": comp.normalized_name or comp.raw_name,
                "sales_nav_url": comp.sales_nav_url or "",
                "domain": comp.domain or "",
                "sdr_assigned": comp.sdr_assigned or "",
                "email_format": comp.email_format or "",
                "account_type": comp.account_type or "",
                "account_size": comp.account_size or "",
                "linkedin_org_id": comp.linkedin_org_id or "",
                "linkedin_confidence": comp.linkedin_confidence,
                "domain_confidence": comp.domain_confidence,
                "email_confidence": comp.email_confidence,
                "size_confidence": comp.size_confidence,
                "agent_notes": comp.agent_notes,
                "linkedin_candidates": comp.linkedin_candidates,
                "auto_committed": comp.auto_committed,
                "selection_reasoning": comp.selection_reasoning,
                "card_status": card_status,  # "sent" | "pending" | "error"
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass


async def _background_n8n_submit(comp: TargetCompany, row_num: int, sdr_name: str, thread_id: str | None):
    """Submit to n8n in background — doesn't block enrichment of other companies."""
    from backend.utils.progress import emit_log as _emit_log
    try:
        await _emit_log(thread_id, f"[{comp.raw_name}] n8n: submitting...", "info")
        await _auto_submit_n8n(comp, row_num, sdr_name)
        await _emit_log(thread_id, f"[{comp.raw_name}] n8n: submitted", "success")
        # Emit update so frontend can mark n8n status
        await _emit_n8n_status(thread_id, comp.raw_name, "submitted")
    except Exception as e:
        logger.warning("background_n8n_failed", company=comp.raw_name, error=str(e))
        await _emit_log(thread_id, f"[{comp.raw_name}] n8n: failed — {e}", "warning")
        await _emit_n8n_status(thread_id, comp.raw_name, "failed")


async def _emit_n8n_status(thread_id: str | None, raw_name: str, status: str):
    """Emit n8n status update so frontend can update the card badge."""
    if not thread_id:
        return
    from backend.utils.progress import _queues
    q = _queues.get(thread_id)
    if not q:
        return
    from datetime import datetime, timezone
    try:
        await q.put({
            "type": "n8n_status",
            "data": {"raw_name": raw_name, "status": status},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass


async def _auto_commit_to_sheet(company: TargetCompany, sdr_name: str, region: str) -> int:
    """Write an auto-committed company to the Target Accounts sheet. Returns row number."""
    from backend.tools import sheets

    row_data = [
        company.normalized_name or company.raw_name,  # A: Company Name
        company.raw_name,                              # B: Parent/Raw Name
        company.sales_nav_url or "",                   # C: Sales Navigator Link
        company.domain or "",                          # D: Company Domain
        sdr_name or "",                                # E: SDR Name
        company.email_format or "",                    # F: Email Format
        company.account_type or region or "Global",    # G: Account Type
        company.account_size or "Medium",              # H: Account Size
        "",                                            # I: (reserved)
        "",                                            # J: n8n status
    ]
    written_row = await sheets.append_row(sheets.TARGET_ACCOUNTS, row_data)
    return written_row


async def _auto_submit_n8n(company: TargetCompany, row_num: int, sdr_name: str):
    """Submit auto-committed company to n8n webhook."""
    from backend.tools.n8n import submit_to_n8n, build_payload

    payload = build_payload(
        row=row_num,
        company_name=company.normalized_name or company.raw_name,
        parent_company_name=company.raw_name,
        sales_nav_url=company.sales_nav_url or "",
        domain=company.domain or "",
        sdr_assigned=sdr_name or "",
        email_format=company.email_format or "",
        account_type=company.account_type or "Global",
        account_size=company.account_size or "Medium",
    )
    await submit_to_n8n(payload)


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
    os.makedirs(os.path.dirname(settings.checkpoint_db_abs), exist_ok=True)

    graph = StateGraph(FiniState)

    graph.add_node("parallel_enrich_all", parallel_enrich_all)
    graph.set_entry_point("parallel_enrich_all")
    
    graph.add_edge("parallel_enrich_all", END)

    raw_conn = await aiosqlite.connect(settings.checkpoint_db_abs)
    conn = AioSqliteConnectionWrapper(raw_conn)
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    serde = JsonPlusSerializer(allowed_msgpack_modules=True)
    checkpointer = AsyncSqliteSaver(conn, serde=serde)

    return graph.compile(checkpointer=checkpointer)
