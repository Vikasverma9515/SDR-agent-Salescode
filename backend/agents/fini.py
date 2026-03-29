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
    Think about the company name like a human researcher would, then find it on LinkedIn.

    Two-step:
    1. Fast LLM analysis (no web) — classify company type, extract clean name,
       derive LinkedIn slug conventions, flag if company likely has no own page.
    2. Targeted LLM web search (with context from step 1) — find the actual page.

    Returns:
        {
          "org_id": str | None,      # numeric LinkedIn org ID if found directly
          "slugs": list[str],        # ordered slug candidates to try with Unipile
          "company_type": str,       # e.g. "distributor", "regional_branch", "brand"
          "clean_name": str,         # e.g. "Atocha Vallecas" (no parentheticals/suffixes)
          "parent_brand": str | None,
          "has_own_page": bool | None,  # None = unknown
          "notes": str,
        }
    """
    from backend.tools.llm import llm_complete, llm_web_search
    import json as _json

    result: dict = {
        "org_id": None,
        "slugs": [],
        "parent_slugs": [],  # slugs that belong to the parent brand (only used as last resort)
        "company_type": "unknown",
        "clean_name": raw_name,
        "parent_brand": None,
        "has_own_page": None,
        "notes": "",
    }

    # ---- Step 1: Reason about the name (fast, no web) ----
    analysis: dict = {}
    try:
        region_hint = f" (operating region: {region})" if region else ""
        analysis_raw = await llm_complete(
            f"You are a B2B sales researcher who knows LinkedIn company pages deeply.\n"
            f"Analyze this company name{region_hint} and return a JSON object.\n\n"
            f"COMPANY NAME: \"{raw_name}\"\n\n"
            f"WHAT TO ANALYZE:\n"
            f"1. company_type — one of: brand, subsidiary, distributor, regional_branch, holding, "
            f"joint_venture, independent. "
            f"Clues: 'Distribuciones/Distribuidora/Distribuidor' = distributor; "
            f"'(X owned)/(X distributor)' = distributor of X; "
            f"'España/Iberia/France/...' = regional branch; "
            f"'Grupo/Group' = holding; "
            f"'JV' or '(+ X)' = joint_venture.\n"
            f"2. clean_name — the company's actual trading name. "
            f"Strip: legal suffixes (S.L., S.A., SLU, Ltd, GmbH, etc.), "
            f"regional qualifiers (España, Iberia, Ibérica, France, etc.), "
            f"ownership parentheticals ('(Mahou)','(Heineken distributor)' → strip entirely). "
            f"Keep: brand abbreviations like CCEP, DDI if they ARE the company's name.\n"
            f"3. primary_slug — most likely LinkedIn URL slug (lowercase, hyphens, no legal suffix). "
            f"Examples: 'heineken', 'coca-cola-europacific-partners', 'ddi', 'atocha-vallecas'.\n"
            f"4. alt_slugs — list of 3 alternative slugs (e.g. with -sl, -sa, without prefix word "
            f"like 'grupo', abbreviated form, full long-form).\n"
            f"5. parent_brand — the parent company or owning brand if identifiable from parentheticals "
            f"or known business relationships. Null if the company is independent or a major brand.\n"
            f"6. country — ISO 2-letter code if identifiable. Spanish names: ES. "
            f"'Bodegas','Grupo','Distribuciones','Hostelería','Hijos de' → ES.\n"
            f"7. has_own_page — true if company is large/well-known enough to have its own LinkedIn "
            f"company page; false if it's a tiny local distributor unlikely to be on LinkedIn; "
            f"maybe if uncertain.\n\n"
            f"Return ONLY a valid JSON object, no markdown, no explanation.\n"
            f"Example: {{\"company_type\":\"distributor\",\"clean_name\":\"Atocha Vallecas\","
            f"\"primary_slug\":\"atocha-vallecas\",\"alt_slugs\":[\"atocha-vallecas-sl\"],"
            f"\"parent_brand\":\"Mahou\",\"country\":\"ES\",\"has_own_page\":\"maybe\"}}",
            max_tokens=350,
        )
        # Strip markdown fences if present
        cleaned_raw = re.sub(r'^```(?:json)?\s*|\s*```$', '', (analysis_raw or "").strip())
        analysis = _json.loads(cleaned_raw)
        result["company_type"] = analysis.get("company_type", "unknown")
        result["clean_name"] = analysis.get("clean_name") or raw_name
        result["parent_brand"] = analysis.get("parent_brand")
        result["has_own_page"] = analysis.get("has_own_page")  # true/false/"maybe"
        primary = analysis.get("primary_slug", "")
        alts = analysis.get("alt_slugs") or []
        if primary:
            result["slugs"].append(primary)
        for s in alts:
            if s and s not in result["slugs"]:
                result["slugs"].append(s)
        result["notes"] = (
            f"type={result['company_type']}, country={analysis.get('country','?')}, "
            f"has_own_page={result['has_own_page']}"
        )
        logger.info("smart_lookup_analysis", company=raw_name,
                    company_type=result["company_type"],
                    clean_name=result["clean_name"],
                    primary_slug=primary,
                    has_own_page=result["has_own_page"])
    except Exception as e:
        logger.warning("smart_lookup_analysis_error", company=raw_name, error=str(e))

    # If the analysis says this company almost certainly has no LinkedIn page, skip web search
    if result["has_own_page"] is False or result["has_own_page"] == "false":
        logger.info("smart_lookup_no_page_expected", company=raw_name)
        return result

    # ---- Step 2: Targeted web search (uses analysis context) ----
    try:
        clean_name = result["clean_name"]
        parent = result["parent_brand"] or ""
        company_type = result["company_type"]
        country_code = analysis.get("country", "")
        primary_slug = result["slugs"][0] if result["slugs"] else ""

        # Build a rich, contextual search prompt
        context_parts: list[str] = []
        if company_type and company_type != "unknown":
            context_parts.append(company_type)
        if parent:
            context_parts.append(f"owned by / distribution partner of {parent}")
        if country_code:
            context_parts.append(f"based in {country_code}")
        context_str = "; ".join(context_parts)

        prompt = (
            f"You are a B2B sales researcher. Find the LinkedIn company page for:\n\n"
            f"Company: \"{clean_name}\"\n"
        )
        if context_str:
            prompt += f"Context: {context_str}\n"
        if primary_slug:
            prompt += f"Most likely LinkedIn slug: linkedin.com/company/{primary_slug}\n"
        prompt += (
            "\nIMPORTANT: Find the COMPANY'S OWN LinkedIn page — NOT its parent brand or owner.\n"
            "If this specific company has no LinkedIn page, then use the parent company's page.\n\n"
            "Return ONLY ONE of these formats:\n"
            "  ORG_ID: 12345678           (the numeric LinkedIn organization ID)\n"
            "  URL: linkedin.com/company/slug   (the company page URL)\n"
            "Nothing else."
        )

        content = await llm_web_search(prompt)
        logger.info("smart_lookup_web_result", company=raw_name, content=(content or "")[:200])

        if not content:
            return result

        # Extract numeric org ID (most reliable)
        m = re.search(r'ORG_ID:\s*(\d+)', content)
        if m:
            result["org_id"] = m.group(1)
            return result

        # Org ID embedded in Sales Nav URL
        m = re.search(r'organization%(?:25)?3A(\d+)', content)
        if m:
            result["org_id"] = m.group(1)
            return result

        # Numeric company slug: linkedin.com/company/12345
        m = re.search(r'linkedin\.com/company/(\d{6,})', content, re.IGNORECASE)
        if m:
            result["org_id"] = m.group(1)
            return result

        # Text slug: linkedin.com/company/slug
        # Take only the FIRST URL the LLM returns — we asked for ONE answer.
        # Taking multiple causes parent-brand pollution (e.g. both "industrias-marjo" and
        # "mahou-san-miguel" end up in the slug list when the LLM mentions both).
        m = re.search(r'linkedin\.com/company/([a-z0-9][a-z0-9\-]{1,60})', content, re.IGNORECASE)
        if m:
            slug = m.group(1).rstrip('/')
            if slug not in result["slugs"]:
                result["slugs"].insert(0, slug)  # put web-found slug FIRST

        # If the found slug looks like the parent brand (not the company itself), demote it.
        # E.g. parent_brand="Mahou" → filter out "mahou", "mahou-san-miguel" from primary slugs.
        parent_brand = result.get("parent_brand") or ""
        if parent_brand and result["slugs"]:
            from backend.tools.unipile import _ascii_lower as _ali
            parent_root = _ali(parent_brand).split()[0]  # "mahou" from "Mahou San Miguel"
            clean_root = _ali(result["clean_name"]).split()[0]  # "industrias" from "Industrias Marjo"
            filtered: list[str] = []
            demoted: list[str] = []
            for s in result["slugs"]:
                slug_root = s.split('-')[0]  # "mahou" from "mahou-san-miguel"
                if parent_root and slug_root == parent_root and slug_root != clean_root:
                    demoted.append(s)   # clearly the parent's slug, not the company
                else:
                    filtered.append(s)
            result["slugs"] = filtered
            result["parent_slugs"] = demoted  # type: ignore[assignment]

    except Exception as e:
        logger.warning("smart_lookup_web_error", company=raw_name, error=str(e))

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
    
    response = await llm_complete(prompt, max_tokens=1000, temperature=0)
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
        sales_nav_url = _build_sales_nav_url(
            org_id,
            company.normalized_name or company.raw_name,
            effective_region,
        )

    # ---- Fallback: keyword-based Sales Nav URL ----
    if not sales_nav_url:
        logger.warning("enrich_no_org_id_keyword_fallback", company=company.raw_name,
                       candidates_tried=len(candidate_names))
        await _log(f"[{company.raw_name}] no LinkedIn page found — using keyword search fallback", "warning")
        encoded_name = quote(lookup_name, safe="")
        sales_nav_url = (
            f"https://www.linkedin.com/sales/search/people?"
            f"query=(keywords%3A{encoded_name})"
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

    # --- Step 3: Domain + email format ---
    await _log(f"[{company.raw_name}] discovering domain + email format")
    domain_confidence = "low"
    email_confidence = "low"
    try:
        name = company.normalized_name or company.raw_name
        domain_info = await discover_domain(name)
        if domain_info["domain"]:
            dc = domain_info.get("confidence", "medium")
            domain_confidence = dc
            # email confidence matches domain confidence if format was probed, else low
            email_confidence = dc if domain_info["email_format"] and "@" in domain_info["email_format"] else "low"
            if email_confidence == "low" and domain_info["email_format"]:
                email_confidence = "medium"  # default pattern but domain is known
        company = company.model_copy(update={
            "domain": domain_info["domain"],
            "email_format": domain_info["email_format"],
            "domain_confidence": domain_confidence,
            "email_confidence": email_confidence,
        })
        if domain_info["domain"]:
            notes_parts.append(f"Domain '{domain_info['domain']}' found ({domain_confidence} confidence).")
            await _log(f"[{company.raw_name}] domain: {domain_info['domain']} · email: {domain_info.get('email_format') or 'unknown'}")
        else:
            notes_parts.append("Could not determine company domain.")
    except Exception as e:
        logger.warning("enrich_domain_error", company=company.raw_name, error=str(e))
        notes_parts.append("Domain lookup failed.")

    # ---- Account size confidence ----
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
        linkedin_confidence=linkedin_confidence,
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
    Enrich ALL companies concurrently.
    Returns the enriched list for frontend review.
    """
    if state.enrichment_done:
        return state

    logger.info("fini_parallel_enrich_start", count=len(state.companies))

    tasks = [
        _enrich_with_progress(company, state.region, state.thread_id)
        for company in state.companies
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    companies = list(state.companies)
    errors = []

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            errors.append(f"Enrichment failed for {companies[i].raw_name}: {result}")
            logger.error("enrich_company_failed", company=companies[i].raw_name, error=str(result))
        else:
            companies[i] = result

    logger.info("fini_parallel_enrich_done", count=len(companies), errors=len(errors))

    return state.model_copy(update={
        "companies": companies,
        "enrichment_done": True,
        "status": "completed",
        "errors": errors,
    })


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
