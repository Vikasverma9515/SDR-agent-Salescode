"""
AI Scout — LangGraph-powered lead enrichment agent.

Graph flow (conditional):
  parse_intent ──► [greeting/not-search/no-company] ──► synthesize ──► END
                └► prepare (dup-check + company-lookup in parallel) ──► research ──► enrich ──► synthesize ──► END

Key design principles:
- Greetings and vague queries return in <1s (no external calls)
- Global 50s timeout wraps the entire pipeline
- LinkedIn search capped at 3 title variants (not 8)
- Each external call has its own tight timeout
- "No company" detected early → ask SDR to clarify
"""
from __future__ import annotations

import asyncio
import json
import re
import unicodedata

from typing import Any, Literal, TypedDict

import httpx
from langgraph.graph import END, StateGraph

from backend.tools import sheets
from backend.tools.llm import _bedrock_claude
from backend.utils.logging import get_logger

logger = get_logger("scout")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GLOBAL_TIMEOUT_SECS = 48       # hard cap on entire pipeline
PERPLEXITY_TIMEOUT  = 18       # per Perplexity call
LINKEDIN_TIMEOUT    = 15       # per Unipile search call (search_people runs titles internally in parallel)
VERIFY_TIMEOUT      = 10       # per verify_profile call
MAX_LI_TITLES       = 3        # LinkedIn search title variants (each = 1 API call)
MAX_ENRICH          = 3        # max candidates to verify
MAX_VERIFY_CONCUR   = 3        # max parallel verify_profile calls

_GREETINGS = re.compile(
    r"^\s*(hi|hello|hey|sup|howdy|yo|hiya|good\s*(morning|afternoon|evening)|what'?s?\s+up|"
    r"how\s+are\s+you|test|testing|ping|ok|okay|yes|no|sure|thanks|thank\s+you|"
    r"great|cool|nice|got\s+it|sounds?\s+good)\s*[!?.]*\s*$",
    re.IGNORECASE,
)

# Informational / conversational queries — not lead searches
_NON_SEARCH = re.compile(
    r"^\s*(what\s+(is|are|does|do|was|were)\b|tell\s+me\s+(about|more)\b|"
    r"explain\b|describe\b|how\s+(does|do|did)\b|why\s+is\b|"
    r"give\s+me\s+(info|details|an?\s+overview)|overview\s+of\b|"
    r"can\s+you\s+(explain|tell|describe)\b|i\s+(want|need)\s+to\s+(know|understand)\b)",
    re.IGNORECASE,
)
# Keywords that signal a REAL search request even if the query starts with the above
_SEARCH_SIGNALS = re.compile(
    r"\b(find|search|look\s+for|who\s+is\s+the|who'?s\s+the|get\s+me|"
    r"cmo|ceo|cto|cdo|cfo|coo|vp\b|svp|evp|head\s+of|director\s+of|"
    r"manager\s+at|contact\s+at|leads?\s+at|decision\s+maker)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class ScoutState(TypedDict):
    query: str
    company: str           # hint from frontend companies field
    history: list[dict]
    # Parsed
    company_name: str
    target_roles: list[str]
    intent: str            # "search" | "greeting" | "need_company"
    # Company context
    company_domain: str
    email_format: str
    company_org_id: str
    company_account_type: str
    company_account_size: str
    # Research raw
    perplexity_text: str
    linkedin_raw: list[dict]
    # Enriched
    candidates: list[dict]
    # Duplicates
    duplicates: list[dict]
    # Output
    message: str
    error: str


def _empty_state() -> ScoutState:
    return ScoutState(
        query="", company="", history=[],
        company_name="", target_roles=[], intent="search",
        company_domain="", email_format="", company_org_id="",
        company_account_type="", company_account_size="",
        perplexity_text="", linkedin_raw=[],
        candidates=[], duplicates=[],
        message="", error="",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s or "")
    return nfkd.encode("ascii", "ignore").decode("ascii").lower().strip()


def _names_match(a: str, b: str) -> bool:
    na, nb = _normalize(a), _normalize(b)
    if na == nb:
        return True
    if len(na) > 3 and (na in nb or nb in na):
        return True
    wa, wb = set(na.split()), set(nb.split())
    if len(wa) >= 2 and len(wb) >= 2 and len(wa & wb) >= 2:
        return True
    return False


def _companies_match(a: str, b: str) -> bool:
    _STRIP = re.compile(
        r'\b(ltd|limited|inc|corp|llc|pvt|plc|sa|sl|gmbh|ag|group|holdings?|'
        r'international|global|solutions?|services?)\b|[.,]',
        re.IGNORECASE,
    )
    na = _STRIP.sub("", _normalize(a)).strip()
    nb = _STRIP.sub("", _normalize(b)).strip()
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    wa, wb = set(na.split()), set(nb.split())
    if wa and wb and len(wa & wb) / max(len(wa), len(wb)) >= 0.6:
        return True
    return False


def _apply_email_format(fmt: str, first: str, last: str, domain: str) -> str:
    if not fmt or not (first or last):
        return ""
    f = re.sub(r"[^a-z]", "", _normalize(first))
    l = re.sub(r"[^a-z]", "", _normalize(last))
    fi = f[0] if f else ""
    li = l[0] if l else ""
    result = fmt.strip()
    result = result.replace("{first}", f).replace("{last}", l)
    result = result.replace("{f}", fi).replace("{l}", li)
    if "@" not in result and domain:
        result = f"{result}@{domain}"
    return result.lower()


def _parse_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split(" ", 1)
    return (parts[0] if parts else ""), (parts[1] if len(parts) > 1 else "")


def _buying_role(role_title: str) -> str:
    r = role_title.lower()
    if any(k in r for k in [
        "chief", "ceo", "cfo", "cmo", "cto", "cdo", "coo", "cpo", "cso",
        "vp ", "vice president", "svp", "evp", "head of", "director",
        "president", "owner", "founder", "partner",
    ]):
        return "Decision Maker"
    return "Influencer"


async def _safe(coro, *, timeout: float, default: Any, label: str) -> Any:
    """Run a coroutine with a timeout; return default on error."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("scout_timeout", step=label, timeout=timeout)
        return default
    except Exception as e:
        logger.warning("scout_step_error", step=label, error=str(e))
        return default


# ---------------------------------------------------------------------------
# Node 1: parse_intent
# ---------------------------------------------------------------------------

async def parse_intent(state: ScoutState) -> dict:
    query = state["query"].strip()
    company_hint = state["company"].strip()

    # ── Fast path: greeting / small talk ────────────────────────────────
    if _GREETINGS.match(query):
        return {
            "intent": "greeting",
            "company_name": company_hint or "",
            "target_roles": [],
            "message": (
                "Hey! I'm your AI Scout — I find and enrich leads at target companies. "
                "Try asking: *\"Find the CMO at Diageo España\"* or "
                "*\"Who's the VP Ecommerce at Zalando?\"*"
            ),
        }

    # ── Fast path: informational question, not a lead search ─────────────
    if _NON_SEARCH.match(query) and not _SEARCH_SIGNALS.search(query):
        example = company_hint or "Meta"
        return {
            "intent": "not_search",
            "company_name": company_hint or "",
            "target_roles": [],
            "message": (
                "I'm specialized for finding B2B leads and decision-makers — not general knowledge. "
                f"Try: *\"Find the CMO at {example}\"* or *\"Who's the VP Ecommerce at Zalando?\"*"
            ),
        }

    # ── Fast path: very short query with no company clue ────────────────
    words = query.split()
    has_company_hint = bool(company_hint)
    if len(words) <= 2 and not has_company_hint:
        return {
            "intent": "need_company",
            "company_name": "",
            "target_roles": [query],
            "message": (
                f"Which company are you searching at? "
                f"Try: *\"Find the {query} at [Company Name]\"*"
            ),
        }

    # ── Claude parse ─────────────────────────────────────────────────────
    history_ctx = ""
    for h in state["history"][-3:]:
        history_ctx += f"{h.get('role','').upper()}: {h.get('content','')[:150]}\n"

    prompt = (
        "Extract structured info from this B2B SDR query. Return ONLY valid JSON.\n\n"
        f"Company hint: \"{company_hint}\"\n"
        + (f"Recent context:\n{history_ctx}\n" if history_ctx else "")
        + f"Query: \"{query}\"\n\n"
        "{\n"
        "  \"company_name\": \"exact company name\",\n"
        "  \"target_roles\": [\"role1\", \"role2\"],\n"
        "  \"intent\": \"search\"\n"
        "}\n"
        "Rules:\n"
        "- company_name: use the hint if the query doesn't mention one\n"
        "- target_roles: specific titles (e.g. 'Chief Marketing Officer', not 'marketing')\n"
        "- intent: always 'search' unless there is literally no company to search at"
    )

    raw = await _safe(
        _bedrock_claude(prompt, max_tokens=200, temperature=0),
        timeout=15, default="{}", label="parse_intent",
    )

    company_name = company_hint or "unknown"
    target_roles: list[str] = [query]

    try:
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
            company_name = parsed.get("company_name") or company_hint or "unknown"
            target_roles = parsed.get("target_roles") or [query]
    except Exception:
        pass

    # If still no company found, ask user
    if company_name in ("unknown", "the company", "", "the company."):
        return {
            "intent": "need_company",
            "company_name": "",
            "target_roles": target_roles,
            "message": (
                "Which company should I search at? "
                "Be specific, e.g.: *\"Find the CMO at [Company Name]\"*"
            ),
        }

    return {
        "intent": "search",
        "company_name": company_name,
        "target_roles": target_roles[:5],
    }


# ---------------------------------------------------------------------------
# Node 2: prepare — duplicates + company lookup in parallel (saves ~8s)
# ---------------------------------------------------------------------------

async def prepare(state: ScoutState) -> dict:
    """Run duplicate check AND company lookup simultaneously."""
    company_name = state["company_name"]

    duplicates: list[dict] = []
    updates: dict = {
        "company_domain": "",
        "email_format": "",
        "company_org_id": "",
        "company_account_type": "",
        "company_account_size": "",
    }

    async def _dup_check() -> None:
        if not company_name:
            return

        async def _read(tab: str) -> list[dict]:
            return await _safe(
                sheets.read_all_records(tab),
                timeout=8, default=[], label=f"dup_read_{tab}",
            )

        fcl_records = await _read(sheets.FIRST_CLEAN_LIST)

        def _scan(records: list[dict], sheet_name: str) -> None:
            for i, row in enumerate(records, start=2):
                row_company = row.get("Company Name", "")
                if not _companies_match(row_company, company_name):
                    continue
                first = row.get("First Name", "")
                last  = row.get("Last Name", "")
                full  = f"{first} {last}".strip()
                if not full:
                    continue
                role   = row.get("Job titles (English)", "") or row.get("Job Title (English)", "")
                email  = row.get("Email", "")
                status = row.get("Overall Status", "")
                duplicates.append({
                    "full_name": full,
                    "role_title": role,
                    "company": row_company,
                    "email": email,
                    "sheet_name": sheet_name,
                    "row_number": i,
                    "overall_status": status,
                })

        _scan(fcl_records, "First Clean List")

    async def _company_lookup() -> None:
        if not company_name:
            return

        async def _sheet() -> None:
            records = await _safe(
                sheets.read_all_records(sheets.TARGET_ACCOUNTS),
                timeout=8, default=[], label="lookup_sheet",
            )
            for row in records:
                if _companies_match(row.get("Company Name", ""), company_name):
                    updates["company_domain"]       = row.get("Company Domain", "")
                    updates["email_format"]         = row.get("Email Format( Firstname-amy , Lastname- williams)", "")
                    updates["company_account_type"] = row.get("Account type", "")
                    updates["company_account_size"] = row.get("Account Size", "")
                    break

        async def _unipile() -> None:
            try:
                from backend.tools.unipile import get_company_org_id
                org_info = await _safe(
                    get_company_org_id(company_name),
                    timeout=12, default=None, label="lookup_unipile",
                )
                if org_info and org_info.get("org_id"):
                    updates["company_org_id"] = str(org_info["org_id"])
            except Exception as e:
                logger.warning("scout_unipile_org_error", error=str(e))

        await asyncio.gather(_sheet(), _unipile(), return_exceptions=True)

    # Both sub-tasks run fully in parallel
    await asyncio.gather(_dup_check(), _company_lookup(), return_exceptions=True)
    return {"duplicates": duplicates, **updates}


# ---------------------------------------------------------------------------
# Node 4: research
# ---------------------------------------------------------------------------

async def research(state: ScoutState) -> dict:
    company_name = state["company_name"]
    target_roles = state["target_roles"]
    org_id       = state["company_org_id"]
    query        = state["query"]
    roles_str    = ", ".join(target_roles[:3])

    # ── Perplexity ────────────────────────────────────────────────────────
    async def _perplexity() -> str:
        try:
            from backend.config import get_settings
            settings = get_settings()
            if not settings.perplexity_api_key:
                return ""
            async with httpx.AsyncClient(timeout=PERPLEXITY_TIMEOUT) as client:
                resp = await client.post(
                    "https://api.perplexity.ai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {settings.perplexity_api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "sonar-pro",
                        "messages": [
                            {
                                "role": "system",
                                "content": (
                                    "You are a B2B research expert. Find real currently-employed "
                                    "executives. Provide full names, current titles, LinkedIn URLs. "
                                    "Be factual and concise."
                                ),
                            },
                            {
                                "role": "user",
                                "content": (
                                    f"Find people with these roles at {company_name}: {roles_str}\n"
                                    f"Request: {query}\n"
                                    "For each: full name, current title, LinkedIn URL. "
                                    "Only include people currently at this company."
                                ),
                            },
                        ],
                        "max_tokens": 1500,
                        "search_recency_filter": "month",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            content = data["choices"][0]["message"]["content"]
            citations = data.get("citations", [])
            if citations:
                content += "\nSources: " + ", ".join(citations[:3])
            return content
        except Exception as e:
            logger.warning("scout_perplexity_error", error=str(e))
            return ""

    # ── Unipile LinkedIn — capped at MAX_LI_TITLES ────────────────────────
    async def _linkedin() -> list[dict]:
        try:
            from backend.tools.unipile import search_people, get_company_org_id

            resolved_id = org_id
            if not resolved_id:
                org_info = await _safe(
                    get_company_org_id(company_name),
                    timeout=12, default=None, label="research_unipile_org",
                )
                resolved_id = str(org_info.get("org_id") or "") if org_info else ""
            if not resolved_id:
                logger.info("scout_no_org_id", company=company_name)
                return []

            # Generate MAX_LI_TITLES title variants — NOT 8
            titles_prompt = (
                f"Generate exactly {MAX_LI_TITLES} LinkedIn job title search strings for: "
                f"\"{roles_str}\" at {company_name}.\n"
                f"Return ONLY a JSON array of {MAX_LI_TITLES} strings."
            )
            titles_raw = await _safe(
                _bedrock_claude(titles_prompt, max_tokens=150, temperature=0),
                timeout=12, default="[]", label="research_titles",
            )
            titles: list[str] = []
            m = re.search(r'\[.*?\]', titles_raw, re.DOTALL)
            if m:
                try:
                    titles = json.loads(m.group(0))
                except Exception:
                    pass
            if not titles:
                titles = [r.strip() for r in target_roles[:MAX_LI_TITLES]]

            # search_people already runs titles in parallel internally — single timeout
            people = await _safe(
                search_people(resolved_id, titles[:MAX_LI_TITLES], limit=20),
                timeout=LINKEDIN_TIMEOUT,
                default=[],
                label="research_search_people",
            )
            return [
                {
                    "full_name": p.get("full_name", ""),
                    "role_title": p.get("headline", ""),
                    "company": company_name,
                    "linkedin_url": p.get("linkedin_url", ""),
                    "linkedin_verified": True,
                    "source": "linkedin",
                    "confidence": "medium",
                }
                for p in people if p.get("full_name")
            ]
        except Exception as e:
            logger.warning("scout_linkedin_error", error=str(e))
            return []

    perp, li = await asyncio.gather(
        _safe(_perplexity(), timeout=PERPLEXITY_TIMEOUT + 2, default="", label="perplexity_outer"),
        _safe(_linkedin(), timeout=LINKEDIN_TIMEOUT * MAX_LI_TITLES + 5, default=[], label="linkedin_outer"),
        return_exceptions=True,
    )

    return {
        "perplexity_text": perp if isinstance(perp, str) else "",
        "linkedin_raw": li if isinstance(li, list) else [],
    }


# ---------------------------------------------------------------------------
# Node 5: enrich
# ---------------------------------------------------------------------------

async def enrich(state: ScoutState) -> dict:
    company_name = state["company_name"]
    email_format = state["email_format"]
    domain       = state["company_domain"]
    org_id       = state["company_org_id"]
    linkedin_raw = state["linkedin_raw"]
    perp_text    = state["perplexity_text"]

    # Extract additional LinkedIn URLs from Perplexity text
    perp_urls = re.findall(
        r'https?://(?:www\.)?linkedin\.com/in/[^\s\)\],>"\']+',
        perp_text,
    )

    # Build candidate list to enrich — cap at MAX_ENRICH
    to_enrich = [c for c in linkedin_raw if c.get("linkedin_url")][:MAX_ENRICH]
    seen_urls = {c["linkedin_url"] for c in to_enrich}
    for url in perp_urls:
        url = url.rstrip(".,;)")
        if url not in seen_urls and len(to_enrich) < MAX_ENRICH:
            to_enrich.append({
                "full_name": "", "role_title": "", "company": company_name,
                "linkedin_url": url, "linkedin_verified": False,
                "source": "web", "confidence": "low",
            })
            seen_urls.add(url)

    if not to_enrich:
        # No LinkedIn candidates — still try to return what we have without verification
        return {"candidates": []}

    sem = asyncio.Semaphore(MAX_VERIFY_CONCUR)

    async def _enrich_one(candidate: dict) -> dict:
        result = dict(candidate)
        result.setdefault("email", "")
        result.setdefault("email_status", "")
        result.setdefault("linkedin_status", "UNCONFIRMED")
        result.setdefault("employment_verified", "UNCERTAIN")
        result.setdefault("title_match", "UNKNOWN")
        result.setdefault("actual_title", "")

        async with sem:
            # If web-only (no LinkedIn URL), try name-based LinkedIn search first
            if not result.get("linkedin_url") and result.get("full_name"):
                try:
                    from backend.tools.unipile import search_person_by_name
                    candidates = await _safe(
                        search_person_by_name(result["full_name"], org_id=org_id, limit=5),
                        timeout=10, default=[], label="name_search",
                    )
                    for p in candidates:
                        if _names_match(p.get("full_name", ""), result["full_name"]):
                            result["linkedin_url"] = p["linkedin_url"]
                            result["source"] = "linkedin"
                            logger.info("scout_name_search_hit", name=result["full_name"], url=p["linkedin_url"])
                            break
                except Exception as e:
                    logger.warning("scout_name_search_error", error=str(e))

            # LinkedIn verification
            if result.get("linkedin_url"):
                try:
                    from backend.tools.unipile import verify_profile
                    pv = await _safe(
                        verify_profile(result["linkedin_url"], company_name),
                        timeout=VERIFY_TIMEOUT, default=None, label="verify_profile",
                    )
                    if pv and pv.get("valid"):
                        result["linkedin_verified"] = True
                        result["linkedin_status"] = "CONFIRMED"
                        if pv.get("full_name") and not result["full_name"]:
                            result["full_name"] = pv["full_name"]
                        if pv.get("current_role"):
                            result["actual_title"] = pv["current_role"]
                            if not result["role_title"]:
                                result["role_title"] = pv["current_role"]
                        result["title_match"] = "MATCH" if pv.get("at_target_company") else "MISMATCH"
                        result["employment_verified"] = (
                            "CONFIRMED"
                            if pv.get("still_employed") and pv.get("at_target_company")
                            else "UNCERTAIN"
                        )
                        if result["employment_verified"] == "CONFIRMED":
                            result["confidence"] = "high"
                except Exception as e:
                    logger.warning("scout_verify_error", error=str(e))

            # Email generation + ZeroBounce validation
            # Always write the generated email — Veri will re-validate via ZeroBounce
            if result.get("full_name") and (email_format or domain):
                first, last = _parse_name(result["full_name"])
                email = _apply_email_format(email_format, first, last, domain)
                if email and "@" in email:
                    result["email"] = email  # always write; let Veri validate
                    try:
                        from backend.tools.zerobounce import validate_email
                        zb = await _safe(
                            validate_email(email),
                            timeout=15, default={"status": "unknown"}, label="zerobounce",
                        )
                        result["email_status"] = zb.get("status", "unknown")
                    except Exception as e:
                        logger.warning("scout_zerobounce_error", error=str(e))
                        result["email_status"] = "unknown"

        return result

    enriched_results = await asyncio.gather(
        *[_enrich_one(c) for c in to_enrich],
        return_exceptions=True,
    )
    enriched_clean = [c for c in enriched_results if isinstance(c, dict) and c.get("full_name")]
    return {"candidates": enriched_clean}


# ---------------------------------------------------------------------------
# Node 6: synthesize
# ---------------------------------------------------------------------------

async def synthesize(state: ScoutState) -> dict:
    # Fast-path: greeting / non-search / no-company — message already set in parse_intent
    intent = state["intent"]
    if intent in ("greeting", "need_company", "not_search"):
        return {}  # message was set in parse_intent

    company_name         = state["company_name"]
    query                = state["query"]
    perp_text            = state["perplexity_text"]
    enriched             = state["candidates"]
    duplicates           = state["duplicates"]
    company_domain       = state.get("company_domain", "")
    company_account_type = state.get("company_account_type", "")
    company_account_size = state.get("company_account_size", "")

    # Build enriched section
    enriched_section = ""
    if enriched:
        enriched_section = "\n## Enriched LinkedIn contacts:\n"
        for c in enriched:
            enriched_section += (
                f"- {c['full_name']} | {c['role_title']} | "
                f"LI:{c.get('linkedin_status','?')} emp:{c.get('employment_verified','?')} "
                f"email:{c.get('email','—')}({c.get('email_status','?')}) "
                f"conf:{c.get('confidence','?')}\n"
            )

    dup_section = ""
    if duplicates:
        dup_section = "\n## Already in sheets:\n"
        for d in duplicates:
            dup_section += (
                f"- {d['full_name']} ({d['role_title']}) in {d['sheet_name']} "
                f"row {d['row_number']}"
                + (f", status: {d['overall_status']}" if d.get("overall_status") else "")
                + "\n"
            )

    synthesis_prompt = (
        "Expert B2B SDR assistant. Sales rep asked:\n"
        f"Company: {company_name} | Request: \"{query}\"\n"
        + dup_section
        + (f"\n## Perplexity web research:\n{perp_text[:1500]}\n" if perp_text else "")
        + enriched_section
        + "\n\n## Task:\n"
        "1. Extract ALL matching contacts. Prefer enriched/LinkedIn-verified over web-only.\n"
        "2. Merge duplicates by name — keep richest data.\n"
        "3. For web-only contacts not enriched, include with confidence=low.\n"
        "4. Assign buying_role: Decision Maker for C-suite/VP/SVP/EVP/Director/Head Of; else Influencer.\n"
        "5. Message: mention sheet duplicates as warnings. Be concise (2 sentences max).\n\n"
        "Return ONLY valid JSON (no markdown):\n"
        '{"message":"...","candidates":[{'
        '"full_name":"","role_title":"","company":"","linkedin_url":"","linkedin_verified":false,'
        '"linkedin_status":"CONFIRMED|UNCONFIRMED","employment_verified":"CONFIRMED|UNCERTAIN|REJECTED",'
        '"actual_title":"","email":"","email_status":"","buying_role":"Decision Maker|Influencer",'
        '"source":"linkedin|web","confidence":"high|medium|low"}]}'
    )

    candidates: list[dict] = []
    message = ""

    raw = await _safe(
        _bedrock_claude(synthesis_prompt, max_tokens=3000, temperature=0),
        timeout=30, default="{}", label="synthesize",
    )

    try:
        clean = re.sub(r'```(?:json)?\s*', '', raw).strip().rstrip('`').strip()
        m = re.search(r'\{.*\}', clean, re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
            message = parsed.get("message", "")
            for i, c in enumerate(parsed.get("candidates", [])):
                if not (isinstance(c, dict) and c.get("full_name")):
                    continue
                full_name = c.get("full_name", "")
                # Restore enriched data that Claude may have dropped
                enriched_match = next(
                    (e for e in enriched if _names_match(e.get("full_name", ""), full_name)),
                    None,
                )
                dup_match = next(
                    (d for d in duplicates if _names_match(d.get("full_name", ""), full_name)),
                    None,
                )
                merged: dict = {
                    "index": i,
                    "full_name": full_name,
                    "role_title": c.get("role_title", ""),
                    "company": c.get("company") or company_name,
                    "linkedin_url": c.get("linkedin_url") or "",
                    "linkedin_verified": bool(c.get("linkedin_verified", False)),
                    "linkedin_status": c.get("linkedin_status", "UNCONFIRMED"),
                    "employment_verified": c.get("employment_verified", "UNCERTAIN"),
                    "title_match": c.get("title_match", "UNKNOWN"),
                    "actual_title": c.get("actual_title", ""),
                    "email": c.get("email", ""),
                    "email_status": c.get("email_status", ""),
                    "buying_role": c.get("buying_role") or _buying_role(c.get("role_title", "")),
                    "source": c.get("source", "web"),
                    "confidence": c.get("confidence", "low"),
                    "group": "scout",
                    "is_new": True,
                    "exists_in_sheet": dup_match is not None,
                    "sheet_name": dup_match["sheet_name"] if dup_match else None,
                    "sheet_row": dup_match["row_number"] if dup_match else None,
                    # Company context from Target Accounts — needed for sheet commit
                    "company_domain": company_domain,
                    "company_account_type": company_account_type,
                    "company_account_size": company_account_size,
                }
                if enriched_match:
                    for field in ("linkedin_url", "linkedin_verified", "linkedin_status",
                                  "employment_verified", "title_match", "actual_title",
                                  "email", "email_status", "confidence"):
                        if enriched_match.get(field):
                            merged[field] = enriched_match[field]
                candidates.append(merged)
    except Exception as e:
        logger.warning("scout_synthesis_parse_error", error=str(e))

    # ── Post-synthesis enrichment ───────────────────────────────────────────
    # Candidates from Perplexity text (not LinkedIn search) may reach here
    # without linkedin_url or email.  Fill them in now so they reach the sheet.
    org_id       = state.get("company_org_id", "")
    email_format = state.get("email_format", "")
    domain       = company_domain

    async def _post_enrich(cand: dict) -> None:
        # 1. Missing LinkedIn URL → name-based search
        if not cand.get("linkedin_url") and cand.get("full_name"):
            try:
                from backend.tools.unipile import search_person_by_name
                results = await _safe(
                    search_person_by_name(cand["full_name"], org_id=org_id, limit=5),
                    timeout=10, default=[], label="post_synth_li_search",
                )
                for p in results:
                    if _names_match(p.get("full_name", ""), cand["full_name"]):
                        cand["linkedin_url"] = p.get("linkedin_url", "")
                        cand["linkedin_verified"] = True
                        cand["source"] = "linkedin"
                        logger.info("scout_post_synth_li_found", name=cand["full_name"], url=cand["linkedin_url"])
                        break
            except Exception as e:
                logger.warning("scout_post_synth_li_error", name=cand.get("full_name"), error=str(e))

        # 2. Missing email → generate from email_format + domain
        if not cand.get("email") and cand.get("full_name") and (email_format or domain):
            first, last = _parse_name(cand["full_name"])
            email = _apply_email_format(email_format, first, last, domain)
            if email and "@" in email:
                cand["email"] = email
                logger.info("scout_post_synth_email_gen", name=cand["full_name"], email=email)

    needs_enrichment = [c for c in candidates if not c.get("linkedin_url") or not c.get("email")]
    if needs_enrichment:
        logger.info("scout_post_synth_enriching", count=len(needs_enrichment))
        await asyncio.gather(*[_post_enrich(c) for c in needs_enrichment], return_exceptions=True)
    # ────────────────────────────────────────────────────────────────────────

    # Fallback message
    if not message:
        if candidates:
            message = f"Found {len(candidates)} contact{'s' if len(candidates) != 1 else ''} at {company_name}."
        else:
            message = (
                f"Searched LinkedIn and the web for \"{query}\" at {company_name} — "
                "no matches found. Try a different role title or verify the company name."
            )

    # Duplicate warning prefix
    if duplicates and "⚠" not in message:
        names = ", ".join(d["full_name"] for d in duplicates[:2])
        extra = f" +{len(duplicates)-2} more" if len(duplicates) > 2 else ""
        message = f"⚠ Already in your sheets: {names}{extra}. " + message

    return {"candidates": candidates, "message": message}


# ---------------------------------------------------------------------------
# Conditional routing
# ---------------------------------------------------------------------------

def _route_after_parse(state: ScoutState) -> Literal["check_duplicates", "synthesize"]:
    """Skip research entirely for greetings, non-search questions, and no-company queries."""
    if state["intent"] in ("greeting", "need_company", "not_search"):
        return "synthesize"
    return "check_duplicates"


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

def _build_graph() -> Any:
    g: StateGraph = StateGraph(ScoutState)  # type: ignore[type-arg]
    g.add_node("parse_intent", parse_intent)
    g.add_node("prepare",      prepare)      # dup-check + company-lookup in parallel
    g.add_node("research",     research)
    g.add_node("enrich",       enrich)
    g.add_node("synthesize",   synthesize)

    g.set_entry_point("parse_intent")
    g.add_conditional_edges(
        "parse_intent",
        _route_after_parse,
        {"check_duplicates": "prepare", "synthesize": "synthesize"},
    )
    g.add_edge("prepare",    "research")
    g.add_edge("research",   "enrich")
    g.add_edge("enrich",     "synthesize")
    g.add_edge("synthesize", END)

    return g.compile()


_scout_graph = _build_graph()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_scout(query: str, company: str, history: list[dict]) -> dict:
    """
    Run the AI Scout pipeline with a hard GLOBAL_TIMEOUT_SECS cap.
    Returns {"message": str, "candidates": list, "duplicates": list}.
    """
    initial: ScoutState = {
        **_empty_state(),
        "query": query,
        "company": company,
        "history": history,
    }
    try:
        final_state = await asyncio.wait_for(
            _scout_graph.ainvoke(initial),
            timeout=GLOBAL_TIMEOUT_SECS,
        )
        return {
            "message":    final_state.get("message", ""),
            "candidates": final_state.get("candidates", []),
            "duplicates": final_state.get("duplicates", []),
        }
    except asyncio.TimeoutError:
        logger.error("scout_global_timeout", query=query, company=company)
        return {
            "message": (
                "The search took too long — try a more specific company name or role. "
                "LinkedIn search is sometimes slow; please retry."
            ),
            "candidates": [],
            "duplicates": [],
        }
    except Exception as e:
        logger.error("scout_graph_error", error=str(e))
        return {
            "message": "Scout encountered an error. Please retry.",
            "candidates": [],
            "duplicates": [],
        }


async def commit_to_sheet(candidate: dict, company_context: dict | None = None) -> dict:
    """
    Write a fully enriched Scout candidate to First Clean List (cols A–P).
    Pre-checks for duplicates. Returns {"status": "ok"|"duplicate", "row": int, "sheet": str}.
    """
    full_name = candidate.get("full_name", "")
    company   = candidate.get("company", "")
    first, last = _parse_name(full_name)

    # Pre-write duplicate check (single source: First Clean List)
    try:
        fcl = await _safe(
            sheets.read_all_records(sheets.FIRST_CLEAN_LIST), timeout=8, default=[], label="commit_dup_fcl",
        )
        for i, row in enumerate(fcl, start=2):
            rc = row.get("Company Name", "")
            rf = row.get("First Name", "")
            rl = row.get("Last Name", "")
            rn = f"{rf} {rl}".strip()
            if _companies_match(rc, company) and _names_match(rn, full_name):
                return {
                    "status": "duplicate",
                    "row": i,
                    "sheet": "First Clean List",
                    "detail": f"{full_name} already exists in First Clean List at row {i}",
                }
    except Exception as e:
        logger.warning("scout_commit_dup_check_error", error=str(e))

    ctx          = company_context or {}
    role_title   = candidate.get("role_title", "")
    buying_role  = candidate.get("buying_role") or _buying_role(role_title)
    linkedin_url = candidate.get("linkedin_url", "")
    email        = candidate.get("email", "")

    # Safety net: generate email at commit time if still missing
    domain = ctx.get("domain", "")
    if not email and full_name and domain:
        # Try basic firstname.lastname@domain pattern as fallback
        f = re.sub(r"[^a-z]", "", _normalize(first))
        l = re.sub(r"[^a-z]", "", _normalize(last))
        if f and l:
            email = f"{f}.{l}@{domain}"
            logger.info("scout_commit_email_fallback", name=full_name, email=email)

    # Write cols A–P to First Clean List so Veri picks it up for full verification
    row = [
        company,                    # A  Company Name
        company,                    # B  Normalized Company Name
        ctx.get("domain", ""),      # C  Company Domain Name
        ctx.get("account_type",""), # D  Account Type
        ctx.get("account_size",""), # E  Account Size
        "",                         # F  Country
        first,                      # G  First Name
        last,                       # H  Last Name
        role_title,                 # I  Job Title (English)
        buying_role,                # J  Buying Role
        linkedin_url,               # K  LinkedIn URL
        email,                      # L  Email
        "",                         # M  Phone-1
        "",                         # N  Phone-2
        "scout",                    # O  Source
        "",                         # P  Pipeline Status
    ]

    await sheets.ensure_headers(sheets.FIRST_CLEAN_LIST, sheets.FIRST_CLEAN_LIST_HEADERS)
    written_row = await sheets.append_row(sheets.FIRST_CLEAN_LIST, row)
    logger.info("scout_committed_to_fcl", name=full_name, row=written_row)
    return {"status": "ok", "row": written_row, "sheet": sheets.FIRST_CLEAN_LIST}
