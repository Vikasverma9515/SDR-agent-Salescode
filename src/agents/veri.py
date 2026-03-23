"""
Veri - Contact QC Agent

Graph:
START -> read_final_list -> ddg_sweep -> theorg_check -> tavily_fallback
      -> perplexity_deep -> linkedin_audit -> zerobounce_validate
      -> score_and_label -> write_verification -> advance_or_finish -> (loop or END)

Scoring is fully deterministic — no LLM calls.
LinkedIn verification is via Unipile API (no OCR, no Playwright).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from langgraph.graph import StateGraph, END

from src.config import get_settings
from src.state import Contact, VeriState
from src.tools import sheets, zerobounce as zb
from src.tools import theorg
from src.tools.search import search
from src.utils.logging import get_logger

logger = get_logger("veri")

# Per-contact evidence accumulator (keyed by contact index)
_evidence: dict[int, dict] = {}

# Keywords in snippets that signal the person has LEFT this company
_STALE_SIGNALS = [
    "former ", "ex-", " left ", "previously at", "previously worked",
    "now at ", "now works at", "joined ", "has joined", "moved to",
    "departed", "resigned", "no longer", "used to be",
]


# ---------------------------------------------------------------------------
# Node: read_final_list
# ---------------------------------------------------------------------------

async def read_final_list(state: VeriState) -> VeriState:
    """Pull contacts from First Clean List sheet."""
    _evidence.clear()
    logger.info("veri_read_list", step="read_final_list", source=sheets.FIRST_CLEAN_LIST)

    try:
        raw_all = await sheets.read_all_rows(sheets.FIRST_CLEAN_LIST)
        records = await sheets.read_all_records(sheets.FIRST_CLEAN_LIST)

        if state.row_start is not None:
            start = state.row_start - 1
            end = state.row_end if state.row_end is not None else len(records)
            raw_all = raw_all[start:end]
            records = records[start:end]
            logger.info("veri_row_range", row_start=state.row_start, row_end=state.row_end, count=len(records))

        contacts = []
        raw_rows: dict[int, list] = {}

        for idx, (row, raw_row) in enumerate(zip(records, raw_all)):
            first = str(row.get("First Name", "") or "").strip()
            last = str(row.get("Last Name", "") or "").strip()
            full_name = f"{first} {last}".strip()

            if not full_name:
                continue

            phone1 = str(row.get("Phone-1", "") or "").strip()
            phone2 = str(row.get("Phone-2", "") or "").strip()
            phones = [p for p in [phone1, phone2] if p]

            buying_role = str(row.get("Buying Role", "") or "").strip() or "Unknown"
            role_bucket_map = {
                "Decision Maker": "DM", "DM": "DM", "Champion": "DM",
                "Influencer": "Influencer",
                "GateKeeper": "GateKeeper", "Gate Keeper": "GateKeeper", "End User": "GateKeeper",
            }
            role_bucket = role_bucket_map.get(buying_role, "Unknown")

            linkedin_url = (
                row.get("LinkedIn URL")
                or row.get("Linekdin Url")
                or row.get("LinkedIn Url")
                or None
            )
            if linkedin_url:
                linkedin_url = str(linkedin_url).strip() or None

            contact = Contact(
                full_name=full_name,
                company=str(row.get("Company Name", "") or "").strip(),
                domain=str(row.get("Company Domain Name", "") or "").strip(),
                role_title=str(row.get("Job titles (English)") or row.get("Job Title (English)") or "").strip() or None,
                role_bucket=role_bucket,
                linkedin_url=linkedin_url,
                linkedin_verified=False,
                email=str(row.get("Email") or "").strip() or None,
                email_status="pending",
                phones=phones,
                provenance=[],
                verification_status="PENDING",
            )
            contact_idx = len(contacts)
            contacts.append(contact)
            # Ensure exactly 14 scalar string values for cols A-N
            flat_row = [str(v) if v is not None else "" for v in raw_row]
            flat_row += [""] * max(0, 14 - len(flat_row))
            raw_rows[contact_idx] = flat_row[:14]

        logger.info("veri_contacts_loaded", count=len(contacts))
        await sheets.ensure_headers(sheets.FINAL_FILTERED_LIST, sheets.FINAL_FILTERED_LIST_HEADERS)
        return state.model_copy(update={"contacts": contacts, "current_index": 0, "raw_rows": raw_rows})

    except Exception as e:
        logger.error("veri_read_error", error=str(e))
        return state.model_copy(update={"status": "failed"})


# ---------------------------------------------------------------------------
# Node: ddg_sweep
# ---------------------------------------------------------------------------

async def ddg_sweep(state: VeriState) -> VeriState:
    """3 DDG queries per contact. Extracts positive co-occurrence and stale signals."""
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    logger.info("veri_ddg_sweep", contact=contact.full_name, company=contact.company)

    queries = [
        f'"{contact.full_name}" {contact.company}',
        f'"{contact.full_name}" {contact.role_title or ""} {contact.company}',
        f'"{contact.full_name}" LinkedIn {contact.company}',
    ]

    snippets = []
    for query in queries:
        results = await search(query, provider="ddg", max_results=5)
        snippets.extend([r.snippet for r in results])

    combined = "\n".join(snippets[:10])
    stale = _has_stale_signal(combined, contact.full_name, contact.company)
    positive = _has_positive_signal(combined, contact.full_name, contact.company)

    _evidence[state.current_index] = {
        "ddg": combined[:1000],
        "ddg_stale": stale,
        "ddg_positive": positive,
    }
    return state


# ---------------------------------------------------------------------------
# Node: theorg_check
# ---------------------------------------------------------------------------

async def theorg_check(state: VeriState) -> VeriState:
    """TheOrg org chart lookup."""
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    logger.info("veri_theorg_check", contact=contact.full_name)
    evidence = _evidence.setdefault(state.current_index, {})

    try:
        entry = await theorg.lookup_person(contact.full_name, contact.company)
        if entry:
            evidence["theorg_found"] = True
            evidence["theorg_title"] = entry.get("role_title", "")
            evidence["theorg_company"] = entry.get("company", "")
            evidence["theorg"] = f"Found on TheOrg: {entry['full_name']}, {entry.get('role_title','')} at {contact.company}"
        else:
            evidence["theorg_found"] = False
            evidence["theorg"] = "Not found on TheOrg"
    except Exception as e:
        evidence["theorg_found"] = False
        evidence["theorg"] = f"TheOrg error: {e}"

    return state


# ---------------------------------------------------------------------------
# Node: tavily_fallback
# ---------------------------------------------------------------------------

async def tavily_fallback(state: VeriState) -> VeriState:
    """Tavily search if DDG + TheOrg inconclusive."""
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    evidence = _evidence.get(state.current_index, {})

    # Skip if DDG already gave a clear result
    if evidence.get("ddg_positive") or evidence.get("ddg_stale"):
        evidence["tavily"] = "Skipped (DDG sufficient)"
        evidence["tavily_stale"] = evidence.get("ddg_stale", False)
        evidence["tavily_positive"] = evidence.get("ddg_positive", False)
        return state

    logger.info("veri_tavily", contact=contact.full_name)
    results = await search(
        f'"{contact.full_name}" {contact.company} {contact.role_title or ""}',
        provider="tavily",
        max_results=5,
    )
    combined = "\n".join(r.snippet for r in results[:5])
    evidence["tavily"] = combined[:500]
    evidence["tavily_stale"] = _has_stale_signal(combined, contact.full_name, contact.company)
    evidence["tavily_positive"] = _has_positive_signal(combined, contact.full_name, contact.company)
    return state


# ---------------------------------------------------------------------------
# Node: perplexity_deep
# ---------------------------------------------------------------------------

async def perplexity_deep(state: VeriState) -> VeriState:
    """Date-aware Perplexity search to catch recent role changes."""
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    logger.info("veri_perplexity", contact=contact.full_name)

    results = await search(
        f'"{contact.full_name}" current role {contact.company} 2024 2025',
        provider="perplexity",
        max_results=5,
    )
    combined = "\n".join(r.snippet for r in results[:5])
    evidence = _evidence.setdefault(state.current_index, {})
    evidence["perplexity"] = combined[:500]
    evidence["perplexity_stale"] = _has_stale_signal(combined, contact.full_name, contact.company)
    evidence["perplexity_positive"] = _has_positive_signal(combined, contact.full_name, contact.company)
    evidence["perplexity_recent"] = bool(re.search(r'\b(2024|2025)\b', combined))
    return state


# ---------------------------------------------------------------------------
# Node: linkedin_audit
# ---------------------------------------------------------------------------

async def linkedin_audit_node(state: VeriState) -> VeriState:
    """
    Verify LinkedIn profile via Unipile API.
    Returns structured work_experience data — current company, current role, still employed.
    No OCR, no Playwright.
    """
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    evidence = _evidence.setdefault(state.current_index, {})

    if not contact.linkedin_url:
        evidence["linkedin_audit"] = {"valid": False, "error": "No LinkedIn URL in sheet"}
        return state

    logger.info("veri_linkedin_audit", contact=contact.full_name, url=contact.linkedin_url)

    try:
        from src.tools.unipile import verify_profile
        audit = await verify_profile(contact.linkedin_url, contact.company)
        evidence["linkedin_audit"] = audit

        if audit["valid"]:
            contacts = list(state.contacts)
            contacts[state.current_index] = contact.model_copy(update={
                "linkedin_verified": audit["at_target_company"] and audit["still_employed"],
                # Update role_title with what LinkedIn actually shows (more current)
                "role_title": audit["current_role"] or contact.role_title,
            })
            return state.model_copy(update={"contacts": contacts})

    except Exception as e:
        evidence["linkedin_audit"] = {"valid": False, "error": str(e)}
        logger.warning("veri_linkedin_audit_error", contact=contact.full_name, error=str(e))

    return state


# ---------------------------------------------------------------------------
# Node: zerobounce_validate
# ---------------------------------------------------------------------------

async def zerobounce_validate(state: VeriState) -> VeriState:
    """Email validation — cached within run."""
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    evidence = _evidence.setdefault(state.current_index, {})

    if not contact.email:
        evidence["zerobounce"] = {"status": "no_email"}
        return state

    from src.config import get_settings as _get_settings
    if not _get_settings().zerobounce_api_key:
        evidence["zerobounce"] = {"status": "skipped_no_key"}
        return state

    logger.info("veri_zerobounce", contact=contact.full_name, email=contact.email)

    try:
        result = await zb.validate_email(contact.email)
        evidence["zerobounce"] = result

        valid_statuses = ["valid", "invalid", "catch-all", "unknown"]
        contacts = list(state.contacts)
        contacts[state.current_index] = contact.model_copy(update={
            "email_status": result["status"] if result["status"] in valid_statuses else "unknown",
            "zerobounce_score": result.get("score"),
        })
        return state.model_copy(update={"contacts": contacts})

    except Exception as e:
        evidence["zerobounce"] = {"status": "error", "error": str(e)}
        logger.warning("veri_zerobounce_error", error=str(e))

    return state


# ---------------------------------------------------------------------------
# Node: score_and_label — deterministic rule engine, no LLM
# ---------------------------------------------------------------------------

async def score_and_label(state: VeriState) -> VeriState:
    """
    Deterministic scoring using Unipile LinkedIn data as primary signal.

    Evidence priority:
    1. LinkedIn (Unipile) — current_company, current_role, at_target_company, still_employed
    2. ZeroBounce email status
    3. Web snippets (DDG / Tavily / Perplexity) for identity + stale signals
    4. TheOrg for identity confirmation

    Verdict:
      VERIFIED  — LinkedIn confirms at company + role reasonable + email deliverable
      REVIEW    — partial confirmation (LinkedIn ok but email bad, or no LinkedIn but web signals ok)
      REJECT    — LinkedIn shows different company, or stale signals from 2+ sources, or no evidence at all
    """
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    evidence = _evidence.get(state.current_index, {})
    logger.info("veri_score", contact=contact.full_name)

    audit = evidence.get("linkedin_audit", {})
    if isinstance(audit, str):
        try:
            audit = json.loads(audit)
        except Exception:
            audit = {}

    zb_result = evidence.get("zerobounce", {})
    if isinstance(zb_result, str):
        zb_result = {}

    identity, employment, role_match = _check_all(contact, audit, evidence)
    email_ok = zb_result.get("status") in ("valid", "catch-all")

    status, notes = _build_verdict(
        identity, employment, role_match, email_ok,
        contact, audit, zb_result, evidence,
    )

    timestamp = datetime.now(timezone.utc).isoformat()
    contacts = list(state.contacts)
    contacts[state.current_index] = contact.model_copy(update={
        "verification_status": status,
        "verification_notes": notes,
        "verification_timestamp": timestamp,
    })

    verified_count = state.verified_count + (1 if status == "VERIFIED" else 0)
    review_count = state.review_count + (1 if status == "REVIEW" else 0)
    rejected_count = state.rejected_count + (1 if status == "REJECT" else 0)

    logger.info("veri_scored", contact=contact.full_name, status=status,
                identity=identity, employment=employment, role_match=role_match,
                email_ok=email_ok)

    return state.model_copy(update={
        "contacts": contacts,
        "verified_count": verified_count,
        "review_count": review_count,
        "rejected_count": rejected_count,
    })


def _check_all(contact: Contact, audit: dict, evidence: dict) -> tuple[str, str, str]:
    """
    Returns (identity, employment, role_match) using Unipile data as primary source.

    identity:   CONFIRMED | UNCONFIRMED
    employment: CONFIRMED | UNCERTAIN | REJECTED
    role_match: MATCH | MISMATCH | UNKNOWN
    """
    li_valid = audit.get("valid", False)
    li_at_company = audit.get("at_target_company", False)
    li_still_employed = audit.get("still_employed", False)
    li_current_company = audit.get("current_company") or ""
    li_current_role = audit.get("current_role") or ""

    # ---- Identity ----
    # LinkedIn profile loaded = identity confirmed (we navigated to their specific profile URL)
    if li_valid:
        identity = "CONFIRMED"
    elif evidence.get("theorg_found"):
        identity = "CONFIRMED"
    else:
        positive_count = sum([
            bool(evidence.get("ddg_positive")),
            bool(evidence.get("tavily_positive")),
            bool(evidence.get("perplexity_positive")),
        ])
        identity = "CONFIRMED" if positive_count >= 2 else "UNCONFIRMED"

    # ---- Employment ----
    if li_valid:
        if li_at_company and li_still_employed:
            employment = "CONFIRMED"
        elif li_at_company and not li_still_employed:
            # Profile loaded but no current position found — possibly freelance/contractor gap
            employment = "UNCERTAIN"
        elif li_current_company and not li_at_company:
            # LinkedIn shows a DIFFERENT current company — clear stale signal
            employment = "REJECTED"
        else:
            # Profile loaded but no experience data returned (private profile)
            stale_count = sum([
                bool(evidence.get("ddg_stale")),
                bool(evidence.get("tavily_stale")),
                bool(evidence.get("perplexity_stale")),
            ])
            employment = "REJECTED" if stale_count >= 2 else "UNCERTAIN"
    else:
        # No LinkedIn data — rely purely on web snippets
        stale_count = sum([
            bool(evidence.get("ddg_stale")),
            bool(evidence.get("tavily_stale")),
            bool(evidence.get("perplexity_stale")),
        ])
        if stale_count >= 2:
            employment = "REJECTED"
        else:
            employment = "UNCERTAIN"

    # ---- Role match ----
    if not contact.role_title:
        role_match = "UNKNOWN"
    elif li_current_role:
        role_match = _compare_titles(contact.role_title, li_current_role)
    elif evidence.get("theorg_title"):
        role_match = _compare_titles(contact.role_title, evidence["theorg_title"])
    else:
        role_match = "UNKNOWN"

    return identity, employment, role_match


def _compare_titles(sheet_title: str, found_title: str) -> str:
    """Compare two job titles — MATCH, MISMATCH, or UNKNOWN."""
    stopwords = {"the", "a", "an", "and", "of", "for", "in", "at", "senior", "associate", "jr", "sr"}
    sheet_words = set(re.sub(r"[^a-z0-9 ]", "", sheet_title.lower()).split()) - stopwords
    found_words = set(re.sub(r"[^a-z0-9 ]", "", found_title.lower()).split()) - stopwords

    if not sheet_words or not found_words:
        return "UNKNOWN"

    overlap = len(sheet_words & found_words) / max(len(sheet_words), len(found_words))
    if overlap >= 0.4:
        return "MATCH"
    if _is_different_function(sheet_title, found_title):
        return "MISMATCH"
    return "UNKNOWN"


def _build_verdict(
    identity: str, employment: str, role_match: str,
    email_ok: bool, contact: Contact, audit: dict, zb_result: dict, evidence: dict,
) -> tuple[str, str]:
    """
    Build verdict + comprehensive human-readable notes.

    Notes include:
    - What LinkedIn showed (current company, current role, at_target_company)
    - Email status and ZeroBounce result
    - Web signal summary
    - TheOrg finding
    - Final reasoning
    """
    # --- Build evidence summary ---
    parts: list[str] = []

    # LinkedIn (primary)
    li_valid = audit.get("valid", False)
    li_company = audit.get("current_company") or ""
    li_role = audit.get("current_role") or ""
    li_at = audit.get("at_target_company", False)
    li_employed = audit.get("still_employed", False)
    li_error = audit.get("error") or ""

    if li_valid:
        li_summary = f"LinkedIn: profile loaded"
        if li_company:
            li_summary += f", current company='{li_company}'"
        if li_role:
            li_summary += f", current role='{li_role}'"
        li_summary += f", at_target={li_at}, still_employed={li_employed}"
        parts.append(li_summary)
    elif li_error:
        parts.append(f"LinkedIn: {li_error}")
    else:
        parts.append("LinkedIn: no URL or profile inaccessible")

    # Email
    zb_status = zb_result.get("status", "no_email")
    zb_score = zb_result.get("score")
    email_val = contact.email or "none"
    if zb_score is not None:
        parts.append(f"Email: {email_val} → {zb_status} (score={zb_score})")
    else:
        parts.append(f"Email: {email_val} → {zb_status}")

    # Web signals
    web_positives = [
        src for src in ["ddg", "tavily", "perplexity"]
        if evidence.get(f"{src}_positive")
    ]
    web_stales = [
        src for src in ["ddg", "tavily", "perplexity"]
        if evidence.get(f"{src}_stale")
    ]
    if web_positives:
        parts.append(f"Web confirms presence: {', '.join(web_positives)}")
    if web_stales:
        parts.append(f"Web stale signals: {', '.join(web_stales)}")
    if not web_positives and not web_stales:
        parts.append("Web: no strong signals")

    # TheOrg
    if evidence.get("theorg_found"):
        theorg_title = evidence.get("theorg_title", "")
        parts.append(f"TheOrg: found{f', title={theorg_title}' if theorg_title else ''}")

    # Role comparison
    if li_role and contact.role_title:
        parts.append(f"Role: sheet='{contact.role_title}' vs LinkedIn='{li_role}' → {role_match}")
    elif contact.role_title:
        parts.append(f"Role: sheet='{contact.role_title}' → {role_match} (no LinkedIn role to compare)")

    # --- Verdict ---
    evidence_str = " | ".join(parts)

    if identity == "UNCONFIRMED":
        if not email_ok:
            return "REJECT", f"REJECT: No profile or web confirmation found, email invalid. {evidence_str}"
        return "REVIEW", f"REVIEW: Email valid but no profile/web confirmation. {evidence_str}"

    # Identity CONFIRMED from here
    if employment == "REJECTED":
        if li_company and not li_at:
            return "REJECT", f"REJECT: LinkedIn shows person now at '{li_company}', not {contact.company}. {evidence_str}"
        return "REJECT", f"REJECT: Stale role — 2+ sources indicate person left {contact.company}. {evidence_str}"

    if employment == "CONFIRMED":
        if email_ok and role_match in ("MATCH", "UNKNOWN"):
            return "VERIFIED", f"VERIFIED: LinkedIn confirms at {contact.company}, email deliverable. {evidence_str}"
        if email_ok and role_match == "MISMATCH":
            return "REVIEW", f"REVIEW: At company but role changed (sheet='{contact.role_title}', now='{li_role}'). {evidence_str}"
        if not email_ok and role_match == "MATCH":
            return "REVIEW", f"REVIEW: LinkedIn confirmed but email {zb_status}. {evidence_str}"
        if not email_ok:
            return "REVIEW", f"REVIEW: LinkedIn confirmed but email {zb_status}. {evidence_str}"
        return "VERIFIED", f"VERIFIED: LinkedIn confirms at {contact.company}. {evidence_str}"

    # Employment UNCERTAIN
    if email_ok and identity == "CONFIRMED":
        return "REVIEW", f"REVIEW: Identity confirmed, email valid, but employment uncertain (LinkedIn private or no experience data). {evidence_str}"
    return "REVIEW", f"REVIEW: Insufficient evidence — LinkedIn inaccessible or private. {evidence_str}"


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _fuzzy_name_match(a: str, b: str) -> float:
    try:
        from rapidfuzz import fuzz
        return fuzz.token_sort_ratio(a.lower(), b.lower()) / 100.0
    except ImportError:
        tokens_a = set(a.lower().split())
        tokens_b = set(b.lower().split())
        if not tokens_a or not tokens_b:
            return 0.0
        return len(tokens_a & tokens_b) / max(len(tokens_a), len(tokens_b))


def _has_stale_signal(text: str, name: str, company: str) -> bool:
    text_lower = text.lower()
    name_parts = name.lower().split()
    if not any(part in text_lower for part in name_parts if len(part) > 3):
        return False
    for signal in _STALE_SIGNALS:
        if signal in text_lower:
            return True
    return False


def _has_positive_signal(text: str, name: str, company: str) -> bool:
    text_lower = text.lower()
    name_parts = name.lower().split()
    company_slug = re.sub(r"[^a-z0-9 ]", "", company.lower())
    company_words = [w for w in company_slug.split() if len(w) > 3]

    name_found = any(part in text_lower for part in name_parts if len(part) > 3)
    company_found = any(w in text_lower for w in company_words)
    return name_found and company_found


def _is_different_function(title_a: str, title_b: str) -> bool:
    functions = {
        "marketing": {"marketing", "brand", "growth", "digital", "content", "social", "cmi"},
        "finance": {"finance", "financial", "cfo", "accounting", "treasury", "controller"},
        "technology": {"technology", "cto", "engineering", "software", "it", "data"},
        "sales": {"sales", "revenue", "commercial", "business development", "bd"},
        "hr": {"hr", "human resources", "people", "talent", "chro", "recruiting"},
        "operations": {"operations", "supply chain", "logistics", "procurement", "coo"},
        "legal": {"legal", "compliance", "counsel", "regulatory"},
    }
    def get_func(title: str) -> str | None:
        t = title.lower()
        for func, keywords in functions.items():
            if any(k in t for k in keywords):
                return func
        return None

    func_a = get_func(title_a)
    func_b = get_func(title_b)
    return bool(func_a and func_b and func_a != func_b)


# ---------------------------------------------------------------------------
# Node: write_verification
# ---------------------------------------------------------------------------

async def write_verification(state: VeriState) -> VeriState:
    """
    Append verified contact as a new row to Final Filtered List.
    Cols A-N: copied from First Clean List (flat string values).
    Cols O-U: verification results.
    """
    if state.current_index >= len(state.contacts):
        return state

    contact = state.contacts[state.current_index]
    evidence = _evidence.get(state.current_index, {})
    logger.info("veri_write", contact=contact.full_name, status=contact.verification_status)

    try:
        audit = evidence.get("linkedin_audit", {})
        if isinstance(audit, str):
            try:
                audit = json.loads(audit)
            except Exception:
                audit = {}

        # Col O: LinkedIn Status
        linkedin_status = (
            "Verified" if contact.linkedin_verified
            else ("Found" if (contact.linkedin_url and audit.get("valid")) else
                  ("URL Present" if contact.linkedin_url else "Not Found"))
        )

        # Col P: Employment Verified
        employment_verified = "Yes" if (audit.get("at_target_company") and audit.get("still_employed")) else "No"

        # Col Q: Title Match
        actual_title = str(audit.get("current_role") or contact.role_title or "")
        if contact.role_title and actual_title:
            title_match = "Yes" if _compare_titles(contact.role_title, actual_title) == "MATCH" else "No"
        else:
            title_match = "Unknown"

        # Col R: Actual Title Found
        actual_title_col = actual_title[:200]

        # Col S: Overall Status
        overall_status = contact.verification_status

        # Col T: Verification Notes (comprehensive)
        notes = (contact.verification_notes or "")[:500]

        # Col U: Verified On
        verified_on = (
            contact.verification_timestamp[:10]
            if contact.verification_timestamp
            else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

        # raw_rows keys may be strings after JSON checkpoint roundtrip
        raw_cols_an = (
            state.raw_rows.get(state.current_index)
            or state.raw_rows.get(str(state.current_index))
            or []
        )
        # Ensure exactly 14 flat string values
        raw_cols_an = [str(v) if v is not None else "" for v in raw_cols_an]
        raw_cols_an += [""] * max(0, 14 - len(raw_cols_an))
        raw_cols_an = raw_cols_an[:14]

        verification_cols = [
            linkedin_status,       # O
            employment_verified,   # P
            title_match,           # Q
            actual_title_col,      # R
            overall_status,        # S
            notes,                 # T
            verified_on,           # U
        ]
        full_row = raw_cols_an + verification_cols  # exactly 21 flat strings

        row_num = await sheets.append_row(sheets.FINAL_FILTERED_LIST, full_row)
        logger.info("veri_write_ok", contact=contact.full_name, status=contact.verification_status, row=row_num)

    except Exception as e:
        import traceback
        logger.error("veri_write_error", contact=contact.full_name, error=str(e), traceback=traceback.format_exc())
        errors = list(state.errors) + [f"Write failed for {contact.full_name}: {e}"]
        return state.model_copy(update={"errors": errors})

    return state


# ---------------------------------------------------------------------------
# Node: advance_or_finish
# ---------------------------------------------------------------------------

def advance_or_finish(state: VeriState) -> VeriState:
    """Advance to next contact or mark completed."""
    next_index = state.current_index + 1

    if next_index >= len(state.contacts):
        logger.info(
            "veri_completed",
            total=len(state.contacts),
            verified=state.verified_count,
            review=state.review_count,
            rejected=state.rejected_count,
            errors=len(state.errors),
        )
        return state.model_copy(update={"status": "completed"})

    _evidence.pop(state.current_index, None)
    return state.model_copy(update={"current_index": next_index})


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def should_continue(state: VeriState) -> str:
    if state.status in ("completed", "failed"):
        return END
    if state.current_index >= len(state.contacts):
        return END
    return "ddg_sweep"


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


async def build_veri_graph():
    import aiosqlite
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    import os

    settings = get_settings()
    os.makedirs(os.path.dirname(settings.checkpoint_db), exist_ok=True)

    graph = StateGraph(VeriState)

    graph.add_node("read_final_list", read_final_list)
    graph.add_node("ddg_sweep", ddg_sweep)
    graph.add_node("theorg_check", theorg_check)
    graph.add_node("tavily_fallback", tavily_fallback)
    graph.add_node("perplexity_deep", perplexity_deep)
    graph.add_node("linkedin_audit", linkedin_audit_node)
    graph.add_node("zerobounce_validate", zerobounce_validate)
    graph.add_node("score_and_label", score_and_label)
    graph.add_node("write_verification", write_verification)
    graph.add_node("advance_or_finish", advance_or_finish)

    graph.set_entry_point("read_final_list")

    graph.add_edge("read_final_list", "ddg_sweep")
    graph.add_edge("ddg_sweep", "theorg_check")
    graph.add_edge("theorg_check", "tavily_fallback")
    graph.add_edge("tavily_fallback", "perplexity_deep")
    graph.add_edge("perplexity_deep", "linkedin_audit")
    graph.add_edge("linkedin_audit", "zerobounce_validate")
    graph.add_edge("zerobounce_validate", "score_and_label")
    graph.add_edge("score_and_label", "write_verification")
    graph.add_edge("write_verification", "advance_or_finish")
    graph.add_conditional_edges("advance_or_finish", should_continue)

    raw_conn = await aiosqlite.connect(settings.checkpoint_db)
    conn = AioSqliteConnectionWrapper(raw_conn)
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    serde = JsonPlusSerializer()
    checkpointer = AsyncSqliteSaver(conn, serde=serde)
    return graph.compile(checkpointer=checkpointer)
