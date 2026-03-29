"""
Veri - Contact QC Agent

Graph:
START -> read_final_list -> parallel_verify_all -> END

All contacts are verified concurrently (Semaphore(6)).
Within each contact, DDG queries, TheOrg, and Perplexity run in parallel.
LinkedIn + ZeroBounce run in parallel after web phase.
Sheet writes are protected by asyncio.Lock.

Scoring is fully deterministic — no LLM calls.
LinkedIn verification is via Unipile API (no OCR, no Playwright).
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone

from langgraph.graph import StateGraph, END

from backend.config import get_settings
from backend.state import Contact, VeriState
from backend.tools import sheets, zerobounce as zb
from backend.tools import theorg
from backend.tools.search import search
from backend.utils.logging import get_logger

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
# Node: parallel_verify_all
# ---------------------------------------------------------------------------

async def parallel_verify_all(state: VeriState) -> VeriState:
    """Verify all contacts concurrently (Semaphore(6) to respect rate limits)."""
    if not state.contacts:
        return state.model_copy(update={"status": "completed"})

    sem = asyncio.Semaphore(6)
    sheet_lock = asyncio.Lock()

    contacts = list(state.contacts)
    results: list[Contact | None] = [None] * len(contacts)
    errors: list[str] = []
    verified_count = 0
    review_count = 0
    rejected_count = 0

    async def _process(idx: int, contact: Contact) -> None:
        async with sem:
            try:
                verified = await _verify_one(idx, contact, state, sheet_lock)
                results[idx] = verified
            except Exception as e:
                logger.error("veri_contact_error", contact=contact.full_name, error=str(e))
                errors.append(f"Error verifying {contact.full_name}: {e}")
                results[idx] = contact

    await asyncio.gather(*[_process(i, c) for i, c in enumerate(contacts)])

    # Collect final contacts and counts
    final_contacts = []
    for contact in results:
        if contact is None:
            continue
        final_contacts.append(contact)
        if contact.verification_status == "VERIFIED":
            verified_count += 1
        elif contact.verification_status == "REVIEW":
            review_count += 1
        elif contact.verification_status == "REJECT":
            rejected_count += 1

    logger.info(
        "veri_completed",
        total=len(final_contacts),
        verified=verified_count,
        review=review_count,
        rejected=rejected_count,
        errors=len(errors),
    )

    return state.model_copy(update={
        "contacts": final_contacts,
        "verified_count": verified_count,
        "review_count": review_count,
        "rejected_count": rejected_count,
        "status": "completed",
        "errors": errors,
    })


async def _verify_one(
    idx: int,
    contact: Contact,
    state: VeriState,
    sheet_lock: asyncio.Lock,
) -> Contact:
    """Full verification pipeline for a single contact."""
    logger.info("veri_start", contact=contact.full_name, company=contact.company)
    _evidence[idx] = {}

    # --- Phase 1: Web searches in parallel ---
    # DDG: 3 queries concurrently
    queries = [
        f'"{contact.full_name}" {contact.company}',
        f'"{contact.full_name}" {contact.role_title or ""} {contact.company}'.strip(),
        f'"{contact.full_name}" LinkedIn {contact.company}',
    ]

    async def _ddg_query(q: str):
        try:
            return await search(q, provider="ddg", max_results=5)
        except Exception:
            return []

    async def _theorg_lookup():
        try:
            return await theorg.lookup_person(contact.full_name, contact.company)
        except Exception:
            return None

    async def _perplexity_search():
        try:
            return await search(
                f'"{contact.full_name}" current role {contact.company} 2024 2025',
                provider="perplexity",
                max_results=5,
            )
        except Exception:
            return []

    # Run DDG queries, TheOrg, and Perplexity all in parallel
    ddg_result_sets, theorg_entry, perplexity_results = await asyncio.gather(
        asyncio.gather(*[_ddg_query(q) for q in queries]),
        _theorg_lookup(),
        _perplexity_search(),
    )

    # Process DDG results
    snippets = []
    for result_set in ddg_result_sets:
        snippets.extend([r.snippet for r in result_set])
    combined_ddg = "\n".join(snippets[:10])
    ddg_stale = _has_stale_signal(combined_ddg, contact.full_name, contact.company)
    ddg_positive = _has_positive_signal(combined_ddg, contact.full_name, contact.company)

    evidence = _evidence[idx]
    evidence.update({
        "ddg": combined_ddg[:1000],
        "ddg_stale": ddg_stale,
        "ddg_positive": ddg_positive,
    })

    # Process TheOrg results
    if theorg_entry:
        evidence["theorg_found"] = True
        evidence["theorg_title"] = theorg_entry.get("role_title", "")
        evidence["theorg_company"] = theorg_entry.get("company", "")
        evidence["theorg"] = f"Found on TheOrg: {theorg_entry['full_name']}, {theorg_entry.get('role_title','')} at {contact.company}"
    else:
        evidence["theorg_found"] = False
        evidence["theorg"] = "Not found on TheOrg"

    # Process Perplexity results
    combined_perplexity = "\n".join(r.snippet for r in perplexity_results[:5])
    evidence["perplexity"] = combined_perplexity[:500]
    evidence["perplexity_stale"] = _has_stale_signal(combined_perplexity, contact.full_name, contact.company)
    evidence["perplexity_positive"] = _has_positive_signal(combined_perplexity, contact.full_name, contact.company)
    evidence["perplexity_recent"] = bool(re.search(r'\b(2024|2025)\b', combined_perplexity))

    # Tavily fallback if DDG was inconclusive
    if not ddg_positive and not ddg_stale:
        try:
            logger.info("veri_tavily", contact=contact.full_name)
            tavily_results = await search(
                f'"{contact.full_name}" {contact.company} {contact.role_title or ""}',
                provider="tavily",
                max_results=5,
            )
            combined_tavily = "\n".join(r.snippet for r in tavily_results[:5])
            evidence["tavily"] = combined_tavily[:500]
            evidence["tavily_stale"] = _has_stale_signal(combined_tavily, contact.full_name, contact.company)
            evidence["tavily_positive"] = _has_positive_signal(combined_tavily, contact.full_name, contact.company)
        except Exception as e:
            evidence["tavily"] = f"Tavily error: {e}"
            evidence["tavily_stale"] = False
            evidence["tavily_positive"] = False
    else:
        evidence["tavily"] = "Skipped (DDG sufficient)"
        evidence["tavily_stale"] = ddg_stale
        evidence["tavily_positive"] = ddg_positive

    # --- Phase 2: LinkedIn + ZeroBounce in parallel ---
    async def _linkedin_audit():
        if not contact.linkedin_url:
            return {"valid": False, "error": "No LinkedIn URL in sheet"}
        try:
            from backend.tools.unipile import verify_profile
            return await verify_profile(contact.linkedin_url, contact.company)
        except Exception as e:
            logger.warning("veri_linkedin_audit_error", contact=contact.full_name, error=str(e))
            return {"valid": False, "error": str(e)}

    async def _zerobounce_check():
        if not contact.email:
            return {"status": "no_email"}
        from backend.config import get_settings as _gs
        if not _gs().zerobounce_api_key:
            return {"status": "skipped_no_key"}
        try:
            return await zb.validate_email(contact.email)
        except Exception as e:
            logger.warning("veri_zerobounce_error", error=str(e))
            return {"status": "error", "error": str(e)}

    logger.info("veri_linkedin_zerobounce", contact=contact.full_name)
    audit, zb_result = await asyncio.gather(_linkedin_audit(), _zerobounce_check())

    evidence["linkedin_audit"] = audit
    evidence["zerobounce"] = zb_result

    # Update contact with LinkedIn and ZeroBounce findings
    linkedin_verified = bool(
        audit.get("valid") and audit.get("at_target_company") and audit.get("still_employed")
    )
    updated_role = audit.get("current_role") or contact.role_title

    valid_statuses = ["valid", "invalid", "catch-all", "unknown"]
    email_status = zb_result.get("status") if zb_result.get("status") in valid_statuses else "unknown"

    contact = contact.model_copy(update={
        "linkedin_verified": linkedin_verified,
        "role_title": updated_role,
        "email_status": email_status,
        "zerobounce_score": zb_result.get("score"),
    })

    # --- Phase 3: Score and label ---
    identity, employment, role_match = _check_all(contact, audit, evidence)
    email_ok = zb_result.get("status") in ("valid", "catch-all")
    status, notes = _build_verdict(identity, employment, role_match, email_ok, contact, audit, zb_result, evidence)

    timestamp = datetime.now(timezone.utc).isoformat()
    contact = contact.model_copy(update={
        "verification_status": status,
        "verification_notes": notes,
        "verification_timestamp": timestamp,
    })

    logger.info("veri_scored", contact=contact.full_name, status=status,
                identity=identity, employment=employment, role_match=role_match, email_ok=email_ok)

    # --- Phase 4: Write to Final Filtered List (serialized via lock) ---
    async with sheet_lock:
        try:
            li_valid = audit.get("valid", False)
            li_at = audit.get("at_target_company", False)
            li_employed = audit.get("still_employed", False)
            li_role = audit.get("current_role") or ""

            linkedin_status = (
                "Verified" if contact.linkedin_verified
                else ("Found" if (contact.linkedin_url and li_valid) else
                      ("URL Present" if contact.linkedin_url else "Not Found"))
            )
            employment_verified = "Yes" if (li_at and li_employed) else "No"
            actual_title = str(li_role or contact.role_title or "")
            if contact.role_title and actual_title:
                title_match = "Yes" if _compare_titles(contact.role_title, actual_title) == "MATCH" else "No"
            else:
                title_match = "Unknown"

            raw_cols_an = (
                state.raw_rows.get(idx)
                or state.raw_rows.get(str(idx))
                or []
            )
            raw_cols_an = [str(v) if v is not None else "" for v in raw_cols_an]
            raw_cols_an += [""] * max(0, 14 - len(raw_cols_an))
            raw_cols_an = raw_cols_an[:14]

            verification_cols = [
                linkedin_status,
                employment_verified,
                title_match,
                actual_title[:200],
                contact.verification_status,
                (contact.verification_notes or "")[:500],
                (contact.verification_timestamp or "")[:10],
            ]
            full_row = raw_cols_an + verification_cols

            row_num = await sheets.append_row(sheets.FINAL_FILTERED_LIST, full_row)
            logger.info("veri_write_ok", contact=contact.full_name, status=contact.verification_status, row=row_num)

        except Exception as e:
            import traceback
            logger.error("veri_write_error", contact=contact.full_name, error=str(e),
                         traceback=traceback.format_exc())

    _evidence.pop(idx, None)
    return contact


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

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
            employment = "UNCERTAIN"
        elif li_current_company and not li_at_company:
            employment = "REJECTED"
        else:
            stale_count = sum([
                bool(evidence.get("ddg_stale")),
                bool(evidence.get("tavily_stale")),
                bool(evidence.get("perplexity_stale")),
            ])
            employment = "REJECTED" if stale_count >= 2 else "UNCERTAIN"
    else:
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
    """Build verdict + comprehensive human-readable notes."""
    parts: list[str] = []

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

    zb_status = zb_result.get("status", "no_email")
    zb_score = zb_result.get("score")
    email_val = contact.email or "none"
    if zb_score is not None:
        parts.append(f"Email: {email_val} → {zb_status} (score={zb_score})")
    else:
        parts.append(f"Email: {email_val} → {zb_status}")

    web_positives = [s for s in ["ddg", "tavily", "perplexity"] if evidence.get(f"{s}_positive")]
    web_stales = [s for s in ["ddg", "tavily", "perplexity"] if evidence.get(f"{s}_stale")]
    if web_positives:
        parts.append(f"Web confirms presence: {', '.join(web_positives)}")
    if web_stales:
        parts.append(f"Web stale signals: {', '.join(web_stales)}")
    if not web_positives and not web_stales:
        parts.append("Web: no strong signals")

    if evidence.get("theorg_found"):
        theorg_title = evidence.get("theorg_title", "")
        parts.append(f"TheOrg: found{f', title={theorg_title}' if theorg_title else ''}")

    if li_role and contact.role_title:
        parts.append(f"Role: sheet='{contact.role_title}' vs LinkedIn='{li_role}' → {role_match}")
    elif contact.role_title:
        parts.append(f"Role: sheet='{contact.role_title}' → {role_match} (no LinkedIn role to compare)")

    evidence_str = " | ".join(parts)

    if identity == "UNCONFIRMED":
        if not email_ok:
            return "REJECT", f"REJECT: No profile or web confirmation found, email invalid. {evidence_str}"
        return "REVIEW", f"REVIEW: Email valid but no profile/web confirmation. {evidence_str}"

    if employment == "REJECTED":
        if li_company and not li_at:
            return "REJECT", f"REJECT: LinkedIn shows person now at '{li_company}', not {contact.company}. {evidence_str}"
        return "REJECT", f"REJECT: Stale role — 2+ sources indicate person left {contact.company}. {evidence_str}"

    if employment == "CONFIRMED":
        if email_ok and role_match in ("MATCH", "UNKNOWN"):
            return "VERIFIED", f"VERIFIED: LinkedIn confirms at {contact.company}, email deliverable. {evidence_str}"
        if email_ok and role_match == "MISMATCH":
            return "REVIEW", f"REVIEW: At company but role changed (sheet='{contact.role_title}', now='{li_role}'). {evidence_str}"
        if not email_ok:
            return "REVIEW", f"REVIEW: LinkedIn confirmed but email {zb_status}. {evidence_str}"
        return "VERIFIED", f"VERIFIED: LinkedIn confirms at {contact.company}. {evidence_str}"

    if email_ok and identity == "CONFIRMED":
        return "REVIEW", f"REVIEW: Identity confirmed, email valid, but employment uncertain. {evidence_str}"
    return "REVIEW", f"REVIEW: Insufficient evidence — LinkedIn inaccessible or private. {evidence_str}"


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


def _has_stale_signal(text: str, name: str, _company: str) -> bool:
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
    os.makedirs(os.path.dirname(settings.checkpoint_db_abs), exist_ok=True)

    graph = StateGraph(VeriState)

    graph.add_node("read_final_list", read_final_list)
    graph.add_node("parallel_verify_all", parallel_verify_all)

    graph.set_entry_point("read_final_list")
    graph.add_edge("read_final_list", "parallel_verify_all")
    graph.add_edge("parallel_verify_all", END)

    raw_conn = await aiosqlite.connect(settings.checkpoint_db_abs)
    conn = AioSqliteConnectionWrapper(raw_conn)
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    serde = JsonPlusSerializer()
    checkpointer = AsyncSqliteSaver(conn, serde=serde)
    return graph.compile(checkpointer=checkpointer)
