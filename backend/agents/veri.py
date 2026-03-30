"""
Veri - Contact QC Agent  (v2 — multi-step cross-reasoning)

Pipeline per contact
────────────────────
Phase 0  LinkedIn discovery  — if no URL in sheet, search Unipile by name+company
Phase 1  Web intelligence    — DDG ×3 + Perplexity (structured role query) + TheOrg
                               Tavily fallback when DDG is inconclusive
Phase 2  LinkedIn audit      — Unipile verify_profile (confirms identity, employment,
                               current title) — runs in parallel with ZeroBounce
Phase 3  LLM cross-reasoning — gpt-4.1-mini reads all evidence and produces
                               identity / employment / title_match verdicts +
                               a short explanation
Phase 4  Verdict + routing   — deterministic rules on the three signals + email:
         VERIFIED  → Final Filtered List
         REVIEW    → Final Filtered List  (flagged)
         REJECT    → Rejected Profiles tab

Routing rules (strict):
  • LinkedIn confirmed at company + title MISMATCH (different dept) → REJECT
  • LinkedIn shows person moved to another company → REJECT
  • No identity confirmation AND bad email → REJECT
  • Everything else → VERIFIED or REVIEW based on evidence strength

Concurrency: Semaphore(6) per contact; sheet writes serialised via asyncio.Lock.
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
from backend.tools.llm import llm_complete
from backend.tools.search import search
from backend.utils.logging import get_logger
from backend.utils.progress import emit_veri_contact, emit_veri_step, emit_system_warning

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
    """
    Pull contacts from Final Filtered List.
    Tracks the actual sheet row number of each contact so we can update cols O-U in place.
    """
    _evidence.clear()
    logger.info("veri_read_list", step="read_final_list", source=sheets.FINAL_FILTERED_LIST)

    try:
        raw_all = await sheets.read_all_rows(sheets.FINAL_FILTERED_LIST)
        records = await sheets.read_all_records(sheets.FINAL_FILTERED_LIST)

        # row_offset: 0-based index of the first record we process.
        # records[i] = sheet row (i + 2)  — header is row 1, first data row is row 2.
        # So to map records[idx] → sheet_row:  sheet_row = row_offset + idx + 2
        # For row_start=1123: start_index = 1123 - 2 = 1121, row_offset = 1121
        # → sheet_row_nums[0] = 1121 + 0 + 2 = 1123 ✓
        row_offset = (state.row_start - 2) if state.row_start is not None else 0

        if state.row_start is not None:
            start = state.row_start - 2          # 0-based index of first wanted row
            end   = (state.row_end - 1) if state.row_end is not None else len(records)  # inclusive end → exclusive slice
            raw_all = raw_all[start:end]
            records = records[start:end]
            logger.info("veri_row_range", row_start=state.row_start, row_end=state.row_end, count=len(records))

        contacts = []
        raw_rows: dict[int, list] = {}
        sheet_row_nums: dict[int, int] = {}

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

            # Store A-N cols for copying to Rejected Profiles if needed
            flat_row = [str(v) if v is not None else "" for v in raw_row]
            flat_row += [""] * max(0, 14 - len(flat_row))
            raw_rows[contact_idx] = flat_row[:14]

            # Actual 1-based sheet row: header=row1, first data row=row2
            sheet_row_nums[contact_idx] = row_offset + idx + 2

        logger.info("veri_contacts_loaded", count=len(contacts),
                    row_range=f"{min(sheet_row_nums.values(), default=0)}–{max(sheet_row_nums.values(), default=0)}")

        # Ensure Rejected Profiles tab exists with correct headers
        await sheets.ensure_headers(sheets.REJECTED_PROFILES, sheets.REJECTED_PROFILES_HEADERS)

        return state.model_copy(update={
            "contacts": contacts,
            "current_index": 0,
            "raw_rows": raw_rows,
            "sheet_row_nums": sheet_row_nums,
        })

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

    # ── ZeroBounce credit check ──────────────────────────────────────────────
    zb_credits = await zb.check_credits_upfront()
    if zb_credits == 0:
        await emit_system_warning(
            state.thread_id,
            "zb_no_credits",
            "ZeroBounce has 0 credits — email validation is disabled for this run. "
            "Contacts are verified by LinkedIn signals only. "
            "Refill credits at zerobounce.com to re-enable email filtering.",
        )
        logger.warning("veri_zb_disabled", reason="0 credits")
    elif zb_credits > 0:
        logger.info("veri_zb_credits", remaining=zb_credits)

    sem = asyncio.Semaphore(6)
    sheet_lock = asyncio.Lock()
    rejected_sheet_rows: list[int] = []  # Collect REJECT row nums for batch delete

    contacts = list(state.contacts)
    results: list[Contact | None] = [None] * len(contacts)
    errors: list[str] = []
    verified_count = 0
    review_count = 0
    rejected_count = 0

    async def _process(idx: int, contact: Contact) -> None:
        if state.thread_id:
            from backend.utils.pause import await_if_paused
            await await_if_paused(state.thread_id)
        async with sem:
            try:
                verified = await _verify_one(idx, contact, state, sheet_lock, rejected_sheet_rows)
                results[idx] = verified
            except Exception as e:
                logger.error("veri_contact_error", contact=contact.full_name, error=str(e))
                errors.append(f"Error verifying {contact.full_name}: {e}")
                results[idx] = contact

    await asyncio.gather(*[_process(i, c) for i, c in enumerate(contacts)])

    # Delete REJECT rows from Final Filtered List in reverse order so indices don't shift
    if rejected_sheet_rows:
        try:
            logger.info("veri_deleting_reject_rows", count=len(rejected_sheet_rows), rows=sorted(rejected_sheet_rows, reverse=True))
            await sheets.delete_rows_batch(sheets.FINAL_FILTERED_LIST, rejected_sheet_rows)
            logger.info("veri_reject_rows_deleted", count=len(rejected_sheet_rows))
        except Exception as e:
            logger.error("veri_delete_rows_error", error=str(e))
            errors.append(f"Failed to delete rejected rows from Final Filtered List: {e}")

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


# ---------------------------------------------------------------------------
# Core verification pipeline
# ---------------------------------------------------------------------------

async def _verify_one(
    idx: int,
    contact: Contact,
    state: VeriState,
    sheet_lock: asyncio.Lock,
    rejected_sheet_rows: list[int],
) -> Contact:
    """Full multi-phase verification pipeline for a single contact."""
    logger.info("veri_start", contact=contact.full_name, company=contact.company)
    _evidence[idx] = {}
    evidence = _evidence[idx]
    await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "queued")

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 0: LinkedIn URL discovery (if not already present in sheet)
    # ─────────────────────────────────────────────────────────────────────────
    if not contact.linkedin_url:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "linkedin_discovery", "no URL in sheet — searching Unipile by name", "info")
        try:
            from backend.tools.unipile import search_person_by_name
            candidates = await search_person_by_name(contact.full_name, limit=5)
            for cand in candidates:
                if _fuzzy_name_match(cand.get("full_name", ""), contact.full_name) >= 0.85:
                    contact = contact.model_copy(update={"linkedin_url": cand.get("linkedin_url")})
                    evidence["linkedin_url_discovered"] = cand.get("linkedin_url")
                    logger.info("veri_linkedin_discovered", contact=contact.full_name,
                                url=contact.linkedin_url)
                    await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                        "web", "linkedin_discovery", f"URL found → {contact.linkedin_url}", "success")
                    break
            else:
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "web", "linkedin_discovery", "no LinkedIn URL found via name search", "warning")
        except Exception as e:
            logger.warning("veri_linkedin_discovery_error", contact=contact.full_name, error=str(e))
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "web", "linkedin_discovery", f"search error: {e}", "error")

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 1: Web intelligence — DDG ×3 + Perplexity (role extraction) + TheOrg
    # ─────────────────────────────────────────────────────────────────────────
    await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "web")

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
        # Structured query to extract current role — much more targeted than generic search
        role_query = (
            f'What is {contact.full_name}\'s current job title and employer as of 2025? '
            f'Are they currently working at {contact.company}? '
            f'What department do they work in?'
        )
        try:
            return await search(role_query, provider="perplexity", max_results=5)
        except Exception:
            return []

    ddg_result_sets, theorg_entry, perplexity_results = await asyncio.gather(
        asyncio.gather(*[_ddg_query(q) for q in queries]),
        _theorg_lookup(),
        _perplexity_search(),
    )

    # Process DDG
    snippets = []
    for result_set in ddg_result_sets:
        snippets.extend([r.snippet for r in result_set])
    combined_ddg = "\n".join(snippets[:10])
    ddg_stale = _has_stale_signal(combined_ddg, contact.full_name, contact.company)
    ddg_positive = _has_positive_signal(combined_ddg, contact.full_name, contact.company)

    evidence.update({
        "ddg": combined_ddg[:1000],
        "ddg_stale": ddg_stale,
        "ddg_positive": ddg_positive,
    })

    if ddg_positive:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "ddg", f"positive — name + company found across {len(snippets)} snippets", "success")
    elif ddg_stale:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "ddg", f"stale signals detected — may have left {contact.company}", "warning")
    else:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "ddg", f"inconclusive — {len(snippets)} snippets, no clear signal", "info")

    # Process TheOrg
    if theorg_entry:
        evidence["theorg_found"] = True
        evidence["theorg_title"] = theorg_entry.get("role_title", "")
        evidence["theorg_company"] = theorg_entry.get("company", "")
        evidence["theorg"] = f"Found on TheOrg: {theorg_entry['full_name']}, {theorg_entry.get('role_title','')} at {contact.company}"
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "theorg",
            f"found · title='{evidence['theorg_title']}' · company='{evidence['theorg_company']}'",
            "success")
    else:
        evidence["theorg_found"] = False
        evidence["theorg"] = "Not found on TheOrg"
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "theorg", "not found in org chart", "info")

    # Process Perplexity — also try to extract role text for title comparison
    combined_perplexity = "\n".join(r.snippet for r in perplexity_results[:5])
    evidence["perplexity"] = combined_perplexity[:800]
    evidence["perplexity_stale"] = _has_stale_signal(combined_perplexity, contact.full_name, contact.company)
    evidence["perplexity_positive"] = _has_positive_signal(combined_perplexity, contact.full_name, contact.company)
    evidence["perplexity_recent"] = bool(re.search(r'\b(2024|2025)\b', combined_perplexity))

    if evidence["perplexity_positive"]:
        recent_tag = " · 2024/25 mention" if evidence["perplexity_recent"] else ""
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "perplexity", f"confirms presence at {contact.company}{recent_tag}", "success")
    elif evidence["perplexity_stale"]:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "perplexity", f"stale signals — may have left {contact.company}", "warning")
    else:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "perplexity", "no strong signals (inconclusive)", "info")

    # Tavily fallback if DDG was inconclusive
    if not ddg_positive and not ddg_stale:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "tavily", "DDG inconclusive — triggering Tavily deep search", "info")
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
            if evidence["tavily_positive"]:
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "web", "tavily", f"confirms presence at {contact.company}", "success")
            elif evidence["tavily_stale"]:
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "web", "tavily", f"stale signals — may have left", "warning")
            else:
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "web", "tavily", "no strong signals", "info")
        except Exception as e:
            evidence["tavily"] = f"Tavily error: {e}"
            evidence["tavily_stale"] = False
            evidence["tavily_positive"] = False
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "web", "tavily", f"error: {e}", "error")
    else:
        evidence["tavily"] = "Skipped (DDG sufficient)"
        evidence["tavily_stale"] = ddg_stale
        evidence["tavily_positive"] = ddg_positive

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 2: LinkedIn audit + ZeroBounce in parallel
    # ─────────────────────────────────────────────────────────────────────────
    await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "linkedin_zb")

    async def _linkedin_audit():
        if not contact.linkedin_url:
            return {"valid": False, "error": "No LinkedIn URL in sheet or discovered"}
        try:
            from backend.tools.unipile import verify_profile
            return await verify_profile(contact.linkedin_url, contact.company)
        except Exception as e:
            logger.warning("veri_linkedin_audit_error", contact=contact.full_name, error=str(e))
            return {"valid": False, "error": str(e)}

    async def _zerobounce_check():
        if not contact.email:
            return {"status": "no_email"}
        if not get_settings().zerobounce_api_key:
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

    # Emit LinkedIn result
    if audit.get("valid"):
        li_c = audit.get("current_company", "")
        li_r = audit.get("current_role", "")
        li_at = audit.get("at_target_company", False)
        li_emp = audit.get("still_employed", False)
        li_level = "success" if (li_at and li_emp) else "warning"
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "linkedin",
            f"profile loaded · company='{li_c}' · role='{li_r}' · at_target={li_at} · employed={li_emp}",
            li_level)
    elif audit.get("error"):
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "linkedin", f"error: {audit['error']}", "error")
    else:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "linkedin", "profile not accessible or no URL provided", "warning")

    # Emit ZeroBounce result
    zb_email = contact.email or "none"
    zb_st = zb_result.get("status", "no_email")
    zb_sc = zb_result.get("score")
    zb_score_tag = f" · score={zb_sc}" if zb_sc is not None else ""
    if zb_st in ("valid", "catch-all"):
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "zerobounce", f"{zb_email} → {zb_st}{zb_score_tag}", "success")
    elif zb_st == "no_email":
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "zerobounce", "no email address in sheet", "warning")
    elif zb_st == "skipped_no_key":
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "zerobounce", "skipped — no ZeroBounce API key configured", "info")
    else:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_zb", "zerobounce", f"{zb_email} → {zb_st}{zb_score_tag}", "error")

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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 3: LLM cross-reasoning — synthesise all signals
    # ─────────────────────────────────────────────────────────────────────────
    await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "scoring")

    li_valid = audit.get("valid", False)
    li_current_role = audit.get("current_role") or ""
    sheet_title = (updated_role or contact.role_title or "")

    # Title comparison: use LLM for semantic accuracy when both titles exist
    _title_compared = False
    if li_valid and li_current_role and sheet_title and li_current_role != sheet_title:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "scoring", "llm_title",
            f"comparing: '{sheet_title}' vs '{li_current_role}'", "info")
        role_match = await _llm_compare_titles(sheet_title, li_current_role, contact.company)
        _title_compared = True
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "scoring", "llm_title",
            f"→ {role_match}",
            "success" if role_match == "MATCH" else ("error" if role_match == "MISMATCH" else "info"))
    elif evidence.get("theorg_title") and sheet_title:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "scoring", "llm_title",
            f"comparing (TheOrg): '{sheet_title}' vs '{evidence['theorg_title']}'", "info")
        role_match = await _llm_compare_titles(sheet_title, evidence["theorg_title"], contact.company)
        _title_compared = True
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "scoring", "llm_title",
            f"→ {role_match}",
            "success" if role_match == "MATCH" else ("error" if role_match == "MISMATCH" else "info"))
    else:
        _, _, role_match = _check_all_fast(contact, audit, evidence)
        if not _title_compared and sheet_title:
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "scoring", "llm_title",
                f"no second title to compare — role_match={role_match}", "info")

    # Full identity + employment check
    identity, employment, _ = _check_all_fast(contact, audit, evidence)
    await emit_veri_step(state.thread_id, contact.full_name, contact.company,
        "scoring", "signals",
        f"identity={identity} · employment={employment} · role_match={role_match}",
        "info")

    # LLM cross-reasoning for uncertain cases
    if employment == "UNCERTAIN" or identity == "UNCONFIRMED":
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "scoring", "llm_reason",
            f"uncertain signals — calling LLM cross-reasoning (identity={identity}, employment={employment})",
            "info")
        lm_verdict = await _llm_cross_reason(contact, audit, evidence)
        if lm_verdict.get("identity"):
            identity = lm_verdict["identity"]
        if lm_verdict.get("employment"):
            employment = lm_verdict["employment"]
        if lm_verdict.get("role_match"):
            role_match = lm_verdict["role_match"]
        evidence["llm_reasoning"] = lm_verdict.get("explanation", "")
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "scoring", "llm_reason",
            f"LLM → identity={identity} · employment={employment} · {evidence['llm_reasoning']}",
            "success" if employment == "CONFIRMED" else "warning")

    evidence["role_match_final"] = role_match

    email_ok = zb_result.get("status") in ("valid", "catch-all")
    status, notes, reject_reason, review_flags = _build_verdict(
        identity, employment, role_match, email_ok, contact, audit, zb_result, evidence
    )

    # Emit verdict
    verdict_level = "success" if status == "VERIFIED" else ("warning" if status == "REVIEW" else "error")
    verdict_detail = reject_reason if reject_reason else f"identity={identity} · employment={employment} · email_ok={email_ok}"
    await emit_veri_step(state.thread_id, contact.full_name, contact.company,
        "scoring", "verdict", f"{status} — {verdict_detail}", verdict_level)

    timestamp = datetime.now(timezone.utc).isoformat()
    contact = contact.model_copy(update={
        "verification_status": status,
        "verification_notes": notes,
        "verification_timestamp": timestamp,
    })

    logger.info("veri_scored", contact=contact.full_name, status=status,
                identity=identity, employment=employment, role_match=role_match, email_ok=email_ok)

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 4: Write to correct sheet tab (serialised via lock)
    # ─────────────────────────────────────────────────────────────────────────
    _final_sheet_row: int | None = None      # row in Final Filtered List
    _reject_sheet_row: int | None = None     # row written to Reject profiles

    async with sheet_lock:
        try:
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
            title_match_str = {"MATCH": "Yes", "MISMATCH": "No"}.get(role_match, "Unknown")

            # Cols O–U (7 columns, col 15 = O) — written to Final Filtered List in place
            verification_cols = [
                linkedin_status,                                    # O
                employment_verified,                                # P
                title_match_str,                                    # Q
                actual_title[:200],                                 # R
                contact.verification_status,                        # S (VERIFIED/REVIEW/REJECT)
                (contact.verification_notes or "")[:500],           # T
                (contact.verification_timestamp or "")[:10],        # U
            ]

            # Get the actual row number in the sheet for this contact
            sheet_row = (
                state.sheet_row_nums.get(idx)
                or state.sheet_row_nums.get(str(idx))
            )
            _final_sheet_row = sheet_row

            # Always update cols O-U in Final Filtered List (in place, no new rows)
            if sheet_row:
                await sheets.update_row_cells(
                    sheets.FINAL_FILTERED_LIST, sheet_row,
                    sheets.FINAL_FILTERED_LIST_VERI_COL_START,  # col 15 = O
                    verification_cols,
                )
                logger.info("veri_ffl_updated", contact=contact.full_name,
                            status=contact.verification_status, row=sheet_row)
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "scoring", "sheet",
                    f"Final Filtered List row {sheet_row} updated → {contact.verification_status}",
                    "success" if contact.verification_status == "VERIFIED" else
                    ("warning" if contact.verification_status == "REVIEW" else "error"))
            else:
                logger.warning("veri_no_sheet_row", contact=contact.full_name, idx=idx)

            # REJECT → copy full row (A-U) to "Reject profiles" tab, then delete from Final Filtered List
            if contact.verification_status == "REJECT":
                raw_cols_an = state.raw_rows.get(idx) or state.raw_rows.get(str(idx)) or []
                raw_cols_an = [str(v) if v is not None else "" for v in raw_cols_an]
                raw_cols_an += [""] * max(0, 14 - len(raw_cols_an))
                raw_cols_an = raw_cols_an[:14]

                reject_cols = [
                    linkedin_status,
                    employment_verified,
                    title_match_str,
                    actual_title[:200],
                    reject_reason[:300],
                    (contact.verification_notes or "")[:500],
                    (contact.verification_timestamp or "")[:10],
                ]
                reject_row_num = await sheets.append_row(
                    sheets.REJECTED_PROFILES, raw_cols_an + reject_cols
                )
                _reject_sheet_row = reject_row_num
                logger.info("veri_rejected", contact=contact.full_name,
                            reason=reject_reason, row=reject_row_num)
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "scoring", "sheet",
                    f"copied to Reject profiles · row {reject_row_num} · reason: {reject_reason[:80]}",
                    "warning")

                # Mark this sheet row for deletion after all contacts are processed
                if sheet_row:
                    rejected_sheet_rows.append(sheet_row)

        except Exception as e:
            import traceback
            logger.error("veri_write_error", contact=contact.full_name, error=str(e),
                         traceback=traceback.format_exc())
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "scoring", "sheet", f"write error: {e}", "error")

    _evidence.pop(idx, None)
    _zb_skipped = bool(zb_result.get("error"))

    # ── Per-signal data for frontend coloured blocks ────────────────────────
    _li_valid    = audit.get("valid", False)
    _li_at       = audit.get("at_target_company", False)
    _li_employed = audit.get("still_employed", False)
    _li_company  = audit.get("current_company") or ""
    _li_role     = audit.get("current_role") or ""
    _li_error    = audit.get("error") or ""

    if _li_valid and _li_at and _li_employed:
        _li_signal = "confirmed"
        _li_detail = f"at {contact.company}"
        if _li_role:
            _li_detail += f", {_li_role}"
    elif _li_valid and _li_at and not _li_employed:
        _li_signal = "uncertain"
        _li_detail = "profile found but employment uncertain"
    elif _li_valid and _li_company and not _li_at:
        _li_signal = "moved"
        _li_detail = f"now at {_li_company}"
    elif _li_error:
        _li_signal = "error"
        _err_short = _li_error[:60]
        _li_detail = f"{_err_short}"
    elif not contact.linkedin_url:
        _li_signal = "no_url"
        _li_detail = "no LinkedIn URL in sheet"
    else:
        _li_signal = "uncertain"
        _li_detail = "profile inaccessible"

    _zb_status = zb_result.get("status", "no_email")
    _email_val = contact.email or ""
    _zb_score  = zb_result.get("score", 0)
    if _zb_skipped:
        _email_signal = "skipped"
        _email_detail = "ZeroBounce offline — not validated"
    elif _zb_status == "valid":
        _email_signal = "valid"
        _email_detail = f"{_email_val} · valid"
    elif _zb_status == "catch-all":
        _email_signal = "catch-all"
        _email_detail = f"{_email_val} · catch-all domain"
    elif _zb_status == "unknown":
        _email_signal = "unknown"
        _email_detail = f"{_email_val} · unverifiable (score={_zb_score})"
    elif _zb_status == "invalid":
        _email_signal = "invalid"
        _email_detail = f"{_email_val} · bouncing"
    elif _zb_status == "no_email" or not _email_val:
        _email_signal = "no_email"
        _email_detail = "no email address found"
    else:
        _email_signal = _zb_status
        _email_detail = f"{_email_val} · {_zb_status}"

    _web_pos   = [s for s in ["ddg", "tavily", "perplexity"] if evidence.get(f"{s}_positive")]
    _web_stale = [s for s in ["ddg", "tavily", "perplexity"] if evidence.get(f"{s}_stale")]
    _theorg    = evidence.get("theorg_found", False)
    if _web_pos:
        _web_signal = "positive"
        sources = [s.upper() if s == "ddg" else s.capitalize() for s in _web_pos]
        if _theorg:
            sources.append("TheOrg")
        _web_detail = " + ".join(sources) + " confirmed"
    elif _theorg:
        _web_signal = "positive"
        _web_detail = "TheOrg confirmed"
    elif len(_web_stale) >= 2:
        _web_signal = "stale"
        sources = [s.upper() if s == "ddg" else s.capitalize() for s in _web_stale]
        _web_detail = " + ".join(sources) + " show stale signals"
    else:
        _web_signal = "inconclusive"
        _web_detail = "no definitive web signals"

    _title_signal = role_match.lower()  # "match" | "mismatch" | "unknown"
    _sheet_title  = contact.role_title or ""
    if _title_signal == "match":
        _title_detail = f"'{_sheet_title}' confirmed"
        if _li_role:
            _title_detail = f"'{_sheet_title}' ≈ '{_li_role}'"
    elif _title_signal == "mismatch":
        _title_detail = f"'{_sheet_title}' ≠ '{_li_role or '?'}'"
    else:
        _title_detail = "no LinkedIn role to compare"

    _signals = {
        "linkedin":        _li_signal,
        "linkedin_detail": _li_detail,
        "email":           _email_signal,
        "email_detail":    _email_detail,
        "web":             _web_signal,
        "web_detail":      _web_detail,
        "title":           _title_signal,
        "title_detail":    _title_detail,
    }
    # ────────────────────────────────────────────────────────────────────────

    await emit_veri_contact(
        state.thread_id, contact.full_name, contact.company, "done",
        status=contact.verification_status,
        reject_reason=reject_reason if contact.verification_status == "REJECT" else "",
        sheet_row=_final_sheet_row if contact.verification_status != "REJECT" else None,
        reject_sheet_row=_reject_sheet_row,
        review_flags=review_flags if contact.verification_status == "REVIEW" else [],
        email_validated=not _zb_skipped,
        signals=_signals,
    )
    return contact


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

async def _llm_compare_titles(sheet_title: str, found_title: str, company: str) -> str:
    """
    Use GPT-4.1-mini to semantically compare two job titles.
    Returns: MATCH | MISMATCH | UNKNOWN
    """
    # Fast path: identical or near-identical titles
    if sheet_title.lower().strip() == found_title.lower().strip():
        return "MATCH"
    fast = _compare_titles_fast(sheet_title, found_title)
    if fast == "MATCH":
        return "MATCH"

    prompt = (
        f"You are a B2B sales expert. Compare these two job titles for the same person at {company}.\n"
        f"Sheet title:    \"{sheet_title}\"\n"
        f"Actual title:   \"{found_title}\"\n\n"
        f"Rules:\n"
        f"- MATCH if they are the same function/seniority (e.g. VP Marketing ≈ VP of Marketing, "
        f"Head of Digital ≈ Chief Digital Officer)\n"
        f"- MISMATCH if they are clearly different departments/functions "
        f"(e.g. Marketing vs Finance, CEO vs IT Manager)\n"
        f"- UNKNOWN if you cannot determine clearly\n\n"
        f"Reply with exactly one of: MATCH, MISMATCH, UNKNOWN — then a pipe | then one short sentence reason."
    )
    try:
        raw = await llm_complete(prompt, max_tokens=60, temperature=0)
        raw = raw.strip().upper()
        if raw.startswith("MATCH"):
            return "MATCH"
        if raw.startswith("MISMATCH"):
            return "MISMATCH"
    except Exception as e:
        logger.warning("veri_llm_title_error", error=str(e))

    return fast if fast != "UNKNOWN" else "UNKNOWN"


async def _llm_cross_reason(
    contact: Contact,
    audit: dict,
    evidence: dict,
) -> dict:
    """
    Ask GPT-4.1-mini to synthesise all collected evidence and return
    structured identity/employment/role_match verdicts + a brief explanation.
    Only called for uncertain cases to save API budget.
    """
    web_summary = []
    if evidence.get("ddg_positive"):
        web_summary.append("DDG confirms presence")
    if evidence.get("ddg_stale"):
        web_summary.append("DDG shows stale signals")
    if evidence.get("perplexity_positive"):
        web_summary.append("Perplexity confirms presence")
    if evidence.get("perplexity_stale"):
        web_summary.append("Perplexity shows stale signals")
    if evidence.get("perplexity_recent"):
        web_summary.append("Perplexity mentions 2024/2025")
    if evidence.get("theorg_found"):
        web_summary.append(f"TheOrg: {evidence.get('theorg_title','?')} at {contact.company}")

    li = audit
    li_summary = (
        f"LinkedIn valid={li.get('valid')}, at_target={li.get('at_target_company')}, "
        f"still_employed={li.get('still_employed')}, "
        f"current_company='{li.get('current_company','')}', "
        f"current_role='{li.get('current_role','')}'."
    )

    prompt = (
        f"You are a sales intelligence analyst verifying a contact.\n"
        f"Contact: {contact.full_name} | Target company: {contact.company} | "
        f"Sheet role: {contact.role_title or 'unknown'}\n\n"
        f"LinkedIn audit: {li_summary}\n"
        f"Web signals: {'; '.join(web_summary) or 'none'}\n"
        f"Perplexity snippet: {evidence.get('perplexity','')[:300]}\n"
        f"Email status: {contact.email_status}\n\n"
        f"Based on ALL evidence, answer:\n"
        f"1. IDENTITY: CONFIRMED or UNCONFIRMED (is this person real and findable?)\n"
        f"2. EMPLOYMENT: CONFIRMED, UNCERTAIN, or REJECTED (are they currently at {contact.company}?)\n"
        f"3. ROLE_MATCH: MATCH, MISMATCH, or UNKNOWN (does the sheet role match what's found?)\n"
        f"4. EXPLANATION: one sentence\n\n"
        f"Format exactly:\nIDENTITY: X\nEMPLOYMENT: Y\nROLE_MATCH: Z\nEXPLANATION: ..."
    )

    out: dict = {}
    try:
        raw = await llm_complete(prompt, max_tokens=120, temperature=0)
        for line in raw.strip().splitlines():
            if line.startswith("IDENTITY:"):
                val = line.split(":", 1)[1].strip().upper()
                if val in ("CONFIRMED", "UNCONFIRMED"):
                    out["identity"] = val
            elif line.startswith("EMPLOYMENT:"):
                val = line.split(":", 1)[1].strip().upper()
                if val in ("CONFIRMED", "UNCERTAIN", "REJECTED"):
                    out["employment"] = val
            elif line.startswith("ROLE_MATCH:"):
                val = line.split(":", 1)[1].strip().upper()
                if val in ("MATCH", "MISMATCH", "UNKNOWN"):
                    out["role_match"] = val
            elif line.startswith("EXPLANATION:"):
                out["explanation"] = line.split(":", 1)[1].strip()
    except Exception as e:
        logger.warning("veri_llm_cross_reason_error", error=str(e))

    return out


# ---------------------------------------------------------------------------
# Scoring helpers (deterministic fast path)
# ---------------------------------------------------------------------------

def _check_all_fast(contact: Contact, audit: dict, evidence: dict) -> tuple[str, str, str]:
    """
    Deterministic identity / employment / role_match from Unipile + web signals.
    Used as fast path; LLM cross-reasoning upgrades uncertain cases.
    """
    li_valid = audit.get("valid", False)
    li_at_company = audit.get("at_target_company", False)
    li_still_employed = audit.get("still_employed", False)
    li_current_company = audit.get("current_company") or ""
    li_current_role = audit.get("current_role") or ""

    # ── Identity ──
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

    # ── Employment ──
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

    # ── Role match (fast word-overlap) ──
    sheet_title = contact.role_title or ""
    if not sheet_title:
        role_match = "UNKNOWN"
    elif li_current_role:
        role_match = _compare_titles_fast(sheet_title, li_current_role)
    elif evidence.get("theorg_title"):
        role_match = _compare_titles_fast(sheet_title, evidence["theorg_title"])
    else:
        role_match = "UNKNOWN"

    return identity, employment, role_match


def _compare_titles_fast(sheet_title: str, found_title: str) -> str:
    """Word-overlap title comparison used as fast path before LLM."""
    stopwords = {"the", "a", "an", "and", "of", "for", "in", "at", "senior",
                 "associate", "jr", "sr", "global", "regional", "head"}
    sheet_words = set(re.sub(r"[^a-z0-9 ]", "", sheet_title.lower()).split()) - stopwords
    found_words = set(re.sub(r"[^a-z0-9 ]", "", found_title.lower()).split()) - stopwords

    if not sheet_words or not found_words:
        return "UNKNOWN"

    overlap = len(sheet_words & found_words) / max(len(sheet_words), len(found_words))
    if overlap >= 0.5:
        return "MATCH"
    if _is_different_function(sheet_title, found_title):
        return "MISMATCH"
    return "UNKNOWN"


def _build_verdict(
    identity: str, employment: str, role_match: str,
    email_ok: bool, contact: Contact, audit: dict, zb_result: dict, evidence: dict,
) -> tuple[str, str, str, list[str]]:
    """
    Returns (status, notes, reject_reason, review_flags).
    status: VERIFIED | REVIEW | REJECT
    reject_reason: short string explaining why rejected (empty if not REJECT)
    review_flags: list of specific issues to review (for REVIEW contacts)
    """
    parts: list[str] = []

    li_valid = audit.get("valid", False)
    li_company = audit.get("current_company") or ""
    li_role = audit.get("current_role") or ""
    li_at = audit.get("at_target_company", False)
    li_employed = audit.get("still_employed", False)
    li_error = audit.get("error") or ""
    zb_status = zb_result.get("status", "no_email")
    zb_score = zb_result.get("score")
    zb_error = zb_result.get("error") or ""      # non-empty = ZeroBounce unavailable (0 credits / API error)
    zb_skipped = bool(zb_error)                  # True → email validation didn't run; don't penalize contact
    has_email = bool(contact.email and contact.email.strip())
    email_val = contact.email or "none"

    # LinkedIn fully confirmed = profile loaded + at target + still employed
    li_fully_confirmed = li_valid and li_at and li_employed

    if li_valid:
        li_summary = "LinkedIn: profile loaded"
        if li_company:
            li_summary += f", company='{li_company}'"
        if li_role:
            li_summary += f", role='{li_role}'"
        li_summary += f", at_target={li_at}, employed={li_employed}"
        parts.append(li_summary)
    elif li_error:
        parts.append(f"LinkedIn error: {li_error[:120]}")
    else:
        parts.append("LinkedIn: no URL or profile inaccessible")

    if zb_score is not None:
        parts.append(f"Email: {email_val} → {zb_status} (score={zb_score})")
    else:
        parts.append(f"Email: {email_val} → {zb_status}")

    web_positives = [s for s in ["ddg", "tavily", "perplexity"] if evidence.get(f"{s}_positive")]
    web_stales = [s for s in ["ddg", "tavily", "perplexity"] if evidence.get(f"{s}_stale")]
    if web_positives:
        parts.append(f"Web confirms: {', '.join(web_positives)}")
    if web_stales:
        parts.append(f"Web stale signals: {', '.join(web_stales)}")
    if evidence.get("theorg_found"):
        theorg_title = evidence.get("theorg_title", "")
        parts.append(f"TheOrg: found{f', title={theorg_title}' if theorg_title else ''}")
    if evidence.get("llm_reasoning"):
        parts.append(f"LLM: {evidence['llm_reasoning']}")
    if li_role and contact.role_title:
        parts.append(f"Role: sheet='{contact.role_title}' vs LinkedIn='{li_role}' → {role_match}")
    elif contact.role_title:
        parts.append(f"Role: sheet='{contact.role_title}' → {role_match} (no LinkedIn role)")

    evidence_str = " | ".join(parts)

    # ── Decision tree ────────────────────────────────────────────────────────

    # 1. No identity confirmation at all
    if identity == "UNCONFIRMED":
        # Only REJECT if email is definitively bad — not when ZeroBounce was unavailable
        if not email_ok and not (zb_skipped and has_email):
            return ("REJECT",
                    f"REJECT: No profile or web confirmation, email invalid. {evidence_str}",
                    "Identity unconfirmed and email invalid",
                    [])
        flags_unconfirmed: list[str] = ["No LinkedIn profile or web confirmation found"]
        if zb_skipped and has_email:
            flags_unconfirmed.append("Email present but not validated (ZeroBounce offline)")
        return ("REVIEW",
                f"REVIEW: Email valid but no profile/web confirmation. {evidence_str}",
                "",
                flags_unconfirmed)

    # 2. LinkedIn shows they left this company
    if employment == "REJECTED":
        if li_company and not li_at:
            reason = f"LinkedIn shows person now at '{li_company}', not {contact.company}"
            return "REJECT", f"REJECT: {reason}. {evidence_str}", reason, []
        reason = f"2+ sources indicate person left {contact.company}"
        return "REJECT", f"REJECT: Stale role — {reason}. {evidence_str}", reason, []

    # 3. LinkedIn confirmed at company
    if employment == "CONFIRMED":
        if role_match == "MISMATCH":
            reason = (
                f"Title mismatch: sheet='{contact.role_title}' vs "
                f"LinkedIn='{li_role or evidence.get('theorg_title', '?')}'"
            )
            return "REJECT", f"REJECT: {reason}. {evidence_str}", reason, []

        # Smart upgrade: LinkedIn FULLY confirms presence (profile loaded, at company, still employed)
        # + email is "unknown" (corporate domain blocks probing) OR ZeroBounce was unavailable (0 credits / API error)
        # → VERIFIED. LinkedIn employment signal is stronger than ZeroBounce probe failure.
        if li_fully_confirmed and (zb_status == "unknown" or zb_skipped) and role_match in ("MATCH", "UNKNOWN"):
            note = "Email domain blocks probing but not invalid" if not zb_skipped else "Email validation skipped (ZeroBounce unavailable)"
            return ("VERIFIED",
                    f"VERIFIED: LinkedIn confirms at {contact.company}. {note}. {evidence_str}",
                    "",
                    [])

        # ZeroBounce skipped but has email + LinkedIn confirmed → treat as deliverable
        if zb_skipped and has_email and role_match in ("MATCH", "UNKNOWN"):
            return ("VERIFIED",
                    f"VERIFIED: LinkedIn confirms at {contact.company}. Email present but not validated (ZeroBounce unavailable). {evidence_str}",
                    "",
                    [])

        if email_ok and role_match in ("MATCH", "UNKNOWN"):
            return ("VERIFIED",
                    f"VERIFIED: LinkedIn confirms at {contact.company}, email deliverable. {evidence_str}",
                    "",
                    [])

        # Remaining REVIEW cases — build specific flags
        flags: list[str] = []
        if zb_skipped and not has_email:
            flags.append("No email address found — needs enrichment")
        elif zb_status == "invalid":
            flags.append(f"Email invalid/bouncing ({email_val})")
        elif zb_status == "no_email":
            flags.append("No email address found — needs enrichment")
        elif zb_status == "unknown" and not li_fully_confirmed:
            flags.append(f"Email unverifiable ({email_val} · score={zb_score})")
        elif not email_ok:
            flags.append(f"Email status: {zb_status} ({email_val})")
        if li_error:
            flags.append(f"LinkedIn error: {li_error[:80]}")
        return ("REVIEW",
                f"REVIEW: LinkedIn confirmed but email {zb_status}. {evidence_str}",
                "",
                flags)

    # 4. Uncertain employment — build specific flags
    flags: list[str] = []
    if li_error:
        flags.append(f"LinkedIn error: {li_error[:80]}")
    elif not li_valid:
        flags.append("LinkedIn profile inaccessible — verify manually")
    if not email_ok:
        if zb_status == "no_email":
            flags.append("No email address — needs enrichment")
        elif zb_status == "invalid":
            flags.append(f"Email invalid ({email_val})")
        else:
            flags.append(f"Email unverifiable ({email_val} · {zb_status})")
    if identity == "CONFIRMED" and not flags:
        flags.append("Employment uncertain — LinkedIn audit incomplete")

    if email_ok and identity == "CONFIRMED":
        return ("REVIEW",
                f"REVIEW: Identity confirmed, email valid, but employment uncertain. {evidence_str}",
                "",
                flags)
    return ("REVIEW",
            f"REVIEW: Insufficient evidence — LinkedIn inaccessible or private. {evidence_str}",
            "",
            flags)


# ---------------------------------------------------------------------------
# Utility helpers
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
        "marketing": {"marketing", "brand", "growth", "digital", "content", "social", "cmi", "cmo"},
        "finance": {"finance", "financial", "cfo", "accounting", "treasury", "controller"},
        "technology": {"technology", "cto", "engineering", "software", "it", "data", "tech"},
        "sales": {"sales", "revenue", "commercial", "business development", "bd", "cso"},
        "hr": {"hr", "human resources", "people", "talent", "chro", "recruiting"},
        "operations": {"operations", "supply chain", "logistics", "procurement", "coo"},
        "legal": {"legal", "compliance", "counsel", "regulatory", "clo"},
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
