"""
Veri - Contact QC Agent  (v2 — multi-step cross-reasoning)

Pipeline per contact
────────────────────
Phase 0  LinkedIn discovery  — if no URL in sheet, search Unipile by name+company
Phase 1  Web intelligence    — DDG ×3 + Perplexity (structured role query) + TheOrg
                               Tavily fallback when DDG is inconclusive
Phase 2  LinkedIn audit      — Unipile verify_profile (confirms identity, employment,
                               current title)
Phase 3  LLM cross-reasoning — gpt-4.1 reads all evidence and produces
                               identity / employment / title_match verdicts +
                               a short explanation
Phase 4  Verdict + routing   — deterministic rules on the three signals:
         VERIFIED  → stays in First Clean List
         REVIEW    → stays in First Clean List (flagged)
         REJECT    → moved to Rejected Profiles tab

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
from backend.tools import sheets
from backend.tools import theorg
from backend.tools.llm import llm_complete
from backend.tools.search import search
from backend.utils.logging import get_logger
from backend.utils.progress import emit_veri_contact, emit_veri_step

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
# Node: read_contacts
# ---------------------------------------------------------------------------

async def read_contacts(state: VeriState) -> VeriState:
    """
    Pull contacts from First Clean List that need verification.

    Auto-detect mode (default): finds rows where col U (Overall Status) is empty.
    If company_filter is set, only picks rows matching that company.
    If row_start/row_end are set, uses those as an explicit range override.

    Tracks the actual sheet row number of each contact so we can update cols Q-W in place.
    """
    _evidence.clear()
    logger.info("veri_read_list", step="read_contacts", source=sheets.FIRST_CLEAN_LIST,
                company_filter=state.company_filter or "(all)")

    try:
        raw_all = await sheets.read_all_rows(sheets.FIRST_CLEAN_LIST)
        records = await sheets.read_all_records(sheets.FIRST_CLEAN_LIST)

        # If explicit row range provided, slice to that range
        if state.row_start is not None:
            row_offset = state.row_start - 2
            start = state.row_start - 2
            end = (state.row_end - 1) if state.row_end is not None else len(records)
            raw_all = raw_all[start:end]
            records = records[start:end]
            logger.info("veri_row_range", row_start=state.row_start, row_end=state.row_end, count=len(records))
        else:
            row_offset = 0

        contacts = []
        raw_rows: dict[int, list] = {}
        source_values: dict[int, str] = {}
        sheet_row_nums: dict[int, int] = {}

        for idx, (row, raw_row) in enumerate(zip(records, raw_all)):
            sheet_row = row_offset + idx + 2  # header=row1, first data=row2

            # Auto-detect: skip rows that already have Overall Status filled
            overall = str(row.get("Overall Status", "") or "").strip()
            if overall:
                continue  # already verified/reviewed/rejected — skip

            first = str(row.get("First Name", "") or "").strip()
            last = str(row.get("Last Name", "") or "").strip()
            full_name = f"{first} {last}".strip()

            if not full_name:
                continue

            # Company filter: if set, only process contacts matching any of the given companies
            company_name = str(row.get("Company Name", "") or "").strip()
            if state.company_filter:
                filter_companies = [c.strip().lower() for c in state.company_filter.split(",") if c.strip()]
                company_lower = company_name.lower()
                if not any(fc in company_lower or company_lower in fc for fc in filter_companies):
                    continue

            phone1 = str(row.get("Phone-1", "") or "").strip()
            phone2 = str(row.get("Phone-2", "") or "").strip()
            phones = [p for p in [phone1, phone2] if p]

            buying_role = str(row.get("Buying Role", "") or "").strip() or "Unknown"
            role_bucket_map = {
                # LLM buying-role tags (from Searcher GPT-4.1 classifier)
                "FDM": "DM", "KDM": "DM",
                "P1 Influencer": "Influencer", "Influencer": "Influencer",
                "Irrelevant": "GateKeeper",
                # Legacy 5-tier system
                "CEO/MD": "DM", "CTO/CIO": "DM", "CSO/Head of Sales": "DM",
                "Gatekeeper": "GateKeeper",
                # Legacy labels
                "Decision Maker": "DM", "DM": "DM", "Champion": "DM",
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
                company=company_name,
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

            # Store A-P cols for copying to Rejected Profiles if needed (16 cols)
            flat_row = [str(v) if v is not None else "" for v in raw_row]
            flat_row += [""] * max(0, 16 - len(flat_row))
            raw_rows[contact_idx] = flat_row[:16]

            # Track source column value
            source_values[contact_idx] = str(row.get("Source", "") or "").strip() or "n8n"

            sheet_row_nums[contact_idx] = sheet_row

        logger.info("veri_contacts_loaded", count=len(contacts),
                    company_filter=state.company_filter or "(all)",
                    row_range=f"{min(sheet_row_nums.values(), default=0)}–{max(sheet_row_nums.values(), default=0)}")

        # Ensure Rejected Profiles tab exists with correct headers
        await sheets.ensure_headers(sheets.REJECTED_PROFILES, sheets.REJECTED_PROFILES_HEADERS)

        return state.model_copy(update={
            "contacts": contacts,
            "current_index": 0,
            "raw_rows": raw_rows,
            "source_values": source_values,
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

    # Delete REJECT rows from First Clean List in reverse order so indices don't shift
    if rejected_sheet_rows:
        try:
            logger.info("veri_deleting_reject_rows", count=len(rejected_sheet_rows), rows=sorted(rejected_sheet_rows, reverse=True))
            await sheets.delete_rows_batch(sheets.FIRST_CLEAN_LIST, rejected_sheet_rows)
            logger.info("veri_reject_rows_deleted", count=len(rejected_sheet_rows))
        except Exception as e:
            logger.error("veri_delete_rows_error", error=str(e))
            errors.append(f"Failed to delete rejected rows from First Clean List: {e}")

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
    # Phase 1: Web intelligence — GPT-5 PRIMARY + DDG/Perplexity/TheOrg secondary
    # GPT-5 does the real verification (like a human researcher), others supplement.
    # ─────────────────────────────────────────────────────────────────────────
    await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "web")

    # ── PRIMARY: GPT-5 deep web verification ──
    async def _gpt5_web_verify():
        """GPT-5 is the PRIMARY verifier — searches the web like a real analyst."""
        try:
            from backend.tools.llm import llm_web_search
            result = await llm_web_search(
                f"You are verifying a B2B sales contact. Search the web thoroughly.\n\n"
                f"Person: {contact.full_name}\n"
                f"Target company: {contact.company}\n"
                f"Target domain: {contact.domain or 'unknown'}\n"
                f"Claimed role: {contact.role_title or 'unknown'}\n"
                f"LinkedIn: {contact.linkedin_url or 'none'}\n\n"
                f"SEARCH THE WEB and answer:\n"
                f"1. Does this person CURRENTLY work at EXACTLY '{contact.company}'?\n"
                f"2. What is their ACTUAL current employer (full legal name from LinkedIn)?\n"
                f"3. What is their ACTUAL current job title?\n\n"
                f"CRITICAL RULES:\n"
                f"- Companies with SIMILAR names are DIFFERENT companies:\n"
                f"  'Marico' (Indian FMCG) ≠ 'Marico Investments (Pty) Ltd' (Botswana)\n"
                f"  'Pansari Group' ≠ 'Veda Pansari Group'\n"
                f"  'Parle Agro' ≠ 'Parle Products'\n"
                f"  'Godrej Consumer' ≠ 'Godrej Properties'\n"
                f"- If the LinkedIn company has EXTRA words like Investments, Properties,\n"
                f"  Infotech, Waters, Holdings, Ventures → it's a DIFFERENT company.\n"
                f"- Check the COUNTRY. If target is India but person is in Botswana → DIFFERENT.\n"
                f"- Check the DOMAIN. Target domain is '{contact.domain or '?'}'. Does the\n"
                f"  person's actual company use this domain? If not → DOMAIN_MATCH: NO.\n"
                f"- Fake profiles: if the person has <5 connections, no photo, duplicate\n"
                f"  name (e.g. 'Ramanuj Acharya Ramanuj.Acharya') → EMPLOYED: NO.\n\n"
                f"Reply in this format:\n"
                f"EMPLOYED: YES or NO or UNCERTAIN\n"
                f"ACTUAL_COMPANY: the FULL company name from LinkedIn (exact)\n"
                f"ACTUAL_ROLE: their actual current title\n"
                f"DOMAIN_MATCH: YES or NO\n"
                f"REASONING: 1-2 sentences explaining your findings",
                model="gpt-5"
            )
            return result or ""
        except Exception:
            return ""

    # ── SECONDARY: DDG + Perplexity + TheOrg (run in parallel with GPT-5) ──
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
        role_query = (
            f'What is {contact.full_name}\'s current job title and employer as of 2026? '
            f'Are they currently working at {contact.company}? '
            f'What department do they work in?'
        )
        try:
            return await search(role_query, provider="perplexity", max_results=5)
        except Exception:
            return []

    # All run in parallel — GPT-5 is primary, others are secondary signals
    gpt5_verification, ddg_result_sets, theorg_entry, perplexity_results = await asyncio.gather(
        _gpt5_web_verify(),
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
    evidence["perplexity_recent"] = bool(re.search(r'\b(2025|2026)\b', combined_perplexity))

    if evidence["perplexity_positive"]:
        recent_tag = " · 2025/26 mention" if evidence["perplexity_recent"] else ""
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "perplexity", f"confirms presence at {contact.company}{recent_tag}", "success")
    elif evidence["perplexity_stale"]:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "perplexity", f"stale signals — may have left {contact.company}", "warning")
    else:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "web", "perplexity", "no strong signals (inconclusive)", "info")

    # ── Process GPT-5 PRIMARY verification ──
    gpt5_employed = "UNCERTAIN"
    gpt5_actual_company = ""
    gpt5_actual_role = ""
    gpt5_domain_match = True
    gpt5_reasoning = ""

    if gpt5_verification:
        evidence["gpt5_web"] = gpt5_verification[:800]
        for _line in gpt5_verification.strip().splitlines():
            _line = _line.strip()
            if _line.upper().startswith("EMPLOYED:"):
                _val = _line.split(":", 1)[1].strip().upper()
                if "YES" in _val:
                    gpt5_employed = "YES"
                elif "NO" in _val:
                    gpt5_employed = "NO"
                else:
                    gpt5_employed = "UNCERTAIN"
            elif _line.upper().startswith("ACTUAL_COMPANY:"):
                gpt5_actual_company = _line.split(":", 1)[1].strip()
            elif _line.upper().startswith("ACTUAL_ROLE:"):
                gpt5_actual_role = _line.split(":", 1)[1].strip()
            elif _line.upper().startswith("DOMAIN_MATCH:"):
                gpt5_domain_match = "NO" not in _line.upper().split(":", 1)[1]
            elif _line.upper().startswith("REASONING:"):
                gpt5_reasoning = _line.split(":", 1)[1].strip()

        evidence["gpt5_employed"] = gpt5_employed
        evidence["gpt5_actual_company"] = gpt5_actual_company
        evidence["gpt5_actual_role"] = gpt5_actual_role
        evidence["gpt5_domain_match"] = gpt5_domain_match
        evidence["gpt5_reasoning"] = gpt5_reasoning

        if gpt5_employed == "YES" and gpt5_domain_match:
            evidence["gpt5_positive"] = True
            evidence["gpt5_stale"] = False
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "web", "gpt5",
                f"CONFIRMED at {contact.company}" + (f" as {gpt5_actual_role}" if gpt5_actual_role else ""),
                "success")
        elif gpt5_employed == "NO" or not gpt5_domain_match:
            evidence["gpt5_positive"] = False
            evidence["gpt5_stale"] = True
            _detail = gpt5_reasoning or f"actually at {gpt5_actual_company}" if gpt5_actual_company else "not at target"
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "web", "gpt5", f"NOT at {contact.company}: {_detail}", "error")
        else:
            evidence["gpt5_positive"] = False
            evidence["gpt5_stale"] = False
            await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                "web", "gpt5", f"uncertain — {gpt5_reasoning or 'no clear signal'}", "info")
    else:
        evidence["gpt5_web"] = ""
        evidence["gpt5_positive"] = False
        evidence["gpt5_stale"] = False
        evidence["gpt5_employed"] = "UNCERTAIN"
        evidence["gpt5_reasoning"] = ""

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
    # Phase 2: LinkedIn audit
    # ─────────────────────────────────────────────────────────────────────────
    await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "linkedin_audit")

    async def _linkedin_audit():
        if not contact.linkedin_url:
            return {"valid": False, "error": "No LinkedIn URL in sheet or discovered"}
        try:
            from backend.tools.unipile import verify_profile
            return await verify_profile(contact.linkedin_url, contact.company)
        except Exception as e:
            logger.warning("veri_linkedin_audit_error", contact=contact.full_name, error=str(e))
            return {"valid": False, "error": str(e)}

    logger.info("veri_linkedin_audit", contact=contact.full_name)
    audit = await _linkedin_audit()

    evidence["linkedin_audit"] = audit

    # Emit LinkedIn result
    li_connections = audit.get("connections_count")
    li_followers = audit.get("follower_count")
    if audit.get("valid"):
        li_c = audit.get("current_company", "")
        li_r = audit.get("current_role", "")
        li_at = audit.get("at_target_company", False)
        li_emp = audit.get("still_employed", False)
        li_level = "success" if (li_at and li_emp) else "warning"
        conn_str = f" · connections={li_connections}" if li_connections is not None else ""
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_audit", "linkedin",
            f"profile loaded · company='{li_c}' · role='{li_r}' · at_target={li_at} · employed={li_emp}{conn_str}",
            li_level)
    elif audit.get("error"):
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_audit", "linkedin", f"error: {audit['error']}", "error")
    else:
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_audit", "linkedin", "profile not accessible or no URL provided", "warning")

    evidence["connections_count"] = li_connections
    evidence["follower_count"] = li_followers

    # ── Early REJECT: low connection/follower count = fake/irrelevant profile ──
    # Real B2B decision-makers at target companies have 500+ connections.
    # Profiles with fewer are almost always fake, aspirational, or junior.
    # Fallback: if connections_count unavailable, use follower_count < 50 as proxy.
    _MIN_CONNECTIONS = 500
    _MIN_FOLLOWERS = 50  # fallback when connections unavailable

    _should_reject_low_network = False
    _reject_signal = ""

    if li_connections is not None and li_connections < _MIN_CONNECTIONS:
        _should_reject_low_network = True
        _reject_signal = f"Low LinkedIn connections ({li_connections} < {_MIN_CONNECTIONS})"
    elif li_connections is None and li_followers is not None and li_followers < _MIN_FOLLOWERS:
        _should_reject_low_network = True
        _reject_signal = f"Low LinkedIn followers ({li_followers} < {_MIN_FOLLOWERS}) — connections unavailable"
    elif li_connections is None and li_followers is None:
        logger.warning("veri_no_network_data",
                       contact=contact.full_name, company=contact.company,
                       msg="Unipile returned neither connections nor followers — fake check skipped")

    if _should_reject_low_network:
        reject_reason = f"{_reject_signal} — likely fake or irrelevant profile"
        logger.info("veri_low_network_reject",
                    contact=contact.full_name, company=contact.company,
                    connections=li_connections, followers=li_followers)
        await emit_veri_step(state.thread_id, contact.full_name, contact.company,
            "linkedin_audit", "connections",
            f"REJECT — {_reject_signal}", "error")

        timestamp = datetime.now(timezone.utc).isoformat()
        contact = contact.model_copy(update={
            "verification_status": "REJECT",
            "verification_notes": f"REJECT: {reject_reason}",
            "verification_timestamp": timestamp,
        })

        # Write REJECT to sheet immediately and return — skip Phases 3-4
        async with sheet_lock:
            try:
                sheet_row = state.sheet_row_nums.get(idx)
                if sheet_row:
                    await sheets.update_row_cells(sheets.FIRST_CLEAN_LIST, sheet_row, {
                        17: "Low connections",    # Q - LinkedIn Status
                        18: "NO",                 # R - Employment Verified
                        19: "N/A",                # S - Title Match
                        20: audit.get("current_role") or "",  # T - Actual Title Found
                        21: "REJECT",             # U - Overall Status
                        22: reject_reason,        # V - Verification Notes
                        23: timestamp,            # W - Verified On
                    })
                # Copy to Rejected Profiles
                raw_cols = state.raw_rows.get(idx, [""] * 16)
                await sheets.ensure_headers(sheets.REJECTED_PROFILES, sheets.REJECTED_PROFILES_HEADERS)
                await sheets.append_row(sheets.REJECTED_PROFILES,
                    raw_cols[:16] + [
                        "Low connections",        # Q
                        "NO",                     # R
                        "N/A",                    # S
                        audit.get("current_role") or "",  # T
                        reject_reason,            # U - Reject Reason
                        reject_reason,            # V - Verification Notes
                        timestamp,                # W
                    ])
                if sheet_row:
                    rejected_sheet_rows.append(sheet_row)
            except Exception as e:
                logger.error("veri_low_conn_sheet_error", contact=contact.full_name, error=str(e))

        await emit_veri_contact(state.thread_id, contact.full_name, contact.company, "done")
        return contact

    linkedin_verified = bool(
        audit.get("valid") and audit.get("at_target_company") and audit.get("still_employed")
    )
    updated_role = audit.get("current_role") or contact.role_title

    contact = contact.model_copy(update={
        "linkedin_verified": linkedin_verified,
        "role_title": updated_role,
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

    status, notes, reject_reason, review_flags = _build_verdict(
        identity, employment, role_match, contact, audit, evidence
    )

    # ── Buying-role relevance check ──
    # If this contact would be VERIFIED or REVIEW, check if the role is actually
    # relevant for our B2B sales targeting. Use GPT-4.1 to reason about whether
    # the actual title (from LinkedIn or sheet) is FDM/KDM/P1/Influencer or Irrelevant.
    # This prevents n8n-sourced contacts with irrelevant roles from passing verification.
    if status in ("VERIFIED", "REVIEW"):
        actual_title = audit.get("current_role") or contact.role_title or ""
        if actual_title:
            role_relevance = await _check_role_relevance(actual_title, contact.company)
            if role_relevance.get("tag") == "Irrelevant":
                reason = role_relevance.get("reason", "Role not relevant for B2B sales targeting")
                status = "REJECT"
                reject_reason = f"Role irrelevant: {actual_title} — {reason}"
                notes = f"REJECT: {reject_reason}. {notes}"
                review_flags = []
                logger.info("veri_role_irrelevant_reject",
                            contact=contact.full_name, role=actual_title,
                            reason=reason)
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "scoring", "role_check",
                    f"REJECT — role '{actual_title}' is Irrelevant: {reason}", "error")
            else:
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "scoring", "role_check",
                    f"role '{actual_title}' → {role_relevance.get('tag', '?')} ✓", "success")

    # Emit verdict
    verdict_level = "success" if status == "VERIFIED" else ("warning" if status == "REVIEW" else "error")
    verdict_detail = reject_reason if reject_reason else f"identity={identity} · employment={employment}"
    await emit_veri_step(state.thread_id, contact.full_name, contact.company,
        "scoring", "verdict", f"{status} — {verdict_detail}", verdict_level)

    timestamp = datetime.now(timezone.utc).isoformat()
    contact = contact.model_copy(update={
        "verification_status": status,
        "verification_notes": notes,
        "verification_timestamp": timestamp,
    })

    logger.info("veri_scored", contact=contact.full_name, status=status,
                identity=identity, employment=employment, role_match=role_match)

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 4: Write to correct sheet tab (serialised via lock)
    # ─────────────────────────────────────────────────────────────────────────
    _final_sheet_row: int | None = None      # row in First Clean List
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

            # Cols Q–W (7 columns, col 17 = Q) — written to First Clean List in place
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

            # Always update cols Q-W in First Clean List (in place, no new rows)
            if sheet_row:
                await sheets.update_row_cells(
                    sheets.FIRST_CLEAN_LIST, sheet_row,
                    sheets.FCL_VERI_COL_START,  # col 17 = Q
                    verification_cols,
                )
                logger.info("veri_ffl_updated", contact=contact.full_name,
                            status=contact.verification_status, row=sheet_row)
                await emit_veri_step(state.thread_id, contact.full_name, contact.company,
                    "scoring", "sheet",
                    f"First Clean List row {sheet_row} updated → {contact.verification_status}",
                    "success" if contact.verification_status == "VERIFIED" else
                    ("warning" if contact.verification_status == "REVIEW" else "error"))
            else:
                logger.warning("veri_no_sheet_row", contact=contact.full_name, idx=idx)

            # REJECT → copy full row (A-W) to "Reject profiles" tab, then delete from First Clean List
            if contact.verification_status == "REJECT":
                # A-N (14 cols) from raw_rows
                raw_cols_an = state.raw_rows.get(idx) or state.raw_rows.get(str(idx)) or []
                raw_cols_an = [str(v) if v is not None else "" for v in raw_cols_an]
                raw_cols_an += [""] * max(0, 14 - len(raw_cols_an))
                raw_cols_an = raw_cols_an[:14]

                # O-P (Source + Pipeline Status)
                source_val = state.source_values.get(idx, "n8n")
                meta_cols = [source_val, "rejected"]

                # Q-W (Veri cols, with U = reject_reason instead of overall status)
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
                    sheets.REJECTED_PROFILES, raw_cols_an + meta_cols + reject_cols
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

    _email_val = contact.email or ""
    _email_signal = "present" if _email_val else "no_email"
    _email_detail = _email_val if _email_val else "no email address found"

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
        email_validated=False,
        signals=_signals,
    )
    return contact


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

async def _llm_compare_titles(sheet_title: str, found_title: str, company: str) -> str:
    """
    Use GPT-4.1 to semantically compare two job titles with reasoning.
    Handles role equivalences across languages and naming conventions.
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
        f"- MATCH if they cover the same FUNCTION and similar SENIORITY, even if wording differs.\n"
        f"  Examples: 'VP Marketing' ≈ 'VP of Marketing', 'Head of Digital' ≈ 'Chief Digital Officer',\n"
        f"  'Director Comercial' ≈ 'Sales Director', 'Gerente General' ≈ 'General Manager',\n"
        f"  'Director de Ventas' ≈ 'Head of Sales', 'Leiter Vertrieb' ≈ 'Sales Director'.\n"
        f"  Companies use different titles for the SAME responsibility — focus on what they DO.\n"
        f"- MISMATCH only if they are CLEARLY different departments/functions\n"
        f"  (e.g. Marketing vs Finance, CEO vs IT Support, HR Director vs Sales Director)\n"
        f"- UNKNOWN if you cannot determine clearly\n"
        f"- When in doubt, prefer MATCH over MISMATCH — do NOT reject someone just because\n"
        f"  the company uses a non-standard title for the role.\n\n"
        f"Reply with exactly one of: MATCH, MISMATCH, UNKNOWN — then a pipe | then one short sentence reason."
    )
    try:
        raw = await llm_complete(prompt, model="gpt-4.1", max_tokens=80, temperature=0)
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
    Use GPT-4.1 to reason about all collected evidence and return
    structured identity/employment/role_match verdicts + explanation.

    Smart handling of:
    - Company name variants (FamPay vs Fam, brand names vs legal names)
    - Company name changes (Quintiles → IQVIA, person hasn't updated LinkedIn)
    - Multiple concurrent positions (employee + freelancer/consultant)
    - Role equivalences (different title, same responsibility)
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
        web_summary.append("Perplexity mentions 2025/2026")
    if evidence.get("theorg_found"):
        web_summary.append(f"TheOrg: {evidence.get('theorg_title','?')} at {contact.company}")
    if evidence.get("gpt5_positive"):
        web_summary.append("GPT-5 web search confirms presence")
    if evidence.get("gpt5_stale"):
        web_summary.append("GPT-5 web search: may NOT be at target company")

    li = audit
    li_summary = (
        f"LinkedIn valid={li.get('valid')}, at_target={li.get('at_target_company')}, "
        f"still_employed={li.get('still_employed')}, "
        f"current_company='{li.get('current_company','')}', "
        f"current_role='{li.get('current_role','')}'."
    )

    prompt = (
        f"You are a sales intelligence analyst verifying a B2B contact. REASON carefully.\n\n"
        f"Contact: {contact.full_name}\n"
        f"Target company: {contact.company}\n"
        f"Sheet role: {contact.role_title or 'unknown'}\n"
        f"Domain: {contact.domain or 'unknown'}\n\n"
        f"LinkedIn audit: {li_summary}\n"
        f"Web signals: {'; '.join(web_summary) or 'none'}\n"
        f"Perplexity snippet: {evidence.get('perplexity','')[:300]}\n"
        f"GPT-5 web verification: {evidence.get('gpt5_web','')[:300]}\n\n"
        f"IMPORTANT REASONING RULES:\n"
        f"1. COMPANY NAME MATCHING: Companies often have different names on LinkedIn vs their\n"
        f"   official name. Examples: 'FamPay' may appear as 'Fam' on LinkedIn; 'Red Bull España'\n"
        f"   may appear as 'Red Bull'. If the LinkedIn company is a shortened/parent/brand variant\n"
        f"   of the target company, treat it as the SAME company → CONFIRMED.\n"
        f"2. COMPANY NAME CHANGES: Companies rebrand (Quintiles→IQVIA, Facebook→Meta,\n"
        f"   Andersen Consulting→Accenture). If the LinkedIn company is a former/current name of\n"
        f"   the target, the person may simply not have updated their profile → CONFIRMED.\n"
        f"3. MULTIPLE POSITIONS: People often hold multiple concurrent roles (employee + advisor,\n"
        f"   freelancer + full-time). If ANY of their current positions is at the target company,\n"
        f"   employment = CONFIRMED. The target company may not be listed first.\n"
        f"4. ROLE EQUIVALENCE: Different companies use different titles for the same responsibility.\n"
        f"   'Head of Digital' ≈ 'Chief Digital Officer' ≈ 'VP Digital'. 'Director Comercial' ≈\n"
        f"   'Sales Director'. Focus on FUNCTION and SENIORITY, not exact wording.\n\n"
        f"Based on ALL evidence, answer:\n"
        f"1. IDENTITY: CONFIRMED or UNCONFIRMED (is this person real and findable?)\n"
        f"2. EMPLOYMENT: CONFIRMED, UNCERTAIN, or REJECTED (are they currently at {contact.company}?)\n"
        f"3. ROLE_MATCH: MATCH, MISMATCH, or UNKNOWN (does the sheet role match what's found?)\n"
        f"4. EXPLANATION: one sentence explaining your reasoning\n\n"
        f"Format exactly:\nIDENTITY: X\nEMPLOYMENT: Y\nROLE_MATCH: Z\nEXPLANATION: ..."
    )

    out: dict = {}
    try:
        raw = await llm_complete(prompt, model="gpt-4.1", max_tokens=200, temperature=0)
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
# Buying-role relevance check (LLM-based)
# ---------------------------------------------------------------------------

async def _check_role_relevance(role_title: str, company: str) -> dict:
    """
    Use GPT-4.1 to check if a role is relevant for B2B FMCG/CPG sales targeting.
    Returns {"tag": "FDM|KDM|P1 Influencer|Influencer|Irrelevant", "reason": "..."}.

    Uses reasoning — NOT keyword matching. Handles equivalent roles across companies
    (different title, same responsibility) and multilingual titles.
    """
    prompt = (
        f"You are a buying-role classifier for B2B FMCG/CPG sales systems.\n"
        f"Company: {company}\n"
        f"Role: \"{role_title}\"\n\n"
        f"Classify this role into ONE of: FDM, KDM, P1 Influencer, Influencer, Irrelevant.\n\n"
        f"RELEVANT roles (FDM/KDM/P1/Influencer):\n"
        f"- FDM: CEO, MD, President, COO, CFO, CTO, CIO, Country Manager, VP (enterprise), Director General\n"
        f"- KDM: Sales Director, VP Sales, Head of Sales, CRO, IT Director, General Manager, Director (generic)\n"
        f"- P1: Commercial/Sales Excellence Director, CDO, RTM/GTM Director, Strategy Director, BI/Analytics Director\n"
        f"- Influencer: Trade Marketing Manager, Sales Automation Lead, RTM/GTM Manager, Analytics Manager\n\n"
        f"IRRELEVANT roles (always exclude):\n"
        f"- Marketing-only (Brand, CMO, Performance Marketing), Commercial Director\n"
        f"- HR, Legal, Procurement, Finance (except CFO), Operations, SCM, Logistics, Manufacturing\n"
        f"- Territory/Area Sales Managers without national/multi-country scope\n"
        f"- IT Support, Engineer, Admin, Coordinator, Intern\n\n"
        f"IMPORTANT: Companies use different titles for the same job. Focus on the ACTUAL\n"
        f"responsibility, not the exact title. 'Director Comercial' in some companies = Sales Director\n"
        f"(KDM), but the generic 'Commercial Director' without context = Irrelevant per policy.\n\n"
        f"Reply with exactly: TAG | reason (one sentence)\n"
        f"Example: FDM | Country Manager has national P&L authority"
    )
    try:
        raw = await llm_complete(prompt, model="gpt-4.1", max_tokens=80, temperature=0)
        raw = raw.strip()
        if "|" in raw:
            tag_part, reason = raw.split("|", 1)
            tag = tag_part.strip().upper()
            # Normalize tag
            tag_map = {
                "FDM": "FDM", "KDM": "KDM",
                "P1 INFLUENCER": "P1 Influencer", "P1": "P1 Influencer",
                "INFLUENCER": "Influencer",
                "IRRELEVANT": "Irrelevant",
            }
            return {"tag": tag_map.get(tag, "Irrelevant"), "reason": reason.strip()}
        # Fallback: just check first word
        first_word = raw.split()[0].upper() if raw else ""
        if first_word in ("FDM", "KDM"):
            return {"tag": first_word, "reason": raw}
        if "IRRELEVANT" in raw.upper():
            return {"tag": "Irrelevant", "reason": raw}
    except Exception as e:
        logger.warning("veri_role_relevance_error", role=role_title, error=str(e))

    # On error, don't reject — let the contact through
    return {"tag": "Unknown", "reason": "role relevance check failed"}


# ---------------------------------------------------------------------------
# Scoring helpers (deterministic fast path)
# ---------------------------------------------------------------------------

def _check_all_fast(contact: Contact, audit: dict, evidence: dict) -> tuple[str, str, str]:
    """
    Identity / employment / role_match from GPT-5 (primary) + Unipile + web signals.
    GPT-5 verdict is the strongest signal — overrides LinkedIn when it disagrees.
    """
    li_valid = audit.get("valid", False)
    li_at_company = audit.get("at_target_company", False)
    li_still_employed = audit.get("still_employed", False)
    li_current_company = audit.get("current_company") or ""
    li_current_role = audit.get("current_role") or ""

    # GPT-5 is the PRIMARY signal
    gpt5_employed = evidence.get("gpt5_employed", "UNCERTAIN")
    gpt5_domain_match = evidence.get("gpt5_domain_match", True)

    # ── Identity ──
    if li_valid:
        identity = "CONFIRMED"
    elif gpt5_employed in ("YES", "NO"):
        identity = "CONFIRMED"  # GPT-5 found them — they exist
    elif evidence.get("theorg_found"):
        identity = "CONFIRMED"
    else:
        positive_count = sum([
            bool(evidence.get("ddg_positive")),
            bool(evidence.get("tavily_positive")),
            bool(evidence.get("perplexity_positive")),
            bool(evidence.get("gpt5_positive")),
        ])
        identity = "CONFIRMED" if positive_count >= 1 else "UNCONFIRMED"

    # ── Employment ──
    # GPT-5 is PRIMARY: if it says NO or domain doesn't match → REJECTED
    # This catches cases like "Rajdhani chicken & food" ≠ "Rajdhani Besan"
    if gpt5_employed == "NO" or (gpt5_employed != "UNCERTAIN" and not gpt5_domain_match):
        return identity, "REJECTED", "UNKNOWN"

    # GPT-5 says YES → trust it even if LinkedIn disagrees
    if gpt5_employed == "YES" and gpt5_domain_match:
        # Still check role match below, but employment is confirmed
        role_match = "UNKNOWN"
        sheet_title = contact.role_title or ""
        gpt5_role = evidence.get("gpt5_actual_role", "")
        if not sheet_title:
            role_match = "UNKNOWN"
        elif gpt5_role:
            role_match = _compare_titles_fast(sheet_title, gpt5_role)
        elif li_current_role:
            role_match = _compare_titles_fast(sheet_title, li_current_role)
        return identity, "CONFIRMED", role_match

    # GPT-5 is uncertain — fall back to LinkedIn + other signals
    # Smart company matching: if Unipile's strict matcher said at_target=False,
    # do a soft check — the LinkedIn company might be a shortened name (Fam vs FamPay),
    # parent brand (Red Bull vs Red Bull España), or a rebranded name (Quintiles vs IQVIA).
    # If the soft check suggests a possible match, defer to LLM instead of hard-rejecting.
    _soft_company_match = False
    if li_valid and li_current_company and not li_at_company:
        _li_co = li_current_company.lower().strip()
        _target_co = contact.company.lower().strip()
        # Check if one name is a prefix/substring of the other (Fam ⊂ FamPay, Red Bull ⊂ Red Bull España)
        if _li_co in _target_co or _target_co in _li_co:
            _soft_company_match = True
        # Check if they share a significant word (Red Bull ∩ Red Bull España)
        _li_words = set(re.sub(r'[^a-z0-9]', ' ', _li_co).split())
        _target_words = set(re.sub(r'[^a-z0-9]', ' ', _target_co).split())
        _common = _li_words & _target_words - {"the", "de", "del", "la", "el", "and", "of", "sa", "sl", "ltd", "inc"}
        if _common and len(_common) >= 1:
            _soft_company_match = True

    if li_valid:
        if li_at_company and li_still_employed:
            employment = "CONFIRMED"
        elif li_at_company and not li_still_employed:
            employment = "UNCERTAIN"
        elif li_current_company and not li_at_company:
            # If soft match detected, don't hard-reject — let LLM decide
            if _soft_company_match:
                employment = "UNCERTAIN"  # will trigger LLM cross-reasoning
            else:
                employment = "REJECTED"
        else:
            stale_count = sum([
                bool(evidence.get("ddg_stale")),
                bool(evidence.get("tavily_stale")),
                bool(evidence.get("perplexity_stale")),
                bool(evidence.get("gpt5_stale")),
            ])
            employment = "REJECTED" if stale_count >= 2 else "UNCERTAIN"
    else:
        stale_count = sum([
            bool(evidence.get("ddg_stale")),
            bool(evidence.get("tavily_stale")),
            bool(evidence.get("perplexity_stale")),
            bool(evidence.get("gpt5_stale")),
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
    contact: Contact, audit: dict, evidence: dict,
) -> tuple[str, str, str, list[str]]:
    """
    Returns (status, notes, reject_reason, review_flags).
    status: VERIFIED | REVIEW | REJECT
    reject_reason: short string explaining why rejected (empty if not REJECT)
    review_flags: list of specific issues to review (for REVIEW contacts)

    Email verification is handled externally (already checked before Veri runs).
    Verdicts are based on LinkedIn, web intelligence, and role matching only.
    """
    parts: list[str] = []

    li_valid = audit.get("valid", False)
    li_company = audit.get("current_company") or ""
    li_role = audit.get("current_role") or ""
    li_at = audit.get("at_target_company", False)
    li_employed = audit.get("still_employed", False)
    li_error = audit.get("error") or ""

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
        return ("REVIEW",
                f"REVIEW: No profile or web confirmation found. {evidence_str}",
                "",
                ["No LinkedIn profile or web confirmation found"])

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

        if role_match in ("MATCH", "UNKNOWN"):
            return ("VERIFIED",
                    f"VERIFIED: LinkedIn confirms at {contact.company}. {evidence_str}",
                    "",
                    [])

        # Remaining REVIEW cases
        flags: list[str] = []
        if li_error:
            flags.append(f"LinkedIn error: {li_error[:80]}")
        return ("REVIEW",
                f"REVIEW: LinkedIn confirmed but role unclear. {evidence_str}",
                "",
                flags)

    # 4. Uncertain employment
    flags: list[str] = []
    if li_error:
        flags.append(f"LinkedIn error: {li_error[:80]}")
    elif not li_valid:
        flags.append("LinkedIn profile inaccessible — verify manually")
    if identity == "CONFIRMED" and not flags:
        flags.append("Employment uncertain — LinkedIn audit incomplete")

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

    graph.add_node("read_contacts", read_contacts)
    graph.add_node("parallel_verify_all", parallel_verify_all)

    graph.set_entry_point("read_contacts")
    graph.add_edge("read_contacts", "parallel_verify_all")
    graph.add_edge("parallel_verify_all", END)

    raw_conn = await aiosqlite.connect(settings.checkpoint_db_abs)
    conn = AioSqliteConnectionWrapper(raw_conn)
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    serde = JsonPlusSerializer()
    checkpointer = AsyncSqliteSaver(conn, serde=serde)
    return graph.compile(checkpointer=checkpointer)
