"""
Command Center Orchestrator — pipeline status, AI analysis, auto-mode.

Reads all Google Sheets, computes per-company pipeline state, uses LLM to
suggest next actions, and optionally auto-triggers agents.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

from backend.tools import sheets
from backend.utils.logging import get_logger

logger = get_logger("orchestrator")

# ---------------------------------------------------------------------------
# Must-have role tiers (mirrors searcher.py MUST_HAVE_TIERS)
# ---------------------------------------------------------------------------

_CEO_MD_KW = [
    "chief executive", "ceo", "managing director", "md", "president",
    "founder", "co-founder", "general manager", "country manager",
    "chief operating", "coo", "director general", "geschäftsführer",
]
_CTO_CIO_KW = [
    "chief technology", "cto", "chief information", "cio",
    "chief digital", "cdo", "chief product", "cpo",
    "vp technology", "vp engineering", "head of technology",
    "head of engineering", "head of it", "head of digital",
]
_CSO_SALES_KW = [
    "chief sales", "cso", "chief revenue", "cro",
    "vp sales", "head of sales", "sales director",
    "chief marketing", "cmo", "head of marketing",
    "commercial director", "chief commercial",
]

MUST_HAVE_ROLES = {
    "ceo_md": _CEO_MD_KW,
    "cto_cio": _CTO_CIO_KW,
    "cso_sales": _CSO_SALES_KW,
}

TIER_LABELS = {
    "ceo_md": "CEO/MD",
    "cto_cio": "CTO/CIO",
    "cso_sales": "CSO/Sales",
}


def _match_tier(title: str) -> str | None:
    t = title.lower()
    for tier_id, keywords in MUST_HAVE_ROLES.items():
        if any(kw in t for kw in keywords):
            return tier_id
    return None


# ---------------------------------------------------------------------------
# Status cache (10-second TTL)
# ---------------------------------------------------------------------------

_status_cache: dict[str, Any] = {}
_status_cache_ts: float = 0
_CACHE_TTL = 10  # seconds


# ---------------------------------------------------------------------------
# Compute full pipeline status
# ---------------------------------------------------------------------------

async def compute_full_status(company_filter: str = "", start_row: int = 1) -> dict:
    """Read all sheets and compute per-company pipeline state.

    start_row: 1-based row in Target Accounts (rows before this are skipped).
               If start_row > total Target Account rows, it's treated as a
               First Clean List row filter instead — showing only companies
               that have contacts at or after that row.
    """
    global _status_cache, _status_cache_ts

    # Check cache
    now = time.time()
    cache_key = f"{company_filter.lower().strip()}:{start_row}"
    if now - _status_cache_ts < _CACHE_TTL and cache_key in _status_cache:
        return _status_cache[cache_key]

    # Read all sheets in parallel
    try:
        ta_records, ffl_records, rej_records = await asyncio.gather(
            sheets.read_all_records(sheets.TARGET_ACCOUNTS),
            sheets.read_all_records(sheets.FIRST_CLEAN_LIST),
            _safe_read(sheets.REJECTED_PROFILES),
        )
    except Exception as e:
        logger.error("orchestrator_sheet_read_error", error=str(e))
        return {"timestamp": _now_iso(), "summary": _empty_summary(), "companies": [], "error": str(e)}

    # Group FFL and Reject by company name
    ffl_by_company = _group_by_company(ffl_records)
    rej_by_company = _group_by_company(rej_records)

    # Determine filter mode: if start_row > Target Accounts row count,
    # treat it as a First Clean List row filter instead.
    ta_total_rows = len(ta_records) + 1  # +1 for header
    use_ffl_filter = start_row > ta_total_rows

    # If filtering by FFL row, find which companies have contacts at/after that row
    ffl_companies_in_range: set[str] | None = None
    if use_ffl_filter:
        ffl_companies_in_range = set()
        for row_idx, rec in enumerate(ffl_records):
            ffl_sheet_row = row_idx + 2
            if ffl_sheet_row >= start_row:
                name = str(rec.get("Company Name", "") or "").strip()
                if name:
                    ffl_companies_in_range.add(name)

    companies = []
    for row_idx, ta in enumerate(ta_records):
        sheet_row = row_idx + 2
        # Apply Target Accounts row filter (when start_row is within TA range)
        if not use_ffl_filter and sheet_row < start_row:
            continue
        name = str(ta.get("Company Name", "") or "").strip()
        if not name:
            continue
        if company_filter and name.lower() != company_filter.lower():
            continue
        # Apply FFL row filter (when start_row is beyond TA range)
        if ffl_companies_in_range is not None and name not in ffl_companies_in_range:
            continue

        ffl = ffl_by_company.get(name, [])
        rej = rej_by_company.get(name, [])
        companies.append(_build_company_status(ta, ffl, rej))

    result = {
        "timestamp": _now_iso(),
        "summary": _compute_summary(companies),
        "companies": companies,
    }

    # Cache
    _status_cache[cache_key] = result
    _status_cache_ts = now
    return result


async def _safe_read(tab: str) -> list[dict]:
    try:
        return await sheets.read_all_records(tab)
    except Exception:
        return []


def _group_by_company(records: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for r in records:
        name = str(r.get("Company Name", "") or "").strip()
        if name:
            groups.setdefault(name, []).append(r)
    return groups


def _build_company_status(ta: dict, ffl_contacts: list[dict], rejected: list[dict]) -> dict:
    name = str(ta.get("Company Name", "") or "").strip()
    domain = str(ta.get("Company Domain", "") or "").strip()
    sales_nav = str(ta.get("Sales Navigator Link", "") or "").strip()
    email_fmt_key = next((k for k in ta if str(k).startswith("Email Format")), "Email Format")
    email_format = str(ta.get(email_fmt_key, "") or "").strip()
    sdr = str(ta.get("SDR Name", ta.get("SDR_Name", "")) or "").strip()
    account_type = str(ta.get("Account type", ta.get("Account Type", "")) or "").strip()
    account_size = str(ta.get("Account Size", "") or "").strip()

    # Count contacts by status
    verified = 0
    review = 0
    pending = 0
    contacts_detail = []

    for i, c in enumerate(ffl_contacts):
        overall = str(c.get("Overall Status", "") or "").strip().upper()
        title = str(c.get("Job Title (English)", c.get("Job titles (English)", "")) or "").strip()
        first = str(c.get("First Name", "") or "").strip()
        last = str(c.get("Last Name", "") or "").strip()
        buying_role = str(c.get("Buying Role", "") or "").strip()

        if overall == "VERIFIED":
            verified += 1
        elif overall == "REVIEW":
            review += 1
        else:
            pending += 1

        contacts_detail.append({
            "first_name": first,
            "last_name": last,
            "full_name": f"{first} {last}".strip(),
            "title": title,
            "buying_role": buying_role,
            "email": str(c.get("Email", "") or "").strip(),
            "linkedin_url": str(c.get("LinkedIn URL", c.get("Linekdin Url", "")) or "").strip(),
            "overall_status": overall or "PENDING",
        })

    total = len(ffl_contacts)

    # Role coverage
    role_coverage = _compute_role_coverage(contacts_detail)
    filled_count = sum(1 for v in role_coverage.values() if v["filled"])
    coverage_pct = int(filled_count / len(MUST_HAVE_ROLES) * 100) if MUST_HAVE_ROLES else 0

    # Compute stage
    stage = _compute_stage(domain, sales_nav, total, verified, review, pending, filled_count)

    return {
        "company_name": name,
        "normalized_name": str(ta.get("Parent Company Name", ta.get("Normalized Company Name (Parent Group)", "")) or name).strip(),
        "domain": domain,
        "sales_nav_url": sales_nav,
        "email_format": email_format,
        "sdr_name": sdr,
        "account_type": account_type,
        "account_size": account_size,
        "stage": stage,
        "total_contacts": total,
        "verified_count": verified,
        "review_count": review,
        "pending_count": pending,
        "rejected_count": len(rejected),
        "role_coverage": role_coverage,
        "role_coverage_pct": coverage_pct,
        "contacts": contacts_detail,
    }


def _compute_role_coverage(contacts: list[dict]) -> dict:
    coverage = {}
    for tier_id in MUST_HAVE_ROLES:
        coverage[tier_id] = {
            "filled": False,
            "contact_name": None,
            "title": None,
            "status": None,
            "label": TIER_LABELS[tier_id],
        }

    for c in contacts:
        title = c.get("title", "")
        tier = _match_tier(title)
        if tier and not coverage[tier]["filled"]:
            coverage[tier] = {
                "filled": True,
                "contact_name": c.get("full_name", ""),
                "title": title,
                "status": c.get("overall_status", "PENDING"),
                "label": TIER_LABELS[tier],
            }
    return coverage


def _compute_stage(domain, sales_nav, total, verified, review, pending, filled_roles) -> str:
    if not domain and not sales_nav:
        return "enrichment_pending"
    if total == 0:
        return "contacts_pending"
    if pending == total:
        return "verification_pending"
    if pending > 0:
        return "verification_partial"
    if filled_roles >= 3:
        return "ready_for_outreach"
    return "verification_complete"


def _compute_summary(companies: list[dict]) -> dict:
    return {
        "total_companies": len(companies),
        "fully_enriched": sum(1 for c in companies if c["domain"]),
        "contacts_found": sum(1 for c in companies if c["total_contacts"] > 0),
        "fully_verified": sum(1 for c in companies if c["stage"] in ("verification_complete", "ready_for_outreach")),
        "needs_attention": sum(1 for c in companies if c["stage"] not in ("ready_for_outreach",)),
        "rejected_count": sum(c["rejected_count"] for c in companies),
    }


def _empty_summary() -> dict:
    return {"total_companies": 0, "fully_enriched": 0, "contacts_found": 0,
            "fully_verified": 0, "needs_attention": 0, "rejected_count": 0}


# ---------------------------------------------------------------------------
# AI Analysis
# ---------------------------------------------------------------------------

async def run_ai_analysis(companies: list[dict], focus: str = "") -> dict:
    """Use LLM to analyze pipeline state and suggest actions."""
    from backend.tools.llm import llm_complete

    compressed = _compress_for_llm(companies)

    focus_instruction = ""
    if focus == "missing_roles":
        focus_instruction = "\nFOCUS: Only suggest actions for companies missing must-have roles."
    elif focus == "stuck":
        focus_instruction = "\nFOCUS: Only suggest actions for companies stuck at a stage."

    prompt = (
        f"You are a B2B sales pipeline orchestrator AI. Analyze the pipeline and suggest next actions.\n\n"
        f"PIPELINE STATE:\n{compressed}\n\n"
        f"MUST-HAVE ROLES per company: CEO/MD, CTO/CIO, CSO/Head of Sales.{focus_instruction}\n\n"
        f"For each company needing attention, suggest ONE action:\n"
        f"- \"run_searcher\" — missing must-have roles (specify dm_roles)\n"
        f"- \"run_veri\" — contacts exist but unverified\n"
        f"- \"review\" — contacts in REVIEW status need human check\n"
        f"- \"complete\" — all 3 must-have roles verified, ready for outreach\n\n"
        f"Return ONLY JSON (no markdown):\n"
        f'{{"summary": "1-2 sentences", "suggestions": ['
        f'{{"company": "name", "action": "run_searcher", "reason": "why", '
        f'"priority": "high|medium|low", "auto_executable": true, '
        f'"params": {{"dm_roles": "CTO,CIO"}}}}'
        f']}}'
    )

    try:
        raw = await llm_complete(prompt, model="gpt-4.1", max_tokens=1500, temperature=0)
        cleaned = re.sub(r'^```(?:json)?\s*|\s*```$', '', (raw or "").strip())
        result = json.loads(cleaned)
        result["generated_at"] = _now_iso()
        logger.info("ai_analysis_done", suggestions=len(result.get("suggestions", [])))
        return result
    except Exception as e:
        logger.warning("ai_analysis_error", error=str(e))
        return {
            "generated_at": _now_iso(),
            "summary": f"Analysis failed: {e}",
            "suggestions": [],
        }


def _compress_for_llm(companies: list[dict]) -> str:
    rows = []
    for c in companies:
        rows.append({
            "name": c["company_name"],
            "stage": c["stage"],
            "contacts": c["total_contacts"],
            "verified": c["verified_count"],
            "pending": c["pending_count"],
            "review": c["review_count"],
            "rejected": c["rejected_count"],
            "roles": {k: v["filled"] for k, v in c["role_coverage"].items()},
            "missing": [TIER_LABELS[k] for k, v in c["role_coverage"].items() if not v["filled"]],
        })
    return json.dumps(rows, indent=2)


# ---------------------------------------------------------------------------
# Auto-mode
# ---------------------------------------------------------------------------

_auto_mode_task: asyncio.Task | None = None
_auto_mode_config: dict = {"enabled": False}
_auto_mode_log: list[dict] = []
_AUTO_MODE_MAX_LOG = 50


async def start_auto_mode(
    poll_interval: int,
    auto_searcher: bool,
    auto_veri: bool,
    dry_run: bool,
    trigger_fn=None,
) -> str:
    """Start the server-side auto-mode polling loop."""
    global _auto_mode_task, _auto_mode_config

    await stop_auto_mode()

    _auto_mode_config = {
        "enabled": True,
        "poll_interval": poll_interval,
        "auto_searcher": auto_searcher,
        "auto_veri": auto_veri,
        "dry_run": dry_run,
        "last_check": None,
    }

    _auto_mode_task = asyncio.create_task(
        _auto_mode_loop(poll_interval, auto_searcher, auto_veri, dry_run, trigger_fn)
    )
    task_id = f"auto-{int(time.time())}"
    logger.info("auto_mode_started", interval=poll_interval, dry_run=dry_run)
    return task_id


async def stop_auto_mode():
    global _auto_mode_task, _auto_mode_config
    if _auto_mode_task and not _auto_mode_task.done():
        _auto_mode_task.cancel()
        try:
            await _auto_mode_task
        except asyncio.CancelledError:
            pass
    _auto_mode_task = None
    _auto_mode_config["enabled"] = False
    logger.info("auto_mode_stopped")


def get_auto_mode_status() -> dict:
    return {
        "enabled": _auto_mode_config.get("enabled", False),
        "last_check": _auto_mode_config.get("last_check"),
        "poll_interval": _auto_mode_config.get("poll_interval", 60),
        "dry_run": _auto_mode_config.get("dry_run", False),
        "actions_taken": _auto_mode_log[-20:],
    }


async def _auto_mode_loop(
    interval: int,
    auto_searcher: bool,
    auto_veri: bool,
    dry_run: bool,
    trigger_fn=None,
):
    """Background polling loop — checks status and triggers agents."""
    while True:
        try:
            _auto_mode_config["last_check"] = _now_iso()

            # Get fresh status (bypass cache)
            global _status_cache_ts
            _status_cache_ts = 0
            status = await compute_full_status()
            companies = status.get("companies", [])

            # Simple rule-based triggers (no LLM needed for auto-mode)
            for c in companies:
                stage = c["stage"]
                name = c["company_name"]

                if stage == "verification_pending" and auto_veri:
                    action = f"Auto-trigger Veri for {name} ({c['pending_count']} unverified)"
                    _log_action(action, dry_run)
                    if not dry_run and trigger_fn:
                        await trigger_fn("veri", [name])

                elif stage in ("verification_complete",) and auto_searcher:
                    missing = [TIER_LABELS[k] for k, v in c["role_coverage"].items() if not v["filled"]]
                    if missing:
                        action = f"Auto-trigger Searcher for {name} (missing: {', '.join(missing)})"
                        _log_action(action, dry_run)
                        if not dry_run and trigger_fn:
                            await trigger_fn("searcher", [name])

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("auto_mode_loop_error", error=str(e))

        await asyncio.sleep(interval)


def _log_action(action: str, dry_run: bool):
    prefix = "[DRY RUN] " if dry_run else ""
    entry = {"ts": _now_iso(), "action": f"{prefix}{action}"}
    _auto_mode_log.append(entry)
    if len(_auto_mode_log) > _AUTO_MODE_MAX_LOG:
        _auto_mode_log.pop(0)
    logger.info("auto_mode_action", action=entry["action"])


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
