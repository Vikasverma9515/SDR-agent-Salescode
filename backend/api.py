"""
FastAPI application for the SCAI ProspectOps web UI.
Provides REST endpoints + WebSocket for real-time log streaming.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.config import get_settings
from backend.utils.logging import configure_logging, get_logger

logger = get_logger("api")

# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------

class FiniRunRequest(BaseModel):
    companies: str  # comma-separated
    sdr: str = ""
    submit_n8n: bool = False
    region: str = ""  # e.g. "India", "LATAM", "Southeast Asia"
    auto_mode: bool = False  # When True: auto-commit high-confidence, pause on ambiguous

class SearcherRunRequest(BaseModel):
    # Comma-separated company names (must already exist in Target Accounts)
    companies: str
    dm_roles: str = "VP Ecommerce,CDO,Head of Digital,CTO,CMO,VP Marketing,VP Sales"
    target_contact_count: int = 10  # max contacts to find per company (0 = unlimited)
    auto_approve: bool = False  # if True, skip SDR pause steps and auto-write all matched contacts
    auto_trigger_veri: bool = True  # if True, automatically start Veri after contacts are written

class VeriRunRequest(BaseModel):
    row_start: Optional[int] = None       # 1-based, inclusive (data rows, not header) — optional override
    row_end: Optional[int] = None         # 1-based, inclusive — optional override
    company_filter: Optional[str] = None  # only verify contacts for this company (auto-detect mode)

class N8nCompleteRequest(BaseModel):
    row_start: Optional[int] = None  # 1-based data row range that n8n populated
    row_end: Optional[int] = None

def _extract_field(item: dict, *keys: str, default: str = "") -> str:
    """Extract a field from a dict by trying multiple possible key names (case-insensitive)."""
    item_lower = {k.lower().replace(" ", "_").replace("-", "_"): v for k, v in item.items()}
    for key in keys:
        k = key.lower().replace(" ", "_").replace("-", "_")
        if k in item_lower and item_lower[k] is not None:
            val = str(item_lower[k]).strip()
            if val:
                return val
    return default


_n8n_log_last_error: dict = {}  # surfaces sheet-write errors via /api/n8n/debug

async def _log_n8n_contact(
    sheets_mod, timestamp: str, raw: dict, normalized: dict,
    status: str, skip_reason: str,
) -> None:
    """Log one contact row to the N8N Webhook Log tab. Fire-and-forget."""
    import json as _j
    try:
        await sheets_mod.ensure_headers(sheets_mod.N8N_WEBHOOK_LOG, sheets_mod.N8N_WEBHOOK_LOG_HEADERS)
        await sheets_mod.append_row(sheets_mod.N8N_WEBHOOK_LOG, [
            timestamp,                              # A
            normalized.get("company_name", ""),      # B
            normalized.get("first_name", ""),        # C
            normalized.get("last_name", ""),         # D
            normalized.get("job_title", ""),         # E
            normalized.get("email", ""),             # F
            normalized.get("linkedin_url", ""),      # G
            normalized.get("domain", ""),            # H
            normalized.get("phone_1", ""),           # I
            normalized.get("buying_role", ""),       # J
            normalized.get("country", ""),           # K
            status,                                  # L
            skip_reason,                             # M
            _j.dumps(raw, default=str)[:500],        # N
        ])
        _n8n_log_last_error.clear()
    except Exception as e:
        logger.warning("n8n_webhook_log_write_error", error=str(e))
        _n8n_log_last_error.clear()
        _n8n_log_last_error["error"] = str(e)
        _n8n_log_last_error["traceback"] = __import__("traceback").format_exc()


def _normalize_contact(raw: dict) -> dict:
    """
    Accept ANY JSON shape from n8n and map it to our standard fields.
    Tries multiple common field name variants for each field.
    """
    email = _extract_field(raw,
        "email", "address", "email_address", "work_email", "e_mail", "mail")
    domain = _extract_field(raw, "domain", "company_domain", "company_domain_name")
    company = _extract_field(raw,
        "company_name", "company", "account", "organization", "org", "account_name")

    # If no domain but have email, extract domain from email
    if not domain and email and "@" in email:
        domain = email.split("@")[1]

    # If no company but have account field from ZeroBounce-style data, try domain
    if not company and domain:
        company = domain.split(".")[0].capitalize()

    return {
        "company_name": company,
        "normalized_name": _extract_field(raw,
            "normalized_name", "parent_company", "normalized_company_name", "parent_company_name") or company,
        "domain": domain,
        "account_type": _extract_field(raw, "account_type", "type", "region"),
        "account_size": _extract_field(raw, "account_size", "size", "company_size"),
        "country": _extract_field(raw, "country", "location", "geo", "region"),
        "first_name": _extract_field(raw,
            "first_name", "firstname", "first", "given_name", "fname"),
        "last_name": _extract_field(raw,
            "last_name", "lastname", "last", "family_name", "surname", "lname"),
        "job_title": _extract_field(raw,
            "job_title", "title", "job_titles", "job_titles_(english)", "job_title_(english)",
            "role", "position", "designation"),
        "buying_role": _extract_field(raw,
            "buying_role", "role_type", "buyer_role", "contact_type"),
        "linkedin_url": _extract_field(raw,
            "linkedin_url", "linkedin", "linekdin_url", "linkedin_profile",
            "li_url", "linkedin_link", "profile_url"),
        "email": email,
        "phone_1": _extract_field(raw, "phone_1", "phone", "phone1", "mobile", "telephone"),
        "phone_2": _extract_field(raw, "phone_2", "phone2", "secondary_phone"),
    }

class OperatorConfirmRequest(BaseModel):
    thread_id: str
    confirmed: bool
    normalized_name: Optional[str] = None
    domain: Optional[str] = None
    email_format: Optional[str] = None
    sdr_assigned: Optional[str] = None
    account_type: Optional[str] = None
    account_size: Optional[str] = None
    linkedin_org_id: Optional[str] = None
    sales_nav_url: Optional[str] = None

class FiniCommitRequest(BaseModel):
    """Commit a single reviewed company to the Google Sheet."""
    company_name: str
    raw_name: str
    sales_nav_url: str = ""
    domain: str = ""
    sdr_assigned: str = ""
    email_format: str = ""
    account_type: str = ""
    account_size: str = ""
    submit_n8n: bool = False

class RunResponse(BaseModel):
    thread_id: str
    status: str
    message: str

class DmSelectionRequest(BaseModel):
    selected_indices: list[int]  # indices into the pending_dm_candidates list

class SelectRolesRequest(BaseModel):
    selected_bucket_ids: list[str]  # e.g. ["c_suite", "digital_ecommerce"]

class FindMoreRequest(BaseModel):
    prompt: str  # SDR's natural-language request, e.g. "find the CEO and CFO"

class AutoPipelineRequest(BaseModel):
    """Trigger full auto pipeline: Fini → n8n → Veri → Searcher → Veri round 2."""
    companies: str              # comma-separated company names
    sdr: str = ""
    region: str = ""

class ProspectChatRequest(BaseModel):
    query: str                       # SDR's message
    company: str = ""                # company context (optional)
    history: list[dict] = []         # [{"role": "user"|"assistant", "content": str}]

class ScoutCommitRequest(BaseModel):
    full_name: str
    role_title: str = ""
    company: str = ""
    linkedin_url: str = ""
    linkedin_verified: bool = False
    linkedin_status: str = ""
    employment_verified: str = ""
    title_match: str = ""
    actual_title: str = ""
    email: str = ""
    email_status: str = ""
    buying_role: str = ""
    source: str = "scout"
    confidence: str = "medium"
    # Company context (passed from frontend store)
    company_domain: str = ""
    company_account_type: str = ""
    company_account_size: str = ""

# ---------------------------------------------------------------------------
# Active runs tracker
# ---------------------------------------------------------------------------

_active_runs: dict[str, dict] = {}
_n8n_last_received: dict = {}  # Debug: stores last n8n payload for /api/n8n/debug
_active_tasks: dict[str, asyncio.Task] = {}  # task handles for cancellation
_pending_confirmations: dict[str, asyncio.Event] = {}
_confirmation_data: dict[str, dict] = {}
_log_queues: dict[str, asyncio.Queue] = {}
_fini_results: dict[str, dict] = {}  # cache completed enrichment data for WS replay
_last_pause_event: dict[str, dict] = {}  # cache last role/contact pause event for WS reconnect replay


_PAUSE_EVENT_TYPES = {"role_selection_required", "contact_selection_required"}


class _PipelineQueue(asyncio.Queue):
    """asyncio.Queue that caches the most recent pause event for WS reconnect replay."""

    def __init__(self, thread_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._thread_id = thread_id

    async def put(self, item):
        if isinstance(item, dict) and item.get("type") in _PAUSE_EVENT_TYPES:
            _last_pause_event[self._thread_id] = item
        await super().put(item)


async def _emit_log(thread_id: str, level: str, message: str, data: dict = None):
    """Emit a log event to any connected WebSocket for this thread."""
    queue = _log_queues.get(thread_id)
    if queue:
        await queue.put({
            "type": "log",
            "level": level,
            "message": message,
            "data": data or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })


async def _emit_event(thread_id: str, event_type: str, data: dict):
    """Emit a structured event to the WebSocket queue."""
    queue = _log_queues.get(thread_id)
    if queue:
        await queue.put({
            "type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_dir_abs)

    app = FastAPI(
        title="SalesCode Mapping Pipeline",
        description="B2B Prospecting Pipeline API",
        version="1.0.0",
    )

    _origins = [o.strip() for o in settings.allowed_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---------------------------------------------------------------------------
    # Health
    # ---------------------------------------------------------------------------

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    # ---------------------------------------------------------------------------
    # Config check
    # ---------------------------------------------------------------------------

    @app.get("/api/config/check")
    async def config_check() -> dict[str, bool]:
        """Check which API keys are configured."""
        s = get_settings()
        return {
            "google_sheets": bool(s.spreadsheet_id and s.google_service_account_json),
            "unipile": bool(s.unipile_api_key and s.unipile_account_id),
            "tavily": bool(s.tavily_api_key),
            "perplexity": bool(s.perplexity_api_key),
            "zerobounce": bool(s.zerobounce_api_key),
            "n8n": bool(s.n8n_webhook_url),
        }

    # ---------------------------------------------------------------------------
    # Fini endpoints
    # ---------------------------------------------------------------------------

    @app.post("/api/fini/run", response_model=RunResponse)
    async def run_fini(req: FiniRunRequest):
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = _PipelineQueue(thread_id)
        _active_runs[thread_id] = {"agent": "fini", "status": "running", "thread_id": thread_id, "started_at": datetime.now(timezone.utc).isoformat()}

        _active_tasks[thread_id] = asyncio.create_task(_fini_task(thread_id, req))

        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Fini started for: {req.companies}",
        )

    @app.post("/api/fini/confirm")
    async def confirm_operator(req: OperatorConfirmRequest) -> dict[str, str]:
        """Operator confirmation for Fini human-in-the-loop step."""
        if req.thread_id not in _pending_confirmations:
            raise HTTPException(status_code=404, detail="No pending confirmation for this thread")

        _confirmation_data[req.thread_id] = {
            "confirmed": req.confirmed,
            "normalized_name": req.normalized_name,
            "domain": req.domain,
            "email_format": req.email_format,
            "sdr_assigned": req.sdr_assigned,
            "account_type": req.account_type,
            "account_size": req.account_size,
            "linkedin_org_id": req.linkedin_org_id,
            "sales_nav_url": req.sales_nav_url,
        }
        _pending_confirmations[req.thread_id].set()
        return {"status": "ok"}

    @app.post("/api/fini/commit")
    async def fini_commit(req: FiniCommitRequest) -> dict[str, Any]:
        """Commit a single SDR-reviewed company to the Google Sheet."""
        from backend.tools import sheets
        try:
            await sheets.ensure_headers(sheets.TARGET_ACCOUNTS, sheets.TARGET_ACCOUNTS_HEADERS)
            row = [
                req.company_name,
                req.raw_name,
                req.sales_nav_url,
                req.domain,
                req.sdr_assigned,
                req.email_format,
                req.account_type,
                req.account_size,
            ]
            written_row = await sheets.append_row(sheets.TARGET_ACCOUNTS, row)
            logger.info("fini_commit_written", company=req.company_name, row=written_row)

            if req.submit_n8n:
                try:
                    from backend.tools.n8n import submit_to_n8n as _submit_n8n, build_payload
                    payload = build_payload(
                        company_name=req.company_name,
                        parent_company_name=req.raw_name,
                        sales_nav_url=req.sales_nav_url,
                        domain=req.domain,
                        sdr_assigned=req.sdr_assigned,
                        email_format=req.email_format,
                        account_type=req.account_type,
                        account_size=req.account_size,
                        row=written_row,
                    )
                    success = await _submit_n8n(payload)
                    if success:
                        try:
                            await sheets.update_row_cells(sheets.TARGET_ACCOUNTS, written_row, 10, ["✓"])
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning("fini_commit_n8n_error", company=req.company_name, error=str(e))

            return {"status": "ok", "row": written_row}
        except Exception as e:
            logger.error("fini_commit_error", company=req.company_name, error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    # ---------------------------------------------------------------------------
    # Searcher endpoints
    # ---------------------------------------------------------------------------

    @app.post("/api/searcher/run", response_model=RunResponse)
    async def run_searcher(req: SearcherRunRequest):
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = _PipelineQueue(thread_id)
        _active_runs[thread_id] = {"agent": "searcher", "status": "running", "thread_id": thread_id, "started_at": datetime.now(timezone.utc).isoformat()}

        _active_tasks[thread_id] = asyncio.create_task(_searcher_task(thread_id, req, auto_trigger_veri=req.auto_trigger_veri))

        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Searcher (gap-fill) started for: {req.companies}",
        )

    # Static routes MUST come before /{thread_id} parameterized routes
    @app.post("/api/searcher/prospect-chat")
    async def prospect_chat(req: ProspectChatRequest) -> dict[str, Any]:
        """Standalone SDR chat — Perplexity + LinkedIn + Claude parallel search."""
        try:
            result = await _prospect_chat(req.query, req.company, req.history)
            return result
        except Exception as e:
            logger.error("prospect_chat_error", error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/searcher/scout-commit")
    async def scout_commit(req: ScoutCommitRequest) -> dict[str, Any]:
        """Write enriched scout candidate to First Clean List (cols A–U)."""
        try:
            result = await _scout_commit(req.model_dump())
            return result
        except Exception as e:
            logger.error("scout_commit_error", error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/searcher/{thread_id}/select-dms")
    async def select_dms(thread_id: str, body: DmSelectionRequest):
        """SDR submits which bonus DM candidates to include in the discovery run."""
        from backend.utils import dm_selection as _dm_sel
        ok = await _dm_sel.submit(thread_id, body.selected_indices)
        if not ok:
            raise HTTPException(status_code=404, detail="No pending DM selection for this thread")
        _last_pause_event.pop(thread_id, None)  # clear replay cache — contact pause is resolved
        return {"ok": True, "selected": len(body.selected_indices)}

    @app.post("/api/searcher/{thread_id}/select-roles")
    async def select_roles(thread_id: str, body: SelectRolesRequest):
        """SDR submits which role-function buckets to include."""
        from backend.utils import role_selection as _rs
        ok = await _rs.submit(thread_id, body.selected_bucket_ids)
        if not ok:
            raise HTTPException(status_code=404, detail="No active role selection session for this thread")
        n = len(body.selected_bucket_ids)
        _last_pause_event.pop(thread_id, None)  # clear replay cache — role pause is resolved
        await _emit_log(thread_id, "info",
                        f"Role selection confirmed — scoring contacts from {n} department(s), please wait…")
        return {"ok": True, "selected": n, "buckets": body.selected_bucket_ids}

    @app.post("/api/searcher/{thread_id}/find-more")
    async def find_more(thread_id: str, body: FindMoreRequest):
        """SDR requests additional candidates via a natural-language prompt."""
        from backend.utils import dm_selection as _dm_sel
        ok = await _dm_sel.request_more(thread_id, body.prompt.strip())
        if not ok:
            raise HTTPException(status_code=404, detail="No active selection session for this thread")
        return {"ok": True, "prompt": body.prompt}

    @app.get("/api/searcher/{thread_id}/dm-pending")
    async def dm_pending(thread_id: str):
        """Check if a DM selection is still waiting for this thread."""
        from backend.utils import dm_selection as _dm_sel
        return {"waiting": _dm_sel.is_waiting(thread_id)}

    # ---------------------------------------------------------------------------
    # Veri endpoints
    # ---------------------------------------------------------------------------

    @app.post("/api/veri/run", response_model=RunResponse)
    async def run_veri(req: VeriRunRequest = None):
        if req is None:
            req = VeriRunRequest()
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = _PipelineQueue(thread_id)
        _active_runs[thread_id] = {"agent": "veri", "status": "running", "thread_id": thread_id, "started_at": datetime.now(timezone.utc).isoformat()}

        _active_tasks[thread_id] = asyncio.create_task(
            _veri_task(thread_id, req.row_start, req.row_end, company_filter=req.company_filter)
        )

        row_msg = f" (rows {req.row_start}–{req.row_end})" if req.row_start else (
            f" (company: {req.company_filter})" if req.company_filter else " (all unverified)"
        )
        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Veri started{row_msg}",
        )

    # ---------------------------------------------------------------------------
    # n8n callback — auto-trigger Veri after n8n populates First Clean List
    # ---------------------------------------------------------------------------

    @app.post("/api/n8n/complete", response_model=RunResponse)
    async def n8n_complete(req: N8nCompleteRequest = None):
        """Callback for n8n to trigger Veri after populating First Clean List."""
        if req is None:
            req = N8nCompleteRequest()
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = _PipelineQueue(thread_id)
        _active_runs[thread_id] = {
            "agent": "veri",
            "status": "running",
            "thread_id": thread_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "auto_triggered_by": "n8n",
        }
        asyncio.create_task(_veri_task(thread_id, row_start=req.row_start, row_end=req.row_end))
        row_msg = f" (rows {req.row_start}–{req.row_end})" if req.row_start else " (all pending)"
        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Veri auto-triggered by n8n{row_msg}",
        )

    # ---------------------------------------------------------------------------
    # n8n JSON contacts endpoint — receives contacts, writes to sheet, triggers chain
    # ---------------------------------------------------------------------------

    @app.post("/api/n8n/contacts", response_model=RunResponse)
    async def n8n_contacts(request: Request):
        """
        n8n sends enriched contacts as JSON — accepts ANY format.
        Auto-detects field names (email/address, first_name/firstname, etc.)

        Accepted formats:
        - {"contacts": [{...}, {...}]}          — array of contacts
        - [{...}, {...}]                        — bare array
        - {"contact": {...}}                    — single contact
        - {...}                                 — single contact object

        1. Logs EVERY webhook hit to "N8N Webhook Log" tab (full visibility)
        2. Writes valid contacts to First Clean List (cols A–P)
        3. Immediately triggers Veri → Searcher → Veri round 2
        """
        import json as _json
        from backend.tools import sheets

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        # Parse body
        try:
            body = await request.json()
        except Exception:
            await _log_n8n_contact(sheets, timestamp, {}, {}, "error", "Invalid JSON body")
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        # Normalize to a list of contact dicts
        raw_contacts: list[dict] = []
        if isinstance(body, dict):
            if "contacts" in body:
                raw_contacts = body["contacts"] if isinstance(body["contacts"], list) else [body["contacts"]]
            elif "contact" in body:
                raw_contacts = [body["contact"]] if isinstance(body["contact"], dict) else body["contact"]
            else:
                raw_contacts = [body]
        elif isinstance(body, list):
            raw_contacts = body
        else:
            await _log_n8n_contact(sheets, timestamp, body if isinstance(body, dict) else {"_raw": str(body)[:200]},
                                   {}, "error", "Could not parse contacts from body")
            raise HTTPException(status_code=400, detail="Could not parse contacts from request body")

        if not raw_contacts:
            await _log_n8n_contact(sheets, timestamp, {}, {}, "error", "No contacts found in request")
            raise HTTPException(status_code=400, detail="No contacts found in request")

        # Step 1: Normalize and write contacts to First Clean List
        # Log EVERY contact to webhook log — written or skipped
        companies_seen: set[str] = set()
        rows_written = 0
        skipped = 0
        skip_reasons: list[str] = []
        try:
            await sheets.ensure_headers(sheets.FIRST_CLEAN_LIST, sheets.FIRST_CLEAN_LIST_HEADERS)
            for raw in raw_contacts:
                if not isinstance(raw, dict):
                    skipped += 1
                    skip_reasons.append(f"Not a dict: {str(raw)[:80]}")
                    await _log_n8n_contact(sheets, timestamp, {"_raw": str(raw)[:200]},
                                           {}, "skipped", "Not a valid contact object")
                    continue
                c = _normalize_contact(raw)

                # Skip contacts with no name
                if not c["first_name"] and not c["last_name"]:
                    skipped += 1
                    company_hint = c["company_name"] or "(no company)"
                    reason = f"No first/last name — keys: {list(raw.keys())[:6]}"
                    skip_reasons.append(f"{company_hint}: {reason}")
                    await _log_n8n_contact(sheets, timestamp, raw, c, "skipped", reason)
                    continue

                row = [
                    c["company_name"],                    # A
                    c["normalized_name"],                 # B
                    c["domain"],                          # C
                    c["account_type"],                    # D
                    c["account_size"],                    # E
                    c["country"],                         # F
                    c["first_name"],                      # G
                    c["last_name"],                       # H
                    c["job_title"],                       # I
                    c["buying_role"],                     # J
                    c["linkedin_url"],                    # K
                    c["email"],                           # L
                    c["phone_1"],                         # M
                    c["phone_2"],                         # N
                    "n8n",                                # O (Source)
                    "",                                   # P (Pipeline Status)
                ]
                await sheets.append_row(sheets.FIRST_CLEAN_LIST, row)
                rows_written += 1
                if c["company_name"]:
                    companies_seen.add(c["company_name"])
                # Log the written contact too
                await _log_n8n_contact(sheets, timestamp, raw, c, "written", "")
            logger.info("n8n_contacts_written", count=rows_written, skipped=skipped,
                        companies=list(companies_seen))
        except Exception as e:
            logger.error("n8n_contacts_write_error", error=str(e))
            raise HTTPException(status_code=500, detail=f"Failed to write contacts to sheet: {e}")

        # Step 2: Trigger Veri → Searcher → Veri chain
        thread_id = str(uuid.uuid4())
        if rows_written > 0:
            _log_queues[thread_id] = _PipelineQueue(thread_id)
            _active_runs[thread_id] = {
                "agent": "veri",
                "status": "running",
                "thread_id": thread_id,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "auto_triggered_by": "n8n",
            }
            company_filter = ",".join(companies_seen) if companies_seen else None
            asyncio.create_task(_veri_task(thread_id, company_filter=company_filter))
            logger.info("n8n_contacts_chain_started",
                        contacts=rows_written, companies=list(companies_seen), thread_id=thread_id)
        else:
            logger.info("n8n_contacts_no_chain", reason="0 rows written", skipped=skipped)

        # Store last received data for debugging
        _n8n_last_received.clear()
        _n8n_last_received.update({
            "received_at": timestamp,
            "raw_contacts_count": len(raw_contacts),
            "rows_written": rows_written,
            "skipped": skipped,
            "skip_reasons": skip_reasons,
            "companies": list(companies_seen),
            "sample_raw": raw_contacts[0] if raw_contacts else {},
            "sample_normalized": _normalize_contact(raw_contacts[0]) if raw_contacts else {},
        })

        chain_msg = "Veri → Searcher → Veri chain started" if rows_written > 0 else "No chain — 0 contacts written"
        skip_msg = f" Skipped {skipped}: {'; '.join(skip_reasons[:3])}" if skipped else ""

        return RunResponse(
            thread_id=thread_id,
            status="started" if rows_written > 0 else "empty",
            message=f"Received {rows_written} contacts for {len(companies_seen)} companies. "
                    f"{chain_msg}.{skip_msg}",
        )

    @app.get("/api/n8n/debug")
    async def n8n_debug():
        """Check the last data received from n8n — for debugging field mapping."""
        result = dict(_n8n_last_received) if _n8n_last_received else {"message": "No data received yet"}
        if _n8n_log_last_error:
            result["_webhook_log_error"] = _n8n_log_last_error
        return result

    # ---------------------------------------------------------------------------
    # Auto-pipeline: Fini → n8n → Veri → Searcher → Veri (fully automatic)
    # ---------------------------------------------------------------------------

    @app.post("/api/pipeline/auto", response_model=RunResponse)
    async def auto_pipeline(req: AutoPipelineRequest):
        """
        Fully automatic pipeline. Steps:
        1. Fini enriches companies → submits to n8n
        2. n8n populates First Clean List
        3. Veri auto-verifies all unverified rows
        4. Searcher finds role gaps → writes new contacts
        5. Veri round 2 verifies Searcher's contacts
        """
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = _PipelineQueue(thread_id)
        _active_runs[thread_id] = {
            "agent": "auto_pipeline",
            "status": "running",
            "thread_id": thread_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        # Start Fini with auto_mode + n8n relay — the chain triggers automatically:
        # Fini → n8n callback → Veri (auto_triggered_by=n8n) → Searcher → Veri round 2
        fini_req = FiniRunRequest(
            companies=req.companies,
            sdr=req.sdr,
            submit_n8n=True,
            region=req.region,
            auto_mode=True,
        )

        fini_thread = str(uuid.uuid4())
        _log_queues[fini_thread] = _PipelineQueue(fini_thread)
        _active_runs[fini_thread] = {
            "agent": "fini",
            "status": "running",
            "thread_id": fini_thread,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "auto_triggered_by": "auto_pipeline",
        }

        _active_tasks[fini_thread] = asyncio.create_task(
            _auto_pipeline_task(thread_id, fini_thread, fini_req, req.companies)
        )

        company_list = [c.strip() for c in req.companies.split(",") if c.strip()]
        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Auto-pipeline started for {len(company_list)} companies: {', '.join(company_list[:5])}",
        )

    # ---------------------------------------------------------------------------
    # Run status
    # ---------------------------------------------------------------------------

    @app.get("/api/runs")
    async def list_runs() -> list[dict[str, Any]]:
        return list(_active_runs.values())

    # Static routes MUST come before /{thread_id} parameterized routes
    @app.post("/api/pipeline/stop-all")
    async def stop_all_runs() -> dict:
        """Emergency kill switch — cancel ALL running agents."""
        cancelled = []
        for thread_id, task in list(_active_tasks.items()):
            if task and not task.done():
                task.cancel()
                _active_runs.get(thread_id, {})["status"] = "cancelled"
                cancelled.append(thread_id)
        logger.info("stop_all_runs", cancelled=len(cancelled))
        return {
            "status": "all_stopped",
            "cancelled_count": len(cancelled),
            "cancelled_threads": cancelled,
        }

    @app.get("/api/runs/{thread_id}")
    async def get_run(thread_id: str) -> dict[str, Any]:
        if thread_id not in _active_runs:
            raise HTTPException(status_code=404, detail="Run not found")
        return _active_runs[thread_id]

    @app.post("/api/runs/{thread_id}/cancel")
    async def cancel_run(thread_id: str) -> dict[str, str]:
        """Cancel a running pipeline task."""
        task = _active_tasks.get(thread_id)
        if not task or task.done():
            raise HTTPException(status_code=404, detail="No active task for this thread")
        task.cancel()
        _active_runs.get(thread_id, {})["status"] = "cancelling"
        return {"status": "cancelling", "thread_id": thread_id}

    @app.post("/api/runs/{thread_id}/pause")
    async def pause_run(thread_id: str) -> dict[str, str]:
        """Pause a running pipeline at the next checkpoint."""
        from backend.utils import pause as _pause
        if not _pause.pause(thread_id):
            raise HTTPException(status_code=404, detail="No active run for this thread")
        _active_runs.get(thread_id, {})["status"] = "paused"
        await _emit_event(thread_id, "paused", {"message": "Paused — will stop at next checkpoint"})
        return {"status": "paused", "thread_id": thread_id}

    @app.post("/api/runs/{thread_id}/resume")
    async def resume_run(thread_id: str) -> dict[str, str]:
        """Resume a paused pipeline run."""
        from backend.utils import pause as _pause
        if not _pause.resume(thread_id):
            raise HTTPException(status_code=404, detail="No active run for this thread")
        _active_runs.get(thread_id, {})["status"] = "running"
        await _emit_event(thread_id, "resumed", {"message": "Resumed"})
        return {"status": "running", "thread_id": thread_id}

    # ---------------------------------------------------------------------------
    # Orchestrator / Command Center endpoints
    # ---------------------------------------------------------------------------

    class OrchestratorTriggerRequest(BaseModel):
        agent: str  # "fini" | "searcher" | "veri"
        companies: list[str]
        dm_roles: str = ""
        row_start: Optional[int] = None
        row_end: Optional[int] = None
        auto_approve: bool = True

    class OrchestratorAnalyzeRequest(BaseModel):
        focus: str = ""

    class AutoModeRequest(BaseModel):
        enabled: bool
        poll_interval_secs: int = 60
        auto_trigger_searcher: bool = True
        auto_trigger_veri: bool = True
        dry_run: bool = False

    @app.get("/api/orchestrator/status")
    async def orchestrator_status(include_ai: bool = False, company: str = "", start_row: int = 1):
        from backend.orchestrator import compute_full_status, run_ai_analysis
        status = await compute_full_status(company_filter=company, start_row=start_row)
        if include_ai:
            status["ai_analysis"] = await run_ai_analysis(status["companies"])
        return status

    @app.post("/api/orchestrator/trigger")
    async def orchestrator_trigger(req: OrchestratorTriggerRequest):
        results = []
        for company in req.companies:
            thread_id = str(uuid.uuid4())
            _log_queues[thread_id] = _PipelineQueue(thread_id)
            _active_runs[thread_id] = {
                "agent": req.agent, "status": "running",
                "thread_id": thread_id,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "auto_triggered_by": "orchestrator",
                "company": company,
            }
            if req.agent == "fini":
                fini_req = FiniRunRequest(companies=company, auto_mode=True)
                _active_tasks[thread_id] = asyncio.create_task(
                    _fini_task(thread_id, fini_req)
                )
            elif req.agent == "searcher":
                searcher_req = SearcherRunRequest(
                    companies=company,
                    dm_roles=req.dm_roles or "CEO,CTO,CIO,CSO,Head of Sales,CMO",
                    auto_approve=req.auto_approve,
                )
                _active_tasks[thread_id] = asyncio.create_task(
                    _searcher_task(thread_id, searcher_req)
                )
            elif req.agent == "veri":
                _active_tasks[thread_id] = asyncio.create_task(
                    _veri_task(thread_id, req.row_start, req.row_end)
                )
            results.append({"company": company, "agent": req.agent, "thread_id": thread_id, "status": "started"})
        return {"triggered": results}

    @app.post("/api/orchestrator/analyze")
    async def orchestrator_analyze(req: OrchestratorAnalyzeRequest):
        from backend.orchestrator import compute_full_status, run_ai_analysis
        status = await compute_full_status()
        return await run_ai_analysis(status["companies"], focus=req.focus)

    @app.post("/api/orchestrator/auto-mode")
    async def orchestrator_auto_mode_toggle(req: AutoModeRequest):
        from backend.orchestrator import start_auto_mode, stop_auto_mode
        if req.enabled:
            # Create a trigger function that uses the existing task launchers
            async def _trigger(agent: str, companies: list[str]):
                for company in companies:
                    tid = str(uuid.uuid4())
                    _log_queues[tid] = _PipelineQueue(tid)
                    _active_runs[tid] = {
                        "agent": agent, "status": "running", "thread_id": tid,
                        "started_at": datetime.now(timezone.utc).isoformat(),
                        "auto_triggered_by": "auto_mode", "company": company,
                    }
                    if agent == "searcher":
                        _active_tasks[tid] = asyncio.create_task(
                            _searcher_task(tid, SearcherRunRequest(
                                companies=company, auto_approve=True,
                            ))
                        )
                    elif agent == "veri":
                        _active_tasks[tid] = asyncio.create_task(
                            _veri_task(tid)
                        )

            task_id = await start_auto_mode(
                req.poll_interval_secs, req.auto_trigger_searcher,
                req.auto_trigger_veri, req.dry_run, trigger_fn=_trigger,
            )
            return {"auto_mode": True, "poll_interval_secs": req.poll_interval_secs, "task_id": task_id}
        else:
            await stop_auto_mode()
            return {"auto_mode": False}

    @app.get("/api/orchestrator/auto-mode/status")
    async def orchestrator_auto_mode_status():
        from backend.orchestrator import get_auto_mode_status
        return get_auto_mode_status()

    # ---------------------------------------------------------------------------
    # WebSocket for real-time logs
    # ---------------------------------------------------------------------------

    class FiniReenrichRequest(BaseModel):
        company_name: str
        region: str = ""

    @app.post("/api/fini/reenrich")
    async def fini_reenrich(req: FiniReenrichRequest) -> dict[str, Any]:
        """Re-enrich a single company with a corrected name. Returns CompanyData."""
        from backend.agents.fini import _enrich_single_company
        from backend.state import TargetCompany

        company = TargetCompany(raw_name=req.company_name)
        try:
            enriched = await _enrich_single_company(company, req.region)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return {
            "raw_name": enriched.raw_name,
            "company_name": enriched.normalized_name or enriched.raw_name,
            "sales_nav_url": enriched.sales_nav_url or "",
            "domain": enriched.domain or "",
            "sdr_assigned": enriched.sdr_assigned or "",
            "email_format": enriched.email_format or "",
            "account_type": enriched.account_type or "",
            "account_size": enriched.account_size or "",
            "linkedin_org_id": enriched.linkedin_org_id or "",
            "linkedin_confidence": enriched.linkedin_confidence,
            "domain_confidence": enriched.domain_confidence,
            "email_confidence": enriched.email_confidence,
            "size_confidence": enriched.size_confidence,
            "agent_notes": enriched.agent_notes,
            "linkedin_candidates": enriched.linkedin_candidates,
        }

    @app.get("/api/fini/results/{thread_id}")
    async def get_fini_results(thread_id: str) -> dict[str, Any]:
        """Return cached enrichment results for a completed fini run."""
        if thread_id not in _fini_results:
            run = _active_runs.get(thread_id)
            if not run:
                raise HTTPException(status_code=404, detail="Run not found")
            return {"status": run.get("status", "unknown"), "companies": []}
        return {"status": "completed", **_fini_results[thread_id]}

    @app.websocket("/ws/{thread_id}")
    async def websocket_endpoint(websocket: WebSocket, thread_id: str):
        await websocket.accept()
        logger.info("ws_connected", thread_id=thread_id)

        # If this run already completed (e.g. WS reconnect after disconnect),
        # immediately replay the cached completed event so the frontend gets results.
        if _active_runs.get(thread_id, {}).get("status") == "completed" and thread_id in _fini_results:
            try:
                await websocket.send_json({
                    "type": "completed",
                    "data": _fini_results[thread_id],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                pass
            return

        # Use existing queue (created by the task on start)
        # If not ready yet, wait briefly
        for _ in range(20):
            if thread_id in _log_queues:
                break
            await asyncio.sleep(0.1)
        if thread_id not in _log_queues:
            _log_queues[thread_id] = _PipelineQueue(thread_id)

        queue = _log_queues[thread_id]

        # On WS reconnect: if the pipeline is paused waiting for SDR input,
        # replay the last pause event so the UI re-shows the panel.
        if thread_id in _last_pause_event and queue.empty():
            try:
                await websocket.send_json(_last_pause_event[thread_id])
                logger.info("ws_replayed_pause_event", thread_id=thread_id,
                            event_type=_last_pause_event[thread_id].get("type"))
            except Exception:
                pass

        try:
            while True:
                try:
                    # Wait for messages with timeout
                    msg = await asyncio.wait_for(queue.get(), timeout=60)
                    await websocket.send_json(msg)

                    # If run completed or errored, send final status and close
                    if msg.get("type") in ("completed", "error"):
                        break

                except asyncio.TimeoutError:
                    # Send heartbeat to keep connection alive
                    try:
                        await websocket.send_json({"type": "heartbeat"})
                    except Exception:
                        break

        except WebSocketDisconnect:
            logger.info("ws_disconnected", thread_id=thread_id)
        except Exception as e:
            logger.error("ws_error", thread_id=thread_id, error=str(e))
        finally:
            # Only remove the queue if the run is done (completed/failed)
            run = _active_runs.get(thread_id, {})
            if run.get("status") in ("completed", "failed"):
                _log_queues.pop(thread_id, None)

    # ---------------------------------------------------------------------------
    # ZeroBounce credits check
    # ---------------------------------------------------------------------------

    @app.get("/api/zerobounce/credits")
    async def zerobounce_credits() -> dict[str, Any]:
        try:
            from backend.tools.zerobounce import get_credits
            credits = await get_credits()
            return {"credits": credits}
        except Exception as e:
            return {"credits": None, "error": str(e)}

    # No static file serving for now, as we use Next.js on :3000 in dev.
    @app.get("/")
    async def root() -> dict[str, str]:
        return {
            "name": "SCAI ProspectOps API",
            "version": "1.0.0",
            "status": "online",
            "nextjs_ui": "http://localhost:3000"
        }

    return app


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _fini_task(thread_id: str, req: FiniRunRequest):
    """Background task running the Fini agent with WebSocket logging."""
    from backend.agents.fini import build_fini_graph
    from backend.state import FiniState, TargetCompany
    from backend.utils.progress import register as _reg_progress

    company_names = [c.strip() for c in req.companies.split(",") if c.strip()]
    sdr = req.sdr.strip() or "Amy"
    companies = [TargetCompany(raw_name=name, sdr_assigned=sdr) for name in company_names]
    state = FiniState(
        companies=companies, submit_to_n8n=req.submit_n8n, region=req.region,
        thread_id=thread_id, auto_mode=req.auto_mode, sdr_name=sdr,
    )
    config = {"configurable": {"thread_id": thread_id}}

    # Register queue so fini.py can emit per-company progress events
    if thread_id in _log_queues:
        _reg_progress(thread_id, _log_queues[thread_id])

    from backend.utils import pause as _pause_util
    _pause_util.register(thread_id)

    try:
        # Small delay so the UI WebSocket can connect before first events are emitted
        await asyncio.sleep(0.5)
        await _emit_log(thread_id, "info", f"Starting Fini for {len(company_names)} companies")
        app_graph = await build_fini_graph()

        local_config = {**config, "recursion_limit": 500}
        while True:
            # Check pause gate between company iterations
            await _pause_util.await_if_paused(thread_id)
            result = await app_graph.ainvoke(state, local_config)
            if isinstance(result, dict):
                state = FiniState(**result)
            else:
                state = result

            if state.status == "completed":
                _active_runs[thread_id]["status"] = "completed"
                # Serialize companies for frontend review cards
                companies_data = [
                    {
                        "raw_name": c.raw_name,
                        "company_name": c.normalized_name or c.raw_name,
                        "sales_nav_url": c.sales_nav_url or "",
                        "domain": c.domain or "",
                        "sdr_assigned": c.sdr_assigned or "",
                        "email_format": c.email_format or "",
                        "account_type": c.account_type or "",
                        "account_size": c.account_size or "",
                        "linkedin_org_id": c.linkedin_org_id or "",
                        "linkedin_confidence": c.linkedin_confidence,
                        "domain_confidence": c.domain_confidence,
                        "email_confidence": c.email_confidence,
                        "size_confidence": c.size_confidence,
                        "agent_notes": c.agent_notes,
                        "linkedin_candidates": c.linkedin_candidates,
                        "auto_committed": c.auto_committed,
                        "selection_reasoning": c.selection_reasoning,
                    }
                    for c in state.companies
                ]
                completed_payload = {
                    "companies_processed": len(state.companies),
                    "companies": companies_data,
                    "errors": state.errors,
                }
                # Cache results so WS reconnects and REST polling can retrieve them
                _fini_results[thread_id] = completed_payload
                await _emit_event(thread_id, "completed", completed_payload)
                break

            if state.status == "awaiting_confirmation":
                company = state.companies[state.current_index]
                _pending_confirmations[thread_id] = asyncio.Event()

                await _emit_event(thread_id, "confirmation_required", {
                    "company_index": state.current_index,
                    "total_companies": len(state.companies),
                    "raw_name": company.raw_name,
                    "normalized_name": company.normalized_name,
                    "domain": company.domain,
                    "email_format": company.email_format,
                    "linkedin_org_id": company.linkedin_org_id,
                    "sales_nav_url": company.sales_nav_url,
                    "sdr_assigned": company.sdr_assigned,
                    "account_type": company.account_type,
                    "account_size": company.account_size,
                })
                _active_runs[thread_id]["status"] = "awaiting_confirmation"

                # Wait for operator response (up to 10 minutes)
                try:
                    await asyncio.wait_for(_pending_confirmations[thread_id].wait(), timeout=3600)
                except asyncio.TimeoutError:
                    await _emit_log(thread_id, "warning", "Confirmation timeout. Skipping company.")
                    break

                conf_data = _confirmation_data.pop(thread_id, {})
                if conf_data.get("confirmed"):
                    companies = list(state.companies)
                    c = companies[state.current_index]
                    updated = c.model_copy(update={
                        "normalized_name": conf_data.get("normalized_name") or c.normalized_name,
                        "domain": conf_data.get("domain") or c.domain,
                        "email_format": conf_data.get("email_format") or c.email_format,
                        "sdr_assigned": conf_data.get("sdr_assigned") or c.sdr_assigned,
                        "account_type": conf_data.get("account_type") or c.account_type,
                        "account_size": conf_data.get("account_size") or c.account_size,
                        "linkedin_org_id": conf_data.get("linkedin_org_id") or c.linkedin_org_id,
                        "sales_nav_url": conf_data.get("sales_nav_url") or c.sales_nav_url,
                        "operator_confirmed": True,
                    })
                    companies[state.current_index] = updated
                    # status=running so should_continue routes back to scrape_linkedin_org,
                    # but operator_confirmed=True means confirm_with_operator passes through
                    state = state.model_copy(update={"companies": companies, "status": "running"})
                    _active_runs[thread_id]["status"] = "running"
                    await _emit_log(thread_id, "info", f"Confirmed: {company.raw_name}")
                    # Loop continues — ainvoke called again with updated state
                else:
                    await _emit_log(thread_id, "info", f"Skipped: {company.raw_name}")
                    next_idx = state.current_index + 1
                    if next_idx >= len(state.companies):
                        _active_runs[thread_id]["status"] = "completed"
                        await _emit_event(thread_id, "completed", {
                            "companies_processed": len(state.companies),
                            "errors": state.errors,
                        })
                        break
                    else:
                        state = state.model_copy(update={"current_index": next_idx, "status": "running"})
            else:
                break

    except asyncio.CancelledError:
        logger.info("fini_task_cancelled", thread_id=thread_id)
        _active_runs[thread_id]["status"] = "cancelled"
        await _emit_event(thread_id, "cancelled", {"message": "Run stopped by user"})
    except Exception as e:
        logger.error("fini_task_error", error=str(e), thread_id=thread_id)
        _active_runs[thread_id]["status"] = "failed"
        await _emit_event(thread_id, "error", {"error": str(e)})
    finally:
        _pause_util.unregister(thread_id)


async def _searcher_task(thread_id: str, req: SearcherRunRequest, auto_trigger_veri: bool = True):
    """Background task running the Searcher gap-fill agent."""
    from backend.agents.searcher import build_searcher_graph
    from backend.state import SearcherState
    from backend.utils.progress import register as _reg_progress

    # Register queue for per-company progress events
    _reg_progress(thread_id, _log_queues[thread_id])

    from backend.utils import pause as _pause_util
    _pause_util.register(thread_id)

    # Parse "Company:domain,..." format
    target_companies = []
    for pair in req.companies.split(","):
        pair = pair.strip()
        if ":" in pair:
            name, domain = pair.split(":", 1)
            target_companies.append({"name": name.strip(), "domain": domain.strip()})
        elif pair:
            target_companies.append({"name": pair, "domain": ""})

    dm_roles = [r.strip() for r in req.dm_roles.split(",") if r.strip()]
    first = target_companies[0] if target_companies else {"name": "", "domain": ""}

    # Emit initial queued status for all companies
    from backend.utils.progress import emit as _emit_progress
    for tc in target_companies:
        await _emit_progress(thread_id, tc["name"], "queued")

    state = SearcherState(
        target_company=first["name"],
        target_domain=first["domain"],
        target_companies=target_companies,
        dm_roles=dm_roles,
        target_contact_count=req.target_contact_count,
        thread_id=thread_id,
        auto_approve=req.auto_approve,
    )
    config = {"configurable": {"thread_id": thread_id}}

    try:
        await _emit_log(thread_id, "info", f"Starting Searcher gap-fill for {len(target_companies)} companies")
        app_graph = await build_searcher_graph()

        local_config = {**config, "recursion_limit": 1000}
        result = await app_graph.ainvoke(state, local_config)
        if isinstance(result, dict):
            state = SearcherState(**result)
        else:
            state = result

        # --- Auto-trigger Veri on newly written contacts ---
        # Veri will auto-detect unverified rows (col U empty) for these companies.
        # Using company_filter instead of row ranges — safer and works with auto-detect.
        veri_thread_id = None
        if auto_trigger_veri and state.total_contacts_written > 0:
            # Get unique companies Searcher wrote contacts for
            _searcher_companies: set[str] = set()
            for c in state.discovered_contacts:
                if c.company:
                    _searcher_companies.add(c.company)
            company_str = ",".join(_searcher_companies) if _searcher_companies else state.target_company

            veri_thread_id = str(uuid.uuid4())
            _log_queues[veri_thread_id] = _PipelineQueue(veri_thread_id)
            _active_runs[veri_thread_id] = {
                "agent": "veri",
                "status": "running",
                "thread_id": veri_thread_id,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "auto_triggered_by": "searcher",
            }
            await _emit_log(thread_id, "info",
                f"Auto-triggering Veri for {state.total_contacts_written} new Searcher contacts")
            asyncio.create_task(_veri_task(
                veri_thread_id,
                company_filter=company_str,
            ))

        _active_runs[thread_id]["status"] = "completed"
        _active_runs[thread_id]["contacts_appended"] = state.total_contacts_written
        _active_runs[thread_id]["contacts_discovered"] = len(state.discovered_contacts)
        _active_runs[thread_id]["missing_roles"] = state.missing_dm_roles[:10]
        _active_runs[thread_id]["errors"] = state.errors[:5]
        await _emit_event(thread_id, "completed", {
            "contacts_appended": state.total_contacts_written,
            "contacts": [c.model_dump() for c in state.discovered_contacts[:50]],
            "errors": state.errors,
            "veri_thread_id": veri_thread_id,
        })

    except asyncio.CancelledError:
        logger.info("searcher_task_cancelled", thread_id=thread_id)
        _active_runs[thread_id]["status"] = "cancelled"
        # Unblock any waiting SDR selection queues so they don't leak
        try:
            from backend.utils import dm_selection as _dm_sel, role_selection as _rs
            _dm_sel.unregister(thread_id)
            _rs.unregister(thread_id)
        except Exception:
            pass
        await _emit_event(thread_id, "cancelled", {"message": "Run stopped by user"})
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error("searcher_task_error", error=str(e), traceback=tb)
        print(f"[SEARCHER ERROR] {tb}", flush=True)
        _active_runs[thread_id]["status"] = "failed"
        _active_runs[thread_id]["error"] = str(e)
        _active_runs[thread_id]["traceback"] = tb[-500:]  # last 500 chars
        await _emit_event(thread_id, "error", {"error": str(e)})
    finally:
        _pause_util.unregister(thread_id)


async def _poll_for_new_rows(
    company_names: list[str],
    pipeline_thread: str,
    timeout: int = 600,
    poll_interval: int = 30,
) -> int:
    """
    Poll First Clean List until new unverified rows appear for any of the given companies.
    Returns the count of new rows found.
    Raises TimeoutError if no rows appear within timeout seconds.
    """
    from backend.tools import sheets
    elapsed = 0

    while elapsed < timeout:
        try:
            records = await sheets.read_all_records(sheets.FIRST_CLEAN_LIST)
            count = 0
            for row in records:
                company = str(row.get("Company Name", "") or "").strip()
                overall = str(row.get("Overall Status", "") or "").strip()
                if overall:
                    continue  # already verified — skip
                # Check if this row belongs to one of our companies
                for target in company_names:
                    if target.lower() in company.lower():
                        count += 1
                        break
            if count > 0:
                await _emit_log(pipeline_thread, "info",
                    f"Auto-pipeline: found {count} new unverified rows in First Clean List")
                return count
        except Exception as e:
            logger.warning("auto_pipeline_poll_error", error=str(e))

        await _emit_log(pipeline_thread, "info",
            f"Auto-pipeline: waiting for n8n… ({elapsed}s / {timeout}s)")
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"No new rows appeared in First Clean List after {timeout}s")


async def _auto_pipeline_task(
    pipeline_thread: str, fini_thread: str, fini_req: FiniRunRequest, companies_raw: str,
):
    """
    Full auto-pipeline background task.

    Flow:
      1. Fini enriches companies → submits to n8n
      2. Poll First Clean List until n8n rows appear (up to 10 min)
      3. Veri verifies all unverified rows for these companies
      4. Searcher finds role gaps → writes new contacts (auto-triggered by Veri)
      5. Veri round 2 verifies Searcher's contacts (auto-triggered by Searcher)

    Loop prevention via auto_triggered_by flags:
      Veri(auto_pipeline) → triggers Searcher
      Searcher(veri) → triggers Veri round 2
      Veri(searcher) → STOPS
    """
    company_list = [c.strip() for c in companies_raw.split(",") if c.strip()]
    try:
        # ── Step 1: Run Fini ─────────────────────────────────────────────────
        await _emit_log(pipeline_thread, "info",
            f"Auto-pipeline: Step 1/3 — starting Fini for {len(company_list)} companies")
        await _fini_task(fini_thread, fini_req)
        await _emit_log(pipeline_thread, "info",
            "Auto-pipeline: Fini complete. Waiting for n8n to populate First Clean List…")

        # ── Step 2: Poll until n8n writes rows ───────────────────────────────
        try:
            new_count = await _poll_for_new_rows(
                company_list, pipeline_thread, timeout=600, poll_interval=30,
            )
        except TimeoutError:
            await _emit_log(pipeline_thread, "warning",
                "Auto-pipeline: n8n did not populate within 10 minutes. "
                "Running Veri anyway on any existing unverified rows…")
            new_count = 0

        # ── Step 3: Trigger Veri → Searcher → Veri chain ─────────────────────
        veri_thread = str(uuid.uuid4())
        _log_queues[veri_thread] = _PipelineQueue(veri_thread)
        _active_runs[veri_thread] = {
            "agent": "veri",
            "status": "running",
            "thread_id": veri_thread,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "auto_triggered_by": "auto_pipeline",  # Veri will auto-trigger Searcher on completion
        }
        company_filter = ",".join(company_list)
        await _emit_log(pipeline_thread, "info",
            f"Auto-pipeline: Step 2/3 — triggering Veri for {len(company_list)} companies "
            f"({new_count} new rows detected)")

        # Run Veri (blocking). When it completes, it auto-triggers Searcher
        # (because auto_triggered_by="auto_pipeline"), which then auto-triggers
        # Veri round 2 for newly written Searcher contacts.
        await _veri_task(veri_thread, company_filter=company_filter)

        # At this point: Veri round 1 is done, Searcher + Veri round 2 are running
        # in background (fire-and-forget from _veri_task).
        # Wait a bit for the Searcher chain to complete.
        await _emit_log(pipeline_thread, "info",
            "Auto-pipeline: Step 3/3 — Searcher + Veri round 2 running in background…")

        # Poll for Searcher + Veri round 2 completion (check active runs)
        for _ in range(60):  # up to 30 minutes
            await asyncio.sleep(30)
            # Check if any searcher/veri tasks triggered by this pipeline are still running
            still_running = False
            for run_id, run_info in _active_runs.items():
                if run_info.get("status") == "running" and run_info.get("auto_triggered_by") in (
                    "auto_pipeline", "veri", "searcher"
                ):
                    still_running = True
                    break
            if not still_running:
                break

        _active_runs[pipeline_thread]["status"] = "completed"
        await _emit_log(pipeline_thread, "info",
            f"Auto-pipeline: COMPLETE for {len(company_list)} companies")
        await _emit_event(pipeline_thread, "completed", {
            "message": f"Auto-pipeline complete for {len(company_list)} companies",
            "companies": company_list,
        })

    except asyncio.CancelledError:
        _active_runs[pipeline_thread]["status"] = "cancelled"
        await _emit_event(pipeline_thread, "cancelled", {"message": "Auto-pipeline cancelled"})
    except Exception as e:
        logger.error("auto_pipeline_error", error=str(e))
        _active_runs[pipeline_thread]["status"] = "failed"
        await _emit_event(pipeline_thread, "error", {"error": str(e)})


async def _veri_task(thread_id: str, row_start: int = None, row_end: int = None, company_filter: str = None):
    """Background task running the Veri agent on First Clean List."""
    from backend.agents.veri import build_veri_graph
    from backend.state import VeriState
    from backend.utils import pause as _pause_util
    from backend.utils.progress import register as _reg_progress
    _pause_util.register(thread_id)
    # Wire the WebSocket queue into the progress module so emit_veri_step / emit_veri_contact work
    if thread_id in _log_queues:
        _reg_progress(thread_id, _log_queues[thread_id])

    state = VeriState(
        contacts=[], row_start=row_start, row_end=row_end,
        company_filter=company_filter, thread_id=thread_id,
    )
    config = {"configurable": {"thread_id": thread_id}}

    try:
        await asyncio.sleep(0.5)
        if company_filter:
            range_msg = f" company={company_filter}"
        elif row_start:
            range_msg = f" rows {row_start}–{row_end}"
        else:
            range_msg = " all unverified contacts"
        await _emit_log(thread_id, "info", f"Starting Veri agent —{range_msg}")
        app_graph = await build_veri_graph()

        # 10 nodes per contact loop + buffer. Contacts are loaded inside the graph
        # so we estimate from the row range; default to 10_000 for full runs.
        estimated_contacts = (row_end - row_start + 1) if (row_start and row_end) else 10_000
        local_config = {**config, "recursion_limit": estimated_contacts * 15}
        result = await app_graph.ainvoke(state, local_config)
        if isinstance(result, dict):
            state = VeriState(**result)
        else:
            state = result

        # --- Auto-trigger Searcher gap-fill after Veri completes ---
        run_info = _active_runs.get(thread_id, {})
        triggered_by = run_info.get("auto_triggered_by", "")

        # Build company list from all available sources
        _companies_in_run: set[str] = set()

        # Source 1: contacts that Veri just processed
        for c in state.contacts:
            if c.company:
                _companies_in_run.add(c.company)

        # Source 2: company_filter passed to this Veri task
        if not _companies_in_run and company_filter:
            _companies_in_run = {c.strip() for c in company_filter.split(",") if c.strip()}

        # Source 3: read companies from the row range in sheet
        if not _companies_in_run and (row_start or row_end):
            try:
                from backend.tools import sheets
                records = await sheets.read_all_records(sheets.FIRST_CLEAN_LIST)
                start_idx = (row_start - 2) if row_start else 0
                end_idx = (row_end - 1) if row_end else len(records)
                for rec in records[start_idx:end_idx]:
                    co = str(rec.get("Company Name", "") or "").strip()
                    if co:
                        _companies_in_run.add(co)
            except Exception as e:
                print(f"[VERI CHAIN] row range fallback error: {e}", flush=True)

        print(f"[VERI CHAIN] triggered_by='{triggered_by}', verified={state.verified_count}, "
              f"contacts={len(state.contacts)}, companies={_companies_in_run}", flush=True)

        # Trigger Searcher UNLESS this Veri was triggered by Searcher (prevents infinite loop)
        if triggered_by == "searcher":
            print("[VERI CHAIN] Veri round 2 done — chain complete, NOT triggering Searcher", flush=True)
            await _emit_log(thread_id, "info", "Veri round 2 complete — chain finished")
        elif _companies_in_run:
            # ── SEQUENTIAL COMPANY PROCESSING ──
            # Process ONE company at a time: Searcher(A) → Veri(A) → Searcher(B) → Veri(B) → ...
            # This prevents interleaving (Zepto/Nike/Red Bull rows mixed together)
            # and ensures each company's data is a clean block in the sheet.
            company_list = sorted(_companies_in_run)
            print(f"[VERI CHAIN] SEQUENTIAL mode: {len(company_list)} companies: {company_list}", flush=True)
            await _emit_log(thread_id, "info",
                f"Starting sequential Searcher→Veri for {len(company_list)} companies: {', '.join(company_list)}")

            for i, company in enumerate(company_list):
                company_num = f"[{i+1}/{len(company_list)}]"
                print(f"[VERI CHAIN] {company_num} Starting Searcher for: {company}", flush=True)
                await _emit_log(thread_id, "info", f"{company_num} Searcher starting for: {company}")

                # Run Searcher for this ONE company
                searcher_thread = str(uuid.uuid4())
                _log_queues[searcher_thread] = _PipelineQueue(searcher_thread)
                _active_runs[searcher_thread] = {
                    "agent": "searcher", "status": "running",
                    "thread_id": searcher_thread,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "auto_triggered_by": "veri",
                }
                searcher_req = SearcherRunRequest(
                    companies=company,  # ONE company at a time
                    auto_approve=True,
                    auto_trigger_veri=False,  # We handle Veri ourselves below
                )
                try:
                    await _searcher_task(searcher_thread, searcher_req, auto_trigger_veri=False)
                    await _emit_log(thread_id, "info", f"{company_num} Searcher done for: {company}")
                except Exception as e:
                    print(f"[VERI CHAIN] {company_num} Searcher error for {company}: {e}", flush=True)
                    await _emit_log(thread_id, "error", f"{company_num} Searcher failed for {company}: {e}")

                # Run Veri round 2 for this company's searcher contacts
                veri2_thread = str(uuid.uuid4())
                _log_queues[veri2_thread] = _PipelineQueue(veri2_thread)
                _active_runs[veri2_thread] = {
                    "agent": "veri", "status": "running",
                    "thread_id": veri2_thread,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "auto_triggered_by": "searcher",
                }
                try:
                    from backend.utils import pause as _p2
                    _p2.register(veri2_thread)
                    await _emit_log(thread_id, "info", f"{company_num} Veri R2 starting for: {company}")
                    await _veri_task(veri2_thread, company_filter=company)
                    await _emit_log(thread_id, "info", f"{company_num} Veri R2 done for: {company}")
                except Exception as e:
                    print(f"[VERI CHAIN] {company_num} Veri R2 error for {company}: {e}", flush=True)
                    await _emit_log(thread_id, "error", f"{company_num} Veri R2 failed for {company}: {e}")

            await _emit_log(thread_id, "info",
                f"Sequential pipeline complete — {len(company_list)} companies processed")
        else:
            print("[VERI CHAIN] No companies found — cannot trigger Searcher", flush=True)
            await _emit_log(thread_id, "warning", "No companies found to trigger Searcher")

        _active_runs[thread_id]["status"] = "completed"
        await _emit_event(thread_id, "completed", {
            "verified": state.verified_count,
            "review": state.review_count,
            "rejected": state.rejected_count,
            "errors": state.errors,
        })

    except asyncio.CancelledError:
        logger.info("veri_task_cancelled", thread_id=thread_id)
        _active_runs[thread_id]["status"] = "cancelled"
        await _emit_event(thread_id, "cancelled", {"message": "Run stopped by user"})
    except Exception as e:
        logger.error("veri_task_error", error=str(e))
        _active_runs[thread_id]["status"] = "failed"
        await _emit_event(thread_id, "error", {"error": str(e)})
    finally:
        _pause_util.unregister(thread_id)
        from backend.utils.progress import unregister as _unreg_progress
        _unreg_progress(thread_id)

async def _prospect_chat(query: str, company: str, history: list[dict]) -> dict:
    """Run the LangGraph Scout agent — full enrichment pipeline."""
    from backend.agents.scout import run_scout
    return await run_scout(query, company, history)


async def _scout_commit(candidate: dict) -> dict:
    """Write enriched scout candidate to First Clean List (cols A–U)."""
    from backend.agents.scout import commit_to_sheet
    ctx = {
        "domain": candidate.get("company_domain", ""),
        "account_type": candidate.get("company_account_type", ""),
        "account_size": candidate.get("company_account_size", ""),
    }
    return await commit_to_sheet(candidate, ctx)


app = create_app()
