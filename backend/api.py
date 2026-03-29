"""
FastAPI application for the SCAI ProspectOps web UI.
Provides REST endpoints + WebSocket for real-time log streaming.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
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

class SearcherRunRequest(BaseModel):
    # Comma-separated company names (must already exist in Target Accounts)
    companies: str
    dm_roles: str = "VP Ecommerce,CDO,Head of Digital,CTO,CMO,VP Marketing,VP Sales"
    target_contact_count: int = 10  # max contacts to find per company (0 = unlimited)

class VeriRunRequest(BaseModel):
    row_start: Optional[int] = None  # 1-based, inclusive (data rows, not header)
    row_end: Optional[int] = None    # 1-based, inclusive

class N8nCompleteRequest(BaseModel):
    row_start: Optional[int] = None  # 1-based data row range that n8n populated
    row_end: Optional[int] = None

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
    source: str = "scout"
    confidence: str = "medium"

# ---------------------------------------------------------------------------
# Active runs tracker
# ---------------------------------------------------------------------------

_active_runs: dict[str, dict] = {}
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
        _active_runs[thread_id] = {"agent": "fini", "status": "running", "started_at": datetime.now(timezone.utc).isoformat()}

        asyncio.create_task(_fini_task(thread_id, req))

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
        _active_runs[thread_id] = {"agent": "searcher", "status": "running", "started_at": datetime.now(timezone.utc).isoformat()}

        asyncio.create_task(_searcher_task(thread_id, req))

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
        """Write a scout-found candidate directly to the First Clean List."""
        try:
            row = await _scout_commit(req.model_dump())
            return {"ok": True, "row": row}
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
        _active_runs[thread_id] = {"agent": "veri", "status": "running", "started_at": datetime.now(timezone.utc).isoformat()}

        asyncio.create_task(_veri_task(thread_id, req.row_start, req.row_end))

        row_msg = f" (rows {req.row_start}–{req.row_end})" if req.row_start else " (all pending)"
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
    # Run status
    # ---------------------------------------------------------------------------

    @app.get("/api/runs")
    async def list_runs() -> list[dict[str, Any]]:
        return list(_active_runs.values())

    @app.get("/api/runs/{thread_id}")
    async def get_run(thread_id: str) -> dict[str, Any]:
        if thread_id not in _active_runs:
            raise HTTPException(status_code=404, detail="Run not found")
        return _active_runs[thread_id]

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
    companies = [TargetCompany(raw_name=name, sdr_assigned=req.sdr or None) for name in company_names]
    state = FiniState(companies=companies, submit_to_n8n=req.submit_n8n, region=req.region, thread_id=thread_id)
    config = {"configurable": {"thread_id": thread_id}}

    # Register queue so fini.py can emit per-company progress events
    if thread_id in _log_queues:
        _reg_progress(thread_id, _log_queues[thread_id])

    try:
        # Small delay so the UI WebSocket can connect before first events are emitted
        await asyncio.sleep(0.5)
        await _emit_log(thread_id, "info", f"Starting Fini for {len(company_names)} companies")
        app_graph = await build_fini_graph()

        local_config = {**config, "recursion_limit": 500}
        while True:
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

    except Exception as e:
        logger.error("fini_task_error", error=str(e), thread_id=thread_id)
        _active_runs[thread_id]["status"] = "failed"
        await _emit_event(thread_id, "error", {"error": str(e)})


async def _searcher_task(thread_id: str, req: SearcherRunRequest):
    """Background task running the Searcher gap-fill agent."""
    from backend.agents.searcher import build_searcher_graph
    from backend.state import SearcherState
    from backend.utils.progress import register as _reg_progress

    # Register queue for per-company progress events
    _reg_progress(thread_id, _log_queues[thread_id])

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

        # --- Auto-trigger Veri on newly written rows ---
        veri_thread_id = None
        if state.total_contacts_written > 0 and state.fcl_row_start is not None:
            veri_thread_id = str(uuid.uuid4())
            _log_queues[veri_thread_id] = _PipelineQueue(veri_thread_id)
            _active_runs[veri_thread_id] = {
                "agent": "veri",
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "auto_triggered_by": thread_id,
            }
            await _emit_log(thread_id, "info",
                f"Auto-triggering Veri agent for rows {state.fcl_row_start}–{state.fcl_row_end} "
                f"({state.total_contacts_written} contacts)")
            asyncio.create_task(_veri_task(
                veri_thread_id,
                row_start=state.fcl_row_start,
                row_end=state.fcl_row_end,
            ))

        _active_runs[thread_id]["status"] = "completed"
        await _emit_event(thread_id, "completed", {
            "contacts_appended": state.total_contacts_written,
            "contacts": [c.model_dump() for c in state.discovered_contacts[:50]],
            "errors": state.errors,
            "veri_thread_id": veri_thread_id,
        })

    except Exception as e:
        import traceback
        logger.error("searcher_task_error", error=str(e), traceback=traceback.format_exc())
        print(f"[SEARCHER ERROR] {traceback.format_exc()}", flush=True)
        _active_runs[thread_id]["status"] = "failed"
        await _emit_event(thread_id, "error", {"error": str(e)})


async def _veri_task(thread_id: str, row_start: int = None, row_end: int = None):
    """Background task running the Veri agent."""
    from backend.agents.veri import build_veri_graph
    from backend.state import VeriState

    state = VeriState(contacts=[], row_start=row_start, row_end=row_end)
    config = {"configurable": {"thread_id": thread_id}}

    try:
        await asyncio.sleep(0.5)
        range_msg = f" rows {row_start}–{row_end}" if row_start else " all pending contacts"
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

        _active_runs[thread_id]["status"] = "completed"
        await _emit_event(thread_id, "completed", {
            "verified": state.verified_count,
            "review": state.review_count,
            "rejected": state.rejected_count,
            "errors": state.errors,
        })

    except Exception as e:
        logger.error("veri_task_error", error=str(e))
        _active_runs[thread_id]["status"] = "failed"
        await _emit_event(thread_id, "error", {"error": str(e)})

async def _prospect_chat(query: str, company: str, history: list[dict]) -> dict:
    """
    Advanced SDR prospect finder — runs 3 agents in parallel:
    1. Perplexity sonar-pro  → real-time web research with citations
    2. Unipile LinkedIn      → org_id resolve → verified LinkedIn people search
    3. Claude Bedrock        → synthesize + deduplicate + structure all results
    """
    import json as _json
    import re as _re
    import httpx as _httpx
    from backend.tools.llm import _bedrock_claude
    from backend.config import get_settings as _get_settings

    target_company = company.strip() or "the company"

    # conversation context for Claude synthesis
    history_ctx = ""
    for h in history[-4:]:
        history_ctx += f"{h.get('role','user').upper()}: {h.get('content','')}\n"

    # ── Agent 1: Perplexity sonar-pro — real-time web research ───────────
    async def _perplexity_agent() -> str:
        try:
            settings = _get_settings()
            if not settings.perplexity_api_key:
                return ""
            system_msg = (
                "You are a B2B research expert specializing in finding senior executives. "
                "Always provide real people's full names, job titles, and LinkedIn URLs when available. "
                "Be factual and specific — only mention people you are confident about."
            )
            user_msg = (
                f"Find specific people who work at {target_company}. Request: {query}\n\n"
                "For each person you find, provide:\n"
                "- Full name\n- Current job title\n- LinkedIn URL if available\n- Source\n\n"
                "Focus on currently employed decision-makers. Be concise and factual."
            )
            async with _httpx.AsyncClient(timeout=35) as client:
                resp = await client.post(
                    "https://api.perplexity.ai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {settings.perplexity_api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "sonar-pro",
                        "messages": [
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": user_msg},
                        ],
                        "max_tokens": 2000,
                        "search_recency_filter": "month",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            content = data["choices"][0]["message"]["content"]
            citations = data.get("citations", [])
            if citations:
                content += "\n\nSources: " + ", ".join(citations[:5])
            return content
        except Exception as e:
            logger.warning("scout_perplexity_error", error=str(e))
            return ""

    # ── Agent 2: Unipile LinkedIn — org resolve + people search ──────────
    async def _linkedin_agent() -> list[dict]:
        try:
            from backend.tools.unipile import get_company_org_id, search_people

            # Resolve org_id for the company
            org_info = await get_company_org_id(target_company)
            if not org_info or not org_info.get("org_id"):
                return []
            org_id: str = str(org_info["org_id"])

            # Extract role titles with Claude (fast, small call)
            titles_prompt = (
                f"SDR needs to find: \"{query}\" at {target_company}.\n"
                "Generate 6 specific LinkedIn job title search strings (in English + local language if relevant).\n"
                "Return ONLY a JSON array, no explanation: [\"title1\", \"title2\", ...]"
            )
            titles_raw = await _bedrock_claude(titles_prompt, max_tokens=200, temperature=0)
            titles: list[str] = []
            m = _re.search(r'\[.*?\]', titles_raw, _re.DOTALL)
            if m:
                try:
                    titles = _json.loads(m.group(0))
                except Exception:
                    pass
            if not titles:
                # fallback: split query
                titles = [t.strip() for t in query.split(",") if t.strip()][:6] or [query[:60]]

            people = await search_people(org_id, titles[:6], limit=25)
            return [
                {
                    "full_name": p.get("full_name", ""),
                    "role_title": p.get("headline", ""),
                    "company": target_company,
                    "linkedin_url": p.get("linkedin_url", ""),
                    "linkedin_verified": True,
                    "source": "linkedin",
                    "confidence": "high",
                }
                for p in people
                if p.get("full_name")
            ]
        except Exception as e:
            logger.warning("scout_linkedin_error", error=str(e))
            return []

    # Run both agents in parallel
    perp_content, li_candidates = await asyncio.gather(
        _perplexity_agent(),
        _linkedin_agent(),
        return_exceptions=True,
    )
    if isinstance(perp_content, Exception):
        perp_content = ""
    if isinstance(li_candidates, Exception):
        li_candidates = []

    # ── Agent 3: Claude Bedrock — synthesize + structure ─────────────────
    li_section = ""
    if li_candidates:
        li_section = "\n\n## LinkedIn verified contacts:\n" + "\n".join(
            f"- {c['full_name']} | {c['role_title']} | {c.get('linkedin_url', '')}"
            for c in li_candidates[:30]
        )

    synthesis_prompt = (
        "You are an expert B2B SDR assistant. A sales rep asked:\n"
        f"Company: {target_company}\n"
        f"Request: \"{query}\"\n"
        + (f"Conversation context:\n{history_ctx}\n" if history_ctx else "")
        + "\n## Research findings:\n"
        + (f"### Perplexity web research:\n{perp_content}\n" if perp_content else "")
        + li_section
        + "\n\n## Task:\n"
        "Extract ALL matching contacts from the research above. Prefer LinkedIn-verified entries. "
        "Deduplicate by name. Include web-found people if they are clearly at the target company.\n\n"
        "Return ONLY this JSON (no markdown fences):\n"
        "{\n"
        "  \"message\": \"1-2 sentence conversational summary of what you found\",\n"
        "  \"candidates\": [\n"
        "    {\n"
        "      \"full_name\": \"First Last\",\n"
        "      \"role_title\": \"Exact current job title\",\n"
        "      \"company\": \"Company name\",\n"
        "      \"linkedin_url\": \"https://linkedin.com/in/slug or null\",\n"
        "      \"linkedin_verified\": true,\n"
        "      \"source\": \"linkedin or web\",\n"
        "      \"confidence\": \"high or medium or low\"\n"
        "    }\n"
        "  ]\n"
        "}"
    )

    synthesis_raw = await _bedrock_claude(synthesis_prompt, max_tokens=3000, temperature=0)

    candidates: list[dict] = []
    message = ""
    try:
        clean = _re.sub(r'```(?:json)?\s*', '', synthesis_raw).strip().rstrip('`').strip()
        m2 = _re.search(r'\{.*\}', clean, _re.DOTALL)
        if m2:
            parsed = _json.loads(m2.group(0))
            message = parsed.get("message", "")
            for i, c in enumerate(parsed.get("candidates", [])):
                if isinstance(c, dict) and c.get("full_name"):
                    candidates.append({
                        "index": i,
                        "full_name": c.get("full_name", ""),
                        "role_title": c.get("role_title", ""),
                        "company": c.get("company") or target_company,
                        "linkedin_url": c.get("linkedin_url") or "",
                        "linkedin_verified": bool(c.get("linkedin_verified", False)),
                        "source": c.get("source", "web"),
                        "confidence": c.get("confidence", "medium"),
                        "group": "scout",
                        "is_new": True,
                    })
    except Exception as e:
        logger.warning("scout_synthesis_parse_error", error=str(e), raw=synthesis_raw[:200])

    if not message:
        if candidates:
            n = len(candidates)
            message = f"Found {n} contact{'s' if n != 1 else ''} at {target_company} matching your request."
        else:
            message = (
                f"I searched Perplexity and LinkedIn for \"{query}\" at {target_company} "
                "but couldn't find specific people. Try a different role name or check the company name."
            )

    return {"candidates": candidates, "message": message}


async def _scout_commit(candidate: dict) -> int:
    """Write a scout-found candidate directly to the First Clean List sheet."""
    from backend.tools import sheets

    full_name: str = candidate.get("full_name", "")
    parts = full_name.strip().split(" ", 1)
    first_name = parts[0] if parts else ""
    last_name = parts[1] if len(parts) > 1 else ""

    company = candidate.get("company", "")
    role_title = candidate.get("role_title", "")
    linkedin_url = candidate.get("linkedin_url", "")

    # Buying role classification
    role_lower = role_title.lower()
    if any(k in role_lower for k in ["chief", "ceo", "cfo", "cmo", "cto", "cdo", "coo", "cpo"]):
        buying_role = "DM"
    elif any(k in role_lower for k in ["vp", "vice president", "head of", "director"]):
        buying_role = "DM"
    else:
        buying_role = "Influencer"

    await sheets.ensure_headers(sheets.FIRST_CLEAN_LIST, sheets.FIRST_CLEAN_LIST_HEADERS)
    row = [
        company,        # A  Company Name
        company,        # B  Normalized Company Name
        "",             # C  Domain (unknown)
        "",             # D  Account Type
        "",             # E  Account Size
        "",             # F  Country
        first_name,     # G  First Name
        last_name,      # H  Last Name
        role_title,     # I  Job Title
        buying_role,    # J  Buying Role
        linkedin_url,   # K  LinkedIn URL
        "",             # L  Email
        "",             # M  Phone-1
        "",             # N  Phone-2
    ]
    written_row = await sheets.append_row(sheets.FIRST_CLEAN_LIST, row)
    logger.info("scout_commit_written", name=full_name, row=written_row)
    return written_row


app = create_app()
