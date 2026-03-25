"""
FastAPI application for the SCAI ProspectOps web UI.
Provides REST endpoints + WebSocket for real-time log streaming.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from src.config import get_settings
from src.utils.logging import configure_logging, get_logger

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

class VeriRunRequest(BaseModel):
    row_start: Optional[int] = None  # 1-based, inclusive (data rows, not header)
    row_end: Optional[int] = None    # 1-based, inclusive

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

class RunResponse(BaseModel):
    thread_id: str
    status: str
    message: str

# ---------------------------------------------------------------------------
# Active runs tracker
# ---------------------------------------------------------------------------

_active_runs: dict[str, dict] = {}
_pending_confirmations: dict[str, asyncio.Event] = {}
_confirmation_data: dict[str, dict] = {}
_log_queues: dict[str, asyncio.Queue] = {}


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
    configure_logging(settings.log_dir)

    app = FastAPI(
        title="SalesCode Mapping Pipeline",
        description="B2B Prospecting Pipeline API",
        version="1.0.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---------------------------------------------------------------------------
    # Health
    # ---------------------------------------------------------------------------

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    # ---------------------------------------------------------------------------
    # Config check
    # ---------------------------------------------------------------------------

    @app.get("/api/config/check")
    async def config_check():
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
        _log_queues[thread_id] = asyncio.Queue()
        _active_runs[thread_id] = {"agent": "fini", "status": "running", "started_at": datetime.now(timezone.utc).isoformat()}

        asyncio.create_task(_fini_task(thread_id, req))

        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Fini started for: {req.companies}",
        )

    @app.post("/api/fini/confirm")
    async def confirm_operator(req: OperatorConfirmRequest):
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

    # ---------------------------------------------------------------------------
    # Searcher endpoints
    # ---------------------------------------------------------------------------

    @app.post("/api/searcher/run", response_model=RunResponse)
    async def run_searcher(req: SearcherRunRequest):
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = asyncio.Queue()
        _active_runs[thread_id] = {"agent": "searcher", "status": "running", "started_at": datetime.now(timezone.utc).isoformat()}

        asyncio.create_task(_searcher_task(thread_id, req))

        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Searcher (gap-fill) started for: {req.companies}",
        )

    # ---------------------------------------------------------------------------
    # Veri endpoints
    # ---------------------------------------------------------------------------

    @app.post("/api/veri/run", response_model=RunResponse)
    async def run_veri(req: VeriRunRequest = None):
        if req is None:
            req = VeriRunRequest()
        thread_id = str(uuid.uuid4())
        _log_queues[thread_id] = asyncio.Queue()
        _active_runs[thread_id] = {"agent": "veri", "status": "running", "started_at": datetime.now(timezone.utc).isoformat()}

        asyncio.create_task(_veri_task(thread_id, req.row_start, req.row_end))

        row_msg = f" (rows {req.row_start}–{req.row_end})" if req.row_start else " (all pending)"
        return RunResponse(
            thread_id=thread_id,
            status="started",
            message=f"Veri started{row_msg}",
        )

    # ---------------------------------------------------------------------------
    # Run status
    # ---------------------------------------------------------------------------

    @app.get("/api/runs")
    async def list_runs():
        return list(_active_runs.values())

    @app.get("/api/runs/{thread_id}")
    async def get_run(thread_id: str):
        if thread_id not in _active_runs:
            raise HTTPException(status_code=404, detail="Run not found")
        return _active_runs[thread_id]

    # ---------------------------------------------------------------------------
    # WebSocket for real-time logs
    # ---------------------------------------------------------------------------

    @app.websocket("/ws/{thread_id}")
    async def websocket_endpoint(websocket: WebSocket, thread_id: str):
        await websocket.accept()
        logger.info("ws_connected", thread_id=thread_id)

        # Use existing queue (created by the task on start)
        # If not ready yet, wait briefly
        for _ in range(20):
            if thread_id in _log_queues:
                break
            await asyncio.sleep(0.1)
        if thread_id not in _log_queues:
            _log_queues[thread_id] = asyncio.Queue()

        queue = _log_queues[thread_id]

        try:
            while True:
                try:
                    # Wait for messages with timeout
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
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
    async def zerobounce_credits():
        try:
            from src.tools.zerobounce import get_credits
            credits = await get_credits()
            return {"credits": credits}
        except Exception as e:
            return {"credits": None, "error": str(e)}

    # ---------------------------------------------------------------------------
    # Static files (UI)
    # ---------------------------------------------------------------------------

    ui_dist = Path(__file__).parent.parent / "ui" / "dist"
    if ui_dist.exists():
        app.mount("/assets", StaticFiles(directory=str(ui_dist / "assets")), name="assets")

        @app.get("/{full_path:path}")
        async def serve_ui(full_path: str):
            index = ui_dist / "index.html"
            if index.exists():
                return FileResponse(str(index))
            return JSONResponse({"error": "UI not built. Run: cd ui && npm run build"}, status_code=404)
    else:
        @app.get("/")
        async def root():
            return {"message": "SCAI ProspectOps API. UI not built yet. Run: cd ui && npm run build"}

    return app


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _fini_task(thread_id: str, req: FiniRunRequest):
    """Background task running the Fini agent with WebSocket logging."""
    from src.agents.fini import build_fini_graph
    from src.state import FiniState, TargetCompany

    company_names = [c.strip() for c in req.companies.split(",") if c.strip()]
    companies = [TargetCompany(raw_name=name, sdr_assigned=req.sdr or None) for name in company_names]
    state = FiniState(companies=companies, submit_to_n8n=req.submit_n8n, region=req.region)
    config = {"configurable": {"thread_id": thread_id}}

    try:
        # Small delay so the UI WebSocket can connect before first events are emitted
        await asyncio.sleep(0.5)
        await _emit_log(thread_id, "info", f"Starting Fini for {len(company_names)} companies")
        app_graph = await build_fini_graph()

        local_config = {**config, "recursion_limit": max(100, len(companies) * 2, len(state.companies) + 5)}
        while True:
            result = await app_graph.ainvoke(state, local_config)
            if isinstance(result, dict):
                state = FiniState(**result)
            else:
                state = result

            if state.status == "completed":
                _active_runs[thread_id]["status"] = "completed"
                await _emit_event(thread_id, "completed", {
                    "companies_processed": len(state.companies),
                    "errors": state.errors,
                })
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
                    await asyncio.wait_for(_pending_confirmations[thread_id].wait(), timeout=600)
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
    from src.agents.searcher import build_searcher_graph
    from src.state import SearcherState

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

    state = SearcherState(
        target_company=first["name"],
        target_domain=first["domain"],
        target_companies=target_companies,
        dm_roles=dm_roles,
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
            _log_queues[veri_thread_id] = asyncio.Queue()
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
    from src.agents.veri import build_veri_graph
    from src.state import VeriState

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

app = create_app()
