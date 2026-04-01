"""
Google Sheets wrapper using gspread + service account auth.

Rules:
- Service account auth only. Creds path from .env.
- Every read/write wrapped in retry with exponential backoff.
- NEVER overwrite data. Append-only for new rows.
- update_row_cells for specific column updates (Veri writes to cols Q-W on First Clean List).
- Every write logs row number and timestamp.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import gspread
import gspread.exceptions
from google.oauth2.service_account import Credentials

from backend.config import get_settings
from backend.utils.logging import get_logger

logger = get_logger("sheets")

# Sheet tab name constants — exact names as they appear in Google Sheets
TARGET_ACCOUNTS = "Target Accounts"
FIRST_CLEAN_LIST = "First Clean List"       # Main working sheet: n8n writes A-N, Veri writes Q-W, Searcher appends rows
SEARCHER_OUTPUT = "Searcher Output"         # Searcher's log tab
REJECTED_PROFILES = "Reject profiles"       # Veri moves rejected contacts here
N8N_WEBHOOK_LOG = "N8N Webhook Log"         # Every n8n webhook hit — full visibility for monitoring

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_client: gspread.Client | None = None
_spreadsheet: gspread.Spreadsheet | None = None


def _get_client() -> gspread.Client:
    global _client
    if _client is None:
        import json as _json
        settings = get_settings()
        if settings.google_service_account_json_content:
            # Cloud deployment: credentials provided as JSON string env var
            logger.info("sheets_loading_creds", source="env_content")
            info = _json.loads(settings.google_service_account_json_content)
            creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
        else:
            # Local: credentials from file path
            creds_path = settings.google_service_account_json_abs
            logger.info("sheets_loading_creds", path=creds_path)
            creds = Credentials.from_service_account_file(creds_path, scopes=_SCOPES)
        _client = gspread.authorize(creds)
    return _client


def _get_spreadsheet() -> gspread.Spreadsheet:
    global _spreadsheet
    if _spreadsheet is None:
        settings = get_settings()
        _spreadsheet = _get_client().open_by_key(settings.spreadsheet_id)
    return _spreadsheet


def _get_sheet(tab_name: str) -> gspread.Worksheet:
    return _get_spreadsheet().worksheet(tab_name)


async def _run_sync(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


async def append_row(tab_name: str, values: list[Any], retry_count: int = 3) -> int:
    """
    Append a single row to the given sheet tab, always starting from column A.
    Uses get_all_values to find the true next empty row, then writes with
    sheet.update(range, [values]) to guarantee col A alignment.
    Returns the 1-based row number of the written row.
    """
    for attempt in range(retry_count):
        try:
            sheet = _get_sheet(tab_name)
            # Find next empty row using all_values so we don't land on a row
            # that already has data in any column (avoids overwriting diagonal leftovers)
            all_vals = await _run_sync(sheet.get_all_values)
            next_row = len(all_vals) + 1

            range_notation = f"A{next_row}"
            await _run_sync(
                sheet.update,
                range_notation,
                [values],
                value_input_option="USER_ENTERED",
            )
            logger.info(
                "sheet_row_appended",
                tab=tab_name,
                row=next_row,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            return next_row
        except gspread.exceptions.APIError as e:
            if attempt == retry_count - 1:
                raise
            wait = 2 ** (attempt + 1)
            logger.warning("sheet_append_retry", tab=tab_name, attempt=attempt, wait=wait, error=str(e))
            await asyncio.sleep(wait)

    raise RuntimeError(f"Failed to append row to {tab_name} after {retry_count} attempts")


async def read_all_rows(tab_name: str) -> list[list[Any]]:
    """Read all rows from a sheet tab. Returns list of lists (excludes header)."""
    for attempt in range(3):
        try:
            sheet = _get_sheet(tab_name)
            records = await _run_sync(sheet.get_all_values)
            return records[1:] if len(records) > 1 else []
        except gspread.exceptions.APIError as e:
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** (attempt + 1))
    return []


async def read_all_records(tab_name: str) -> list[dict[str, Any]]:
    """Read all rows as dicts using the header row as keys."""
    for attempt in range(3):
        try:
            sheet = _get_sheet(tab_name)
            records = await _run_sync(sheet.get_all_records)
            return records
        except gspread.exceptions.APIError as e:
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** (attempt + 1))
    return []


async def update_row_cells(tab_name: str, row: int, col_start: int, values: list[Any]) -> None:
    """
    Update a range of cells in a specific row.
    col_start is 1-based column index.
    Used by Veri to write verification results to cols Q-W (17-23).
    """
    col_end = col_start + len(values) - 1
    range_notation = f"{_col_letter(col_start)}{row}:{_col_letter(col_end)}{row}"

    for attempt in range(3):
        try:
            sheet = _get_sheet(tab_name)
            await _run_sync(
                sheet.update,
                range_notation,
                [values],
                value_input_option="USER_ENTERED",
            )
            logger.info(
                "sheet_cells_updated",
                tab=tab_name,
                row=row,
                range=range_notation,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            return
        except gspread.exceptions.APIError as e:
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** (attempt + 1))


# Cache which tabs have already had their headers verified this process lifetime.
# This avoids a read request per commit when "Send All" fires many times in quick succession.
_headers_verified: set[str] = set()


async def ensure_headers(tab_name: str, headers: list[str]) -> None:
    """
    Ensure the first row of the sheet matches the expected headers.
    Creates the row if the sheet is empty.
    Cached per process — only reads the sheet once per tab to avoid 429 rate limits.
    """
    if tab_name in _headers_verified:
        return
    try:
        sheet = _get_sheet(tab_name)
        existing = await _run_sync(sheet.row_values, 1)
        if not existing:
            await _run_sync(sheet.append_row, headers, value_input_option="USER_ENTERED")
            logger.info("sheet_headers_created", tab=tab_name, headers=headers)
        _headers_verified.add(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        spreadsheet = _get_spreadsheet()
        await _run_sync(spreadsheet.add_worksheet, tab_name, rows=1000, cols=26)
        sheet = _get_sheet(tab_name)
        await _run_sync(sheet.append_row, headers, value_input_option="USER_ENTERED")
        logger.info("sheet_tab_created", tab=tab_name, headers=headers)
        _headers_verified.add(tab_name)


def _parse_row_from_range(range_str: str) -> int:
    """Parse row number from a range like 'Sheet1!A5:Z5' -> 5."""
    try:
        if "!" in range_str:
            range_str = range_str.split("!")[1]
        import re
        nums = re.findall(r"\d+", range_str)
        return int(nums[-1]) if nums else 0
    except Exception:
        return 0


def _col_letter(n: int) -> str:
    """Convert 1-based column index to letter (1->A, 26->Z, 27->AA)."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# ---------------------------------------------------------------------------
# Sheet header schemas — match EXACT column names in Google Sheets
# ---------------------------------------------------------------------------

# Columns A-H (col J = "Send to N8N" checkbox is managed by App Script — do not write header)
TARGET_ACCOUNTS_HEADERS = [
    "Company Name",          # A
    "Parent Company Name",   # B
    "Sales Navigator Link",  # C
    "Company Domain",        # D
    "SDR Name",              # E
    "Email Format( Firstname-amy , Lastname- williams)",  # F
    "Account type",          # G
    "Account Size",          # H
]

# Columns A-W (n8n writes A-N, system uses O-P, Veri writes Q-W)
FIRST_CLEAN_LIST_HEADERS = [
    "Company Name",                               # A
    "Normalized Company Name (Parent Group)",     # B
    "Company Domain Name",                        # C
    "Account type",                               # D
    "Account Size",                               # E
    "Country",                                    # F
    "First Name",                                 # G
    "Last Name",                                  # H
    "Job titles (English)",                       # I
    "Buying Role",                                # J
    "Linekdin Url",                               # K  (typo preserved — matches n8n output)
    "Email",                                      # L
    "Phone-1",                                    # M
    "Phone-2",                                    # N
    "Source",                                     # O  (system: "n8n" or "searcher")
    "Pipeline Status",                            # P  (system: tracking field)
    "LinkedIn Status",                            # Q  ← Veri writes from here
    "Employment Verified",                        # R
    "Title Match",                                # S
    "Actual Title Found",                         # T
    "Overall Status",                             # U  (VERIFIED / REVIEW / REJECT)
    "Verification Notes",                         # V
    "Verified On",                                # W
]

# Columns A-H
SEARCHER_OUTPUT_HEADERS = [
    "Company",         # A
    "Full Name",       # B
    "Job Title",       # C
    "Role Bucket",     # D
    "LinkedIn URL",    # E
    "LinkedIn Status", # F
    "Email Address",   # G
    "Email Status",    # H
]

REJECTED_PROFILES_HEADERS = [
    "Company Name",                               # A
    "Normalized Company Name (Parent Group)",     # B
    "Company Domain Name",                        # C
    "Account Type",                               # D
    "Account Size",                               # E
    "Country",                                    # F
    "First Name",                                 # G
    "Last Name",                                  # H
    "Job Title (English)",                        # I
    "Buying Role",                                # J
    "LinkedIn URL",                               # K
    "Email",                                      # L
    "Phone-1",                                    # M
    "Phone-2",                                    # N
    "Source",                                     # O
    "Pipeline Status",                            # P
    "LinkedIn Status",                            # Q
    "Employment Verified",                        # R
    "Title Match",                                # S
    "Actual Title Found",                         # T
    "Reject Reason",                              # U
    "Verification Notes",                         # V
    "Verified On",                                # W
]

# Columns A-J — every n8n webhook hit logged here for Gopal's visibility
N8N_WEBHOOK_LOG_HEADERS = [
    "Timestamp",            # A — when the webhook hit our endpoint
    "Status",               # B — success / partial / error / empty
    "Contacts Received",    # C — total contacts in the payload
    "Written to Sheet",     # D — how many actually written to First Clean List
    "Skipped",              # E — how many dropped (no name, bad data, etc.)
    "Companies",            # F — comma-separated company names found
    "Skip Reasons",         # G — why contacts were skipped
    "Chain Triggered",      # H — which agent chain was started (Veri→Searcher→Veri)
    "Thread ID",            # I — thread_id for tracking in the UI
    "Raw Payload (sample)", # J — first contact's raw JSON for debugging
]

async def delete_rows_batch(tab_name: str, row_nums: list[int]) -> None:
    """
    Delete multiple rows by their 1-based row numbers.
    Must be called with rows sorted in DESCENDING order so that deleting
    a lower-numbered row doesn't shift the indices of remaining rows.
    """
    if not row_nums:
        return
    sorted_rows = sorted(row_nums, reverse=True)
    for attempt in range(3):
        try:
            sheet = _get_sheet(tab_name)
            for row in sorted_rows:
                await _run_sync(sheet.delete_rows, row)
                logger.info("sheet_row_deleted", tab=tab_name, row=row)
            return
        except gspread.exceptions.APIError as e:
            if attempt == 2:
                raise
            wait = 2 ** (attempt + 1)
            logger.warning("sheet_delete_retry", tab=tab_name, attempt=attempt, wait=wait, error=str(e))
            await asyncio.sleep(wait)


# Column index constants for First Clean List (1-based)
FCL_SOURCE_COL = 15          # column O — "n8n" or "searcher"
FCL_PIPELINE_STATUS_COL = 16 # column P — pipeline tracking
FCL_VERI_COL_START = 17      # column Q — Veri writes Q-W (7 columns)
FCL_OVERALL_STATUS_COL = 21  # column U — "VERIFIED" / "REVIEW" / "REJECT"
