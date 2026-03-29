"""
Google Sheets wrapper using gspread + service account auth.

Rules:
- Service account auth only. Creds path from .env.
- Every read/write wrapped in retry with exponential backoff.
- NEVER overwrite data. Append-only for new rows.
- update_row_cells for specific column updates (Veri writes to cols O-U).
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
FIRST_CLEAN_LIST = "First Clean List"
SEARCHER_OUTPUT = "Searcher Output"
FINAL_FILTERED_LIST = "Final Filtered List"

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
    Used by Veri to write verification results to cols O-U (15-21).
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

# Columns A-N (written by n8n after Fini submits)
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
    "Linekdin Url",                               # K  (typo preserved)
    "Email",                                      # L
    "Phone-1",                                    # M
    "Phone-2",                                    # N
]

# Columns A-U (Veri writes cols O-U)
FINAL_FILTERED_LIST_HEADERS = [
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
    "LinkedIn Status",                            # O  ← Veri writes from here
    "Employment Verified",                        # P
    "Title Match",                                # Q
    "Actual Title Found",                         # R
    "Overall Status",                             # S
    "Verification Notes",                         # T
    "Verified On",                                # U
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

# Column index constants for Veri write-back (1-based)
FINAL_FILTERED_LIST_VERI_COL_START = 15  # column O
