"""
Google Sheets integration for persistent storage.

This module provides the same interface as store.py but reads/writes
to Google Sheets instead of in-memory storage.

To use:
1. Create a Google Cloud project and enable the Sheets API
2. Create a service account and download the JSON key
3. Share your spreadsheet with the service account email
4. Set GOOGLE_SHEETS_ID and GOOGLE_SERVICE_ACCOUNT_FILE in .env
"""

from datetime import time
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from app.config import settings
from app.models import Act

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

# Column indices (1-based for gspread)
COL_ARTIST_NAME = 2      # B - "artist name"
COL_SCHEDULED_START = 4  # D - "scheduled start"
COL_SCHEDULED_END = 5    # E - "scheduled end"
COL_ACTUAL_START = 6     # F - "actual time on"
COL_ACTUAL_END = 7       # G - "actual time off"

HEADER_ROW = 5  # Header is on row 5, data starts row 6

_client: Optional[gspread.Client] = None
_sheet: Optional[gspread.Worksheet] = None


def _get_sheet() -> gspread.Worksheet:
    """Get or create the Google Sheets client and worksheet."""
    global _client, _sheet

    if _sheet is None:
        creds = Credentials.from_service_account_file(
            settings.GOOGLE_SERVICE_ACCOUNT_FILE,
            scopes=SCOPES,
        )
        _client = gspread.authorize(creds)
        spreadsheet = _client.open_by_key(settings.GOOGLE_SHEETS_ID)
        if settings.GOOGLE_SHEET_TAB:
            _sheet = spreadsheet.worksheet(settings.GOOGLE_SHEET_TAB)
        else:
            _sheet = spreadsheet.sheet1

    return _sheet


def _parse_time(time_str: str) -> Optional[time]:
    """Parse a time string (HH:MM) to a time object."""
    if not time_str or time_str.strip() == "":
        return None
    try:
        parts = time_str.strip().split(":")
        return time(int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return None


def _format_time(t: Optional[time]) -> str:
    """Format a time object to HH:MM string."""
    if t is None:
        return ""
    return t.strftime("%H:%M")


def _get_cell(row: list, col: int) -> str:
    """Safely get a cell value from a row (col is 1-indexed)."""
    idx = col - 1
    if idx < len(row):
        return str(row[idx])
    return ""


def get_schedule() -> list[Act]:
    """Fetch all acts from the Google Sheet."""
    sheet = _get_sheet()
    # Get all values starting from the data row (after header)
    all_values = sheet.get_all_values()
    data_rows = all_values[HEADER_ROW:]  # Skip header rows (0-indexed, so row 6 = index 5)

    acts = []
    for row in data_rows:
        act_name = _get_cell(row, COL_ARTIST_NAME)
        scheduled_start = _parse_time(_get_cell(row, COL_SCHEDULED_START))
        scheduled_end = _parse_time(_get_cell(row, COL_SCHEDULED_END))
        # Skip rows without act name or required scheduled times
        if not act_name or not scheduled_start or not scheduled_end:
            continue
        act = Act(
            act_name=act_name,
            scheduled_start=scheduled_start,
            scheduled_end=scheduled_end,
            actual_start=_parse_time(_get_cell(row, COL_ACTUAL_START)),
            actual_end=_parse_time(_get_cell(row, COL_ACTUAL_END)),
            notes=None,
        )
        acts.append(act)

    return acts


def get_act(act_name: str) -> Optional[Act]:
    """Get a single act by name."""
    acts = get_schedule()
    for act in acts:
        if act.act_name == act_name:
            return act
    return None


def _find_row(act_name: str) -> Optional[int]:
    """Find the row number for an act (1-indexed, accounting for header on row 5)."""
    sheet = _get_sheet()
    all_values = sheet.get_all_values()
    data_rows = all_values[HEADER_ROW:]  # Skip header rows

    for i, row in enumerate(data_rows):
        if _get_cell(row, COL_ARTIST_NAME) == act_name:
            return i + HEADER_ROW + 1  # Convert back to 1-indexed sheet row

    return None


def update_actual_start(act_name: str, actual_time: time) -> Optional[Act]:
    """Update the actual start time for an act."""
    sheet = _get_sheet()
    row_num = _find_row(act_name)

    if row_num is None:
        return None

    # actual_start column F = column 6
    sheet.update_cell(row_num, COL_ACTUAL_START, _format_time(actual_time))

    return get_act(act_name)


def update_actual_end(act_name: str, actual_time: time) -> Optional[Act]:
    """Update the actual end time for an act."""
    sheet = _get_sheet()
    row_num = _find_row(act_name)

    if row_num is None:
        return None

    # actual_end column G = column 7
    sheet.update_cell(row_num, COL_ACTUAL_END, _format_time(actual_time))

    return get_act(act_name)


def clear_actual_times(act_name: str) -> Optional[Act]:
    """Clear both actual start and end times for an act."""
    sheet = _get_sheet()
    row_num = _find_row(act_name)

    if row_num is None:
        return None

    # Clear actual_start (F) and actual_end (G)
    sheet.update_cell(row_num, COL_ACTUAL_START, "")
    sheet.update_cell(row_num, COL_ACTUAL_END, "")

    return get_act(act_name)


def get_stage_name() -> str:
    """Get the stage name from config."""
    return settings.STAGE_NAME
