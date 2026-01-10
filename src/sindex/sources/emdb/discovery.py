from __future__ import annotations

from datetime import date
from typing import Dict, Optional, Tuple

import requests

from sindex.core.dates import _to_datetime_utc
from sindex.sources.emdb.client import get_emdb_id_record


def _extract_deposition_date(entry: Dict) -> Optional[date]:
    """
    Extract the deposition date as a `date` object from an EMDB entry JSON.

    Primary source (actual structure from EMDB):
        entry["admin"]["key_dates"]["deposition"]

    Returns:
        A `date` object if found and parseable, else None.
    """
    admin = entry.get("admin") or {}
    key_dates = admin.get("key_dates") or {}

    dep_str = key_dates.get("deposition")
    if not isinstance(dep_str, str):
        return None

    dt = _to_datetime_utc(dep_str)
    if dt is None:
        return None
    return dt.date()


def _fetch_entry_and_filter(
    session: requests.Session,
    emdb_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
) -> Tuple[str, Optional[Dict]]:
    """
    Fetch one entry and apply date range filter.

    Needed for concurrent execution.

    Returns:
        (emdb_id, entry_dict or None)

        entry_dict is returned only if the entry has a valid deposition date
        and dep_date <= cutoff_date; otherwise None.
    """
    try:
        entry = get_emdb_id_record(emdb_id, session)
    except Exception as e:
        print(f"\n[WARN] Failed to fetch {emdb_id}: {e}")
        return emdb_id, None

    dep_date = _extract_deposition_date(entry)
    if dep_date is None:
        # If no usable deposition date, skip
        return emdb_id, None

    is_after_start = start_date is None or dep_date >= start_date
    is_before_end = end_date is None or dep_date <= end_date

    if is_after_start and is_before_end:
        return emdb_id, entry

    return emdb_id, None
