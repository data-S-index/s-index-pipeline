from __future__ import annotations

import csv
import io
from typing import List, Optional

import requests

from sindex.core.http import make_session
from sindex.sources.emdb.constants import (
    DEFAULT_TIMEOUT_ENTRY,
    EMDB_IDS_URL,
    ENTRY_BASE_URL,
)


def _fetch_all_emdb_ids(session: Optional[requests.Session] = None) -> List[str]:
    """
    Fetch all released EMDB IDs via the CSV search endpoint.

    Args:
        session:
            Optional pre-configured requests.Session. If None, a new session is
            created.

    Returns:
        A list of EMDB IDs as strings, e.g. ["EMD-1001", "EMD-1002", ...].
    """
    if session is None:
        session = make_session()

    print("Fetching EMDB IDs from CSV search endpoint...")
    resp = session.get(EMDB_IDS_URL, timeout=300)
    resp.raise_for_status()

    text = resp.text
    reader = csv.DictReader(io.StringIO(text))

    ids: List[str] = []
    for row in reader:
        emdb_id = row.get("emdb_id")
        if emdb_id:
            ids.append(emdb_id.strip())

    print(f"Fetched {len(ids)} EMDB IDs.")
    return ids


def get_emdb_record_by_norm_id(
    emdb_id_norm: str,
    *,
    session: requests.Session,
) -> dict:
    """
    Fetch raw JSON record for a single *normalized* EMDB accession ID.

    Args:
        emdb_id_norm: Normalized EMDB accession ID, e.g. "EMD-1001".
        session: Configured requests.Session (ideally from core.make_session()).

    Returns:
        Full EMDB entry JSON dict.

    Raises:
        requests.HTTPError: for non-2xx responses.
    """
    url = f"{ENTRY_BASE_URL}/{emdb_id_norm}"
    resp = session.get(url, timeout=DEFAULT_TIMEOUT_ENTRY)
    resp.raise_for_status()
    return resp.json()
