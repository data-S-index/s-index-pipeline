from __future__ import annotations

import csv
import io
from typing import Dict, List, Optional

import requests

from sindex.core.http import make_session
from sindex.core.ids import _norm_emdb_id
from sindex.sources.emdb.constants import EMDB_IDS_URL, ENTRY_BASE_URL


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


def get_emdb_id_record(emdb_id: str, session: requests.Session | None = None) -> Dict:
    """
    Fetch raw JSON record for a single EMDB id.

    Args:
        session: A configured requests.Session.
        emdb_id: EMDB accession ID, e.g. "EMD-1001".

    Returns:
        A dict with the full entry JSON.
    """

    emdb_id_norm = _norm_emdb_id(emdb_id)
    if not emdb_id_norm:
        raise ValueError(f"Invalid EMDB ID: {emdb_id}")

    s = session or make_session()
    url = f"{ENTRY_BASE_URL}/{emdb_id}"

    resp = s.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()
