# pipeline/external/openalex/discovery.py

from __future__ import annotations

from typing import List, Optional

import requests

from sindex.core.dates import _norm_date_iso, get_realistic_date
from sindex.core.ids import _norm_doi_url

from .client import get_openalex_record, make_openalex_session
from .constants import OA_PER_PAGE


def get_openalex_doi_record(
    doi: str,
    *,
    session: Optional[requests.Session] = None,
    mailto: Optional[str] = None,
) -> dict | None:
    """
    Resolve a dataset DOI/DOI-URL to the OpenAlex work record (raw JSON).
    Returns None if the DOI is invalid or OA returns 404.
    """
    doi_url = _norm_doi_url(doi)
    if not doi_url:
        return None

    s = session or make_openalex_session()
    params = {"mailto": mailto} if mailto else None
    payload = get_openalex_record(f"/works/{doi_url}", session=s, params=params)
    return payload


def extract_openalex_id(work: dict) -> str | None:
    """
    Convert work['id'] = 'https://openalex.org/W123...' -> 'W123...'
    """
    openalex_id_url = work.get("id")
    if not isinstance(openalex_id_url, str) or "/" not in openalex_id_url:
        return None
    return openalex_id_url.rsplit("/", 1)[-1]


def get_all_citing_works_oa(
    openalex_id: str,
    *,
    session: requests.Session,
    mailto: Optional[str] = None,
    per_page: int = OA_PER_PAGE,
) -> List[dict]:
    """
    Fetch all OpenAlex works that cite the given OpenAlex work ID.

    We first find the open alex id of a dataset based on its DOI
    and then use this function to get all the resources citing that id

    Uses the cursor-based pagination API:
      GET /works?filter=cites:{openalex_id}&per-page=200&cursor=*

    Args:
        openalex_id: The OpenAlex work ID (e.g., "W1234567890").
        session: shared `requests.Session`
        mailto: An optional contact email; passed via `mailto` to be polite to the API.
        per_page: results returned per page

    Returns:
        A list of OpenAlex work dicts (raw API objects) that cite the target work.

    Raises:
        requests.HTTPError: If the OpenAlex API returns an HTTP error other than
            pagination completion.
    """
    results: List[dict] = []
    cursor = "*"

    while True:
        params = {
            "filter": f"cites:{openalex_id}",
            "per-page": per_page,
            "cursor": cursor,
        }
        if mailto:
            params["mailto"] = mailto

        payload = get_openalex_record("/works", session=session, params=params)
        results.extend(payload.get("results", []) or [])
        cursor = (payload.get("meta") or {}).get("next_cursor")
        if not cursor:
            break

    return results


def fetch_openalex_pubdate(doi: str, session: requests.Session | None = None) -> str:
    """
    Fetch a publication/created date for a DOI from the OpenAlex API and normalize it.

    Args:
        doi_or_doi_url: DOI identifier or DOI URL.
        session: Optional shared `requests.Session` (uses retry/backoff if None).

    Returns:
        ISO-8601 string (normalized) if available, else "".
    """
    data = get_openalex_doi_record(doi, session=session)
    if not data:  # None (invalid DOI or 404)
        return None

    for key in ["publication_date", "publication_year"]:
        val = data.get(key)

        if not val:
            continue

        try:
            norm_iso = _norm_date_iso(str(val))
            realistic = get_realistic_date(norm_iso)
            if realistic:
                return realistic

        except Exception:
            # Move to the next candidate
            continue

    return None
