from typing import Dict, Iterator, Tuple

import requests

from sindex.core.dates import _norm_date_iso, get_realistic_date
from sindex.core.http import make_session
from sindex.core.ids import _norm_doi

from .client import get_datacite_record_by_norm_doi
from .constants import BASE_API_URL
from .utils import get_best_publication_date_datacite_record


def get_datacite_doi_record(
    doi: str, session: requests.Session | None = None
) -> dict | None:
    """
    Retrieve a single DataCite metadata record for a given DOI or DOI URL.

    Args:
        doi: DOI string (e.g., "10.5061/dryad.ab12cd3") or full DOI URL.
        session: Optional shared `requests.Session` (uses retry/backoff if None).

    Returns:
        The JSON-decoded DataCite record (`data` object) if found, or `None` if
        the DOI does not exist or returns an error.

    Raises:
        requests.HTTPError: If an unexpected HTTP error occurs (e.g., 5xx not handled by retries).

    Notes:
        - The DOI is normalized with _norm_doi to lowercase and stripped of any "https://doi.org/" prefix.
        - DataCite api direct link: https://api.datacite.org/dois/{doi}
    """
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        raise ValueError(f"Invalid DOI: {doi}")

    s = session or make_session(allowed_methods=("GET",))
    return get_datacite_record_by_norm_doi(norm_doi, session=s)


def stream_datacite_records(
    start_date: str,
    end_date: str,
    page_size: int = 1000,
    detail: bool = True,
    session: requests.Session | None = None,  # pass a shared session
    timeout: Tuple[int, int] = (10, 240),  # (connect, read) seconds
) -> Iterator[Dict]:
    """
    Stream raw DataCite Dataset records created within a date range.

    This function performs a cursor-based harvest of DataCite's /dois API using:
        - a fixed resource type filter (`Dataset`)
        - a created-date range query `[start_date TO end_date]` (both included)
        - pagination via `links.next` (DataCite's cursor API)

    Instead of collecting all API results into memory, this function yields
    each record immediately as a Python dictionary, making it suitable for
    large-scale harvesting and low-memory pipelines.

    Args:
        start_date: Inclusive ISO date string (YYYY-MM-DD).
        end_date: Inclusive ISO date string (YYYY-MM-DD).
        page_size: Number of records per API page (cursor). Max is 1000 for DataCite.
        user_agent: Optional override for the User-Agent header.
        session: Shared requests session (recommended) otherwise a new one created.
        timeout: (connect, read) timeout tuple.
            connect = How long to wait while trying to open a TCP connection to the server.
            read_timeout: How long to wait after the connection is established for the server to start sending data.

    Yields:
        dict:
            Each raw DataCite record (`payload["data"][i]`), exactly as
            returned by the REST API. No transformation is applied.
    """
    s = session or make_session()
    base_url = BASE_API_URL
    params = {
        "query": f"types.resourceTypeGeneral:Dataset AND created:[{start_date} TO {end_date}]",
        "page[size]": page_size,
        "page[cursor]": 1,
        "detail": str(detail).lower(),
    }

    while True:
        r = s.get(base_url, params=params, timeout=timeout)
        r.raise_for_status()
        payload = r.json()

        for rec in payload.get("data", []):
            yield rec

        next_url = payload.get("links", {}).get("next")
        if not next_url:
            break

        base_url = next_url
        params = {}


def fetch_datacite_pubdate(doi: str, session: requests.Session | None = None) -> str:
    """
    Fetch a publication/created date for a DOI from the DataCite API and normalize it.

    Args:
        doi_or_doi_url: DOI identifier or DOI URL.
        session: Optional shared `requests.Session` (uses retry/backoff if None).

    Returns:
        ISO-8601 string (normalized) if available, else "".
    """
    datacite_record = get_datacite_doi_record(doi)
    if not datacite_record:  # None (404 / invalid DOI)
        return None

    attrs = datacite_record.get("attributes") or {}

    # Priority: Use "Best Date" logic (Issued date -> Published date -> Published year)
    publication_date = get_best_publication_date_datacite_record(attrs)

    if publication_date:
        try:
            realistic_date = get_realistic_date(publication_date)
            if realistic_date:
                return realistic_date
        except Exception:
            pass  # Move on to try created date

    # Try "created" date
    created_val = attrs.get("created")
    if created_val:
        try:
            norm_iso = _norm_date_iso(created_val)
            realistic_date = get_realistic_date(norm_iso)
            if realistic_date:
                return realistic_date
        except Exception:
            pass

    return None
