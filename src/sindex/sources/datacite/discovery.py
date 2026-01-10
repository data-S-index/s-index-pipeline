from typing import Dict, Iterator, Tuple

import requests

from sindex.core.http import make_session
from sindex.sources.datacite.constants import BASE_API_URL


def stream_datacite_records(
    start_date: str,
    end_date: str,
    page_size: int = 1000,
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
