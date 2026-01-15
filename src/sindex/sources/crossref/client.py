from __future__ import annotations

from urllib.parse import quote

import requests

from .constants import BASE_API_URL, CROSSREF_TIMEOUT


def get_crossref_record_by_norm_doi(
    norm_doi: str,
    *,
    session: requests.Session,
) -> dict | None:
    """
    Fetch Crossref metadata for a normalized DOI.

    Args:
        norm_doi:
            Normalized DOI (lowercase, no https://doi.org/ prefix).
        session:
            Configured requests.Session.

    Returns:
        Full Crossref JSON payload (root object), or None if 404.

    Raises:
        requests.HTTPError: for non-404 HTTP errors.
    """
    doi_encoded = quote(norm_doi, safe="")
    url = f"{BASE_API_URL}/{doi_encoded}"

    resp = session.get(url, timeout=CROSSREF_TIMEOUT)
    if resp.status_code == 404:
        return None

    resp.raise_for_status()
    return resp.json()
