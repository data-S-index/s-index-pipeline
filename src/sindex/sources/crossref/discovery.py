import requests

from sindex.core.dates import _as_iso_from_dateparts, _norm_date_iso, get_realistic_date
from sindex.core.http import make_session
from sindex.core.ids import _norm_doi

from .client import get_crossref_record_by_norm_doi


def get_crossref_doi_record(
    doi: str, session: requests.Session | None = None
) -> dict | None:
    """
    Retrieve a single Crossref metadata record for a given DOI or DOI URL.

    Args:
        doi: DOI string (e.g., "10.5061/dryad.ab12cd3") or full DOI URL.
        session: Optional shared `requests.Session` (uses retry/backoff if None).

    Returns:
        The JSON-decoded Croffref record (`data` object) if found, or `None` if
        the DOI does not exist or returns an error.

    Raises:
        requests.HTTPError: If an unexpected HTTP error occurs (e.g., 5xx not handled by retries).

    Notes:
        - The DOI is normalized with _norm_doi to lowercase and stripped of any "https://doi.org/" prefix.
        - Crossreff api direct link: https://api.crossref.org/works/10.5555%2F12345678
    """
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        raise ValueError(f"Invalid DOI: {doi}")

    s = session or make_session(allowed_methods=("GET",))
    return get_crossref_record_by_norm_doi(norm_doi, session=s)


def fetch_crossref_pubdate(doi: str, session: requests.Session | None = None) -> str:
    """
    Fetch a publication date for a DOI from the Crossref API and normalize it.

    Args:
        doi_or_doi_url: DOI identifier or DOI URL.
        session: Optional shared `requests.Session` (uses retry/backoff if None).

    Returns:
        ISO-8601 string (normalized) if available, else "".
    """
    crossref_record = get_crossref_doi_record(doi, session=session)
    if not crossref_record:  # None / not found
        return None

    msg = crossref_record.get("message") or {}

    # Prefer publisher-declared dates in order
    for key in ("published", "issued", "published-online", "published-print"):
        dp = (msg.get(key) or {}).get("date-parts")
        iso_raw = _as_iso_from_dateparts(dp) if dp else None

        if not iso_raw:
            continue

        try:
            norm_iso = _norm_date_iso(iso_raw)
            realistic = get_realistic_date(norm_iso)
            if realistic:
                return realistic

        except Exception:
            # Move to the next
            continue

    return None
