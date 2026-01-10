import requests

from sindex.core.http import make_session
from sindex.core.ids import _norm_doi
from sindex.sources.datacite.constants import BASE_API_URL


def get_datacite_doi_record(
    doi_or_url: str, session: requests.Session | None = None
) -> dict | None:
    """
    Retrieve a single DataCite metadata record for a given DOI or DOI URL.

    Args:
        doi_or_url: DOI string (e.g., "10.5061/dryad.ab12cd3") or full DOI URL.
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
    doi = _norm_doi(doi_or_url)
    if not doi:
        raise ValueError(f"Invalid DOI: {doi_or_url}")

    s = session or make_session()
    url = f"{BASE_API_URL}/{doi}"

    resp = s.get(url, timeout=(10, 60))
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    payload = resp.json()
    return payload.get("data")
