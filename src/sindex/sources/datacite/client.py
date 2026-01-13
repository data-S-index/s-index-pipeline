import requests

from .constants import BASE_API_URL, DATACITE_TIMEOUT


def get_datacite_record_by_norm_doi(
    norm_doi: str,
    *,
    session: requests.Session,
) -> dict | None:
    """
    Fetch DataCite record by a normalized DOI (lowercase, no doi.org prefix).

    Returns:
      - dict (DataCite `data` object) on success
      - None on 404

    Raises:
      - requests.HTTPError on other non-2xx responses
    """
    url = f"{BASE_API_URL}/{norm_doi}"
    resp = session.get(url, timeout=DATACITE_TIMEOUT)

    if resp.status_code == 404:
        return None

    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data")
