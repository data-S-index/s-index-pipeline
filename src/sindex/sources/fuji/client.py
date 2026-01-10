import requests
from pipeline.normalize import _norm_date_iso
from requests.auth import HTTPBasicAuth

from sindex.core.ids import _norm_doi_url, is_working_doi, is_working_url


def fair_evaluation_doi_url(
    doi_or_url: str,
    base_url: str = "http://localhost:1071",
    username: str | None = None,
    password: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    """
    Run a FAIR evaluation for a given DOI URL or URL using a local F-UJI instance.

    This function sends a request to the F-UJI evaluation API and returns a
    structured response under the "fair_evaluation" key. Authentication must
    match the Basic Auth credentials configured in `fuji_server/config/users.py`.

    Args:
        doi_or_url:
            The dataset DOI (e.g. "10.5281/zenodo.12345"), DOI URL, or a resolvable URL.
        base_url:
            Base URL where the F-UJI server is running
            (e.g. "http://localhost:1071").
        username:
            Basic Auth username for F-UJI, or `None` for no authentication.
        password:
            Basic Auth password for F-UJI, or `None` for no authentication.
        session:
            Optional `requests.Session` for connection reuse. If `None`, a
            one-off request is issued via the top-level `requests` API.

    Returns:
        {
            "fair_score": <percent FAIR score from F-UJI>
        }

    Raises:
        requests.HTTPError:
            If F-UJI returns a non-success HTTP status code.
        requests.RequestException:
            For network-related errors.
    """
    # Identify and Validate
    target_url = None

    if is_working_doi(doi_or_url, session):
        target_url = _norm_doi_url(doi_or_url)
    elif is_working_url(doi_or_url, session):
        target_url = doi_or_url
    else:
        raise ValueError(f"Identifier '{doi_or_url}' is not a working DOI or URL.")

    # Procceed with F-UJI evaluation
    evaluate_url = f"{base_url.rstrip('/')}/fuji/api/v1/evaluate"

    auth = None
    if username is not None and password is not None:
        auth = HTTPBasicAuth(username, password)

    payload = {"object_identifier": target_url}

    if session is None:
        resp = requests.post(evaluate_url, json=payload, auth=auth, timeout=60)
    else:
        resp = session.post(evaluate_url, json=payload, auth=auth, timeout=60)

    resp.raise_for_status()
    results = resp.json()

    # Extract relevant parts of the response
    report = {
        "fair_score": results["summary"]["score_percent"]["FAIR"],
        "evaluation_date": _norm_date_iso(results["end_timestamp"]),
        "fuji_metric_version": results["metric_version"],
        "fuji_software_version": results["software_version"],
    }

    return report
