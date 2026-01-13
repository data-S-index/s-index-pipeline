from __future__ import annotations

import requests

from sindex.core.http import make_session
from sindex.core.ids import _norm_doi_url, is_working_doi, is_working_url

from .client import fuji_evaluate
from .constants import FUJI_DEFAULT_BASE_URL


def resolve_fuji_object_identifier(
    doi_or_url: str,
    *,
    session: requests.Session,
) -> str:
    """
    Resolve input into the URL to send to F-UJI.

    - If it's a working DOI (or DOI URL), return canonical DOI URL.
    - Else if it's a working non-DOI URL, return it as-is.
    - Else raise ValueError.
    """
    if is_working_doi(doi_or_url, session=session):
        return _norm_doi_url(doi_or_url)  # canonical DOI URL
    if is_working_url(doi_or_url, session=session):
        return doi_or_url

    raise ValueError(f"Identifier '{doi_or_url}' is not a working DOI or URL.")


def fair_evaluate_doi_url(
    doi_or_url: str,
    *,
    base_url: str = FUJI_DEFAULT_BASE_URL,
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
        raw F-UJI response
    """
    s = session or make_session(allowed_methods=("POST",))

    object_identifier = resolve_fuji_object_identifier(doi_or_url, session=s)

    return fuji_evaluate(
        object_identifier,
        session=s,
        base_url=base_url,
        username=username,
        password=password,
    )
