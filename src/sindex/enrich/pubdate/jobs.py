from __future__ import annotations

import requests

from sindex.sources.crossref.discovery import fetch_crossref_pubdate
from sindex.sources.datacite.discovery import fetch_datacite_pubdate
from sindex.sources.openalex.discovery import fetch_openalex_pubdate


def best_publication_date_for_doi(
    doi_or_doi_url: str,
    *,
    skip_openalex: bool = False,
    openalex_session: requests.Session | None = None,
    datacite_session: requests.Session | None = None,
    crossref_session: requests.Session | None = None,
) -> str | None:
    """
    Return the best-available normalized publication date for a DOI.

    Policy order:
      1) OpenAlex
      2) DataCite
      3) Crossref

    Returns:
      ISO-8601 string, or None if no date is found.
    """
    if not skip_openalex:
        date = fetch_openalex_pubdate(doi_or_doi_url, session=openalex_session)
        if date:
            return date

    return (
        fetch_datacite_pubdate(doi_or_doi_url, session=datacite_session)
        or fetch_crossref_pubdate(doi_or_doi_url, session=crossref_session)
        or None
    )
