# pipeline/external/openalex/jobs.py

from __future__ import annotations

from typing import Dict, List, Optional

import requests

from .client import make_openalex_session
from .discovery import (
    extract_openalex_id,
    get_all_citing_works_oa,
    get_openalex_doi_record,
)
from .normalize import openalex_citing_works_to_citations


def find_citations_oa(
    doi: str,
    *,
    dataset_pub_date: str = None,
    email: Optional[str] = None,
    session: Optional[requests.Session] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """
    Wrapper:
      - discovery: DOI (canonical or url) -> OA dataset record (raw)
      - discovery: OA id -> citing works (raw)
      - normalize: citing works -> citation objects
    """
    s = session or make_openalex_session(api_key=api_key)

    record = get_openalex_doi_record(doi, session=s, mailto=email)
    if not record:
        return []

    if not record.get("cited_by_count"):
        return []

    openalex_id = extract_openalex_id(record)
    if not openalex_id:
        return []

    citing_records = get_all_citing_works_oa(openalex_id, session=s, mailto=email)

    return openalex_citing_works_to_citations(
        citing_records,
        dataset_id=doi,
        dataset_pub_date=dataset_pub_date,
    )
