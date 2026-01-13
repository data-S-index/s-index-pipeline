from __future__ import annotations

import requests

from .constants import FUJI_DEFAULT_BASE_URL
from .discovery import fair_evaluate_doi_url
from .normalize import fuji_report_from_response


def fair_evaluation_report(
    doi_or_url: str,
    *,
    base_url: str = FUJI_DEFAULT_BASE_URL,
    username: str | None = None,
    password: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    """
    Wrapper:
      - discovery.fair_evaluate_doi_url() -> raw F-UJI JSON
      - normalize.fuji_report_from_response() -> compact FAIR report
    """
    raw = fair_evaluate_doi_url(
        doi_or_url,
        base_url=base_url,
        username=username,
        password=password,
        session=session,
    )
    return fuji_report_from_response(raw)
