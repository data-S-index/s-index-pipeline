from __future__ import annotations

from typing import Iterator, Optional

import requests

from sindex.core.http import make_session

from .constants import OA_BASE_URL, OA_TIMEOUT_SECS, USER_AGENT_OA


def make_openalex_session(
    *,
    api_key: str | None = None,
    user_agent: str = USER_AGENT_OA,
) -> requests.Session:
    s = make_session(
        user_agent=user_agent,
        allowed_methods=("GET",),
        status_forcelist=(429, 500, 502, 503, 504),
    )
    if api_key:
        s.params = getattr(s, "params", {})
        s.params["api_key"] = api_key
    return s


def get_openalex_work_by_doi_url(
    doi_url: str,
    *,
    session: requests.Session,
    mailto: Optional[str] = None,
) -> dict | None:
    url = f"{OA_BASE_URL}/works/{doi_url}"
    params = {"mailto": mailto} if mailto else None

    r = session.get(url, params=params, timeout=OA_TIMEOUT_SECS)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def iter_citing_works(
    openalex_id: str,
    *,
    session: requests.Session,
    mailto: Optional[str] = None,
    per_page: int = 200,
) -> Iterator[dict]:
    cursor = "*"

    while True:
        params = {
            "filter": f"cites:{openalex_id}",
            "per-page": per_page,
            "cursor": cursor,
        }
        if mailto:
            params["mailto"] = mailto

        r = session.get(
            f"{OA_BASE_URL}/works",
            params=params,
            timeout=OA_TIMEOUT_SECS,
        )
        r.raise_for_status()
        data = r.json()

        for w in data.get("results", []) or []:
            yield w

        cursor = (data.get("meta") or {}).get("next_cursor")
        if not cursor:
            break
