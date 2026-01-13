# pipeline/external/openalex/client.py

from __future__ import annotations

from typing import Optional

import requests

from sindex.core.http import make_session

from .constants import OA_BASE_URL, OA_TIMEOUT_SECS, USER_AGENT_OA


def make_openalex_session(
    *,
    api_key: Optional[str] = None,
    user_agent: str = USER_AGENT_OA,
) -> requests.Session:
    """
    Create a requests.Session tuned for OpenAlex with retry/backoff.

    Retries common transient failures and 429/5xx responses, and reuses
    connections across many OpenAlex calls for better performance.

    Args:
        total_retries: Max retry attempts for connection/read/status errors.
        backoff: Exponential backoff factor in seconds.
        api_key: Optional OpenAlex API key. If provided, it will be sent on
                 every request as the `api_key` query parameter.

    Returns:
        A configured `requests.Session` instance.
    """
    s = make_session(
        user_agent=user_agent,
        allowed_methods=("GET",),
        status_forcelist=(429, 500, 502, 503, 504),
        pool_connections=20,
        pool_maxsize=20,
        total_retries=6,
        backoff=1.5,
    )
    if api_key:
        s.params = getattr(s, "params", {})
        s.params["api_key"] = api_key
    return s


def get_openalex_record(
    path: str,
    *,
    session: requests.Session,
    params: dict | None = None,
    timeout: int = OA_TIMEOUT_SECS,
) -> tuple[int, dict]:
    """
    GET OA endpoint and return (status_code, json_dict).
    Raises for invalid JSON, but does NOT call raise_for_status().
    """
    url = f"{OA_BASE_URL}{path}"
    r = session.get(url, params=params, timeout=timeout)
    return r.status_code, r.json()
