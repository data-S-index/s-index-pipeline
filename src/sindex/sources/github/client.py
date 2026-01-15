import os
import time
from typing import Any, Dict, List

import requests

from sindex.sources.github.constants import (
    ACCEPT_HEADER,
    DEFAULT_MAX_PAGES,
    GITHUB_API_VERSION,
    PAUSE_BETWEEN_CALLS,
    PER_PAGE,
    REQUEST_TIMEOUT,
    SEARCH_CODE_URL,
    USER_AGENT,
)


def get_github_token():
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise EnvironmentError("GITHUB_TOKEN is not set in the environment.")
    return token


def build_headers(token: str | None = None) -> dict:
    tok = token or get_github_token()
    return {
        "Authorization": f"Bearer {tok}",
        "Accept": ACCEPT_HEADER,
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": USER_AGENT,
    }


def _sleep_if_rate_limited(resp: requests.Response) -> bool:
    """Return True if we slept and caller should retry the request."""
    if resp.status_code != 403:
        return False
    reset = resp.headers.get("X-RateLimit-Reset") or resp.headers.get(
        "X-Ratelimit-Reset"
    )
    if reset and reset.isdigit():
        wait = max(0, int(reset) - int(time.time())) + 2
        print(f"[rate-limit] Sleeping {wait}s…")
        time.sleep(wait)
        return True
    return False


def search_code(
    query: str,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    session: requests.Session | None = None,
    token: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Run GitHub code search and return concatenated `items`.
    """
    headers = build_headers(token)
    s = session or requests.Session()

    items: list[dict] = []
    for page in range(1, max_pages + 1):
        resp = s.get(
            SEARCH_CODE_URL,
            headers=headers,
            params={"q": query, "per_page": PER_PAGE, "page": page},
            timeout=REQUEST_TIMEOUT,
        )

        if _sleep_if_rate_limited(resp):
            # retry same page after sleeping
            resp = s.get(
                SEARCH_CODE_URL,
                headers=headers,
                params={"q": query, "per_page": PER_PAGE, "page": page},
                timeout=REQUEST_TIMEOUT,
            )

        resp.raise_for_status()
        data = resp.json()
        batch = data.get("items", []) or []
        items.extend(batch)

        if len(batch) < PER_PAGE or len(items) >= data.get("total_count", 0):
            break

        time.sleep(PAUSE_BETWEEN_CALLS)

    return items


def get_repo_meta(
    full_name: str,
    *,
    session: requests.Session | None = None,
    token: str | None = None,
) -> dict:
    """
    Return repository metadata JSON (created_at, fork, etc.).
    """
    headers = build_headers(token)
    s = session or requests.Session()

    url = f"https://api.github.com/repos/{full_name}"
    resp = s.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    if _sleep_if_rate_limited(resp):
        resp = s.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    if not resp.ok:
        return {}
    return resp.json()
