from __future__ import annotations

from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sindex.config.settings import get_user_agent


def make_session(
    *,
    user_agent: str | None = None,
    total_retries: int = 6,
    backoff: float = 2.0,
    pool_connections: int = 20,
    pool_maxsize: int = 20,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods: tuple[str, ...] = ("GET",),
) -> requests.Session:
    """
    Create a pre-configured requests.Session with retry + pooling.

    Sources need to supply their own headers/UA as needed.
    """
    s = requests.Session()
    ua = user_agent or get_user_agent()
    s.headers.update({"User-Agent": ua})

    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
        allowed_methods=list(allowed_methods),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def is_url(s: str) -> bool:
    """
    Return True if `s` looks like a valid absolute HTTP/HTTPS URL.

    Rules:
      - must be a non-empty string
      - scheme must be http or https
      - must have a network location (host)

    Examples:
      is_url("https://example.org") -> True
      is_url("http://example.org/path") -> True
      is_url("example.org") -> False
      is_url("ftp://example.org") -> False
    """
    if not isinstance(s, str):
        return False

    s = s.strip()
    if not s:
        return False

    try:
        p = urlparse(s)
    except Exception:
        return False

    if p.scheme not in ("http", "https"):
        return False

    if not p.netloc:
        return False

    return True


def _is_reachable(
    url: str,
    session: requests.Session | None = None,
    *,
    timeout: float = 10.0,
    allow_blocked: bool = True,
) -> bool:
    caller = session if session else requests

    try:
        resp = caller.head(url, allow_redirects=True, timeout=timeout)

        # Some endpoints don't support HEAD properly
        if resp.status_code in (405, 501) or resp.status_code >= 500:
            resp = caller.get(url, allow_redirects=True, stream=True, timeout=timeout)

        # Success after redirects: any 2xx is good
        if 200 <= resp.status_code < 300:
            return True

        # Often indicates "exists but blocks automation"
        if allow_blocked and resp.status_code in (401, 403):
            return True

        # Sometimes still indicates a resolvable chain
        if 300 <= resp.status_code < 400:
            return True

        return False

    except requests.RequestException:
        return False
