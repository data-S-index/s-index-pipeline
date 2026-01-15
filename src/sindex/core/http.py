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


def _is_reachable(url: str, session: requests.Session | None = None) -> bool:
    """Internal helper to check if a URL returns a successful status code."""
    caller = session if session else requests
    try:
        # Use HEAD first (faster), follow redirects (vital for DOIs)
        resp = caller.head(url, allow_redirects=True, timeout=10)
        # If HEAD is not allowed (403/405), try a streaming GET
        if resp.status_code >= 400:
            resp = caller.get(url, stream=True, timeout=10)
        return resp.status_code == 200
    except requests.RequestException:
        return False
