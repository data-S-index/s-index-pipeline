from __future__ import annotations

DEFAULT_BASE_URL = "https://digitallibrary.un.org"
SEARCH_PATH = "/search"

DEFAULT_HEADERS = {
    "User-Agent": "python-requests/2.x (sindex-undl)",
    "Accept": "text/xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
