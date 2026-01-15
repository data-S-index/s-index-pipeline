SEARCH_CODE_URL = "https://api.github.com/search/code"

REQUEST_TIMEOUT = 60
PAUSE_BETWEEN_CALLS = 0.8  # to avoid getting stuck on the API rate limit
PER_PAGE = 100
DEFAULT_MAX_PAGES = 10  # 10 each page * 100 results so this returns 1000 results from the GitHub search (this is the max, for more results, we could, e.g., run multiple searches across date ranges)

USER_AGENT = "dataset-mentions-on-github"

MAIN_README_NAMES = {
    "readme",
    "readme.md",
    "readme.rst",
    "readme.txt",
}

GITHUB_API_VERSION = "2022-11-28"
ACCEPT_HEADER = "application/vnd.github.text-match+json"
