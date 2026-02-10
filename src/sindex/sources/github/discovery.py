from __future__ import annotations

import time
from typing import Any

import requests

from sindex.core.dates import (
    _DEFAULT_CIT_MEN_DATE,
    _DEFAULT_CIT_MEN_YEAR,
    _norm_date_iso,
    get_realistic_date,
    is_realistic_integer_year,
)
from sindex.core.ids import _norm_dataset_id
from sindex.metrics.weights import mention_weight_year
from sindex.sources.github.client import get_repo_meta, search_code
from sindex.sources.github.constants import (
    DEFAULT_MAX_PAGES,
    MAIN_README_NAMES,
    PAUSE_BETWEEN_CALLS,
)


def find_github_mentions_for_dataset_id(
    dataset_id: str,
    *,
    dataset_pubyear: int | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    include_forks: bool = False,
    session: requests.Session | None = None,
    token: str | None = None,
) -> list[dict]:
    """
    Search GitHub READMEs for mentions of a dataset identifier.

    Uses `_norm_dataset_id()` to extract a normalized identifier for the search.

    Only README files in the root of a GitHub repo are considered:
        README, README.md, README.rst, README.txt

    At most one mention is returned per repository.

    Args:
        dataset_id: Any dataset identifier (e.g., DOI, EMD-####, URL).
        dataset_pub_date: ISO publication date of the dataset.
        max_pages: Maximum GitHub search pages to request.
                   Default is 1,000 max allowed by GitHub
        include_forks: Whether to include forked repositories. No by default.

    Returns:
        List of mention dicts of the form:
            {
                "dataset_id": <original target_id as provided in input>,
                "source": ["github"],
                "mention_link": "https://github.com/<owner>/<repo>",
                "mention_date": <ISO date or omitted>, currently repo creation date
                "mention_weight": <float>
            }
    """

    search_term = _norm_dataset_id(dataset_id)
    query = f'"{search_term}" in:file filename:README'
    items = search_code(query, max_pages=max_pages, session=session, token=token)
    if not items:
        return []

    repo_meta_cache: dict[str, dict[str, Any]] = {}
    repos_with_mentions: dict[str, dict[str, str]] = {}

    for it in items:
        repo_info = it.get("repository") or {}
        full_name = repo_info.get("full_name")
        path = it.get("path") or ""
        if not full_name or not path:
            continue

        p = path.lower()
        if "/" in p:
            continue
        if p not in MAIN_README_NAMES:
            continue

        if full_name in repos_with_mentions:
            continue

        meta = repo_meta_cache.get(full_name)
        if meta is None:
            meta = get_repo_meta(full_name, session=session, token=token) or {}
            repo_meta_cache[full_name] = meta
            time.sleep(PAUSE_BETWEEN_CALLS)

        if not include_forks and meta.get("fork"):
            continue

        m_date_raw = meta.get("created_at")
        mention_date = None
        if m_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(m_date_raw))
                mention_date = get_realistic_date(norm_iso_date)
            except (ValueError, TypeError):
                mention_date = None

        repos_with_mentions[full_name] = {
            "repo_link": f"https://github.com/{full_name}",
            "mention_date": mention_date,
        }

    results: list[dict] = []
    for full_name in sorted(repos_with_mentions.keys()):
        info = repos_with_mentions[full_name]
        mention_date = info.get("mention_date")

        if mention_date:
            m_year_raw = int(mention_date[:4])
        else:
            m_year_raw = None
        mention_year = None
        if is_realistic_integer_year(m_year_raw):
            mention_year = m_year_raw

        rec = {
            "dataset_id": dataset_id,
            "source": ["github"],
            "mention_link": info["repo_link"],
            "mention_weight": mention_weight_year(dataset_pubyear, mention_year),
        }

        if mention_date:
            rec["mention_date"] = mention_date
            rec["placeholder_date"] = False
        else:
            rec["mention_date"] = _DEFAULT_CIT_MEN_DATE
            rec["placeholder_date"] = True

        if mention_year:
            rec["mention_year"] = mention_year
            rec["placeholder_year"] = False
        else:
            rec["mention_year"] = _DEFAULT_CIT_MEN_YEAR
            rec["placeholder_year"] = True

        results.append(rec)

    return results
