from __future__ import annotations

from datetime import datetime
from typing import Dict, List


def dedupe_citations_by_link(citations: List[Dict]) -> List[Dict]:
    """
    Deduplicate citation objects by ``citation_link``.

    This function is used after we get citations for a given dataset from a given source
    (like MDC, Open Alex, or DataCite) to make sure we don't have duplicated citations
    for the dataset from that source (e.g. MDC seems to have duplicated citation records).

    We only deduplicate by "citation_link" because the input citations are for one dataset/doi

    Upstream normalization guarantees for the input:
      - If a citation has a date, it is already normalized using ``_norm_date_iso``.
      - If a citation has *no* date, the key ``"citation_date"`` is simply absent.

    This is how duplication is managed:
      - For a given ``citation_link``, if *none* of the duplicates have a date,
        all entries are treated as equivalent and the *first* occurrence is kept.
      - If exactly one entry has a date, that entry is preferred.
      - If multiple duplicates have dates, the entry with the *earliest*
        ``citation_date`` is kept.

    Args:
        citations:
            A list of citation dictionaries. Each must contain a
            ``"citation_link"`` key and may contain a normalized
            ``"citation_date"`` key, and other keys.

    Returns:
        A list of deduplicated citation dictionaries, preserving the order of
        first appearance of each unique ``citation_link``.
    """
    grouped: Dict[str, Dict] = {}
    order: List[str] = []

    for c in citations:
        link = c.get("citation_link")
        if not isinstance(link, str):
            continue

        # First occurrence of this link, just save the citation dict
        if link not in grouped:
            grouped[link] = c
            order.append(link)
            continue

        # If not first occurence compare with existing one
        existing = grouped[link]

        date_existing_str = existing.get("citation_date")
        date_new_str = c.get("citation_date")

        # Case 1: existing has no date, new has --> keep new
        if date_existing_str is None and date_new_str is not None:
            grouped[link] = c
            continue

        # Case 2: existing has date, new does not --> keep existing
        if date_existing_str is not None and date_new_str is None:
            continue

        # Case 3: both have no date --> keep existing
        if date_existing_str is None and date_new_str is None:
            continue

        # Case 4: both have normalized ISO dates --> keep new if it has earlier citation date
        if datetime.fromisoformat(date_new_str) < datetime.fromisoformat(
            date_existing_str
        ):
            grouped[link] = c

    return [grouped[i] for i in order]
    return [grouped[i] for i in order]
