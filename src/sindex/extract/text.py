from __future__ import annotations

from typing import List, Set

from sindex.core.ids import (
    _DOI_PATTERN,
    _norm_doi,
)

from .patterns import TRAILING_PUNCT, URL_RE


def strip_trailing_punct(s: str) -> str:
    return s.strip().rstrip(TRAILING_PUNCT).strip()


def extract_dois(text: str) -> List[str]:
    """
    Extract DOI-like strings from arbitrary text and return normalized DOI IDs.
    """
    if not text:
        return []

    out: List[str] = []
    seen: Set[str] = set()

    for m in _DOI_PATTERN.finditer(text):
        doi = _norm_doi(m.group("doi"))
        if not doi:
            continue
        key = doi.lower()
        if key not in seen:
            seen.add(key)
            out.append(doi)

    return out


def extract_urls(text: str) -> List[str]:
    """
    Extract URLs from text and return de-duplicated list (preserves order).
    """
    if not text:
        return []

    out: List[str] = []
    seen: Set[str] = set()

    for m in URL_RE.finditer(text):
        u = strip_trailing_punct(m.group(0))
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    return out
