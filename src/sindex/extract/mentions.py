from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from sindex.core.ids import _norm_dataset_id, _norm_doi

from .json import extract_dois_from_json, extract_urls_from_json, json_text
from .text import extract_dois, extract_urls


def dedupe_preserve_order(items: List[str], *, key_fn=None) -> List[str]:
    if key_fn is None:
        key_fn = lambda x: x
    out: List[str] = []
    seen: Set[Any] = set()
    for x in items:
        k = key_fn(x)
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def normalize_doi_list(dois: List[str]) -> List[str]:
    normed = []
    for d in dois:
        nd = _norm_doi(d)
        if nd:
            normed.append(nd)
    return dedupe_preserve_order(normed, key_fn=lambda x: x.lower())


def normalize_url_list(urls: List[str]) -> List[str]:
    # For now, just de-dupe. Can add URL canonicalization later if desired.
    return dedupe_preserve_order(urls)


def extract_mentions_from_metadata(record_json: Any) -> Dict[str, List[str]]:
    """
    Extract DOI + URL mentions from record JSON only.
    """
    # DOIs and URLs can appear in nested fields
    dois = extract_dois_from_json(record_json)
    urls = extract_urls_from_json(record_json)

    # Also scan full JSON string
    blob = json_text(record_json)
    dois = dois + extract_dois(blob)
    urls = urls + extract_urls(blob)

    return {
        "dois": normalize_doi_list(dois),
        "urls": normalize_url_list(urls),
    }


def extract_mentions_from_text(text: str) -> Dict[str, List[str]]:
    return {
        "dois": normalize_doi_list(extract_dois(text)),
        "urls": normalize_url_list(extract_urls(text)),
    }


def normalize_dataset_ids_from_mentions(
    dois: List[str], other_ids: Optional[List[str]] = None
) -> List[str]:
    """
    Optional helper to roll DOI+other IDs into canonical dataset id space.
    """
    out = []
    for x in dois or []:
        y = _norm_dataset_id(x)
        if y:
            out.append(y)
    for x in other_ids or []:
        y = _norm_dataset_id(x)
        if y:
            out.append(y)
    return dedupe_preserve_order(out, key_fn=lambda s: s.lower())
