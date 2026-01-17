from __future__ import annotations

import json
from typing import Any, Iterable, List, Set

from .text import extract_dois, extract_urls


def iter_strings(obj: Any) -> Iterable[str]:
    """Yield all string leaf values from nested JSON-like structures."""
    if obj is None:
        return
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from iter_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from iter_strings(v)


def json_text(obj: Any) -> str:
    """Stable stringify for scanning metadata blobs for IDs."""
    return json.dumps(obj, ensure_ascii=False)


def extract_urls_from_json(obj: Any) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()

    for s in iter_strings(obj):
        for u in extract_urls(s):
            if u not in seen:
                seen.add(u)
                urls.append(u)

    return urls


def extract_dois_from_json(obj: Any) -> List[str]:
    dois: List[str] = []
    seen: Set[str] = set()

    for s in iter_strings(obj):
        for d in extract_dois(s):
            key = d.lower()
            if key not in seen:
                seen.add(key)
                dois.append(d)

    return dois
