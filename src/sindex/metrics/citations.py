from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional

from sindex.core.dates import _to_datetime_utc


def merge_citations_dicts(
    inputs: Iterable[Iterable[dict[str, Any]] | dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Merge multiple collections of citation records (in-memory) into a single
    deduplicated list.

    Each input can be:
      - a list/iterable of citation dicts, OR
      - a single citation dict (it will be treated as one record)

    Expected record shape (generic):
        {
            "dataset_id": <str>,
            "source": [<str>] or <str> or missing,
            "citation_link": <str>,
            "citation_date": <ISO string or empty> (optional),
            "citation_weight": <any numeric> (optional)
        }

    Dedup key: (dataset_id, citation_link)

    Merge behavior:
      - "source" becomes union of sources (sorted)
      - "citation_date" selection:
          * prefer any dated record over no-date record
          * if both dated, keep earliest date
          * if final selection has no date, omit "citation_date" from output
      - "citation_weight" is taken from the selected record (the date winner)

    Returns:
      List of merged records (order not guaranteed).
    """
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    best_dt: dict[tuple[str, str], Optional[datetime]] = {}
    total_input_records = 0

    def iter_records(obj: Iterable[dict[str, Any]] | dict[str, Any]):
        if isinstance(obj, dict):
            yield obj
        else:
            yield from obj

    for obj in inputs:
        if obj is None:
            continue

        for rec in iter_records(obj):
            total_input_records += 1
            if not isinstance(rec, dict):
                continue

            dataset_id = rec.get("dataset_id")
            link = rec.get("citation_link")
            if not dataset_id or not link:
                continue

            key = (str(dataset_id), str(link))

            # Normalize sources -> set[str]
            src = rec.get("source") or []
            if isinstance(src, str):
                src = [src]
            new_sources = {str(s) for s in src if s}

            date_str = rec.get("citation_date") or ""
            dt = _to_datetime_utc(date_str)

            existing = merged.get(key)
            if existing is None:
                entry: dict[str, Any] = {
                    "dataset_id": key[0],
                    "source": sorted(new_sources),
                    "citation_link": key[1],
                    "citation_weight": rec.get("citation_weight"),
                }
                if dt is not None:
                    entry["citation_date"] = str(rec.get("citation_date"))
                merged[key] = entry
                best_dt[key] = dt
                continue

            # Merge sources
            existing_sources = set(existing.get("source") or [])
            existing["source"] = sorted(existing_sources | new_sources)

            existing_dt = best_dt.get(key)
            replace = False

            # Prefer dated over undated
            if existing_dt is None and dt is not None:
                replace = True
            # If both dated, keep earliest
            elif existing_dt is not None and dt is not None and dt < existing_dt:
                replace = True

            if replace:
                existing["citation_weight"] = rec.get("citation_weight")
                if dt is not None:
                    existing["citation_date"] = str(rec.get("citation_date"))
                else:
                    existing.pop("citation_date", None)
                best_dt[key] = dt

    return list(merged.values())
