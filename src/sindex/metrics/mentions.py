from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional

from sindex.core.dates import _to_datetime_utc


def merge_mentions_dicts(
    inputs: Iterable[Iterable[dict[str, Any]] | dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Merge multiple collections of mention records (in-memory) into a single
    deduplicated list.

    Each input can be:
      - a list/iterable of mention dicts, OR
      - a single mention dict (treated as one record)

    Expected record shape:
        {
            "dataset_id": <str>,
            "source": [<str>] or <str> or missing,
            "mention_link": <str>,
            "mention_date": <ISO string or empty> (optional),
            "mention_weight": <any numeric> (optional)
        }

    Dedup key: (dataset_id, mention_link)

    Merge behavior:
      - "source" becomes union of sources (sorted)
      - "mention_date" selection:
          * prefer any dated record over no-date record
          * if both dated, keep earliest date
          * if final selection has no date, omit "mention_date" from output
      - "mention_weight" is taken from the selected record (the date winner)

    Returns:
      List of merged mention records (order not guaranteed).
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
            link = rec.get("mention_link")
            if not dataset_id or not link:
                continue

            key = (str(dataset_id), str(link))

            # Normalize sources -> set[str]
            src = rec.get("source") or []
            if isinstance(src, str):
                src = [src]
            new_sources = {str(s) for s in src if s}

            date_str = rec.get("mention_date") or ""
            dt = _to_datetime_utc(date_str)

            existing = merged.get(key)
            if existing is None:
                entry: dict[str, Any] = {
                    "dataset_id": key[0],
                    "source": sorted(new_sources),
                    "mention_link": key[1],
                    "mention_weight": rec.get("mention_weight"),
                }
                if dt is not None:
                    entry["mention_date"] = str(rec.get("mention_date"))
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
                existing["mention_weight"] = rec.get("mention_weight")
                if dt is not None:
                    existing["mention_date"] = str(rec.get("mention_date"))
                else:
                    existing.pop("mention_date", None)
                best_dt[key] = dt

    return list(merged.values())
