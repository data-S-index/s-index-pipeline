from __future__ import annotations

import os
import sys
from typing import Any, Iterable

import orjson


def merge_mentions_dicts(
    inputs: Iterable[Iterable[dict[str, Any]] | dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Merge multiple collections of mention records (in-memory) into a single
    deduplicated list using mention_year priority.

    Expected record shape:
        {
            "dataset_id": <str>,
            "source": [<str>] or <str> or missing,
            "mention_link": <str>,
            "mention_year": <int> (priority),
            "mention_date": <ISO string or empty> (optional),
            "placeholder_year": <bool>,
            "placeholder_date": <bool>,
            "mention_weight": <any numeric> (optional)
        }

    Dedup key: (dataset_id, mention_link)

    Merge behavior:
      - "source" becomes union of sources (sorted)
      - "mention_year" (and related fields):
          * prefer record with earliest mention_year
          * if "winning" year is selected, we overwrite mention_date,
            placeholder flags, and weight from that same record.
    """
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    def iter_records(obj: Iterable[dict[str, Any]] | dict[str, Any]):
        if isinstance(obj, dict):
            yield obj
        else:
            yield from obj

    for obj in inputs:
        if obj is None:
            continue

        for rec in iter_records(obj):
            if not isinstance(rec, dict):
                continue

            dataset_id = rec.get("dataset_id")
            link = rec.get("mention_link")
            if not dataset_id or not link:
                continue

            # 1. Standardize Key
            key = (str(dataset_id), str(link))

            # 2. Normalize sources -> set[str]
            src = rec.get("source") or []
            if isinstance(src, str):
                src = [src]
            new_sources = {str(s) for s in src if s}

            # 3. Initialization if new
            if key not in merged:
                entry = {
                    "dataset_id": key[0],
                    "mention_link": key[1],
                    "source": sorted(new_sources),
                    "mention_year": rec.get("mention_year"),
                    "mention_date": rec.get("mention_date"),
                    "placeholder_year": rec.get("placeholder_year"),
                    "placeholder_date": rec.get("placeholder_date"),
                    "mention_weight": rec.get("mention_weight"),
                }
                merged[key] = entry
                continue

            # 4. Update Existing Record
            existing = merged[key]

            # A) Merge sources
            existing_sources = set(existing.get("source") or [])
            existing["source"] = sorted(existing_sources | new_sources)

            # B) Check Year Logic
            new_year = rec.get("mention_year")
            ext_year = existing.get("mention_year")

            # Update if we have a year AND (existing has no year OR new is earlier)
            if new_year is not None:
                if ext_year is None or new_year < ext_year:
                    existing["mention_year"] = new_year
                    existing["mention_date"] = rec.get("mention_date")
                    existing["placeholder_year"] = rec.get("placeholder_year")
                    existing["placeholder_date"] = rec.get("placeholder_date")
                    existing["mention_weight"] = rec.get("mention_weight")

    return list(merged.values())


def merge_mentions_from_files_fast(
    input_paths: list[str], output_path: str, update_interval: int = 10000
):
    merged = {}
    total_in = 0

    # Filter out None or empty strings immediately
    valid_paths = [p for p in input_paths if p and isinstance(p, str)]

    print(f"Starting merge of {len(valid_paths)} valid files...")

    for path in valid_paths:
        # Skip if the file doesn't exist
        if not os.path.isfile(path):
            print(f"  ! Warning: Skipping missing file: {path}")
            continue

        with open(path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue

                total_in += 1
                if total_in % update_interval == 0:
                    sys.stdout.write(
                        f"\rRecords processed: {total_in:,} | Unique found: {len(merged):,}"
                    )
                    sys.stdout.flush()

                rec = orjson.loads(line)

                did = rec.get("dataset_id")
                lnk = rec.get("mention_link")
                if not did or not lnk:
                    continue

                # Clean link similar to citation logic (remove anchors/trailing slashes)
                lnk_clean = str(lnk).split("#")[0].rstrip("/").strip()
                rec["mention_link"] = lnk_clean

                key = (did, lnk_clean)

                # Source handling
                src = rec.get("source") or []
                new_src_set = {src} if isinstance(src, str) else set(src)

                if key not in merged:
                    rec["source"] = new_src_set
                    merged[key] = rec
                    continue

                existing = merged[key]
                existing["source"].update(new_src_set)

                # Year/Date/Weight logic
                new_year = rec.get("mention_year")
                ext_year = existing.get("mention_year")

                # We prefer the earliest year.
                # If existing has no year, or the new year is earlier:
                if new_year is not None:
                    if ext_year is None or new_year < ext_year:
                        # Update the "winning" year and its related metadata
                        existing["mention_year"] = new_year
                        existing["placeholder_year"] = rec.get("placeholder_year")

                        # Sync the specific date fields to this winning year
                        existing["mention_date"] = rec.get("mention_date")
                        existing["placeholder_date"] = rec.get("placeholder_date")

                        # Maintain weight based on the best year
                        existing["mention_weight"] = rec.get("mention_weight")

    sys.stdout.write(
        f"\rFinished processing {total_in:,} records. Unique: {len(merged):,}\n"
    )

    # Write
    if not merged:
        print("No records found to write.")
        return

    print(f"Writing to {output_path}...")
    with open(output_path, "wb") as f:
        for rec in merged.values():
            rec["source"] = sorted(rec["source"])
            f.write(orjson.dumps(rec) + b"\n")

    print("Done!")
