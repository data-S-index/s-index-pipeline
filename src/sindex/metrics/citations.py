from __future__ import annotations

import json
import os
import sys
from typing import Any, Iterable

import orjson


def merge_citations_dicts(
    inputs: Iterable[Iterable[dict[str, Any]] | dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Merge multiple collections of citation records (in-memory) into a single
    deduplicated list using citation_year priority.

    Expected record shape (generic):
        {
            "dataset_id": <str>,
            "citation_link": <str>,
            "source": [<str>] or <str>,
            "citation_year": <int> (priority),
            "citation_date": <str>,
            "placeholder_year": <bool>,
            "placeholder_date": <bool>,
            "citation_weight": <numeric>
        }

    Dedup key: (dataset_id, citation_link)

    Merge behavior:
      - "source": union of sources (sorted)
      - "citation_year" (and related fields):
          * prefer record with earliest citation_year
          * if "winning" year is selected, we overwrite citation_date,
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
            link = rec.get("citation_link")
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
                # Create new entry copying all relevant fields
                entry = {
                    "dataset_id": key[0],
                    "citation_link": key[1],
                    "source": sorted(new_sources),
                    "citation_year": rec.get("citation_year"),
                    "citation_date": rec.get("citation_date"),
                    "placeholder_year": rec.get("placeholder_year"),
                    "placeholder_date": rec.get("placeholder_date"),
                    "citation_weight": rec.get("citation_weight"),
                }
                merged[key] = entry
                continue

            # 4. Update Existing Record
            existing = merged[key]

            # Merge sources
            existing_sources = set(existing.get("source") or [])
            existing["source"] = sorted(existing_sources | new_sources)

            # Check Year Logic
            new_year = rec.get("citation_year")
            ext_year = existing.get("citation_year")

            # Update if we have a year AND (existing has no year OR new is earlier)
            if new_year is not None:
                if ext_year is None or new_year < ext_year:
                    existing["citation_year"] = new_year
                    existing["citation_date"] = rec.get("citation_date")
                    existing["placeholder_year"] = rec.get("placeholder_year")
                    existing["placeholder_date"] = rec.get("placeholder_date")
                    existing["citation_weight"] = rec.get("citation_weight")

    return list(merged.values())


def merge_citations_from_files(input_paths: list[str], output_path: str):
    """
    Reads records from NDJSON files, prints line counts per file,
    and saves the merged deduplicated output.
    """
    total_input_count = 0
    file_reports = {}

    def stream_and_count(paths: list[str]):
        nonlocal total_input_count
        for path in paths:
            file_line_count = 0
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        file_line_count += 1
                        yield json.loads(line)

            file_reports[path] = file_line_count
            total_input_count += file_line_count

    # 1. Merge the records
    merged_list = merge_citations_dicts([stream_and_count(input_paths)])
    output_count = len(merged_list)

    # 2. Write the output
    with open(output_path, "w", encoding="utf-8") as f:
        for record in merged_list:
            f.write(json.dumps(record) + "\n")

    # 3. Print the Summary Report
    print("Merge Summary")
    for path, count in file_reports.items():
        print(f"File: {path} | Lines: {count}")
    print(f"Total Input Records:  {total_input_count}")
    print(f"Total Output Records: {output_count}")
    print(f"Duplicates Removed:   {total_input_count - output_count}")
    print(f"Saved to: {output_path}")


def merge_citations_from_files_fast(
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
                lnk = rec.get("citation_link")
                if not did or not lnk:
                    continue
                lnk_clean = (
                    str(lnk).split("#")[0].rstrip("/").strip()
                )  # some MDC links have # at the end we need them removed for proper comparison
                rec["citation_link"] = lnk_clean

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

                # Date/Weight logic
                new_year = rec.get("citation_year")
                ext_year = existing.get("citation_year")

                # We prefer the earliest year.
                # If existing record has no year, or the new year is earlier, we replace it:
                if new_year is not None:
                    if ext_year is None or new_year < ext_year:
                        # Update the "winning" year and its related metadata
                        existing["citation_year"] = new_year
                        existing["placeholder_year"] = rec.get("placeholder_year")

                        # Sync the specific date fields to this winning year
                        existing["citation_date"] = rec.get("citation_date")
                        existing["placeholder_date"] = rec.get("placeholder_date")

                        # Maintain weight based on the best year
                        existing["citation_weight"] = rec.get("citation_weight")

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
