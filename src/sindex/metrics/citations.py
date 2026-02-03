from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from typing import Any, Iterable, Optional

import orjson

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
    print("--- Merge Summary ---")
    for path, count in file_reports.items():
        print(f"File: {path} | Lines: {count}")
    print("-" * 21)
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
                new_date = rec.get("citation_date")
                ext_date = existing.get("citation_date")

                if new_date:
                    if not ext_date or new_date < ext_date:
                        existing["citation_date"] = new_date
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


def combine_citations(input_paths, output_path):
    """
    Combines multiple .ndjson files with a progress counter that overwrites itself.
    Used to combine citation to DOIs and EMDB
    """
    current_now = datetime.now().isoformat()
    processed_count = 0

    try:
        with open(output_path, "w", encoding="utf-8") as outfile:
            for file_path in input_paths:
                with open(file_path, "r", encoding="utf-8") as infile:
                    for line in infile:
                        line = line.strip()
                        if not line:
                            continue

                        entry = json.loads(line)

                        if entry.get("citation_date"):
                            entry["placeholder_date"] = False
                        else:
                            entry["placeholder_date"] = True
                            entry["citation_date"] = current_now

                        outfile.write(json.dumps(entry) + "\n")
                        processed_count += 1

                        # Update progress every 10,000 lines
                        if processed_count % 10000 == 0:
                            sys.stdout.write(f"\rLines processed: {processed_count:,}")
                            sys.stdout.flush()

        # Final print to move to a new line and show total
        print(f"\nFinished! Total entries saved: {processed_count:,}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
