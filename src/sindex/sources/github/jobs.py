# src/sindex/sources/github_mentions/jobs.py

from __future__ import annotations

import json
import os
import time

import requests

from sindex.core.io import _collect_ids_and_pub_dates_from_slimmed_file
from sindex.sources.github.constants import DEFAULT_MAX_PAGES
from sindex.sources.github.discovery import find_github_mentions_for_dataset_id


def harvest_github_mentions_from_slim_ndjson_to_ndjson(
    slim_folder: str,
    output_path: str,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    include_forks: bool = False,
    session: requests.Session | None = None,
    token: str | None = None,
) -> int:
    """
    Read dataset IDs + pub dates from slimmed metadata, find GitHub mentions, write NDJSON.
    """
    _, dataset_info = _collect_ids_and_pub_dates_from_slimmed_file(
        slim_folder, pattern="**/*.ndjson"
    )
    if not dataset_info:
        print(f"No dataset IDs found in slimmed metadata under: {slim_folder}")
        return 0

    dataset_ids = sorted(dataset_info.keys())
    print(f"Found {len(dataset_ids)} unique dataset IDs in slimmed metadata.")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    total_written = 0
    t0 = time.time()
    s = session or requests.Session()

    with open(output_path, "w", encoding="utf-8") as out_f:
        total_ids = len(dataset_ids)

        for idx, dataset_id in enumerate(dataset_ids, start=1):
            info = dataset_info.get(dataset_id, {}) or {}
            dataset_pub_date = info.get("pub_date", "") or ""

            try:
                mentions = find_github_mentions_for_dataset_id(
                    dataset_id,
                    dataset_pub_date,
                    max_pages=max_pages,
                    include_forks=include_forks,
                    session=s,
                    token=token,
                )
            except Exception as e:
                print(f"Error while processing ID {dataset_id}: {e}")
                continue

            for rec in mentions:
                out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                total_written += 1

            if idx % 25 == 0 or idx == total_ids:
                elapsed = time.time() - t0
                print(
                    f"[GitHub mentions] {idx}/{total_ids} IDs processed, "
                    f"{total_written} mentions written (elapsed {elapsed:.1f}s)"
                )

    print(f"Done. Wrote {total_written} GitHub mention records to: {output_path}")
    return total_written
