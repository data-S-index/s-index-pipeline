# normalize.py

import glob
import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set, Tuple


def _collect_dois_and_pub_dates_from_slimmed_file(
    slim_folder: str,
    slim_pattern: str = "**/*.ndjson",
    show_progress: bool = True,
) -> Tuple[Set[str], Dict[str, str]]:
    """
    Collect unique DOIs and their publication dates from slim metadata.

    Reuses `_collect_ids_and_pub_dates_from_slimmed_file` to load all dataset
    identifiers and then filters down to DOIs only, ignoring EMDB, and
    other non-DOI IDs.

    Args:
        slim_folder: Directory containing slim `.ndjson` files.
        slim_pattern: Glob pattern for selecting NDJSON files (default:
            `"**/*.ndjson"` for recursive search).
        show_progress: Whether to print a short summary message.

    Returns:
        A tuple of:
            - A set of unique normalized DOI strings.
            - A dict mapping normalized DOI -> publication_date (ISO string),
              using the publication_date from the slim record.
    """
    norm_to_dataset_id, dataset_info = _collect_ids_and_pub_dates_from_slimmed_file(
        slim_folder, pattern=slim_pattern
    )

    target_dois: Set[str] = set()
    doi_pub_dates: Dict[str, str] = {}

    for norm_id, dataset_id in norm_to_dataset_id.items():
        # dataset_id is the original identifier string
        doi_norm = _norm_doi(dataset_id)
        if not doi_norm:
            continue  # skip non-DOI identifiers (EMDB, SRA, etc.)

        target_dois.add(doi_norm)

        info = dataset_info.get(dataset_id, {})
        pub_date = info.get("pub_date", "") or ""

        existing = doi_pub_dates.get(doi_norm, "")
        if not existing:
            doi_pub_dates[doi_norm] = pub_date
        elif pub_date and pub_date < existing:
            doi_pub_dates[doi_norm] = pub_date

    if show_progress:
        print(f"[Slim Scan] Found {len(target_dois):,} unique DOIs.\n")

    return target_dois, doi_pub_dates
