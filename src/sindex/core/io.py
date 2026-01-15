import glob
import gzip
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

from sindex.core.ids import _norm_dataset_id


def _iter_json_lines(path: Path) -> Iterator[Tuple[Optional[dict], Optional[str]]]:
    """
    Stream a NDJSON or NDJSON.GZ file line-by-line and yield parsed JSON objects.

    Needed for creating slimmed metadata from Datacite records.
    Each non-empty input line is parsed into a dictionary. Invalid JSON lines are not raised as exceptions.
    Instead the function yields `(None, error_string)` where `error_string` identifies the file, line number, and decode problem.

    This makes it safe to run through very large DataCite dumps without crashing the full pipeline.

    Args:
        path: Path to an NDJSON or NDJSON.GZ file.

    Yields:
        Tuple of:
          - A parsed JSON dict if parse succeeds, otherwise None.
          - None if parse succeeded, otherwise a string describing the parse error.
    """
    if path.suffix == ".gz":
        f = gzip.open(path, "rt", encoding="utf-8", errors="strict")
    else:
        f = open(path, "rt", encoding="utf-8", errors="strict")

    with f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line), None
            except Exception as e:
                yield None, f"JSONDecodeError at {path.name}:{lineno} → {e}"


def _collect_ids_and_pub_dates_from_slimmed_file(
    slim_folder: str,
    pattern: str = "**/*.ndjson",
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    """
    Load normalized dataset identifiers and metadata from slim NDJSON files.

    Assumes each record contains a single `"identifiers"` entry. Scans NDJSON
    files recursively, normalizes each identifier using `_norm_dataset_id()`,
    and builds:

      * norm_to_dataset_id: normalized identifier -> dataset_id (original string)
      * dataset_info: dataset_id -> {"dataset_id", "pub_date"}

    Progress is printed in-place (one updating line) to avoid large logs.

    Args:
        slim_folder: Directory containing slim `.ndjson` files.
        pattern: Glob pattern for selecting NDJSON files (default:
            `"**/*.ndjson"` for recursive search).

    Returns:
        A tuple of:
            norm_to_dataset_id: Mapping from normalized identifier to dataset_id.
            dataset_info: Mapping from dataset_id to publication metadata.
    """
    norm_to_dataset_id: Dict[str, str] = {}
    dataset_info: Dict[str, Dict[str, Any]] = {}

    # ---- File discovery ----
    paths = glob.glob(os.path.join(slim_folder, pattern), recursive=True)
    num_files = len(paths)
    print(f"[Slim Scan] Found {num_files} NDJSON file(s).")

    total_records = 0
    file_index = 0

    # ---- Scan each NDJSON file ----
    for path in paths:
        file_index += 1

        print(
            f"\r[Slim Scan] Processing file {file_index}/{num_files}",
            end="",
            flush=True,
        )

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                total_records += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                identifiers = rec.get("identifiers") or []
                if not isinstance(identifiers, list) or not identifiers:
                    continue

                # Exactly one identifier expected
                first = identifiers[0]
                raw_id = (first.get("identifier") or "").strip()
                if not raw_id:
                    continue

                dataset_id = raw_id

                # Publication date
                pub_date = rec.get("publication_date") or ""

                # Save metadata
                if dataset_id not in dataset_info:
                    dataset_info[dataset_id] = {
                        "dataset_id": dataset_id,
                        "pub_date": pub_date,
                    }

                # Normalize identifier
                norm_id = _norm_dataset_id(raw_id)
                if not norm_id:
                    continue

                norm_to_dataset_id.setdefault(norm_id, dataset_id)

    # Finish last progress line cleanly
    print()

    # ---- Summary ----
    print(f"[Slim Scan] Processed {total_records:,} total records.")
    print(f"[Slim Scan] Loaded {len(dataset_info):,} dataset IDs.")
    print(f"[Slim Scan] Generated {len(norm_to_dataset_id):,} normalized IDs.\n")

    return norm_to_dataset_id, dataset_info
