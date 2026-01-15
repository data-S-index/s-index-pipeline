from __future__ import annotations

import gzip
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from sindex.core.http import make_session
from sindex.core.io import _iter_json_lines
from sindex.sources.emdb.client import _fetch_all_emdb_ids
from sindex.sources.emdb.constants import DEFAULT_MAX_WORKERS
from sindex.sources.emdb.discovery import _fetch_entry_and_filter
from sindex.sources.emdb.normalize import slim_emdb_record


def harvest_emdb_datasets_for_date_range_to_ndjson(
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    save_folder: Optional[str] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> int:
    """
    Harvest EMDB entries deposited up to (and including) a given date.

    This function:
      1. Retrieves all released EMDB IDs via the CSV search endpoint provided by EMDB.
      2. Uses a ThreadPoolExecutor to fetch /entry/{id} in parallel.
      3. Reads the deposition date from entry["admin"]["key_dates"]["deposition"].
      4. Keeps only entries whose deposition date <= cutoff_date.
      5. Writes one raw entry JSON per line to an NDJSON file.

    Args:
        start_date_str:
            Start cutoff date (inclusive) as a string in "YYYY-MM-DD" format, e.g. "2025-09-30".
            Only entries with deposition date <= this date are included.
        end_date_str:
            End cutoff date (inclusive) as a string in "YYYY-MM-DD" format, e.g. "2025-09-30".
            Only entries with deposition date <= this date are included.
        output_path:
            Path to the NDJSON file to write. If None, a file named:
                "emdb_deposited_upto_<YYYY-MM-DD>.ndjson"
            is created in the current directory.
        max_workers:
            Number of worker threads to use for concurrent downloads.

    Returns:
        The number of EMDB entries written to the NDJSON file.
    """

    # 1. Parse dates (helper to handle None or String)
    def parse_dt(dt_str):
        if not dt_str:
            return None
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Date must be 'YYYY-MM-DD', got: {dt_str}") from e

    start_date = parse_dt(start_date_str)
    end_date = parse_dt(end_date_str)

    # 2. Handle dynamic output naming
    folder_path = Path(save_folder)
    folder_path.mkdir(parents=True, exist_ok=True)  # Create folder if it doesn't exist

    s_label = start_date_str if start_date_str else "start"
    e_label = end_date_str if end_date_str else "now"
    filename = f"emdb_deposited_{s_label}_to_{e_label}.ndjson"

    out_path = folder_path / filename

    session = make_session()

    # 3) Get all IDs
    emdb_ids = _fetch_all_emdb_ids(session=session)
    total_ids = len(emdb_ids)
    print(
        f"Harvesting entries from {start_date or 'beginning'} to {end_date or 'present'} "
        f"using {max_workers} workers..."
    )

    written = 0
    processed = 0

    # 4) Concurrently fetch and filter
    with out_path.open("w", encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _fetch_entry_and_filter, session, emdb_id, start_date, end_date
                )
                for emdb_id in emdb_ids
            ]

            for fut in as_completed(futures):
                processed += 1
                emdb_id, entry = fut.result()

                if entry is not None:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    written += 1

                # Progress printed every 50 EMDB IDs processed
                if processed % 50 == 0 or processed == total_ids:
                    print(
                        f"Processed {processed}/{total_ids} IDs (written: {written})",
                        end="\r",
                        flush=True,
                    )

    # 5) Print a final summary
    print()
    print(f"\nDone. Wrote {written} entries to {out_path}")
    return written


def batch_slim_emdb_record_to_ndjson(
    src_folder: str,
    dst_folder: str,
    overwrite: bool = False,
    accept_gz: bool = True,
    one_line_progress: bool = True,
) -> dict:
    """
    Stream-process a directory of NDJSON(.gz) EMDB dumps and create slimmed records.

    For each NDJSON/NDJSON.GZ file in the source folder:
      - Stream (iter over lines, not loaded into memory)
      - Slim each record using `slim_emdb_record`
      - Write a matching output NDJSON(.gz) file in dst_folder

    Args:
        src_folder: Folder containing NDJSON or NDJSON.GZ source files.
        dst_folder: Folder where slimmed NDJSON files will be written.
        overwrite: If False (default), existing outputs are left untouched.
        accept_gz: If True (default), also process *.ndjson.gz files.
        one_line_progress: If True (default), progress prints as '[x/y] files completed'.

    Returns:
        A summary dict with counts:
          files_seen, records_read, records_kept, records_bad_json,
          output_dir, elapsed_sec, rate_rec_per_sec.
    """
    src = Path(src_folder)
    dst = Path(dst_folder)
    dst.mkdir(parents=True, exist_ok=True)

    patterns = ["*.ndjson"]
    if accept_gz:
        patterns.append("*.ndjson.gz")

    files = []
    for pat in patterns:
        files.extend(src.glob(pat))
    files.sort()

    num_files = len(files)
    total_in = total_out = total_bad = 0
    t0 = time.time()

    def _progress(i: int, n: int):
        if one_line_progress:
            print(f"\r[{i}/{n}] files completed", end="", flush=True)

    _progress(0, num_files)

    for idx, in_path in enumerate(files, 1):
        name = in_path.name
        if name.endswith(".ndjson.gz"):
            out_name = name.replace(".ndjson.gz", "-slim.ndjson.gz")
        elif name.endswith(".ndjson"):
            out_name = name.replace(".ndjson", "-slim.ndjson")
        out_path = dst / out_name
        if out_path.exists() and not overwrite:
            _progress(idx, num_files)
            continue

        if in_path.suffix == ".gz":

            def out_opener(p):
                return gzip.open(p, "wt", encoding="utf-8")
        else:

            def out_opener(p):
                return open(p, "wt", encoding="utf-8")

        with out_opener(out_path) as out_f:
            for rec, err in _iter_json_lines(in_path):
                if err:
                    total_bad += 1
                    continue

                try:
                    slim = slim_emdb_record(rec)
                except Exception as e:
                    print(f"\nError in file {in_path}: {e!r}")
                    print(json.dumps(rec, indent=2, ensure_ascii=False))
                    raise
                out_f.write(json.dumps(slim, ensure_ascii=False) + "\n")
                total_in += 1
                total_out += 1

        _progress(idx, num_files)

    if one_line_progress:
        print()

    dt = time.time() - t0
    rate = int(total_out / dt) if dt > 0 else 0
    summary = {
        "files_seen": num_files,
        "records_read": total_in,
        "records_kept": total_out,
        "records_bad_json": total_bad,
        "output_dir": str(dst.resolve()),
        "elapsed_sec": round(dt, 2),
        "rate_rec_per_sec": rate,
    }
    print(
        f"Done. files={num_files} kept={total_out:,} bad={total_bad:,} "
        f"time={dt:.1f}s rate≈{rate:,}/s → {summary['output_dir']}"
    )
    return summary
