from __future__ import annotations

import gzip
import json
import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List

import requests

from sindex.core.dates import _parse_date_strict
from sindex.core.http import make_session
from sindex.core.io import _iter_json_lines
from sindex.sources.datacite.discovery import (
    get_datacite_doi_record,
    stream_datacite_records,
)
from sindex.sources.datacite.normalize import slim_datacite_record

from .normalize import datacite_citations_block_to_records


def harvest_datacite_doi_list_to_ndjson(
    doi_list: Iterable[str],
    output_folder: str,
    batch_size: int = 2,
):
    """
    Fetch DataCite metadata for many DOIs.

    This function is used to test our pipeline for given DOIs
    It saves Datacite records to NDJSON files in batches.
    Each NDJSON line contains exactly the JSON object returned in the DataCite
    API response under the `"data"` key, with no additional fields added.

    Args:
        doi_list: List of dois.
        batch_size: Number of metadata records to write per NDJSON output file.
        output_folder: Directory where NDJSON batch files will be written.

    Returns:
        None.
        NDJSON files are written to `output_folder`, each containing
        `batch_size` lines (except the final file, which may contain fewer).
        Each line is a standalone JSON object representing the full DataCite
        metadata record for a single DOI.

    Notes:
        - Uses a shared retry-aware `requests.Session` for performance.
        - DOIs that do not resolve in DataCite are skipped.
        - Files are named sequentially as `datacite-batch-0000.ndjson`,
          `datacite-batch-0001.ndjson`, etc.
    """
    session = make_session()
    out_dir = Path(output_folder)
    out_dir.mkdir(parents=True, exist_ok=True)

    batch_index = 0
    batch = []

    for doi in doi_list:
        print(f"Fetching {doi} ...")

        metadata = get_datacite_doi_record(doi, session=session)
        if metadata is None:
            print(f"  No DataCite record found for {doi}")
            continue

        # Append only the metadata (the DataCite 'data' portion)
        batch.append(metadata)

        if len(batch) == batch_size:
            fname = out_dir / f"datacite-batch-{batch_index:04d}.ndjson"
            print(f"  Writing {len(batch)} metadata records → {fname}")
            with open(fname, "w", encoding="utf-8") as f:
                for obj in batch:
                    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            batch = []
            batch_index += 1

    # Write final partial batch
    if batch:
        fname = out_dir / f"datacite-batch-{batch_index:04d}.ndjson"
        print(f"  Writing {len(batch)} metadata records → {fname}")
        with open(fname, "w", encoding="utf-8") as f:
            for obj in batch:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def harvest_datacite_datasets_for_date_range_to_ndjson(
    start_date_str: str,
    end_date_str: str,
    *,
    window_days: int = 7,
    page_size: int = 1000,
    polite_sleep_seconds: float | None = 1.0,
    skip_empty_files: bool = False,
    save_folder: str | None = None,
    page_floor: int = 100,  # minimum fallback page size
    session: requests.Session | None = None,  # optional shared session
    timeout: tuple[int, int] = (10, 240),  # (connect, read) seconds
) -> int:
    """
    Harvest DataCite dataset records for an inclusive date range, writing one NDJSON file per date window.

    This function harvests DataCite dataset records created within [start_date, end_date] inclusive.
    We noticed that for large responses the DataCite API returns errors so we are chunking the work
    into windows of `window_days` (default 7 days as we found it to work well). It writes each window to a separate
    NDJSON file named: `datacite-<start>-<end>.ndjson`.

    The function iterates windows backwards from `end_date` toward `start_date`.

    Args:
        start_date_str: Inclusive start date (YYYY-MM-DD).
        end_date_str: Inclusive end date (YYYY-MM-DD).
        window_days: Window size in days (inclusive). Default 7.
        page_size: DataCite cursor page size (max 1000).
        polite_sleep_seconds: Optional sleep between windows to avoid hammering API.
        skip_empty_files: If True, delete output file for windows with zero records.
        save_folder: Folder where NDJSON files are written. Defaults to current directory.
        page_floor: Minimum allowed page_size when backing off after ReadTimeouts.
        session: Optional shared requests.Session; if None, a retry-configured session is created.
        timeout: (connect, read) timeout passed to DataCite requests.

    Returns:
        Total number of records written across all windows.

    Raises:
        ValueError: If dates are invalid or end_date < start_date or window_days < 1.
        requests.exceptions.RequestException: If the harvest fails after retries/backoff logic.
    """
    if window_days < 1:
        raise ValueError("window_days must be >= 1")

    if save_folder is None:
        save_folder = "."
    os.makedirs(save_folder, exist_ok=True)

    start_date = _parse_date_strict(start_date_str)
    end_date = _parse_date_strict(end_date_str)
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    total_records = 0
    end = end_date

    # Use provided session or create one (retry-aware)
    s = session or make_session(total_retries=6, backoff=2.0)

    while True:
        # Compute window start (inclusive)
        window_start = end - timedelta(days=window_days - 1)

        # Do not cross calendar-year boundaries (to faciliate post-processing)
        year_start = date(end.year, 1, 1)
        if window_start < year_start:
            window_start = year_start

        # Clamp to requested start_date
        if window_start < start_date:
            window_start = start_date

        start_iso, end_iso = window_start.isoformat(), end.isoformat()
        out_path = os.path.join(save_folder, f"datacite-{start_iso}-{end_iso}.ndjson")

        # Attempt the window; on ReadTimeout shrink page_size and retry the same window
        ps = page_size
        while True:
            print(
                f"Fetching records {start_iso} → {end_iso} "
                f"(window_days={window_days}, page_size={ps})"
            )
            range_count = 0
            wrote_any = False

            try:
                with open(out_path, "w", encoding="utf-8", buffering=1024 * 1024) as f:
                    for rec in stream_datacite_records(
                        start_iso,
                        end_iso,
                        page_size=ps,
                        session=s,
                        timeout=timeout,
                    ):
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        total_records += 1
                        range_count += 1
                        wrote_any = True

                if not wrote_any and skip_empty_files:
                    try:
                        os.remove(out_path)
                        print(f"  No records; removed empty file {out_path}")
                    except OSError:
                        pass
                else:
                    print(f"  Saved {range_count} records → {out_path}")
                break  # window succeeded

            except requests.exceptions.ReadTimeout:
                if ps > page_floor:
                    ps = max(page_floor, ps // 2)
                    print(f"  ReadTimeout; retrying with smaller page_size={ps} ...")
                    continue
                print(
                    f"  ReadTimeout persisted for {start_iso} → {end_iso} at page_size={ps}."
                )
                print(
                    f"  Resume tip: restart later with start_date_str='{start_date_str}' "
                    f"and end_date_str='{end_iso}'"
                )
                if not wrote_any and os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
                raise

            except requests.exceptions.RequestException as e:
                print(f"  Harvest failed at range {start_iso} → {end_iso}: {e}")
                print(
                    f"  Resume tip: restart later with start_date_str='{start_date_str}' "
                    f"and end_date_str='{end_iso}'"
                )
                if not wrote_any and os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
                raise

        # Stop once we've reached the requested start_date
        if window_start == start_date:
            print(f"Finished harvest. Total records saved: {total_records}")
            break

        # Next window ends the day before the current window starts
        end = window_start - timedelta(days=1)
        if polite_sleep_seconds:
            time.sleep(polite_sleep_seconds)

    return total_records


def batch_slim_datacite_record_to_ndjson(
    src_folder: str,
    dst_folder: str,
    overwrite: bool = False,
    accept_gz: bool = True,
    one_line_progress: bool = True,
) -> dict:
    """
    Stream-process a directory of NDJSON(.gz) DataCite dumps and create slimmed records.

    For each NDJSON/NDJSON.GZ file in the source folder:
      - Stream (iter over lines, not loaded into memory)
      - Slim each record using `slim_datacite_record`
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
            out_f = gzip.open(out_path, "wt", encoding="utf-8")
        else:
            out_f = open(out_path, "wt", encoding="utf-8")

        with out_f:
            for rec, err in _iter_json_lines(in_path):
                if err:
                    total_bad += 1
                    continue

                slim = slim_datacite_record(rec)

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


def find_citations_dc_from_citation_block(
    target_doi: str,
    citations: Dict[str, list] | None,
    *,
    dataset_pub_date: str | None = None,
) -> List[Dict[str, object]]:
    """
    Wrapper
    """
    return datacite_citations_block_to_records(
        target_doi=target_doi,
        citations=citations,
        dataset_pub_date=dataset_pub_date,
    )
