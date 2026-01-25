from __future__ import annotations

import gzip
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb
import orjson
import requests

from sindex.core.dates import _parse_date_strict, get_best_dataset_date
from sindex.core.http import make_session
from sindex.core.ids import _norm_doi
from sindex.core.io import _iter_json_lines
from sindex.sources.datacite.discovery import (
    get_datacite_doi_record,
    stream_datacite_records,
)

from .normalize import (
    datacite_citations_block_to_records,
    datacite_citations_block_to_records_optimized,
    slim_datacite_record,
)


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
    detail: bool = True,
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

        # Attempt the window, on ReadTimeout shrink page_size and retry the same window
        ps = page_size
        while True:
            print(
                f"Fetching records {start_iso} → {end_iso} "
                f"(window_days={window_days}, page_size={ps}, detail={detail})"
            )
            range_count = 0
            wrote_any = False

            try:
                with open(out_path, "w", encoding="utf-8", buffering=1024 * 1024) as f:
                    for rec in stream_datacite_records(
                        start_iso,
                        end_iso,
                        page_size=ps,
                        detail=detail,
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


def _worker_process_file(args):
    """
    Worker function: Processes a single NDJSON file.
    For multiprocessing.
    """
    in_path, out_path, overwrite = args

    if out_path.exists() and not overwrite:
        return 0, 0, 0

    r = k = b = 0
    is_gz = in_path.suffix == ".gz"
    open_func = gzip.open if is_gz else open
    mode = "rb"
    out_mode = "wb"

    try:
        with open_func(in_path, mode) as f_in, open_func(out_path, out_mode) as f_out:
            for line in f_in:
                if not line.strip():
                    continue
                try:
                    rec = orjson.loads(line)
                    r += 1

                    slim = slim_datacite_record(rec)
                    f_out.write(orjson.dumps(slim) + b"\n")
                    k += 1
                except Exception:
                    b += 1
    except Exception as e:
        print(f"\n[Error] {in_path.name}: {e}")

    return r, k, b


def batch_slim_datacite_record_to_ndjson_fast(
    src_folder: str,
    dst_folder: str,
    overwrite: bool = False,
    accept_gz: bool = True,
    one_line_progress: bool = True,
    workers: int = os.cpu_count(),
) -> dict:
    src, dst = Path(src_folder), Path(dst_folder)
    dst.mkdir(parents=True, exist_ok=True)

    # Gather files
    patterns = ["*.ndjson"]
    if accept_gz:
        patterns.append("*.ndjson.gz")

    files = []
    for pat in patterns:
        files.extend(src.glob(pat))
    files.sort()

    num_files = len(files)
    if num_files == 0:
        print("No files found.")
        return {}

    # Prepare task arguments
    tasks = []
    for in_path in files:
        # Naming convention: original.ndjson -> original-slim.ndjson
        suffix = ".ndjson.gz" if in_path.name.endswith(".ndjson.gz") else ".ndjson"
        out_name = in_path.name.replace(suffix, f"-slim{suffix}")
        tasks.append((in_path, dst / out_name, overwrite))

    total_in = total_out = total_bad = 0
    t0 = time.time()

    # Process in parallel
    print(f"Processing {num_files} files using {workers} cores...")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_index = {
            executor.submit(_worker_process_file, task): i
            for i, task in enumerate(tasks)
        }

        # Progress reporting
        for idx, future in enumerate(as_completed(future_to_index), 1):
            r, k, b = future.result()
            total_in += r
            total_out += k
            total_bad += b

            if one_line_progress:
                print(f"\r[{idx}/{num_files}] files completed", end="", flush=True)

    if one_line_progress:
        print()  # Line break after progress bar

    # Summary statistics
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
        f"time={dt:.1f}s rate≈{rate:,}/rec-per-sec"
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


def batch_find_citations_dc_from_citation_block(
    input_folder: str, output_filepath: str
):
    input_path = Path(input_folder)

    files = list(input_path.glob("*.ndjson"))
    total_files = len(files)

    if total_files == 0:
        print(f"No .ndjson files found in {input_folder}")
        return

    print(f"Starting processing of {total_files} files")
    count_citations = 0
    with open(output_filepath, "w", encoding="utf-8") as f_out:
        for idx, file_path in enumerate(files, 1):
            print(
                f"\rProcessing file {idx} of {total_files} ({file_path.name})",
                end="",
                flush=True,
            )

            with open(file_path, "r", encoding="utf-8") as f_in:
                for line in f_in:
                    if not line.strip():
                        continue

                    data = json.loads(line)

                    # Check if citations exists and is not empty/None
                    citations = data.get("citations")
                    if not citations or not any(citations.values()):
                        continue

                    # Extract target_doi
                    target_doi = None
                    for item in data.get("identifiers", []):
                        if item.get("identifier_type") == "doi":
                            target_doi = item.get("identifier")
                            break

                    if not target_doi:
                        continue

                    # Best pub date
                    publication_date = data.get("publication_date")
                    created_date = data.get("created_date")
                    best_date = get_best_dataset_date(publication_date, created_date)

                    # Process
                    citation_records = datacite_citations_block_to_records(
                        target_doi=target_doi,
                        citations=citations,
                        dataset_pub_date=best_date,
                    )

                    for record in citation_records:
                        f_out.write(json.dumps(record) + "\n")
                        count_citations += 1

                    print(
                        f"\rProcessing file {idx}/{total_files} | Total Citations: {count_citations:,}",
                        end="",
                        flush=True,
                    )

    # Final newline to clear the progress line
    print(f"\nDone! Saved {count_citations:,} citations")


def batch_find_citations_dc_from_citation_block_optimized(
    input_folder: str, output_filepath: str, db_path: str
):
    input_path = Path(input_folder)
    files = list(input_path.glob("*.ndjson"))

    if not files:
        print(f"[-] No .ndjson files found in {input_folder}")
        return

    print(f"[*] Found {len(files)} file(s). Connecting to DuckDB...")

    with duckdb.connect(db_path, read_only=True) as conn:
        total_count_citations = 0

        with open(output_filepath, "w", encoding="utf-8") as f_out:
            for idx, file_path in enumerate(files, 1):
                start_time = time.time()
                print(f"\n--- Processing: {file_path.name} ({idx}/{len(files)}) ---")

                # --- STEP 1: EXTRACTION ---
                print(
                    f"[{time.strftime('%H:%M:%S')}] Step 1: Extracting DOIs and loading file into memory..."
                )
                unique_dois_in_file = set()
                file_data = []

                with open(file_path, "r", encoding="utf-8") as f_in:
                    for line in f_in:
                        stripped_line = line.strip()
                        if not stripped_line:
                            continue

                        item = json.loads(stripped_line)
                        file_data.append(item)

                        citations = item.get("citations") or {}
                        for raw_doi in citations.get("dois", []) or []:
                            normed = _norm_doi(raw_doi)

                            if normed and isinstance(normed, str) and normed.strip():
                                unique_dois_in_file.add(normed.strip())

                print(
                    f"    -> Extracted {len(file_data):,} rows and {len(unique_dois_in_file):,} unique citation DOIs."
                )
                print(list(unique_dois_in_file)[:10])

                # --- STEP 2: BULK FETCH ---
                print(
                    f"[{time.strftime('%H:%M:%S')}] Step 2: Querying DuckDB for {len(unique_dois_in_file):,} DOIs..."
                )
                db_dates = {}

                if unique_dois_in_file:
                    doi_data = [(d,) for d in unique_dois_in_file]

                    doi_rel = (
                        conn.values(doi_data)
                        .set_alias("t")
                        .project("CAST(col0 AS VARCHAR) AS search_doi")
                    )

                    results = conn.execute("""
                        SELECT t.search_doi, o.pubdate 
                        FROM doi_rel t
                        INNER JOIN openalex_pubdate o ON (t.search_doi = o.doi)
                    """).fetchall()

                    db_dates = {row[0]: str(row[1]) for row in results if row[1]}

                print(f"    -> Found {len(db_dates):,} matches.")

                # --- STEP 3: PROCESSING & WRITING ---
                print(
                    f"[{time.strftime('%H:%M:%S')}] Step 3: Generating records and writing to disk..."
                )
                file_citation_count = 0

                for row_idx, data in enumerate(file_data):
                    target_doi = next(
                        (
                            i.get("identifier")
                            for i in data.get("identifiers", [])
                            if i.get("identifier_type") == "doi"
                        ),
                        None,
                    )
                    if not target_doi:
                        continue

                    best_date = get_best_dataset_date(
                        data.get("publication_date"), data.get("created_date")
                    )

                    citation_records = datacite_citations_block_to_records_optimized(
                        target_doi=target_doi,
                        citations=data.get("citations"),
                        dataset_pub_date=best_date,
                        prefetched_dates=db_dates,
                    )

                    for record in citation_records:
                        f_out.write(json.dumps(record) + "\n")
                        file_citation_count += 1

                    # Progress every 1000 rows
                    if row_idx % 1000 == 0:
                        print(
                            f"\r    -> Row {row_idx:,}/{len(file_data):,} | Total Citations so far: {file_citation_count:,}",
                            end="",
                        )

                elapsed = time.time() - start_time
                total_count_citations += file_citation_count
                print(f"\n[!] Finished {file_path.name} in {elapsed:.2f} seconds.")

    print("\nBatch Completed")
    print(f"Total Citations Saved: {total_count_citations:,}")
    print(f"Output: {output_filepath}")
