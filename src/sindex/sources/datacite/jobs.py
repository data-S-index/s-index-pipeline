from __future__ import annotations

import gzip
import json
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from multiprocessing import Pool
from pathlib import Path
from typing import Dict, List

import duckdb
import orjson
import requests

from sindex.core.dates import _parse_date_strict
from sindex.core.http import make_session
from sindex.core.ids import _norm_doi
from sindex.core.io import _iter_json_lines

from .discovery import (
    stream_datacite_records,
)
from .normalize import (
    datacite_citations_block_to_records_unified,
    slim_datacite_record,
)

_worker_counter = None
_worker_lock = None


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

    # Use provided session or create one
    s = session or make_session(total_retries=6, backoff=2.0)

    while True:
        # Compute window start (inclusive)
        window_start = end - timedelta(days=window_days - 1)

        # Do not cross calendar-year boundaries (decited to faciliate post-processing)
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
        print()

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
    dataset_pubyear: int | None = None,
) -> List[Dict[str, object]]:
    """
    Wrapper
    """
    return datacite_citations_block_to_records_unified(
        target_doi=target_doi,
        citations=citations,
        dataset_pubyear=dataset_pubyear,
    )


# ---------------------------------------------------------
# SLIM FASTEST
def _worker_process_batch(lines):
    """
    Receives a list of raw JSON strings.
    Returns a list of processed bytes (ready to write) and stats.
    """
    output_lines = []
    stats = {"read": 0, "kept": 0, "bad": 0}

    for line in lines:
        # Skip empty whitespace
        if not line.strip():
            continue

        try:
            # 1. Parse
            rec = orjson.loads(line)
            stats["read"] += 1

            # 2. Transform
            slim = slim_datacite_record(rec)

            # 3. Serialize immediately (bytes)
            output_lines.append(orjson.dumps(slim) + b"\n")
            stats["kept"] += 1

        except Exception:
            stats["bad"] += 1

    return output_lines, stats


def _stream_lines_from_files(files, batch_size):
    """
    Yields batches (lists) of lines from a list of file paths.
    Handles opening/closing files automatically.
    """
    batch = []
    for filepath in files:
        is_gz = filepath.suffix == ".gz"
        open_func = gzip.open if is_gz else open

        try:
            with open_func(filepath, "rb") as f:
                for line in f:
                    batch.append(line)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
        except Exception as e:
            print(f"\n[Warning] Could not read file {filepath.name}: {e}")

    # Yield the leftovers
    if batch:
        yield batch


def batch_slim_datacite_chunked(
    src_folder: str,
    dst_folder: str,
    batch_size: int = 100_000,
    workers: int = os.cpu_count(),
) -> dict:
    src, dst = Path(src_folder), Path(dst_folder)
    dst.mkdir(parents=True, exist_ok=True)

    # 1. Gather all files
    all_files = sorted(list(src.glob("*.ndjson")) + list(src.glob("*.ndjson.gz")))

    if not all_files:
        print("No files found.")
        return {}

    print(f"Found {len(all_files)} input files.")
    print(f"Starting processing with {workers} cores. Batch size: {batch_size:,}...")

    t0 = time.time()
    total_read = 0
    total_kept = 0
    total_bad = 0
    batch_idx = 0

    # 2. Start Worker Pool
    with Pool(workers) as pool:
        # Create the generator that feeds the pool
        line_generator = _stream_lines_from_files(all_files, batch_size)

        # Map workers to the generator
        for result_lines, stats in pool.imap_unordered(
            _worker_process_batch, line_generator
        ):
            # Update totals
            total_read += stats["read"]
            total_kept += stats["kept"]
            total_bad += stats["bad"]

            # Write this batch to a new file (slim-0.ndjson.gz, slim-1.ndjson.gz, etc)
            # Using fast compression (compresslevel=1)
            out_name = f"slim-{batch_idx}.ndjson"
            out_path = dst / out_name

            with open(out_path, "wb") as f_out:
                f_out.writelines(result_lines)

            batch_idx += 1

            # Live Progress Update
            print(
                f"\rProcessed: {total_read:,} lines | Batches: {batch_idx} | Bad: {total_bad}",
                end="",
                flush=True,
            )

    # Final stats
    dt = time.time() - t0
    rate = int(total_kept / dt) if dt > 0 else 0
    print()  # Newline after progress bar

    print(" ")
    print(f"DONE in {dt:.2f}s")
    print(f"Total Lines Read: {total_read:,}")
    print(f"Total Lines Kept: {total_kept:,}")
    print(f"Processing Rate:  {rate:,} records/sec")
    print(f"Output Files:     {batch_idx} files written to {dst}")
    print(" ")

    return {"read": total_read, "kept": total_kept, "bad": total_bad, "time": dt}


def batch_slim_datacite_chunked_tuned(
    src_folder: str,
    dst_folder: str,
    batch_size: int = 5000,  # Reduced from 100k to prevent pipe clogging
    workers: int = os.cpu_count(),
) -> dict:
    src, dst = Path(src_folder), Path(dst_folder)
    dst.mkdir(parents=True, exist_ok=True)

    # 1. Gather all files
    all_files = sorted(list(src.glob("*.ndjson")) + list(src.glob("*.ndjson.gz")))
    if not all_files:
        print("No files found.")
        return {}

    print(f"Found {len(all_files)} input files.")
    print(
        f"Streaming with {workers} workers (recycling every 50 tasks). Batch: {batch_size:,}..."
    )

    t0 = time.time()
    total_read = 0
    total_kept = 0
    total_bad = 0
    batch_idx = 0

    # 2. Worker Pool with Recycling
    with Pool(workers, maxtasksperchild=50) as pool:
        # Create the generator
        line_generator = _stream_lines_from_files(all_files, batch_size)

        # Process
        for result_lines, stats in pool.imap_unordered(
            _worker_process_batch, line_generator
        ):
            total_read += stats["read"]
            total_kept += stats["kept"]
            total_bad += stats["bad"]

            # Write output
            out_name = f"slim-{batch_idx}.ndjson"
            out_path = dst / out_name

            # Write bytes directly
            with open(out_path, "wb") as f_out:
                f_out.writelines(result_lines)

            batch_idx += 1

            # Simple progress message one line
            if batch_idx % 10 == 0:
                print(
                    f"\rProcessed: {total_read:,} lines | Batches: {batch_idx} | Bad: {total_bad}",
                    end="",
                    flush=True,
                )

    # Final Stats
    dt = time.time() - t0
    rate = int(total_kept / dt) if dt > 0 else 0
    print(f"\nDONE in {dt:.2f}s | Rate: {rate:,} rec/s")

    return {"read": total_read, "kept": total_kept, "bad": total_bad, "time": dt}


# --------------------


def extract_unique_dois_from_citation_blocks(input_file: str, output_parquet: str):
    """Extracts unique citation DOIs from a citation blocks NDJSON file.

    Iterates through all records in the input file, collects a global set of
    unique cleaned citation DOIs, and saves the final unique set to a Parquet file.
    We use this to get the publication year of citations more efficiently (unique query).

    Args:
        input_file: Path to the input citation_blocks.ndjson file.
        output_parquet: Path to the output Parquet file to write unique DOIs to.
            Parent directories will be created if they do not exist.
    """
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"File not found: {input_file}")
        return

    unique_dois = set()
    total_records_scanned = 0

    print(f"Scanning {input_path.name}...")

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                total_records_scanned += 1

                citations = item.get("citations", {}) or {}
                doi_list = citations.get("dois", []) or []

                for raw_doi in doi_list:
                    normed = _norm_doi(raw_doi)
                    if normed and isinstance(normed, str):
                        clean_doi = normed.strip()
                        if clean_doi:
                            unique_dois.add(clean_doi)
            except json.JSONDecodeError:
                continue

    print(
        f"Scanned {total_records_scanned:,} records. Unique DOIs: {len(unique_dois):,}"
    )

    if unique_dois:
        print(f"Exporting {len(unique_dois):,} unique DOIs to {output_parquet}...")

        os.makedirs(os.path.dirname(output_parquet), exist_ok=True)

        with duckdb.connect(":memory:") as temp_conn:
            temp_conn.execute("CREATE TABLE tmp_dois(doi VARCHAR)")
            temp_conn.executemany(
                "INSERT INTO tmp_dois VALUES (?)", [(d,) for d in unique_dois]
            )
            temp_conn.execute(
                f"COPY tmp_dois TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD')"
            )

        print(f"[SUCCESS] Saved to {output_parquet}")
    else:
        print("No valid DOIs found.")


def lookup_dates_in_oa_snapshot(db_path: str, input_parquet: str, output_parquet: str):
    """
    Joins a Parquet file of DOIs with the OpenAlex database
    and saves the matches to a new Parquet file.
    """
    start_time = time.time()
    print(f"Joining {input_parquet} with OpenAlex database...")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)

    con = duckdb.connect(db_path, read_only=True)

    try:
        # 1. read_parquet(input) acts as a virtual table
        # 2. We join it directly to the 462M row table
        # 3. We use COPY to stream results straight to the output file
        query = f"""
            COPY (
                SELECT 
                    t.doi, 
                    MAX(o.pubdate) as pubdate  -- Take the earliest date if multiple exist
                FROM read_parquet('{input_parquet}') t
                INNER JOIN openalex_pubdate o ON (t.doi = o.doi)
                GROUP BY t.doi                  -- Ensure one row per input DOI
            ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """

        con.execute(query)

        elapsed = time.time() - start_time

        # Validation
        result_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_parquet}')"
        ).fetchone()[0]
        print(f"    [SUCCESS] Found {result_count:,} matches.")
        print(f"    [INFO] Results saved to: {output_parquet}")
        print(f"    [INFO] Time taken: {elapsed:.2f}s")

    except Exception as e:
        print(f"    [!] Error during Join: {e}")
    finally:
        con.close()


# -------------
def init_locks(shared_counter, shared_lock):
    """Initializes only the locks. Cache is passed as argument."""
    global _worker_counter, _worker_lock
    _worker_counter = shared_counter
    _worker_lock = shared_lock


def process_file_chunk(
    file_path, start_byte, end_byte, chunk_id, output_folder, date_map
):
    """
    Worker function. Receives date_map directly (fast for small caches).
    """
    from pathlib import Path

    import orjson

    if date_map is None:
        raise ValueError(f"Worker {chunk_id}: Received None for date_map!")

    processed_in_chunk = 0
    local_counter = 0
    part_file = Path(output_folder) / f"part_{chunk_id}.ndjson"

    with open(file_path, "rb") as f, open(part_file, "wb") as f_out:
        f.seek(start_byte)
        if start_byte != 0:
            f.readline()  # Skip partial line

        while f.tell() < end_byte:
            line = f.readline()
            if not line:
                break

            # Update progress every 50 lines
            local_counter += 1
            if local_counter >= 50:
                with _worker_lock:
                    _worker_counter.value += local_counter
                local_counter = 0

            try:
                data = orjson.loads(line)
            except Exception:
                continue

            citations = data.get("citations")
            if not citations:
                continue

            target_doi = None
            for item in data.get("identifiers", []):
                if item.get("identifier_type") == "doi":
                    target_doi = item.get("identifier")
                    break

            if not target_doi:
                continue

            # CALL THE HELPER
            citation_records = datacite_citations_block_to_records_unified(
                target_doi=target_doi,
                citations=citations,
                date_map=date_map,
                dataset_pubyear=data.get("pubyear"),
                skip_openalex=True,
            )

            for record in citation_records:
                chunk = orjson.dumps(record)
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                f_out.write(chunk + b"\n")
                processed_in_chunk += 1

    # Flush remaining progress
    if local_counter > 0:
        with _worker_lock:
            _worker_counter.value += local_counter

    return processed_in_chunk


# --- 3. MAIN FUNCTION ---


def batch_find_citations_from_dc_parallel(
    input_file: str, output_filepath: str, pubdate_parquet_path: str
):
    start_time = time.time()

    # 1. LOAD CACHE (Main Process)
    # Since file is 9MB, we load it here safely and pass it to workers.
    print(f"[*] Loading cache from {Path(pubdate_parquet_path).name}...")

    # Using DuckDB to read parquet into dict (ignores pyarrow issues usually)
    with duckdb.connect(":memory:") as conn:
        res = conn.execute(
            f"SELECT doi, pubdate FROM read_parquet('{pubdate_parquet_path}')"
        ).fetchall()
        # Create dict: { '10.123/xyz': '2023', ... }
        date_map_main = dict(res)

    print(f"    -> Loaded {len(date_map_main):,} dates into memory.")

    # 2. PREPARE INPUT
    print("[*] Counting exact lines in input file...")
    with open(input_file, "rb") as f:
        total_lines = sum(1 for _ in f)
    print(f"    -> Total workload: {total_lines:,} lines.")

    file_path = Path(input_file)
    file_size = file_path.stat().st_size

    # 4 workers is safe for this setup
    num_cpus = min(32, mp.cpu_count())
    print(f"[*] Using {num_cpus} workers")

    chunk_size = file_size // num_cpus
    temp_dir = Path("./temp_parts")
    temp_dir.mkdir(exist_ok=True)

    offsets = []
    for i in range(num_cpus):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_cpus - 1 else file_size
        offsets.append((start, end))

    manager = mp.Manager()
    shared_counter = manager.Value("i", 0)
    shared_lock = manager.Lock()

    print("[*] Launching workers...")

    # 3. RUN PARALLEL
    with mp.Pool(
        processes=num_cpus,
        initializer=init_locks,
        initargs=(shared_counter, shared_lock),
    ) as pool:
        jobs = [
            pool.apply_async(
                process_file_chunk,
                # Pass date_map_main explicitly
                (input_file, s, e, i, temp_dir, date_map_main),
            )
            for i, (s, e) in enumerate(offsets)
        ]

        # Monitor Loop
        while any(not j.ready() for j in jobs):
            current = shared_counter.value
            pct = (current / total_lines) * 100 if total_lines > 0 else 0
            print(
                f"\rProgress: {pct:.2f}% | Processed: {current:,}/{total_lines:,}",
                end="",
                flush=True,
            )
            time.sleep(0.5)

        # Collect results (raises error if worker crashed)
        total_citations_found = sum(j.get() for j in jobs)

    # 4. MERGE
    print(f"\n[*] Merging {num_cpus} part files into final output...")
    with open(output_filepath, "wb") as f_final:
        for part in sorted(temp_dir.glob("part_*.ndjson")):
            with open(part, "rb") as f_part:
                f_final.write(f_part.read())
            part.unlink()

    temp_dir.rmdir()

    total_time = time.time() - start_time
    print(f"[DONE] Finished in {total_time:.2f}s.")
    print(f"       Total Citations Found: {total_citations_found:,}")


def batch_find_citations_from_dc_serial(
    input_file: str, output_filepath: str, pubdate_parquet_path: str
):
    start_time = time.time()
    print(f"[{datetime.now()}] Starting SERIAL processing...")

    # 1. Load Cache (Once, in memory)
    print(f"[*] Loading cache from {Path(pubdate_parquet_path).name}...")
    try:
        # Using DuckDB to read parquet (ignores pyarrow issues)
        with duckdb.connect(":memory:") as conn:
            res = conn.execute(
                f"SELECT doi, pubdate FROM read_parquet('{pubdate_parquet_path}')"
            ).fetchall()
            date_map = dict(res)
        print(f"    -> Loaded {len(date_map):,} dates into memory.")
    except Exception as e:
        print(f"!!! Error loading cache: {e}")
        return

    # 2. Count Lines for Progress Bar
    print("[*] Counting exact lines in input file...")
    try:
        with open(input_file, "rb") as f:
            total_lines = sum(1 for _ in f)
        print(f"    -> Total workload: {total_lines:,} lines.")
    except Exception as e:
        print(f"!!! Error reading input file: {e}")
        return

    # 3. Process Line by Line
    print("[*] Processing lines...")
    processed_count = 0
    citations_found = 0

    try:
        with open(input_file, "rb") as f_in, open(output_filepath, "wb") as f_out:
            for line in f_in:
                processed_count += 1

                # Progress Update every 1000 lines
                if processed_count % 1000 == 0:
                    pct = (processed_count / total_lines) * 100
                    print(
                        f"\rProgress: {pct:.2f}% of datasets | Found: {citations_found:,} citations",
                        end="",
                        flush=True,
                    )

                try:
                    data = orjson.loads(line)
                except Exception:
                    continue

                citations = data.get("citations")
                if not citations:
                    continue

                # Extract Target DOI
                target_doi = None
                for item in data.get("identifiers", []):
                    if item.get("identifier_type") == "doi":
                        target_doi = item.get("identifier")
                        break

                if not target_doi:
                    continue

                # --- CORE LOGIC ---
                citation_records = datacite_citations_block_to_records_unified(
                    target_doi=target_doi,
                    citations=citations,
                    date_map=date_map,
                    dataset_pubyear=data.get("pubyear"),
                    skip_openalex=True,
                )

                # Write results
                for record in citation_records:
                    chunk = orjson.dumps(record)
                    if isinstance(chunk, str):
                        chunk = chunk.encode("utf-8")
                    f_out.write(chunk + b"\n")
                    citations_found += 1

    except Exception as e:
        print(f"\n\n!!! CRASH ON LINE {processed_count} !!!")
        print(e)
        import traceback

        traceback.print_exc()
        return

    total_time = time.time() - start_time
    print(f"\n\n[DONE] Finished in {total_time:.2f}s.")
    print(f"       Total Lines Processed: {processed_count:,}")
    print(f"       Total Citations Found: {citations_found:,}")
